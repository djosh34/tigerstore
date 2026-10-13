package peer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// tappedConn records raw Write calls, not TLS or plaintext Write calls.
// The hook runs before the socket write, without holding mu.
type tappedConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
	hook   func()
}

func (c *tappedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, bytes.Clone(p))
	hook := c.hook
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return c.Conn.Write(p)
}

func (c *tappedConn) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.writes...)
}

func (c *tappedConn) setHook(hook func()) {
	c.mu.Lock()
	c.hook = hook
	c.mu.Unlock()
}

// bufferedTLSPair negotiates real peer mTLS over a synchronous pipe. A
// handshake buffered before negotiation finishes would time out here.
func bufferedTLSPair(t *testing.T) (*outboundConn, *tls.Conn, *tappedConn) {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	raw := &tappedConn{Conn: a}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	server := tls.Server(b, &tls.Config{
		Certificates: []tls.Certificate{cert}, ClientCAs: ca.Pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
		SessionTicketsDisabled: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.HandshakeContext(ctx) }()
	c, err := handshakeOutbound(ctx, raw, &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: ca.Pool,
		ServerName: "localhost", MinVersion: tls.VersionTLS13,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if c.records.bw.Size() != ioBuffer {
		t.Fatalf("ciphertext buffer is %d bytes, want %d", c.records.bw.Size(), ioBuffer)
	}
	return c, server, raw
}

func testSender() *sender {
	return &sender{
		t:    &TCP{cfg: Config{ID: 1, Unreachable: func(uint64) {}}},
		id:   2,
		ctrl: make(chan *raftpb.Message, 16),
		data: make(chan *raftpb.Message, 16),
	}
}

func runBufferedSender(t *testing.T, s *sender, c *outboundConn) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.write(ctx, c) }()
	t.Cleanup(func() {
		cancel()
		_ = c.Close()
	})
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("sender did not stop after cancellation")
		}
	}
}

func readPeerFrames(t *testing.T, c net.Conn, n int) []*raftpb.Message {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var h [8]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint64(h[:]); got != 1 {
		t.Fatalf("peer ID = %d, want 1", got)
	}
	var out []*raftpb.Message
	for range n {
		if _, err := io.ReadFull(c, h[:4]); err != nil {
			t.Fatal(err)
		}
		size := binary.BigEndian.Uint32(h[:4])
		if size > maxFrame {
			t.Fatalf("frame of %d bytes is over the limit", size)
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(c, b); err != nil {
			t.Fatal(err)
		}
		m := &raftpb.Message{}
		if err := proto.Unmarshal(b, m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func TestOutboundHandshakeAndCiphertextCoalescing(t *testing.T) {
	c, server, raw := bufferedTLSPair(t)
	before := len(raw.snapshot())
	if before == 0 {
		t.Fatal("handshake made no raw writes")
	}
	payload := bytes.Repeat([]byte("record bytes"), 8192)
	if err := c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("TLS Write = %d, %v", n, err)
	}
	if len(raw.snapshot()) != before {
		t.Fatal("sub-capacity ciphertext reached TCP before Flush")
	}
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(server, got)
		readDone <- err
	}()
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, got) {
		t.Fatal("TLS payload changed")
	}
	writes := raw.snapshot()[before:]
	if len(writes) != 1 {
		t.Fatalf("raw writes = %d, want 1", len(writes))
	}
	// Multiple intact TLS records, including their headers, share one
	// raw Write. They remain separate records on the wire.
	records := 0
	for wire := writes[0]; len(wire) > 0; records++ {
		if len(wire) < 5 {
			t.Fatal("truncated TLS record header")
		}
		n := 5 + int(binary.BigEndian.Uint16(wire[3:5]))
		if n > len(wire) {
			t.Fatal("truncated TLS record")
		}
		wire = wire[n:]
	}
	if records < 2 {
		t.Fatalf("only %d TLS records in the raw Write", records)
	}
	if c.records.bw.Buffered() != 0 {
		t.Fatal("ciphertext remains after Flush")
	}
}

func TestOutboundLoneMessageFlushes(t *testing.T) {
	c, server, _ := bufferedTLSPair(t)
	s := testSender()
	want := msg(7, "only message")
	s.send(want)
	stop := runBufferedSender(t, s, c)
	got := readPeerFrames(t, server, 1)
	if !proto.Equal(got[0], want) {
		t.Fatalf("message differs: %v", got[0])
	}
	stop()
}

func TestOutboundIdlePeerIDFlushes(t *testing.T) {
	c, server, _ := bufferedTLSPair(t)
	stop := runBufferedSender(t, testSender(), c)
	readPeerFrames(t, server, 0)
	stop()
}

func TestOutboundLargeFrameAndControlPriority(t *testing.T) {
	c, server, raw := bufferedTLSPair(t)
	s := testSender()
	large := msg(1, string(bytes.Repeat([]byte("v"), 2*ioBuffer+17)))
	empty := msg(2, "")
	forwarded := &raftpb.Message{
		Type: raftpb.MsgProp.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Entries: []*raftpb.Entry{{Data: []byte("forwarded proposal")}},
	}
	firstControl := &raftpb.Message{
		Type: raftpb.MsgReadIndex.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Entries: []*raftpb.Entry{{Data: []byte("read context")}},
	}
	midControl := &raftpb.Message{
		Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Context: []byte("arrives during the large frame"),
	}
	s.send(large)
	s.send(empty)
	s.send(forwarded)
	s.send(firstControl)
	before := len(raw.snapshot())
	var once sync.Once
	raw.setHook(func() { once.Do(func() { s.send(midControl) }) })
	stop := runBufferedSender(t, s, c)
	got := readPeerFrames(t, server, 5)
	for i, want := range []*raftpb.Message{firstControl, large, midControl, empty, forwarded} {
		if !proto.Equal(got[i], want) {
			t.Fatalf("frame %d differs: type %s, index %d", i, got[i].GetType(), got[i].GetIndex())
		}
	}
	stop()
	for _, p := range raw.snapshot()[before:] {
		if len(p) > ioBuffer {
			t.Fatalf("raw Write of %d bytes exceeds the %d-byte buffer", len(p), ioBuffer)
		}
	}
	if s.ctrlBytes.Load() != 0 || s.dataBytes.Load() != 0 {
		t.Fatal("drained messages still count against the queues")
	}
}

func TestOutboundRawCloseInterruptsFlush(t *testing.T) {
	c, _, raw := bufferedTLSPair(t)
	if _, err := c.Write([]byte("buffered application data")); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var once sync.Once
	raw.setHook(func() { once.Do(func() { close(entered) }) })
	done := make(chan error, 1)
	go func() { done <- c.flush() }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Flush never reached the nonreading raw socket")
	}
	before := len(raw.snapshot())
	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("raw Close waited for Flush")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Flush succeeded on a closed socket")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("raw Close did not interrupt Flush")
	}
	if len(raw.snapshot()) != before {
		t.Fatal("Close attempted an extra TLS write")
	}
}

func TestOutboundDeadlineInterruptsCiphertextFlush(t *testing.T) {
	c, _, _ := bufferedTLSPair(t)
	if err := c.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("buffered application data")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.flush() }()
	select {
	case err := <-done:
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("ciphertext Flush = %v, want a timeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write deadline did not interrupt ciphertext Flush")
	}
}

// faultConn accepts writes in memory or injects an error. net.Pipe supplies
// the other net.Conn methods; no reader or TLS is needed for flush faults.
type faultConn struct {
	net.Conn
	err      error
	short    bool
	calls    int
	data     []byte
	deadline time.Time
}

func (c *faultConn) Write(p []byte) (int, error) {
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	if c.short && len(p) > 0 {
		c.data = append(c.data, p[:len(p)-1]...)
		return len(p) - 1, nil
	}
	c.data = append(c.data, p...)
	return len(p), nil
}

func (c *faultConn) SetWriteDeadline(d time.Time) error {
	c.deadline = d
	return c.Conn.SetWriteDeadline(d)
}

func faultSocket(t *testing.T) *faultConn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return &faultConn{Conn: a}
}

func TestOutboundBothFlushErrors(t *testing.T) {
	injected := errors.New("injected write failure")
	for _, layer := range []string{"plaintext", "ciphertext"} {
		for _, short := range []bool{false, true} {
			for _, automatic := range []bool{false, true} {
				name := layer + "/error"
				if short {
					name = layer + "/short-write"
				}
				payload := "one small frame"
				if automatic {
					name += "/automatic-flush"
					payload = string(bytes.Repeat([]byte("v"), 3*ioBuffer+17))
				} else {
					name += "/explicit-flush"
				}
				t.Run(name, func(t *testing.T) {
					raw := faultSocket(t)
					records := &recordConn{Conn: raw, bw: bufio.NewWriterSize(raw, ioBuffer)}
					c := &outboundConn{Conn: records, records: records}
					failing := raw
					if layer == "plaintext" {
						failing = faultSocket(t)
						c.Conn = failing
					}
					failing.short = short
					want := injected
					if short {
						want = io.ErrShortWrite
					} else {
						failing.err = injected
					}
					s := testSender()
					s.send(msg(1, payload))
					if err := s.write(context.Background(), c); !errors.Is(err, want) {
						t.Fatalf("write = %v, want %v", err, want)
					}
					if failing.calls != 1 {
						t.Fatalf("failed writer called %d times, want 1", failing.calls)
					}
					if failing.deadline.IsZero() {
						t.Fatal("failed write had no deadline")
					}
					if layer == "plaintext" && raw.calls != 0 {
						t.Fatal("ciphertext Flush ran after plaintext Flush failed")
					}
				})
			}
		}
	}
}

func TestOutboundCiphertextCopiesAndStickyError(t *testing.T) {
	raw := faultSocket(t)
	records := &recordConn{Conn: raw, bw: bufio.NewWriterSize(raw, ioBuffer)}
	p := []byte("TLS scratch bytes")
	want := bytes.Clone(p)
	if _, err := records.Write(p); err != nil {
		t.Fatal(err)
	}
	clear(p)
	if err := records.bw.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.data, want) {
		t.Fatal("buffer borrowed TLS's reusable scratch storage")
	}
	injected := errors.New("ciphertext flush failed")
	raw.err = injected
	if _, err := records.Write([]byte("next record")); err != nil {
		t.Fatal(err)
	}
	if err := records.bw.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v", err)
	}
	calls := raw.calls
	if _, err := records.Write([]byte("must not retry")); !errors.Is(err, injected) {
		t.Fatalf("Write after failed Flush = %v", err)
	}
	if err := records.bw.Flush(); !errors.Is(err, injected) {
		t.Fatalf("second Flush = %v", err)
	}
	if raw.calls != calls {
		t.Fatal("failed ciphertext was retried")
	}
}

func TestOutboundDialHandshakeTimeoutAndCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		name := "combined-three-second-budget"
		if cancelEarly {
			name = "parent-cancellation"
		}
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			type acceptResult struct {
				conn net.Conn
				err  error
			}
			accepted := make(chan acceptResult, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				peer, err := ln.Accept()
				accepted <- acceptResult{conn: peer, err: err}
				if cancelEarly {
					cancel()
				}
			}()
			start := time.Now()
			c, err := dialOutbound(ctx, ln.Addr().String(), &tls.Config{
				ServerName: "localhost", MinVersion: tls.VersionTLS13,
			})
			elapsed := time.Since(start)
			if c != nil || err == nil {
				if c != nil {
					_ = c.Close()
				}
				t.Fatalf("stalled TLS handshake = %v, %v", c, err)
			}
			want := context.DeadlineExceeded
			bound := dialTimeout + 2*time.Second
			if cancelEarly {
				want, bound = context.Canceled, 2*time.Second
			}
			if !errors.Is(err, want) || elapsed > bound {
				t.Fatalf("handshake error %v after %v, want %v within %v", err, elapsed, want, bound)
			}
			select {
			case result := <-accepted:
				if result.err != nil {
					t.Fatal(result.err)
				}
				peer := result.conn
				defer peer.Close() //nolint:errcheck // closing a test socket
				if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, peer); err != nil {
					t.Fatalf("failed handshake left raw TCP open: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("dial did not reach the listener")
			}
		})
	}
}
