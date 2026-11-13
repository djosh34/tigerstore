package loop_test

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestChanPollerWakes checks the three ways a Wait ends: a token that
// came, a Wake, and the timeout, and that a Wake before the Wait is kept.
func TestChanPollerWakes(t *testing.T) {
	p := loop.NewChanPoller()
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	sock, err := p.Attach(a, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close() //nolint:errcheck // closing a test pipe
	start := time.Now()
	if got, _ := p.Wait(nil, 20*time.Millisecond); len(got) != 0 || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("idle Wait = %v after %v", got, time.Since(start))
	}
	p.Wake()
	start = time.Now()
	if _, err := p.Wait(nil, time.Minute); err != nil || time.Since(start) > 10*time.Second {
		t.Fatalf("Wait after Wake took %v: %v", time.Since(start), err)
	}
	go func() {
		_, _ = b.Write([]byte("x"))
	}()
	got, _ := p.Wait(nil, time.Minute)
	if len(got) != 1 || got[0] != 7 {
		t.Fatalf("Wait for a write = %v, want [7]", got)
	}
	buf := make([]byte, 4)
	for {
		n, err := sock.Read(buf)
		if n == 1 && buf[0] == 'x' {
			break
		}
		if !errors.Is(err, loop.ErrWouldBlock) {
			t.Fatalf("read %d bytes: %v", n, err)
		}
		if _, err := p.Wait(nil, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := sock.Read(buf); n != 0 || !errors.Is(err, loop.ErrWouldBlock) {
		t.Fatalf("read of nothing = %d, %v", n, err)
	}
}

// TestBridgeCarriesTLS runs a TLS handshake over bridges with blocking
// calls, hands both ends to loops, and sends records both ways through
// sockets that would block, as the server and the peer transport do.
func TestBridgeCarriesTLS(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueFor("test", 2, nil, []string{"server"})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	ba, bb := loop.NewBridge(a, 512), loop.NewBridge(b, 512)
	srv := tls.Server(ba, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	cli := tls.Client(bb, &tls.Config{RootCAs: ca.Pool, ServerName: "server", MinVersion: tls.VersionTLS13})
	errc := make(chan error, 1)
	go func() { errc <- srv.Handshake() }()
	if err := cli.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	type end struct {
		l   *loop.Loop
		tc  *tls.Conn
		br  *loop.Bridge
		raw net.Conn
		got bytes.Buffer
	}
	ends := []*end{{l: loop.New(loop.NewChanPoller()), tc: srv, br: ba, raw: a}, {l: loop.New(loop.NewChanPoller()), tc: cli, br: bb, raw: b}}
	msg := bytes.Repeat([]byte("record "), 20000)
	done := make(chan []byte, 2)
	for i, e := range ends {
		h := &echo{e.tc, e.br, &e.got, len(msg), done}
		ready := make(chan error, 1)
		e.l.Post(func() {
			sock, _, err := e.l.Attach(e.raw, h)
			if err == nil {
				e.br.Attach(sock, nil, nil)
				// Both ends write the message once attached.
				_, err = e.tc.Write(msg)
				e.l.Dirty(h)
			}
			ready <- err
		})
		go func() { _ = e.l.Run(ctx) }()
		if err := <-ready; err != nil {
			t.Fatalf("end %d: %v", i, err)
		}
	}
	for range ends {
		select {
		case got := <-done:
			if !bytes.Equal(got, msg) {
				t.Fatalf("got %d bytes, want the %d sent", len(got), len(msg))
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the records did not arrive")
		}
	}
}

// echo reads a TLS connection on the loop until it has n bytes.
type echo struct {
	tc   *tls.Conn
	br   *loop.Bridge
	got  *bytes.Buffer
	n    int
	done chan<- []byte
}

func (e *echo) Ready() {
	buf := make([]byte, 4096)
	for {
		k, err := e.tc.Read(buf)
		e.got.Write(buf[:k])
		if e.got.Len() == e.n {
			e.done <- e.got.Bytes()
			e.n = -1
		}
		if err != nil {
			if !errors.Is(err, loop.ErrWouldBlock) && !errors.Is(err, io.EOF) {
				e.done <- []byte(err.Error())
			}
			break
		}
	}
	e.Flush()
}

func (e *echo) Flush() { _ = e.br.Flush() }

// TestDrainEndsWithItsHandlers checks that a drained loop serves its
// sockets until their handlers go, and then refuses new work.
func TestDrainEndsWithItsHandlers(t *testing.T) {
	l := loop.New(loop.NewChanPoller())
	a, b := net.Pipe()
	h := &closer{l: l, stopped: make(chan struct{})}
	var err error
	if h.sock, h.token, err = l.Attach(a, h); err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() {
		l.Drain()
		close(drained)
	}()
	select {
	case <-h.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not hear of the stop")
	}
	// The other end closes; the handler sees it and goes.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain went on after its last handler went")
	}
	if l.Post(func() {}) {
		t.Fatal("a drained loop took work")
	}
}

// closer forgets its socket once it fails.
type closer struct {
	l       *loop.Loop
	sock    loop.Socket
	token   uint64
	stopped chan struct{}
}

func (c *closer) Ready() {
	if _, err := c.sock.Read(make([]byte, 16)); err != nil && !errors.Is(err, loop.ErrWouldBlock) {
		_ = c.sock.Close()
		c.l.Forget(c.token)
	}
}

func (c *closer) Stopped() { close(c.stopped) }

// TestHeldSocketsStillFlush dispatches a due socket while the owner takes
// no input, as the committer does while the disk is behind: its output is
// flushed, its reading waits, Holding says so, and the next Dispatch that
// takes sockets reads it.
func TestHeldSocketsStillFlush(t *testing.T) {
	l := loop.New(loop.NewChanPoller())
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	h := &counter{}
	sock, _, err := l.Attach(a, h)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close() //nolint:errcheck // closing a test pipe
	l.Dispatch(false)
	l.Flush()
	if h.ready != 0 || h.flushed != 1 || !l.Holding() {
		t.Fatalf("held: %d reads, %d flushes, holding %v; want 0, 1, true", h.ready, h.flushed, l.Holding())
	}
	l.Dispatch(true)
	if h.ready != 1 || l.Holding() {
		t.Fatalf("taken: %d reads, holding %v; want 1, false", h.ready, l.Holding())
	}
}

// counter counts its handler's reads and flushes.
type counter struct{ ready, flushed int }

func (c *counter) Ready() { c.ready++ }
func (c *counter) Flush() { c.flushed++ }

// TestBridgeMovesToBigBuffer writes a bridge more than a small buffer
// holds, while its socket took part of the first write and then takes
// nothing: what waits moves once into a big buffer, the small one goes
// back to its pool, and once the socket takes the rest, the big one goes
// back to its own. The socket gets every byte, in order.
func TestBridgeMovesToBigBuffer(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close() //nolint:errcheck // closing a test pipe
	defer b.Close() //nolint:errcheck // closing a test pipe
	small, big := loop.NewBuffers(1<<10, 4), loop.NewBuffers(64<<10, 4)
	br := loop.NewBridge(a, 1<<10)
	s := &takeAll{limit: 50}
	br.Attach(s, small, big)
	var sent []byte
	write := func(n int) {
		p := make([]byte, n)
		for i := range p {
			p[i] = byte(len(sent) + i)
		}
		sent = append(sent, p...)
		if _, err := br.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	write(100)
	if err := br.Flush(); err != nil {
		t.Fatal(err)
	}
	s.shut = true
	write(10 << 10)
	if br.Retained() != 64<<10 || br.Pending() != 10<<10+50 || small.Kept() != 1 {
		t.Fatalf("%d bytes wait in a buffer of %d, %d small buffers back; want %d in one of %d, 1 back", br.Pending(), br.Retained(), small.Kept(), 10<<10+50, 64<<10)
	}
	s.shut, s.limit = false, 0
	if err := br.Flush(); err != nil {
		t.Fatal(err)
	}
	if br.Retained() != 0 || big.Kept() != 1 {
		t.Fatalf("after the socket took it: %d bytes retained, %d big buffers back", br.Retained(), big.Kept())
	}
	if !bytes.Equal(s.got, sent) {
		t.Fatalf("the socket got %d bytes that differ from the %d written", len(s.got), len(sent))
	}
}

// takeAll is a socket that takes every write unless shut, at most limit
// bytes of each if limit is set, and keeps what it took.
type takeAll struct {
	shut  bool
	limit int
	got   []byte
}

func (s *takeAll) Read([]byte) (int, error) { return 0, loop.ErrWouldBlock }

func (s *takeAll) Write(b []byte) (int, error) {
	if s.shut {
		return 0, loop.ErrWouldBlock
	}
	if s.limit > 0 && len(b) > s.limit {
		s.got = append(s.got, b[:s.limit]...)
		return s.limit, loop.ErrWouldBlock
	}
	s.got = append(s.got, b...)
	return len(b), nil
}

func (s *takeAll) Close() error { return nil }

// TestBridgeKeepsOutput writes 3 MiB in TLS-record pieces through bridges
// without Buffers and flushes them, again and again, as a peer's appends
// go: the bridge that keeps 8 MiB grows its buffer once and allocates
// nothing after, and one that keeps the default 1 MiB grows a new buffer
// for every write.
func TestBridgeKeepsOutput(t *testing.T) {
	record := make([]byte, 16<<10)
	send := func(br *loop.Bridge) {
		for range 3 << 20 / len(record) {
			if _, err := br.Write(record); err != nil {
				t.Fatal(err)
			}
		}
		if err := br.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	allocs := func(keep int) float64 {
		a, b := net.Pipe()
		defer a.Close() //nolint:errcheck // closing a test pipe
		defer b.Close() //nolint:errcheck // closing a test pipe
		br := loop.NewBridge(a, 1<<10)
		br.Attach(discard{}, nil, nil)
		if keep > 0 {
			br.KeepOut(keep)
		}
		send(br)
		return testing.AllocsPerRun(10, func() { send(br) })
	}
	if n := allocs(8 << 20); n != 0 {
		t.Fatalf("a bridge that keeps 8 MiB allocates %v times a 3 MiB write", n)
	}
	if n := allocs(0); n == 0 {
		t.Fatal("a bridge that keeps 1 MiB allocates nothing for a 3 MiB write: the test measures nothing")
	}
}

// discard is a socket that takes every write.
type discard struct{}

func (discard) Read([]byte) (int, error)    { return 0, loop.ErrWouldBlock }
func (discard) Write(b []byte) (int, error) { return len(b), nil }
func (discard) Close() error                { return nil }
