package loop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// pki is a CA and a server certificate for the record layer's tests.
type pki struct {
	ca   *testcert.CA
	cert tls.Certificate
}

func newPKI(t *testing.T) pki {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueFor("test", 2, nil, []string{"server"})
	if err != nil {
		t.Fatal(err)
	}
	return pki{ca, cert}
}

func (p pki) server() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{p.cert}, MinVersion: tls.VersionTLS12}
}

func (p pki) client() *tls.Config {
	return &tls.Config{RootCAs: p.ca.Pool, ServerName: "server", MinVersion: tls.VersionTLS12}
}

// tcpPair is the two ends of a loopback TCP connection, which buffers as
// the network does; a synchronous pipe would block two ends that write at
// once, as on a KeyUpdate.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // closing a test listener
	b, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a, b
}

// pair runs our handshake on one end of a connection, as the server if
// server is set, and crypto/tls's on the other.
func pair(t *testing.T, server bool, ours, theirs *tls.Config) (*Records, *tls.Conn) {
	t.Helper()
	a, b := tcpPair(t)
	var tc *tls.Conn
	if server {
		tc = tls.Client(b, theirs)
	} else {
		tc = tls.Server(b, theirs)
	}
	errc := make(chan error, 1)
	go func() { errc <- tc.Handshake() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, _, err := Handshake(ctx, NewBridge(a, 4<<10), ours, server)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	return r, tc
}

// exchange sends 40 KiB each way, across records, and checks it arrives;
// first, if set, runs once crypto/tls reads.
func exchange(t *testing.T, r *Records, tc *tls.Conn, first func()) {
	t.Helper()
	msg := bytes.Repeat([]byte("records "), 5<<10)
	done := make(chan error, 1)
	go func() {
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(tc, got); err != nil {
			done <- err
			return
		}
		if !bytes.Equal(got, msg) {
			done <- errors.New("crypto/tls read other bytes than we wrote")
			return
		}
		_, err := tc.Write(msg)
		done <- err
	}()
	if first != nil {
		first()
	}
	if _, err := r.Write(msg); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatal("we read other bytes than crypto/tls wrote")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestRecordsWithCryptoTLS carries data both ways between our records and
// crypto/tls, as the server and as the client, against a server that
// sends session tickets after the handshake, and closes with close_notify
// both ways.
func TestRecordsWithCryptoTLS(t *testing.T) {
	p := newPKI(t)
	for _, server := range []bool{true, false} {
		ours, theirs := p.server(), p.client()
		if !server {
			ours, theirs = p.client(), p.server()
		}
		r, tc := pair(t, server, ours, theirs)
		exchange(t, r, tc, nil)
		// Our close_notify is crypto/tls's EOF.
		go func() { _ = r.Close() }()
		if _, err := tc.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("server %v: crypto/tls read %v after our close, want EOF", server, err)
		}
		// And crypto/tls's is ours.
		r2, tc2 := pair(t, server, ours, theirs)
		go func() { _ = tc2.Close() }()
		if _, err := r2.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("server %v: we read %v after crypto/tls's close, want EOF", server, err)
		}
	}
}

// TestRecordsKeyUpdate asks crypto/tls for a KeyUpdate: it answers with
// its own, which we take, and data still goes both ways under the new
// keys.
func TestRecordsKeyUpdate(t *testing.T) {
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	first := r.rd.secret
	exchange(t, r, tc, func() {
		if err := r.record(typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 1}); err != nil {
			t.Fatal(err)
		}
		wr, err := r.suite.updated(&r.wr)
		if err != nil {
			t.Fatal(err)
		}
		r.wr = wr
	})
	if bytes.Equal(r.rd.secret, first) {
		t.Fatal("crypto/tls's KeyUpdate did not change our read keys")
	}
}

// TestRecordsRefuseDamage flips a bit of a record on its way: it does not
// open, and the read fails.
func TestRecordsRefuseDamage(t *testing.T) {
	p := newPKI(t)
	a, b := tcpPair(t)
	flip := &flipper{Conn: b}
	tc := tls.Client(flip, p.client())
	go func() { _ = tc.Handshake() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, _, err := Handshake(ctx, NewBridge(a, 4<<10), p.server(), true)
	if err != nil {
		t.Fatal(err)
	}
	flip.on = true
	go func() {
		_, _ = tc.Write([]byte("hello"))
		// Takes the alert we answer with.
		_, _ = io.Copy(io.Discard, tc)
	}()
	if _, err := r.Read(make([]byte, 16)); err == nil {
		t.Fatal("a damaged record opened")
	}
}

// flipper flips the last byte of each write once on is set.
type flipper struct {
	net.Conn
	on bool
}

func (f *flipper) Write(p []byte) (int, error) {
	if f.on {
		p = bytes.Clone(p)
		p[len(p)-1] ^= 1
	}
	return f.Conn.Write(p)
}

// TestRecordsTLS12 hands a connection that negotiated TLS 1.2 back as
// crypto/tls's, which still carries it.
func TestRecordsTLS12(t *testing.T) {
	p := newPKI(t)
	a, b := tcpPair(t)
	cli := p.client()
	cli.MaxVersion = tls.VersionTLS12
	tc := tls.Client(b, cli)
	go func() { _ = tc.Handshake() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, old, err := Handshake(ctx, NewBridge(a, 4<<10), p.server(), true)
	if !errors.Is(err, ErrNotTLS13) || r != nil || old == nil {
		t.Fatalf("a TLS 1.2 handshake gave %v, %v, %v", r, old, err)
	}
	go func() { _, _ = tc.Write([]byte("hello")) }()
	got := make([]byte, 5)
	if _, err := io.ReadFull(old, got); err != nil || string(got) != "hello" {
		t.Fatalf("crypto/tls read %q, %v", got, err)
	}
}

// TestRecordsOnTheLoop attaches the records to a loop: a read waits with
// ErrWouldBlock for a whole record, and writes go out with Flush, sealed
// in place, as many records as 16 KiB pieces.
func TestRecordsOnTheLoop(t *testing.T) {
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	poller := NewChanPoller()
	sock, err := poller.Attach(r.br.Conn, 1)
	if err != nil {
		t.Fatal(err)
	}
	r.br.Attach(sock, NewBuffers(32<<10, 4), NewBuffers(1<<20, 2))
	if n, err := r.Read(make([]byte, 16)); n != 0 || !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("a read with nothing sent gave %d, %v", n, err)
	}
	msg := bytes.Repeat([]byte("loop "), 8<<10)
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(msg))
		_, _ = io.ReadFull(tc, b)
		got <- b
		_, _ = tc.Write(msg)
	}()
	if _, err := r.Write(msg); err != nil {
		t.Fatal(err)
	}
	// Two whole records are sealed, and the third is open, its header
	// written and its tag not yet.
	if want := len(msg) + 2*(recordHeader+1+tagSize) + recordHeader; r.br.Pending() != want {
		t.Fatalf("%d bytes pending for %d of plaintext, want %d", r.br.Pending(), len(msg), want)
	}
	for r.br.Pending() > 0 {
		if err := r.Flush(); err != nil {
			t.Fatal(err)
		}
		if r.br.Pending() > 0 {
			if _, err := poller.Wait(nil, time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	if b := <-got; !bytes.Equal(b, msg) {
		t.Fatal("crypto/tls read other bytes than the loop wrote")
	}
	read := make([]byte, 0, len(msg))
	buf := make([]byte, 4096)
	deadline := time.Now().Add(10 * time.Second)
	for len(read) < len(msg) && time.Now().Before(deadline) {
		n, err := r.Read(buf)
		read = append(read, buf[:n]...)
		if errors.Is(err, ErrWouldBlock) {
			if _, err := poller.Wait(nil, time.Second); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(read, msg) {
		t.Fatalf("the loop read %d bytes, not the %d crypto/tls wrote", len(read), len(msg))
	}
	// Everything is read: an idle connection holds no read buffer.
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("a read after everything gave %d, %v", n, err)
	}
	if r.br.in != nil || r.plain != nil {
		t.Fatalf("an idle connection holds a read buffer of %d bytes, or a slice into one", len(r.br.in))
	}
}
