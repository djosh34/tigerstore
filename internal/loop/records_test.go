package loop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

// TestRecordsUpdateTheirKey lowers keyUpdateAfter to 3 records: past it
// our side sends a KeyUpdate of its own and writes on under the next key,
// whose sequence starts again at 0, and crypto/tls reads every byte across
// the updates, before Attach and on the loop.
func TestRecordsUpdateTheirKey(t *testing.T) {
	defer func(n uint64) { keyUpdateAfter = n }(keyUpdateAfter)
	keyUpdateAfter = 3
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	first := r.wr.secret
	exchange(t, r, tc, nil)
	exchange(t, r, tc, nil)
	if bytes.Equal(r.wr.secret, first) || r.wr.seq >= keyUpdateAfter+1 {
		t.Fatalf("after 6 records the write key is the first one, or its sequence is at %d", r.wr.seq)
	}
	// On the loop, where records are sealed in place.
	poller := NewChanPoller()
	sock, err := poller.Attach(r.br.Conn, 1)
	if err != nil {
		t.Fatal(err)
	}
	r.br.Attach(sock, NewBuffers(32<<10, 4), NewBuffers(1<<20, 2))
	second := r.wr.secret
	msg := bytes.Repeat([]byte("rotate "), 10<<10)
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(msg))
		_, _ = io.ReadFull(tc, b)
		got <- b
	}()
	if _, err := r.Write(msg); err != nil {
		t.Fatal(err)
	}
	for r.br.Pending() > 0 || r.open > 0 {
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
		t.Fatal("crypto/tls read other bytes than the loop wrote across key updates")
	}
	if bytes.Equal(r.wr.secret, second) {
		t.Fatal("the loop's writes did not update the key")
	}
}

// ours runs our handshake on both ends of a connection.
func ours(t *testing.T) (server, client *Records) {
	t.Helper()
	p := newPKI(t)
	a, b := tcpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type res struct {
		r   *Records
		err error
	}
	cc := make(chan res, 1)
	go func() {
		r, _, err := Handshake(ctx, NewBridge(b, 4<<10), p.client(), false)
		cc <- res{r, err}
	}()
	srv, _, err := Handshake(ctx, NewBridge(a, 4<<10), p.server(), true)
	if err != nil {
		t.Fatal(err)
	}
	c := <-cc
	if c.err != nil {
		t.Fatal(c.err)
	}
	return srv, c.r
}

// send has w send one record of inner type typ with content, under its
// lock.
func send(t *testing.T, w *Records, typ byte, content []byte) {
	t.Helper()
	w.wmu.Lock()
	err := w.record(typ, content)
	w.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// TestRecordsAnswerKeyUpdate has our client ask our server for a key
// update: the server answers while it reads, the client's read key moves
// on, and data still goes both ways.
func TestRecordsAnswerKeyUpdate(t *testing.T) {
	srv, cli := ours(t)
	firstRead := cli.rd.secret
	send(t, cli, typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 1})
	cli.wmu.Lock()
	wr, err := cli.suite.updated(&cli.wr)
	cli.wr = wr
	cli.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = cli.Write([]byte("ping")) }()
	got := make([]byte, 4)
	if _, err := io.ReadFull(srv, got); err != nil || string(got) != "ping" {
		t.Fatalf("the server read %q, %v", got, err)
	}
	go func() { _, _ = srv.Write([]byte("pong")) }()
	if _, err := io.ReadFull(cli, got); err != nil || string(got) != "pong" {
		t.Fatalf("the client read %q, %v", got, err)
	}
	if bytes.Equal(cli.rd.secret, firstRead) {
		t.Fatal("the server did not answer the KeyUpdate: the client reads under its first key")
	}
}

// TestRecordsFatalAlert sends crypto/tls a damaged record: it answers with
// bad_record_mac, which our read returns as the peer's error.
func TestRecordsFatalAlert(t *testing.T) {
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	go func() { _, _ = tc.Read(make([]byte, 16)) }()
	r.wmu.Lock()
	r.wr.seq++ // the next record goes under a nonce crypto/tls does not expect
	err := r.record(typeAppData, []byte("hello"))
	r.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Read(make([]byte, 16))
	var alert tls.AlertError
	if !errors.As(err, &alert) || alert != alertBadRecordMAC {
		t.Fatalf("our read after crypto/tls refused our record gave %v, want its bad_record_mac", err)
	}
}

// TestRecordsWriteWhileAnswering writes from one goroutine while another
// reads and answers the peer's KeyUpdates, as net/http and the peer
// transport do before Attach: every record takes a sequence number of its
// own, so the peer opens every one, and the race detector finds nothing.
func TestRecordsWriteWhileAnswering(t *testing.T) {
	srv, cli := ours(t)
	const updates, writes = 50, 200
	done := make(chan error, 2)
	// The server writes, and reads, answering the client's KeyUpdates.
	go func() {
		for i := range writes {
			if _, err := srv.Write(fmt.Appendf(nil, "%04d", i)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	go func() {
		// A data record after each KeyUpdate, as a connection that is
		// not idle sends: more than 16 KeyUpdates in a row are refused.
		got := make([]byte, updates+4)
		_, err := io.ReadFull(srv, got)
		done <- err
	}()
	for range updates {
		send(t, cli, typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 1})
		cli.wmu.Lock()
		wr, err := cli.suite.updated(&cli.wr)
		cli.wr = wr
		cli.wmu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.Write([]byte("done")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4*writes)
	if _, err := io.ReadFull(cli, got); err != nil {
		t.Fatalf("the client read %d records' worth, then %v", len(got), err)
	}
	for i := range writes {
		if want := fmt.Sprintf("%04d", i); string(got[4*i:4*i+4]) != want {
			t.Fatalf("record %d read %q, want %q", i, got[4*i:4*i+4], want)
		}
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// TestRecordsRefuseWhatTLS13Forbids sends records that RFC 8446 (5.1)
// and crypto/tls refuse, each on a fresh connection, and one alert that
// both drop.
func TestRecordsRefuseWhatTLS13Forbids(t *testing.T) {
	cases := []struct {
		name string
		send func(t *testing.T, w *Records)
		want string
	}{
		{"two KeyUpdates in one record", func(t *testing.T, w *Records) {
			send(t, w, typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 0, msgKeyUpdate, 0, 0, 1, 0})
		}, "after a KeyUpdate"},
		{"data within a handshake message", func(t *testing.T, w *Records) {
			send(t, w, typeHandshake, []byte{msgNewSessionTicket, 0, 0, 8})
			send(t, w, typeAppData, []byte("data"))
		}, "within a handshake message"},
		{"an empty handshake record", func(t *testing.T, w *Records) {
			send(t, w, typeHandshake, nil)
		}, "empty TLS record"},
		{"17 empty records", func(t *testing.T, w *Records) {
			for range maxUseless + 1 {
				send(t, w, typeAppData, nil)
			}
		}, "too many"},
		{"plaintext over 2^14+1", func(t *testing.T, w *Records) {
			send(t, w, typeAppData, make([]byte, maxPlaintext+1))
		}, "over 2^14+1"},
	}
	for _, c := range cases {
		srv, cli := ours(t)
		go c.send(t, cli)
		_, err := srv.Read(make([]byte, 64<<10))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: read %v, want an error about %q", c.name, err, c.want)
		}
	}
	// user_canceled is dropped, and data after it reads.
	srv, cli := ours(t)
	go func() {
		send(t, cli, typeAlert, []byte{1, alertUserCanceled})
		_, _ = cli.Write([]byte("after"))
	}()
	got := make([]byte, 5)
	if _, err := io.ReadFull(srv, got); err != nil || string(got) != "after" {
		t.Fatalf("after user_canceled the server read %q, %v", got, err)
	}
}
