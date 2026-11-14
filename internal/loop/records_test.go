package loop

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
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
	wantSuite(t, r)
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

// TestRecordsUpdateTheirKey lowers a connection's key update to 3 records: past it
// our side sends a KeyUpdate of its own and writes on under the next key,
// whose sequence starts again at 0, and crypto/tls reads every byte across
// the updates, before Attach and on the loop.
func TestRecordsUpdateTheirKey(t *testing.T) {
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	r.updateAfter = 3
	first := r.wr.secret
	exchange(t, r, tc, nil)
	exchange(t, r, tc, nil)
	if bytes.Equal(r.wr.secret, first) || r.wr.seq >= r.updateAfter+1 {
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
	// A refusal a test waits for fails it at the deadline, not at the
	// test binary's timeout, if the code under test lets the record pass.
	for _, r := range []*Records{srv, c.r} {
		if err := r.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	return srv, c.r
}

// sendHandshake has w send one handshake record with content, under its
// lock.
func sendHandshake(t *testing.T, w *Records, content []byte) {
	t.Helper()
	if err := post(w, typeHandshake, content); err != nil {
		t.Fatal(err)
	}
}

// post is send for another goroutine: a failed send shows in what the
// other end reads.
func post(w *Records, typ byte, content []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	return w.record(typ, content)
}

// TestRecordsAnswerKeyUpdate has our client ask our server for a key
// update: the server answers while it reads, the client's read key moves
// on, and data still goes both ways.
func TestRecordsAnswerKeyUpdate(t *testing.T) {
	srv, cli := ours(t)
	firstRead := cli.rd.secret
	sendHandshake(t, cli, []byte{msgKeyUpdate, 0, 0, 1, 1})
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
// reads and answers the peer's KeyUpdates, five times, as net/http and the peer
// transport do before Attach: every record takes a sequence number of its
// own, so the peer opens every one, and the race detector finds nothing.
func TestRecordsWriteWhileAnswering(t *testing.T) {
	for range 5 {
		writeWhileAnswering(t)
	}
}

// writeWhileAnswering is one run of TestRecordsWriteWhileAnswering. The
// client asks for 15 updates: the answers may reach it in a row, and 17
// records in a row without data are refused.
func writeWhileAnswering(t *testing.T) {
	t.Helper()
	srv, cli := ours(t)
	const updates, writes = 15, 200
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
		sendHandshake(t, cli, []byte{msgKeyUpdate, 0, 0, 1, 1})
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
		send func(w *Records)
		want string
	}{
		{"two KeyUpdates in one record", func(w *Records) {
			_ = post(w, typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 0, msgKeyUpdate, 0, 0, 1, 0})
		}, "after a KeyUpdate"},
		{"data within a handshake message", func(w *Records) {
			_ = post(w, typeHandshake, []byte{msgNewSessionTicket, 0, 0, 8})
			_ = post(w, typeAppData, []byte("data"))
		}, "within a handshake message"},
		{"an empty handshake record", func(w *Records) {
			_ = post(w, typeHandshake, nil)
		}, "empty TLS record"},
		{"17 empty records", func(w *Records) {
			for range maxUseless + 1 {
				_ = post(w, typeAppData, nil)
			}
		}, "too many"},
		{"plaintext over 2^14+1", func(w *Records) {
			_ = post(w, typeAppData, make([]byte, maxPlaintext+1))
		}, "over 2^14+1"},
	}
	for _, c := range cases {
		srv, cli := ours(t)
		go c.send(cli)
		_, err := srv.Read(make([]byte, 64<<10))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: read %v, want an error about %q", c.name, err, c.want)
		}
	}
	// user_canceled is dropped, and data after it reads.
	srv, cli := ours(t)
	go func() {
		_ = post(cli, typeAlert, []byte{1, alertUserCanceled})
		_, _ = cli.Write([]byte("after"))
	}()
	got := make([]byte, 5)
	if _, err := io.ReadFull(srv, got); err != nil || string(got) != "after" {
		t.Fatalf("after user_canceled the server read %q, %v", got, err)
	}
}

// TestRecordsChaCha runs the interop and KeyUpdate tests again where AES
// has no hardware (GODEBUG=cpu.aes=off), so that crypto/tls negotiates
// ChaCha20-Poly1305, which the records must carry too.
func TestRecordsChaCha(t *testing.T) {
	if os.Getenv("RECORDS_CHACHA") == "1" {
		t.Skip("the subprocess runs the other tests")
	}
	cmd := exec.Command(os.Args[0], "-test.run", "^(TestRecordsWithCryptoTLS|TestRecordsAnswerKeyUpdate|TestRecordsUpdateTheirKey|TestRecordsOnTheLoop)$", "-test.count=1")
	cmd.Env = append(os.Environ(), "GODEBUG=cpu.aes=off", "RECORDS_CHACHA=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("with ChaCha20-Poly1305: %v\n%s", err, out)
	}
}

// wantSuite fails a test whose handshake negotiated other than ChaCha20
// in TestRecordsChaCha's subprocess.
func wantSuite(t *testing.T, r *Records) {
	t.Helper()
	if os.Getenv("RECORDS_CHACHA") == "1" && r.state.CipherSuite != tls.TLS_CHACHA20_POLY1305_SHA256 {
		t.Fatalf("negotiated %s without AES hardware, want ChaCha20-Poly1305", tls.CipherSuiteName(r.state.CipherSuite))
	}
}

// TestRecordsRotateWhileAnswering lowers the server's key update to 3 records and
// has the server answer a requested KeyUpdate with its third: the answer
// starts no second KeyUpdate under the old key, which the client, already
// on the next one, could not open.
func TestRecordsRotateWhileAnswering(t *testing.T) {
	srv, cli := ours(t)
	srv.updateAfter = 3
	served := make(chan struct{})
	defer func() { <-served }()
	if err := srv.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(served)
		_, _ = srv.Write([]byte("a"))
		_, _ = srv.Write([]byte("b"))
		got := make([]byte, 1)
		_, _ = io.ReadFull(srv, got) // takes the KeyUpdate and answers it
		_, _ = srv.Write([]byte("c"))
	}()
	got := make([]byte, 2)
	if _, err := io.ReadFull(cli, got); err != nil {
		t.Fatal(err)
	}
	sendHandshake(t, cli, []byte{msgKeyUpdate, 0, 0, 1, 1})
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
	// A record that does not open fails the read; one that never comes
	// fails it at the deadline.
	if err := cli.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	last := make([]byte, 1)
	if _, err := io.ReadFull(cli, last); err != nil || string(last) != "c" {
		t.Fatalf("after the answer at the threshold the client read %q, %v", last, err)
	}
}

// sessions records the tickets a crypto/tls client keeps.
type sessions struct{ put int }

func (s *sessions) Get(string) (*tls.ClientSessionState, bool) { return nil, false }
func (s *sessions) Put(_ string, cs *tls.ClientSessionState) {
	if cs != nil {
		s.put++
	}
}

// TestRecordsTickets checks both roles' tickets: our server sends none to
// a crypto/tls client that would keep one, and our client, which offers no
// resumption and so gets none from crypto/tls, ignores a well-formed one
// that a server sends anyway.
func TestRecordsTickets(t *testing.T) {
	p := newPKI(t)
	cache := &sessions{}
	cli := p.client()
	cli.ClientSessionCache = cache
	r, tc := pair(t, true, p.server(), cli)
	exchange(t, r, tc, nil)
	if cache.put != 0 {
		t.Fatalf("our server sent %d session tickets", cache.put)
	}
	srv, ourCli := ours(t)
	go func() {
		_ = post(srv, typeHandshake, []byte{msgNewSessionTicket, 0, 0, 14, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 1, 7, 0, 0})
		_, _ = srv.Write([]byte("data"))
	}()
	got := make([]byte, 4)
	if _, err := io.ReadFull(ourCli, got); err != nil || string(got) != "data" || ourCli.tickets != 1 {
		t.Fatalf("after a ticket our client read %q, %v, with %d tickets ignored", got, err, ourCli.tickets)
	}
}

// TestRecordsRefuseBadTickets refuses a NewSessionTicket from a client,
// and a malformed one from a server.
func TestRecordsRefuseBadTickets(t *testing.T) {
	ticket := []byte{msgNewSessionTicket, 0, 0, 14, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 1, 7, 0, 0}
	srv, cli := ours(t)
	go func() { _ = post(cli, typeHandshake, ticket) }()
	if _, err := srv.Read(make([]byte, 16)); err == nil || !strings.Contains(err.Error(), "from a TLS client") {
		t.Fatalf("a client's ticket gave %v", err)
	}
	srv, cli = ours(t)
	// A nonce of 9 bytes, in a body of 13 that has no room for it.
	bad := []byte{msgNewSessionTicket, 0, 0, 13, 0, 0, 0, 1, 0, 0, 0, 2, 9, 0, 0, 0, 0}
	go func() { _ = post(srv, typeHandshake, bad) }()
	if _, err := cli.Read(make([]byte, 16)); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("a malformed ticket gave %v", err)
	}
}

// TestRecordsTruncated cuts a record short: the read fails with
// io.ErrUnexpectedEOF, not an orderly io.EOF, and keeps failing.
func TestRecordsTruncated(t *testing.T) {
	srv, cli := ours(t)
	cli.wmu.Lock()
	rec := []byte{typeAppData, 3, 3, 0, 40, 1, 2, 3}
	_, err := cli.br.Conn.Write(rec)
	cli.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_ = cli.br.Conn.Close()
	for range 2 {
		if _, err := srv.Read(make([]byte, 16)); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("a record cut short gave %v, want io.ErrUnexpectedEOF", err)
		}
	}
}

// TestRecordsAnswerOnTheLoop asks a server on the loop for a KeyUpdate:
// it answers as it reads, and Flush sends the answer.
func TestRecordsAnswerOnTheLoop(t *testing.T) {
	srv, cli := ours(t)
	wantSuite(t, srv)
	poller := NewChanPoller()
	sock, err := poller.Attach(srv.br.Conn, 1)
	if err != nil {
		t.Fatal(err)
	}
	srv.br.Attach(sock, NewBuffers(32<<10, 4), nil)
	firstRead := cli.rd.secret
	sendHandshake(t, cli, []byte{msgKeyUpdate, 0, 0, 1, 1})
	cli.wmu.Lock()
	wr, err := cli.suite.updated(&cli.wr)
	cli.wr = wr
	cli.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 0, 4)
	buf := make([]byte, 16)
	for deadline := time.Now().Add(10 * time.Second); len(got) < 4 && time.Now().Before(deadline); {
		n, err := srv.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, ErrWouldBlock) {
			_, _ = poller.Wait(nil, 100*time.Millisecond)
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "ping" {
		t.Fatalf("the loop read %q", got)
	}
	if _, err := srv.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	for srv.br.Pending() > 0 || srv.open > 0 {
		if err := srv.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	pong := make([]byte, 4)
	if _, err := io.ReadFull(cli, pong); err != nil || string(pong) != "pong" {
		t.Fatalf("the client read %q, %v", pong, err)
	}
	if bytes.Equal(cli.rd.secret, firstRead) {
		t.Fatal("the loop did not answer the KeyUpdate")
	}
}

// TestRecordsCloseBreaksWrite closes records while a write waits on a
// peer that reads nothing: the close returns at once and the write fails,
// as crypto/tls's close breaks a write.
func TestRecordsCloseBreaksWrite(t *testing.T) {
	srv, _ := ours(t)
	writing := make(chan error, 1)
	go func() {
		_, err := srv.Write(make([]byte, 64<<20))
		writing <- err
	}()
	// Until the write holds the lock, blocked on the peer.
	for deadline := time.Now().Add(5 * time.Second); srv.wmu.TryLock(); {
		srv.wmu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the write never took the lock")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the close waited for a write that waits on the peer")
	}
	select {
	case err := <-writing:
		if err == nil {
			t.Fatal("a write the close broke succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the close did not break the write")
	}
}

// TestRecordsTicketExtensions takes a ticket's extensions as crypto/tls
// parses them: early_data with its 4-byte size, and none cut short.
func TestRecordsTicketExtensions(t *testing.T) {
	ticketWith := func(ext []byte) []byte {
		body := []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 1, 7}
		body = binary.BigEndian.AppendUint16(body, uint16(len(ext)))
		body = append(body, ext...)
		return append([]byte{msgNewSessionTicket, 0, byte(len(body) >> 8), byte(len(body))}, body...)
	}
	for _, c := range []struct {
		name string
		ext  []byte
		ok   bool
	}{
		{"early_data of 4 bytes", []byte{0, extEarlyData, 0, 4, 0, 0, 1, 0}, true},
		{"an unknown extension", []byte{0x12, 0x34, 0, 2, 9, 9}, true},
		{"early_data of 1 byte", []byte{0, extEarlyData, 0, 1, 0}, false},
		{"an extension header cut short", []byte{0}, false},
		{"an extension longer than its list", []byte{0x12, 0x34, 0, 9, 1}, false},
	} {
		srv, cli := ours(t)
		go func() {
			_ = post(srv, typeHandshake, ticketWith(c.ext))
			_, _ = srv.Write([]byte("data"))
		}()
		got := make([]byte, 4)
		_, err := io.ReadFull(cli, got)
		if c.ok && (err != nil || string(got) != "data") {
			t.Errorf("%s: read %q, %v", c.name, got, err)
		}
		if !c.ok && (err == nil || !strings.Contains(err.Error(), "malformed")) {
			t.Errorf("%s: read %v, want the ticket refused", c.name, err)
		}
	}
}

// TestRecordsUselessCountsMessages counts post-handshake messages as
// crypto/tls does: a ticket in 18 records of one byte is one message, and
// 17 whole tickets in one record are 17.
func TestRecordsUselessCountsMessages(t *testing.T) {
	ticket := []byte{msgNewSessionTicket, 0, 0, 14, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 1, 7, 0, 0}
	srv, cli := ours(t)
	go func() {
		for _, b := range ticket {
			_ = post(srv, typeHandshake, []byte{b})
		}
		_, _ = srv.Write([]byte("data"))
	}()
	got := make([]byte, 4)
	if _, err := io.ReadFull(cli, got); err != nil || string(got) != "data" {
		t.Fatalf("a ticket in %d records gave %q, %v", len(ticket), got, err)
	}
	srv, cli = ours(t)
	go func() { _ = post(srv, typeHandshake, bytes.Repeat(ticket, maxUseless+1)) }()
	if _, err := cli.Read(make([]byte, 4)); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("17 tickets in one record gave %v", err)
	}
}

// recording counts the writes on a raw connection.
type recording struct {
	net.Conn
	writes atomic.Int32
}

func (r *recording) Write(p []byte) (int, error) {
	r.writes.Add(1)
	return r.Conn.Write(p)
}

// TestRecordsNoUpdateAfterClose closes after two records of a 3-record
// key: close_notify is the last record, with no KeyUpdate after it.
func TestRecordsNoUpdateAfterClose(t *testing.T) {
	srv, cli := ours(t)
	rec := &recording{Conn: cli.br.Conn}
	cli.br.Conn = rec
	cli.updateAfter = 3
	go func() { _, _ = io.Copy(io.Discard, srv) }()
	for _, b := range []string{"a", "b"} {
		if _, err := cli.Write([]byte(b)); err != nil {
			t.Fatal(err)
		}
	}
	_ = cli.Close()
	if n := rec.writes.Load(); n != 3 {
		t.Fatalf("two records and close_notify took %d writes, want 3: a KeyUpdate followed the close", n)
	}
}

// failing fails its first read or write with a lasting kind of error.
type failing struct {
	net.Conn
	reads, writes atomic.Int32
}

var errBroken = errors.New("broken")

func (f *failing) Read(p []byte) (int, error) {
	if f.reads.Add(1) == 1 {
		return 0, errBroken
	}
	return f.Conn.Read(p)
}

func (f *failing) Write(p []byte) (int, error) {
	if f.writes.Add(1) == 1 {
		return 0, errBroken
	}
	return f.Conn.Write(p)
}

// TestRecordsKeepTheirErrors fails a read and a write of the raw
// connection once: the records keep each error, as crypto/tls does, and
// neither read nor write the connection again.
func TestRecordsKeepTheirErrors(t *testing.T) {
	srv, cli := ours(t)
	f := &failing{Conn: srv.br.Conn}
	srv.br.Conn = f
	go func() { _, _ = cli.Write([]byte("later")) }()
	for range 2 {
		if _, err := srv.Read(make([]byte, 8)); !errors.Is(err, errBroken) {
			t.Fatalf("a read after the raw connection broke gave %v", err)
		}
	}
	for range 2 {
		if _, err := srv.Write([]byte("x")); !errors.Is(err, errBroken) {
			t.Fatalf("a write after the raw connection broke gave %v", err)
		}
	}
	if f.reads.Load() != 1 || f.writes.Load() != 1 {
		t.Fatalf("the records read %d and wrote %d times after an error, want 1 and 1", f.reads.Load(), f.writes.Load())
	}
}

// TestRecordsRefusePaddedOverflow sends a record of 10 bytes and 16,380
// of padding: its TLSInnerPlaintext passes 2^14+1, which is refused
// whatever its content (RFC 8446, 5.4).
func TestRecordsRefusePaddedOverflow(t *testing.T) {
	srv, cli := ours(t)
	go func() {
		cli.wmu.Lock()
		_, _ = cli.br.Conn.Write(sealFor(&cli.wr, typeAppData, make([]byte, 10), 16380))
		cli.wmu.Unlock()
	}()
	if _, err := srv.Read(make([]byte, 64)); err == nil || !strings.Contains(err.Error(), "over 2^14+1") {
		t.Fatalf("a record padded past 2^14+1 gave %v", err)
	}
	// The sender hears record_overflow.
	var alert tls.AlertError
	if _, err := cli.Read(make([]byte, 16)); !errors.As(err, &alert) || alert != alertRecordOverflow {
		t.Fatalf("the sender of a record past 2^14+1 read %v, want record_overflow", err)
	}
}

// TestRecordsClientAnswers has our server ask our client for a key update:
// the client answers while it reads, as the server does.
func TestRecordsClientAnswers(t *testing.T) {
	srv, cli := ours(t)
	if err := srv.UpdateKey(true); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = srv.Write([]byte("ping")) }()
	got := make([]byte, 4)
	if _, err := io.ReadFull(cli, got); err != nil || string(got) != "ping" {
		t.Fatalf("the client read %q, %v", got, err)
	}
	go func() { _, _ = cli.Write([]byte("pong")) }()
	if _, err := io.ReadFull(srv, got); err != nil || string(got) != "pong" {
		t.Fatalf("the server read %q, %v", got, err)
	}
	if srv.KeyUpdatesRead() != 1 || cli.KeyUpdatesRead() != 1 {
		t.Fatalf("key updates read: server %d, client %d; want 1 and 1", srv.KeyUpdatesRead(), cli.KeyUpdatesRead())
	}
}

// TestRecordsReserve writes plaintext in place on the loop, between
// ordinary writes: room that does not fit the record being written goes
// into the next, a record that Commit fills is sealed, and crypto/tls
// reads every byte in order.
func TestRecordsReserve(t *testing.T) {
	p := newPKI(t)
	r, tc := pair(t, true, p.server(), p.client())
	poller := NewChanPoller()
	sock, err := poller.Attach(r.br.Conn, 1)
	if err != nil {
		t.Fatal(err)
	}
	r.br.Attach(sock, NewBuffers(32<<10, 4), NewBuffers(1<<20, 2))
	if b := r.Reserve(MaxReserve + 1); b != nil {
		t.Fatal("Reserve gave room past one record")
	}
	var want []byte
	write := func(b []byte) {
		if _, err := r.Write(b); err != nil {
			t.Fatal(err)
		}
		want = append(want, b...)
	}
	// reserve writes n bytes of ch in place, of room for n+extra.
	reserve := func(n, extra int, ch byte) {
		b := r.Reserve(n + extra)
		if len(b) != 0 || cap(b) != n+extra {
			t.Fatalf("Reserve(%d) gave length %d and capacity %d", n+extra, len(b), cap(b))
		}
		b = append(b, bytes.Repeat([]byte{ch}, n)...)
		// Commit seals a record it fills in place.
		want = append(want, b...)
		r.Commit(len(b))
	}
	write([]byte("first "))
	reserve(100, 20, 'a')
	// 16 300 bytes do not fit after the 106 bytes in the open record: it
	// is sealed, and a second one starts.
	reserve(16300, 0, 'b')
	if r.open != recordHeader+16300 {
		t.Fatalf("the open record holds %d bytes after its header, want 16300", r.open-recordHeader)
	}
	write(bytes.Repeat([]byte("c"), 1000))
	r.seal(typeAppData)
	// A whole record's room, filled, is sealed at once.
	reserve(MaxReserve, 0, 'd')
	if r.open != 0 {
		t.Fatalf("a full record is not sealed: %d bytes open", r.open)
	}
	write([]byte(" last"))
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(want))
		_, _ = io.ReadFull(tc, b)
		got <- b
	}()
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	for r.br.Pending() > 0 {
		if _, err := poller.Wait(nil, time.Second); err != nil {
			t.Fatal(err)
		}
		if err := r.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if b := <-got; !bytes.Equal(b, want) {
		i := 0
		for i < len(b) && b[i] == want[i] {
			i++
		}
		t.Fatalf("crypto/tls read other bytes than were written in place, from byte %d of %d: %q, want %q", i, len(want), b[i:min(i+20, len(b))], want[i:min(i+20, len(want))])
	}
}

// TestBridgeReserve writes in place into a bridge's write buffer, which
// grows into a large buffer once a pool's buffer is full.
func TestBridgeReserve(t *testing.T) {
	a, b := tcpPair(t)
	br := NewBridge(a, 4<<10)
	if br.Reserve(10) != nil {
		t.Fatal("Reserve gave room before Attach")
	}
	poller := NewChanPoller()
	sock, err := poller.Attach(a, 1)
	if err != nil {
		t.Fatal(err)
	}
	br.Attach(sock, NewBuffers(1<<10, 4), NewBuffers(64<<10, 2))
	var want []byte
	for i := range 40 {
		room := br.Reserve(100 + i)
		if len(room) != 0 || cap(room) != 100+i {
			t.Fatalf("Reserve(%d) gave length %d and capacity %d", 100+i, len(room), cap(room))
		}
		room = append(room, bytes.Repeat([]byte{byte('a' + i%26)}, 90+i)...)
		br.Commit(len(room))
		want = append(want, room...)
	}
	if br.Pending() != len(want) {
		t.Fatalf("%d bytes pending, want %d", br.Pending(), len(want))
	}
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, len(want))
		_, _ = io.ReadFull(b, buf)
		got <- buf
	}()
	for br.Pending() > 0 {
		if err := br.Flush(); err != nil {
			t.Fatal(err)
		}
		if br.Pending() > 0 {
			if _, err := poller.Wait(nil, time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	if buf := <-got; !bytes.Equal(buf, want) {
		t.Fatal("the socket read other bytes than were written in place")
	}
}
