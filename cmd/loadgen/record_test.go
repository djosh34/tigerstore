package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"testing"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestRecordLayer talks to crypto/tls's server through the record layer.
// The server sends session tickets after the handshake, which the record
// layer drops; it echoes 200 KiB, which crosses many records both ways;
// and its close_notify reads as the end.
func TestRecordLayer(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	kp, _, _, err := ca.Issue("server", 2)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	const size = 200 << 10
	served := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			served <- err
			return
		}
		tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{kp}, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS13})
		b := make([]byte, size)
		_, err = io.ReadFull(tc, b)
		if err == nil {
			_, err = tc.Write(b)
		}
		served <- errors.Join(err, tc.Close())
	}()

	saved := clientTLS
	clientTLS = &tls.Config{RootCAs: ca.Pool, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS13}
	defer func() { clientTLS = saved }()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	rc, err := clientHandshake(nc, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, size, size+1)
	for i := range want {
		want[i] = byte(rand.Uint32())
	}
	if _, err := rc.Write(want); err != nil {
		t.Fatal(err)
	}
	var got []byte
	for len(got) < size {
		n := min(size-len(got), 9+rand.IntN(maxPlaintext))
		b, err := rc.Peek(n)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, b...)
		if _, err := rc.Discard(n); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the echo differs from what was sent")
	}
	if _, err := rc.Peek(1); !errors.Is(err, io.EOF) {
		t.Errorf("after the server's close_notify: %v, want EOF", err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

// readOnly is a connection that reads from r.
type readOnly struct {
	net.Conn
	r io.Reader
}

func (c readOnly) Read(p []byte) (int, error) { return c.r.Read(p) }

// sealed returns a record layer that reads records of the given content
// types and bodies, sealed under one key.
func sealed(t *testing.T, records ...[]byte) *recordConn {
	t.Helper()
	secret := make([]byte, 32)
	rd, err := newDirection(tls.TLS_AES_128_GCM_SHA256, secret)
	if err != nil {
		t.Fatal(err)
	}
	wr, err := newDirection(tls.TLS_AES_128_GCM_SHA256, secret)
	if err != nil {
		t.Fatal(err)
	}
	var wire []byte
	for _, r := range records {
		// The content type is the record's first byte here, and goes last.
		plain := append(append([]byte(nil), r[1:]...), r[0])
		n := len(plain) + tagLen
		hdr := []byte{typeAppData, 3, 3, byte(n >> 8), byte(n)}
		wire = append(wire, hdr...)
		wire = wr.aead.Seal(wire, wr.next(), plain, hdr)
	}
	return &recordConn{nc: readOnly{r: bytes.NewReader(wire)}, in: make([]byte, recordBuf), pt: make([]byte, recordBuf), rd: rd}
}

// TestSessionTicketLength takes in a session ticket whose length has bit 2
// set, 14 bytes: the length was read as 4 + hi<<16 | mid<<8 | low, which
// ORed the 4 into it and took the ticket for one cut across records.
func TestSessionTicketLength(t *testing.T) {
	// Lifetime, age add, an empty nonce, a one-byte ticket, no extensions.
	m := []byte{msgNewSessionTicket, 0, 0, 14, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 'x', 0, 0}
	if err := postHandshake(m); err != nil {
		t.Fatal(err)
	}
}

// TestBufferedKeepsError reads a record of data and then a key update:
// Buffered opens both, Peek returns the data, and the next Peek the key
// update. Buffered used to drop the error of a record it had taken, and
// the next Peek read past it to the end of the connection.
func TestBufferedKeepsError(t *testing.T) {
	rc := sealed(t, append([]byte{typeAppData}, "hello"...), []byte{typeHandshake, msgKeyUpdate, 0, 0, 1, 0})
	if err := rc.fill(recordHeaderLen); err != nil {
		t.Fatal(err)
	}
	if n := rc.Buffered(); n != 5 {
		t.Fatalf("%d bytes buffered, want 5", n)
	}
	if b, err := rc.Peek(5); err != nil || string(b) != "hello" {
		t.Fatalf("Peek: %q, %v", b, err)
	}
	if _, err := rc.Discard(5); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Peek(1); !errors.Is(err, errKeyUpdate) {
		t.Fatalf("Peek after the data: %v, want %v", err, errKeyUpdate)
	}
}
