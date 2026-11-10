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
