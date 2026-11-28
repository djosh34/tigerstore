package wl

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestTLS12RecordsBothWays has crypto/tls's client talk to the record
// layer over TLS 1.2, with each AEAD suite: what it writes opens, and
// what the record layer seals, records past 16 KiB too, reads back.
func TestTLS12RecordsBothWays(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueFor("node", 1, []net.IP{net.ParseIP("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, suite := range []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	} {
		t.Run(tls.CipherSuiteName(suite), func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			srvCfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}
			got := make(chan *handoff, 1)
			go func() {
				nc, err := ln.Accept()
				if err != nil {
					got <- nil
					return
				}
				h, err := handshake(t.Context(), sysEnv{}, nc, srvCfg, kindClient)
				if err != nil {
					t.Error(err)
				}
				got <- h
			}()
			cli, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
				RootCAs: ca.Pool, NextProtos: []string{"h2"},
				MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12, CipherSuites: []uint16{suite},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cli.Close() }()
			h := <-got
			if h == nil {
				t.Fatal("no handshake")
			}
			defer func() { _ = unix.Close(h.fd) }()
			if err := unix.SetNonblock(h.fd, false); err != nil {
				t.Fatal(err)
			}
			s := newSock(h, &counters{})
			// Client to the record layer.
			want := bytes.Repeat([]byte("client says "), 3000)
			go func() { _, _ = cli.Write(want) }()
			var read []byte
			for len(read) < len(want) {
				if err := s.open(); err != nil {
					t.Fatal(err)
				}
				read = append(read, s.pt[s.pr:s.pw]...)
				s.pr = s.pw
				if len(read) < len(want) {
					if _, err := s.readSock(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !bytes.Equal(read, want) {
				t.Fatal("the record layer read other bytes than the client wrote")
			}
			// The record layer to the client.
			back := bytes.Repeat([]byte("server says "), 3000)
			for p := back; len(p) > 0; {
				b := s.frame(min(len(p), max(s.room(), 1)))
				n := copy(b, p)
				p = p[n:]
			}
			for {
				ok, err := s.flush()
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					break
				}
			}
			buf := make([]byte, len(back))
			if _, err := io.ReadFull(cli, buf); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, back) {
				t.Fatal("the client read other bytes than the record layer sealed")
			}
		})
	}
}
