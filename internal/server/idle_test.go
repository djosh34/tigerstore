package server

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestIdleConnectionMemory opens 400 idle HTTP/2 connections over TLS and
// measures the heap and stacks they take, client and server together:
// that must stay under idleBytes, which the server's connection limit is
// sized from.
func TestIdleConnectionMemory(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueFor("server", 2, []net.IP{net.ParseIP("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := bareServer(t)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.conns, s.gone = map[closer]struct{}{}, newConnMetrics()
	s.cfg.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}
	s.grpcTLS = grpcTLS(s.cfg.TLS)
	go func() { _ = s.loop.Run(t.Context()) }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.ServeListener(Listener{Listener: ln, TLS: true, Traffic: TrafficGRPC}) }()
	defer s.Stop() //nolint:errcheck // stopping a test server
	// dial opens a connection and waits for the answer to a ping, after
	// which both ends are idle.
	dial := func() net.Conn {
		c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: ca.Pool, NextProtos: []string{"h2"}, ServerName: "127.0.0.1"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write([]byte(http2.ClientPreface)); err != nil {
			t.Fatal(err)
		}
		fr := http2.NewFramer(c, c)
		if err := fr.WriteSettings(); err != nil {
			t.Fatal(err)
		}
		if err := fr.WritePing(false, [8]byte{1}); err != nil {
			t.Fatal(err)
		}
		if err := c.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			if p, ok := f.(*http2.PingFrame); ok && p.IsAck() {
				return c
			}
		}
	}
	inUse := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse + m.StackInuse
	}
	conns := []net.Conn{dial()}
	before := inUse()
	const n = 400
	for range n {
		conns = append(conns, dial())
	}
	per := (inUse() - before) / n
	runtime.KeepAlive(conns)
	t.Logf("an idle connection takes %d bytes, client and server together", per)
	if per > idleBytes {
		t.Fatalf("an idle connection takes %d bytes, client and server together, over idleBytes, %d", per, idleBytes)
	}
}
