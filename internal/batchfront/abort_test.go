package batchfront

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

type countedWrites struct {
	net.Conn
	writes atomic.Uint64
}

func (c *countedWrites) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// The owner abort path must not send TLS close_notify. net.Pipe makes a
// nonreading peer block every write, without filling an OS socket buffer.
func TestTLSAbortDoesNotWriteToSlowPeer(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("server", 123)
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	raw := &countedWrites{Conn: a}
	server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	client := tls.Client(b, &tls.Config{RootCAs: ca.Pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	handshakeCtx, handshakeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer handshakeCancel()
	handshake := make(chan error, 1)
	go func() { handshake <- server.HandshakeContext(handshakeCtx) }()
	if err := client.HandshakeContext(handshakeCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-handshake; err != nil {
		t.Fatal(err)
	}
	before := raw.writes.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &conn{Conn: server, ctx: ctx, cancel: cancel}
	done := make(chan struct{})
	go func() { c.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		_ = a.Close()
		<-done
		t.Fatal("owner abort blocked on a nonreading TLS client")
	}
	if after := raw.writes.Load(); after != before {
		t.Fatalf("abort performed %d TLS writes", after-before)
	}
	if ctx.Err() == nil {
		t.Fatal("abort did not cancel connection")
	}
}
