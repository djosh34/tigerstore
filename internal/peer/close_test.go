package peer

import (
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// heldConn is a connection whose Close waits for release, as TLS's Close
// waits up to 5 s to send close_notify to a peer that does not read.
type heldConn struct {
	net.Conn
	closing, release chan struct{}
}

func newHeldConn() *heldConn {
	return &heldConn{closing: make(chan struct{}), release: make(chan struct{})}
}

func (c *heldConn) Close() error {
	close(c.closing)
	<-c.release
	return nil
}

// other tracks a connection on tr whose Close does not wait.
func other(t *testing.T, tr *TCP) net.Conn {
	t.Helper()
	c, end := net.Pipe()
	t.Cleanup(func() { end.Close() }) //nolint:errcheck,gosec // closing a test pipe
	if !tr.track(c) {
		t.Fatal("the transport took no connection")
	}
	return c
}

// endsWhileHeld fails t unless the goroutine of connection other can end
// (untrack) while held's Close waits, then releases held.
func endsWhileHeld(t *testing.T, tr *TCP, held *heldConn, other net.Conn) {
	t.Helper()
	defer close(held.release)
	select {
	case <-held.closing:
	case <-time.After(10 * time.Second):
		t.Fatal("the held connection was never closed")
	}
	ended := make(chan struct{})
	go func() {
		tr.untrack(other)
		close(ended)
	}()
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("a connection's goroutine waited for the close of another (#383)")
	}
}

func newTCP(t *testing.T) *TCP {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1)
	return start(t, 1, lns[1], addrs, cert, ca.Pool).t
}

// Close closes its connections without the lock their goroutines take
// to end, so a peer that does not read slows only Close itself (#383).
func TestCloseLetsConnectionsEnd(t *testing.T) {
	tr := newTCP(t)
	held := newHeldConn()
	if !tr.track(held) {
		t.Fatal("the transport took no connection")
	}
	c := other(t, tr)
	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	endsWhileHeld(t, tr, held, c)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

// SetPeers closes the connections of a removed peer the same way (#383).
func TestRemovingPeerLetsConnectionsEnd(t *testing.T) {
	tr := newTCP(t)
	defer func() {
		if err := tr.Close(); err != nil {
			t.Error(err)
		}
	}()
	held := newHeldConn()
	tr.mu.Lock()
	tr.inbound[held] = inbound{id: 2, version: maxVersion}
	tr.mu.Unlock()
	c := other(t, tr)
	set := make(chan struct{})
	go func() {
		tr.SetPeers(map[uint64]string{}, []uint64{2})
		close(set)
	}()
	endsWhileHeld(t, tr, held, c)
	<-set
}
