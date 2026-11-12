package server

import (
	"net"
	"testing"

	"github.com/djosh34/tigerstore/internal/loop"
)

// gate is a socket whose writes block while shut, as a client's that
// stopped reading for a while, and that counts what it took.
type gate struct {
	shut  bool
	took  int
	calls int
}

func (g *gate) Read([]byte) (int, error) { return 0, loop.ErrWouldBlock }

func (g *gate) Write(b []byte) (int, error) {
	g.calls++
	if g.shut {
		return 0, loop.ErrWouldBlock
	}
	g.took += len(b)
	return len(b), nil
}

func (g *gate) Close() error { return nil }

// TestWriteRetriesHeldOutput writes a connection's first frames to a
// socket that blocks, then lets it take them: the next write sends the
// bytes the bridge held, though no new frame waits.
func TestWriteRetriesHeldOutput(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close() //nolint:errcheck // closing a test pipe
	defer b.Close() //nolint:errcheck // closing a test pipe
	br := loop.NewBridge(a, ioBuffer)
	g := &gate{shut: true}
	br.Attach(g)
	c := newConn(bareServer(t), br, br)
	if _, err := c.write(); err != nil {
		t.Fatal(err)
	}
	held := br.Pending()
	if held == 0 {
		t.Fatal("the blocked socket took the first frames")
	}
	g.shut = false
	if _, err := c.write(); err != nil {
		t.Fatal(err)
	}
	if br.Pending() != 0 || g.took != held {
		t.Fatalf("%d bytes still held and %d taken after the socket opened, want 0 and %d", br.Pending(), g.took, held)
	}
}
