package server

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/net/http2"

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

// TestStopDuringPreface stops the server while a client on a listener
// without TLS has connected and not yet sent its preface. The connection
// that comes after the stop is never handed to the loop, and must not be
// ended twice: before the fix Stop panicked with a negative WaitGroup
// count.
func TestStopDuringPreface(t *testing.T) {
	s := bareServer(t)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.conns, s.gone = map[closer]struct{}{}, newConnMetrics()
	go func() { _ = s.loop.Run(t.Context()) }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.ServeListener(Listener{Listener: ln, Traffic: TrafficAll}) }()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() //nolint:errcheck // closing a test connection
	// The accept is under way once the server waits for the preface.
	for {
		s.mu.Lock()
		n := len(s.lns)
		s.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop() }()
	for {
		s.mu.Lock()
		stop := s.stopped
		s.mu.Unlock()
		if stop {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := nc.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Stop did not return")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
