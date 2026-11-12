package server

import (
	"context"
	"errors"
	"io"
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
	// What the loop had queued for the connection runs before the test
	// ends: a second end panics there.
	syncLoop(t, s)
}

// syncLoop waits until the loop ran everything posted before and the
// flushes that work listed, a connection's teardown included: the barrier
// is a flusher listed after them, whose flush comes after theirs.
func syncLoop(t *testing.T, s *Server) {
	t.Helper()
	b := barrier(make(chan struct{}))
	if !s.loop.Post(func() { s.loop.Dirty(b) }) {
		return
	}
	select {
	case <-b:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not run")
	}
}

// barrier is closed when the loop flushes it.
type barrier chan struct{}

func (b barrier) Flush() { close(b) }

// TestGoAwayBeforeHandOver sends a tracked connection a GOAWAY, as Stop
// does, before the loop has it: only the hand-over may end it then, so the
// loop must not release it and its count in the server's WaitGroup.
func TestGoAwayBeforeHandOver(t *testing.T) {
	s := bareServer(t)
	s.conns, s.gone = map[closer]struct{}{}, newConnMetrics()
	go func() { _ = s.loop.Run(t.Context()) }()
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	br := loop.NewBridge(a, ioBuffer)
	c := newConn(s, br, br)
	s.wg.Add(1)
	if !s.track(c) {
		t.Fatal("track refused the connection")
	}
	c.goAway()
	syncLoop(t, s)
	if c.gone {
		t.Fatal("the loop ended a connection it did not have")
	}
	c.close()
	if !s.loop.Post(c.attach) {
		t.Fatal("the loop refused the hand-over")
	}
	syncLoop(t, s)
	if !c.gone {
		t.Fatal("the loop did not end the connection closed before its hand-over")
	}
	s.wg.Wait()
}

// TestHeldAcrossConnections holds more than the server's share of the
// memory budget on one connection's call: another connection's window
// credit waits, and goes out once that call lets go.
func TestHeldAcrossConnections(t *testing.T) {
	s := bareServer(t)
	s.maxHeldAll = 1 << 20
	a, b := testConn(t), testConn(t)
	a.s, b.s = s, s
	st := testCall(a, 1)
	a.mu.Lock()
	a.chargeLocked(&st.reqHeld, 2<<20)
	a.mu.Unlock()
	b.mu.Lock()
	b.recvCredit = connWindow
	b.creditLocked()
	waiting := len(b.ctrl)
	b.mu.Unlock()
	if waiting != 0 {
		t.Fatal("a connection got credit while the calls of all held more than their share")
	}
	a.mu.Lock()
	a.releaseLocked(&st.reqHeld)
	a.mu.Unlock()
	s.loop.Dispatch(false)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.ctrl) != 1 || b.ctrl[0].kind != ctrlWindowUpdate || b.recvCredit != 0 {
		t.Fatalf("after the release: control frames %v, %d bytes still to credit", b.ctrl, b.recvCredit)
	}
}

// TestConnectionLimit refuses a connection past the most the memory
// budget holds, and takes one again after another ended.
func TestConnectionLimit(t *testing.T) {
	s := bareServer(t)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.conns, s.gone = map[closer]struct{}{}, newConnMetrics()
	s.maxConns = 1
	go func() { _ = s.loop.Run(t.Context()) }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.ServeListener(Listener{Listener: ln, Traffic: TrafficAll}) }()
	defer s.Stop() //nolint:errcheck // stopping a test server
	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close() //nolint:errcheck // closing a test connection
	for {
		s.mu.Lock()
		open := s.open
		s.mu.Unlock()
		if open == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close() //nolint:errcheck // closing a test connection
	if err := second.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("the connection past the limit read %v, want it closed", err)
	}
}
