package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// gate is a socket whose writes block while shut, as a client's that
// stopped reading for a while, and that counts what it took.
type gate struct {
	shut  bool
	took  int
	calls int
	// keep, when set, is how many bytes of each write the socket leaves.
	keep int
}

func (g *gate) Read([]byte) (int, error) { return 0, loop.ErrWouldBlock }

func (g *gate) Write(b []byte) (int, error) {
	g.calls++
	if g.shut {
		return 0, loop.ErrWouldBlock
	}
	if g.keep > 0 && len(b) > g.keep {
		g.took += len(b) - g.keep
		return len(b) - g.keep, loop.ErrWouldBlock
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
	br.Attach(g, nil)
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

// TestOutputWaitsForRoom fills the server's room for output that waits
// for sockets with one connection's, whose client reads nothing: another
// connection takes no new write pass meanwhile, and writes once the first
// client read its bytes.
func TestOutputWaitsForRoom(t *testing.T) {
	s := bareServer(t)
	s.maxOut = 10
	open := func() (*conn, *gate) {
		a, b := net.Pipe()
		t.Cleanup(func() {
			a.Close() //nolint:errcheck // closing a test pipe
			b.Close() //nolint:errcheck // closing a test pipe
		})
		br := loop.NewBridge(a, ioBuffer)
		g := &gate{shut: true}
		br.Attach(g, nil)
		c := newConn(s, br, br)
		// As attach sets it: the client's preface may still come.
		c.prefaceBy = time.Now().Add(time.Hour)
		return c, g
	}
	first, firstGate := open()
	second, secondGate := open()
	first.Flush()
	if s.outAll <= s.maxOut {
		t.Fatalf("the first connection's settings, %d bytes, should fill the room of %d", s.outAll, s.maxOut)
	}
	secondGate.shut = false
	second.Flush()
	if secondGate.calls != 0 {
		t.Fatal("the second connection wrote while the first one's output filled the room")
	}
	firstGate.shut = false
	first.Flush()
	s.loop.Flush()
	if secondGate.took == 0 || s.outAll != 0 {
		t.Fatalf("after the first client read: the second wrote %d bytes, %d wait", secondGate.took, s.outAll)
	}
}

// TestConnectionLimit refuses an HTTP/2 connection past the most the
// memory budget holds: the client gets the server's SETTINGS and a GOAWAY
// that says why, never a silent hang.
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
	// Well before the handshake timeout, which would close it too.
	if err := second.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(second, second)
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("the refused connection ended without a GOAWAY: %v", err)
		}
		if ga, ok := f.(*http2.GoAwayFrame); ok {
			if ga.ErrCode != http2.ErrCodeEnhanceYourCalm || string(ga.DebugData()) != refusal {
				t.Fatalf("GOAWAY %v %q, want ENHANCE_YOUR_CALM %q", ga.ErrCode, ga.DebugData(), refusal)
			}
			break
		}
	}
	// Once the first connection ended, a new one is served.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		s.mu.Lock()
		open := s.open
		s.mu.Unlock()
		if open == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	third, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close() //nolint:errcheck // closing a test connection
	if err := third.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	tf := http2.NewFramer(third, third)
	if err := tf.WriteSettings(); err != nil {
		t.Fatal(err)
	}
	if err := tf.WritePing(false, [8]byte{7}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := tf.ReadFrame()
		if err != nil {
			t.Fatalf("the connection after the first one ended was not served: %v", err)
		}
		if _, ok := f.(*http2.GoAwayFrame); ok {
			t.Fatal("the connection after the first one ended was refused")
		}
		if p, ok := f.(*http2.PingFrame); ok && p.IsAck() {
			break
		}
	}
	if n := testutil.ToFloat64(s.metrics.set.ClientConnsRefused); n != 1 {
		t.Fatalf("%g refused connections counted, want 1", n)
	}
}

// TestWritePassIsBounded checks the bounds of one write pass for a client
// that reads nothing: 30,000 streams with trailers hit maxPassFrames, 64
// streams with 4 KiB trailers hit maxTake's bytes, and one stream's data
// in tiny frames hits maxPassFrames too.
func TestWritePassIsBounded(t *testing.T) {
	pass := func(fill func(c *conn)) (*Server, *loop.Bridge) {
		s := bareServer(t)
		a, b := net.Pipe()
		t.Cleanup(func() {
			a.Close() //nolint:errcheck // closing a test pipe
			b.Close() //nolint:errcheck // closing a test pipe
		})
		br := loop.NewBridge(a, ioBuffer)
		br.Attach(&gate{shut: true}, nil)
		c := newConn(s, br, br)
		c.prefaceBy = time.Now().Add(time.Hour)
		c.mu.Lock()
		fill(c)
		c.mu.Unlock()
		c.Flush()
		return s, br
	}
	trailers := func(c *conn, streams int, size int) {
		block := statusBlock(status.New(codes.Unavailable, strings.Repeat("x", size)), true)
		for i := range streams {
			id := uint32(2*i + 1)
			st := &stream{c: c, id: id, sendWin: 1 << 30, inEOF: true}
			c.streams[id] = st
			st.out = []outItem{{kind: outTrailers, b: block}}
			c.list(st)
		}
	}
	// The connection's own SETTINGS and WINDOW_UPDATE go in the pass too.
	const slack = 64
	s, br := pass(func(c *conn) { trailers(c, 30000, 1) })
	if n := len(s.framesOut); n > maxPassFrames {
		t.Fatalf("one pass took %d frames; maxPassFrames is %d", n, maxPassFrames)
	}
	if p := br.Pending(); p > maxTake+slack {
		t.Fatalf("one pass of small trailers left %d bytes; maxTake is %d", p, maxTake)
	}
	_, br = pass(func(c *conn) { trailers(c, 1000, 4<<10) })
	if p := br.Pending(); p > maxTake+slack {
		t.Fatalf("one pass of 4 KiB trailers left %d bytes; maxTake is %d", p, maxTake)
	}
	s, _ = pass(func(c *conn) {
		c.maxFrame = 16
		st := &stream{c: c, id: 1, sendWin: 1 << 30, inEOF: true}
		c.streams[1] = st
		st.out = []outItem{{kind: outData, b: make([]byte, 64<<10)}}
		c.list(st)
	})
	if n := len(s.framesOut); n > maxPassFrames {
		t.Fatalf("one stream's data took %d frames in one pass; maxPassFrames is %d", n, maxPassFrames)
	}
}

// TestRetainedOutputCounts sends a connection's first frames to a socket
// that takes all but one byte: the whole buffer that keeps that byte counts
// in the server's waiting output, not the byte.
func TestRetainedOutputCounts(t *testing.T) {
	s := bareServer(t)
	a, b := net.Pipe()
	defer a.Close() //nolint:errcheck // closing a test pipe
	defer b.Close() //nolint:errcheck // closing a test pipe
	br := loop.NewBridge(a, ioBuffer)
	br.Attach(&gate{keep: 1}, s.bufs)
	c := newConn(s, br, br)
	c.prefaceBy = time.Now().Add(time.Hour)
	c.Flush()
	if br.Pending() != 1 || s.outAll < connBuffer || s.outAll != br.Retained() {
		t.Fatalf("one byte waits, and the server counts %d bytes; the buffer holding it is %d", s.outAll, br.Retained())
	}
}

// TestHeaderCacheKeepsSmallBlocks checks that a connection keeps the
// headers of a block of indexed fields only while the block is small.
func TestHeaderCacheKeepsSmallBlocks(t *testing.T) {
	c := testConn(t)
	h := &reqHeaders{}
	if !c.cacheable(h, make([]byte, maxBlockKey)) {
		t.Fatal("a block of maxBlockKey bytes is not kept")
	}
	if c.cacheable(h, make([]byte, maxBlockKey+1)) {
		t.Fatal("a block over maxBlockKey bytes is kept")
	}
}
