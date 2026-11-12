package sim

import (
	"errors"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/loop"
)

// TestSwitchingDelaySwitches checks that a switching delay goes back and
// forth between its low and high delays at every switch.
func TestSwitchingDelaySwitches(t *testing.T) {
	s := &Sim{rng: rand.New(rand.NewPCG(1, 2))}
	l := &link{}
	for i := range int64(20) {
		// A switch comes 100 to 400 ms after the last one.
		s.switchDelay(l, i*500*Millisecond)
		want := "low"
		if i%2 == 0 {
			want = "high"
		}
		if high := l.extra >= 20*Millisecond; high != (want == "high") {
			t.Fatalf("switch %d: delay %d ms, want it %s", i, l.extra/Millisecond, want)
		}
	}
}

// writer is one end of a connection between two nodes over links that are
// up, whose other end reads nothing.
func writer() *conn {
	s := &Sim{rng: rand.New(rand.NewPCG(1, 2)), t0: time.Now(), links: [][]link{{{up: true}, {up: true}}, {{up: true}, {up: true}}}}
	a, b := &conn{s: s, from: 0, to: 1}, &conn{s: s, from: 1, to: 0}
	a.in, a.out = newPipe(), newPipe()
	b.in, b.out, a.other, b.other = a.out, a.in, b, a
	return a
}

// TestWriteToClosedPeerResets writes to a connection whose peer closed
// after sending bytes that a link stall holds back: the write fails with
// a reset, and so does the read of those bytes, at once, as TCP's RST
// does. Before #369's fix the read waited for the stall to end.
func TestWriteToClosedPeerResets(t *testing.T) {
	a := writer()
	b := a.other
	a.s.net = network{conns: map[*conn]bool{a: true, b: true}}
	a.s.links[1][0].stallUntil = 1000 * Second
	if _, err := b.Write([]byte("held")); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("x")); !errors.Is(err, errReset) {
		t.Fatalf("write to a closed peer = %v, want a reset", err)
	}
	read := make(chan error, 1)
	go func() {
		_, err := a.Read(make([]byte, 4))
		read <- err
	}()
	select {
	case err := <-read:
		if !errors.Is(err, errReset) {
			t.Fatalf("read after the reset = %v, want a reset", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the read still waits for the bytes the stall holds")
	}
}

// TestWriteDeadline checks that a write fails at its deadline: at once
// when it passed, and after what fits when the buffer fills.
func TestWriteDeadline(t *testing.T) {
	a := writer()
	if err := a.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := a.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write past its deadline = %d, %v", n, err)
	}
	// Also on a half-open link, which swallows the write.
	a.s.links[0][1].halfOpenUntil = Second
	if n, err := a.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write past its deadline on a half-open link = %d, %v", n, err)
	}
	a = writer()
	if err := a.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, err := a.Write(make([]byte, sendBuffer+1000)); n != sendBuffer || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write past the buffer that nobody reads = %d, %v; want %d, a deadline", n, err, sendBuffer)
	}
}

// TestDamagedReadFreesWriter reads damaged bytes on a connection whose
// writer waits for room that its peer, which reads nothing, never makes:
// the read ends the connection, so the write fails with a reset. Before
// #439's fix the writer waited until the link fault ended, and a TLS
// alert of the reader waited for the writer's lock meanwhile, which under
// the race detector stops the synctest clock for good.
func TestDamagedReadFreesWriter(t *testing.T) {
	a := writer()
	b := a.other
	a.s.net = network{conns: map[*conn]bool{a: true, b: true}}
	l := a.s.link(1, 0)
	l.netem, l.netemUntil = flipped, 1000*Second
	wrote := make(chan error, 1)
	go func() {
		_, err := a.Write(make([]byte, 2*sendBuffer))
		wrote <- err
	}()
	if _, err := b.Write([]byte("record")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Read(make([]byte, 16)); err != nil {
		t.Fatalf("the read of the damaged bytes = %v", err)
	}
	select {
	case err := <-wrote:
		if !errors.Is(err, errReset) {
			t.Fatalf("the blocked write = %v, want a reset", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the write still waits for room after its reader got damaged bytes")
	}
}

// TestReadsAtOnceMeetTheirDeadline checks that two reads that wait at once
// on an empty connection, as net.Conn allows, both end at its read
// deadline: they wait on timers of their own, not on one that only one of
// them would hear (#533).
func TestReadsAtOnceMeetTheirDeadline(t *testing.T) {
	b := writer().other
	if err := b.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := b.Read(make([]byte, 1))
			errs <- err
		}()
	}
	for range 2 {
		select {
		case err := <-errs:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Errorf("a read past its deadline gave %v, want %v", err, os.ErrDeadlineExceeded)
			}
		case <-time.After(time.Second):
			t.Fatal("a read waited past its deadline")
		}
	}
}

// TestLoopConnWaitsForNotice drives both ends of a connection as a node's
// event loop does (conn.Notify): a read with nothing there and a write
// past the buffer return loop.ErrWouldBlock at once, and the notice comes
// when the link's delay brings the bytes, and when the reader makes room.
func TestLoopConnWaitsForNotice(t *testing.T) {
	a := writer()
	b := a.other
	a.s.links[0][1].base = 5 * Millisecond
	notices := make(chan string, 16)
	a.Notify(func() { notices <- "writer" })
	b.Notify(func() { notices <- "reader" })
	buf := make([]byte, 2*sendBuffer)
	if n, err := b.Read(buf); n != 0 || !errors.Is(err, loop.ErrWouldBlock) {
		t.Fatalf("read of nothing = %d, %v", n, err)
	}
	if n, err := a.Write(buf); n != sendBuffer || !errors.Is(err, loop.ErrWouldBlock) {
		t.Fatalf("write past the buffer = %d, %v; want %d and a block", n, err, sendBuffer)
	}
	wait := func(who string) {
		t.Helper()
		select {
		case got := <-notices:
			if got != who {
				t.Fatalf("notice for the %s, want the %s", got, who)
			}
		case <-time.After(time.Second):
			t.Fatalf("no notice for the %s", who)
		}
	}
	wait("reader")
	// The bytes are on their way: the read before their delay passed
	// armed a timer, which brings the reader back once they arrived.
	got := 0
	for got < sendBuffer {
		n, err := b.Read(buf)
		got += n
		if errors.Is(err, loop.ErrWouldBlock) {
			wait("reader")
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	wait("writer")
	if n, err := a.Write(buf[:10]); n != 10 || err != nil {
		t.Fatalf("write after the reader made room = %d, %v", n, err)
	}
}
