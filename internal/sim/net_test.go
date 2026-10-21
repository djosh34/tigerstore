package sim

import (
	"errors"
	"math/rand/v2"
	"os"
	"testing"
	"time"
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
