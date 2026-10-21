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

// TestWriteResetBreaksRead checks the fault behind #369: queued bytes
// delayed past a peer's close must not hide a reset from the reader.
func TestWriteResetBreaksRead(t *testing.T) {
	a := writer()
	a.in.chunks = append(a.in.chunks, chunk{at: 10 * Second, b: []byte("pending")})
	a.other.closed = true
	a.in.eof = true
	if n, err := a.Write([]byte("request")); n != 0 || !errors.Is(err, errReset) {
		t.Fatalf("write after peer closed = %d, %v; want reset", n, err)
	}
	// An expired deadline keeps the broken implementation from hanging.
	// A reset takes precedence, just as it does after network.reset.
	if err := a.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := a.Read(make([]byte, 1)); n != 0 || !errors.Is(err, errReset) {
		t.Fatalf("read after write reset = %d, %v; want reset", n, err)
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
