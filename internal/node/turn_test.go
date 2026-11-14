package node

import (
	"testing"
	"time"
)

// TestStoppedNodeHasTurnRoom checks that a node that stopped right after
// a turn that took a full turn's worth of calls still has room for calls:
// its loop drains on without its committer, which no longer starts turns,
// and its connections must read what clients send, so that they hear
// that it stopped (#647).
func TestStoppedNodeHasTurnRoom(t *testing.T) {
	n := &Node{done: make(chan struct{}), turnCalls: 300, turnLimit: 300}
	if r := n.TurnRoom(); r != 0 {
		t.Fatalf("a running node after a full turn has room for %d calls", r)
	}
	close(n.done)
	if r := n.TurnRoom(); r < 300 {
		t.Fatalf("a stopped node after a full turn has room for %d calls", r)
	}
}

// TestTurnFitsCallsInTime checks how many calls a turn takes:
// firstTurnCalls before a turn measured what a call costs, then those
// that fit in the time left before the node's next tick, but in no less
// than minTurnTime and at most a tick, at the moving average of that cost,
// and never fewer than minTurnCalls. A turn that took too few calls
// measures nothing.
func TestTurnFitsCallsInTime(t *testing.T) {
	n := &Node{cfg: Config{Tick: 100 * time.Millisecond}}
	if fit := n.turnFit(50 * time.Millisecond); fit != firstTurnCalls {
		t.Fatalf("before any measure a turn fits %d calls, want %d", fit, firstTurnCalls)
	}
	took := func(calls int, d time.Duration) {
		n.turnCalls, n.turnStart = calls, time.Now().Add(-d)
		n.turnTook()
	}
	took(measuredTurnCalls-1, time.Second)
	if n.perCall != 0 {
		t.Fatalf("a turn of %d calls measured %v a call", measuredTurnCalls-1, n.perCall)
	}
	// 1,000 calls in 10 ms: 10 µs each.
	took(1000, 10*time.Millisecond)
	for _, c := range []struct {
		until    time.Duration
		low, top int
	}{
		{15 * time.Millisecond, 1400, 1500},
		// At most a tick, 100 ms.
		{time.Second, 9500, 10000},
		// Close to the tick, or past it: at least minTurnTime, 15 ms.
		{time.Millisecond, 1400, 1500},
		{-time.Millisecond, 1400, 1500},
	} {
		if fit := n.turnFit(c.until); fit < c.low || fit > c.top {
			t.Fatalf("with %v to the tick at 10 µs a call, a turn fits %d calls, want %d to %d", c.until, fit, c.low, c.top)
		}
	}
	// A turn at 50 µs a call moves the average a quarter of the way.
	took(1000, 50*time.Millisecond)
	if n.perCall < 19*time.Microsecond || n.perCall > 21*time.Microsecond {
		t.Fatalf("the average is %v a call, want about 20 µs", n.perCall)
	}
	// However slow calls are, a turn takes some.
	took(100, time.Second)
	took(100, time.Second)
	if fit := n.turnFit(50 * time.Millisecond); fit != minTurnCalls {
		t.Fatalf("at %v a call a turn fits %d calls, want %d", n.perCall, fit, minTurnCalls)
	}
}

// TestStoppedNodeIsNotBusy checks that a node that stopped with its disk
// behind is not busy: its peers' connections deliver the messages they
// held, which it drops, and read on while its loop drains, instead of
// holding them and asking the drain for a turn again and again.
func TestStoppedNodeIsNotBusy(t *testing.T) {
	n := &Node{done: make(chan struct{}), full: true}
	if !n.Busy() {
		t.Fatal("a running node with its disk behind is not busy")
	}
	close(n.done)
	if n.Busy() {
		t.Fatal("a stopped node is busy")
	}
}
