package sim

import (
	"context"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/wl"
)

// Only a node becoming the leader resets its leases. A follower learning
// a leader later, or another turn of the same leader, gives no new TTL.
func TestLeaseLeaderResetsComeFromOwnTurns(t *testing.T) {
	s := &Sim{}
	n := &node{id: 1}
	p := &proc{n: n, ctx: context.Background(), wl: &wlProc{}}
	n.proc = p
	h := &wlHooks{s: s, p: p}
	for _, c := range []struct {
		at, reset  int64
		lead, term uint64
		count      int
	}{
		{10, 0, 2, 1, 0},
		{100, 100, 1, 2, 1},
		{200, 100, 1, 2, 1},
		{300, 100, 2, 3, 1},
		{400, 400, 1, 4, 2},
		{500, 500, 1, 5, 3},
	} {
		s.t0 = time.Now().Add(-time.Duration(c.at * Second))
		h.Turn(wl.TurnState{Lead: c.lead, Term: c.term}, nil)
		if got := len(s.leaseLeaders); got != c.count {
			t.Fatalf("turn at %d s, leader %d term %d: %d resets, want %d", c.at, c.lead, c.term, got, c.count)
		}
		if c.count > 0 {
			if at := s.leaseLeaders[c.count-1]; at < c.reset*Second || at > (c.reset+1)*Second {
				t.Fatalf("turn at %d s: last reset %d ns, want near %d s", c.at, at, c.reset)
			}
		}
	}
	// The same member leading in a fresh process resets its TTL too.
	n.proc = &proc{n: n, ctx: context.Background(), wl: &wlProc{}}
	s.t0 = time.Now().Add(-600 * time.Second)
	(&wlHooks{s: s, p: n.proc}).Turn(wl.TurnState{Lead: 1, Term: 5}, nil)
	if got := len(s.leaseLeaders); got != 4 {
		t.Fatalf("new process: %d resets, want 4", got)
	}
	// A stale hook from the process it replaced is not new evidence.
	h.Turn(wl.TurnState{Lead: 1, Term: 6}, nil)
	if got := len(s.leaseLeaders); got != 4 {
		t.Fatalf("stale process: %d resets, want 4", got)
	}
}
