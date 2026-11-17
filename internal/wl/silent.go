package wl

import (
	"time"

	"github.com/djosh34/tigerstore/internal/planted"
)

// A leader whose own disk takes too long with its log goes silent: it
// sends its peers nothing until the disk catches up. As in etcd, whose
// leader sends nothing while its disk write blocks, the followers stop
// hearing from it and elect another leader, and it steps down when it
// hears the new term or when CheckQuorum finds no follower answering.
// wl's loop never waits for its disk, so without this a leader on a stuck
// disk would stay leader, commit with its followers, and apply and answer
// nothing (TestWLStalledLeaderIsReplaced). main's rule (core.StallTicks).

const (
	// electionTicks is Raft's election timeout, in ticks.
	electionTicks = 10
	// stallElections is how many election timeouts the oldest append may
	// wait for the disk before the leader goes silent.
	stallElections = 2
	// stallBytesPerTick is the slowest a disk that is not stuck writes:
	// the oldest append also gets a tick for each stallBytesPerTick of
	// the log not yet durable, so a large append is not a stuck disk.
	stallBytesPerTick = 1 << 20
)

// checkStall makes the node silent while it leads on a stalled disk, and
// lets it talk again once the disk caught up.
func (n *node) checkStall() {
	silent := n.stalled()
	if silent != n.silent {
		if silent {
			n.logf("the log's oldest append waited for the disk since %v: silent, so the followers elect another leader", n.stallSince().Format(time.RFC3339Nano))
		} else {
			n.logf("the log caught up: no longer silent")
		}
	}
	n.silent = silent
}

// stallSince is when the oldest append's wait counts from: its own
// start, or the disk's last progress if that came after.
func (n *node) stallSince() time.Time {
	w := n.wal
	if len(w.pending) == 0 {
		return n.now
	}
	return later(w.pending[0].at, w.progressAt)
}

// stalled reports whether the node leads while its oldest append waited
// for the disk past the limit.
func (n *node) stalled() bool {
	w := n.wal
	if planted.On("B17") || !n.leading() || len(w.pending) == 0 {
		return false
	}
	limit := stallElections*electionTicks*tickEvery + time.Duration((w.off-w.done.Load())/stallBytesPerTick)*tickEvery
	return n.now.Sub(n.stallSince()) >= limit
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
