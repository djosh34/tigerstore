package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// burstConfig is a run whose clients of etcd's Go client, 16 on each
// voting node, share one connection to it and call on the same beat
// every 100 ms, with the default faults, while a node's turn takes at
// most 8 calls: its loop takes more calls at once than fit in a turn and
// defers the rest to its next turns, as a node at 256 calls a turn does
// under load (node.Node.TurnRoom, #649). With 400 clients on each node at
// 256 calls a turn, a seed's 95,000 operations, of which hundreds were
// ambiguous at each crash, took the witness checker past its search's
// bound.
func burstConfig(seed uint64) Config {
	// The default 10 s of healing: the liveness rule starts as late as
	// healing, LiveAfter and 2 s more with a node fenced, which a 6 s
	// heal left no calls after (seed 100911).
	cfg := DefaultConfig(seed)
	cfg.Duration = 4 * Second
	cfg.V3Clients = 3 * 16
	cfg.V3Shared = true
	cfg.Burst = 100 * Millisecond
	cfg.TurnCalls = 8
	return cfg
}

// TestCutTurnsStayLinearizable runs bursts of calls that turns of the
// nodes' loops cut, under faults: the checkers judge the history with the
// calls that waited for a later turn in it, the witness checker for
// linearizability, and every seed must have a call's frame that waited
// for a later turn (Stats.DeferredFrames). Porcupine is left out: with 54
// clients at once its search ran past its bound on most seeds, as #244's
// long histories did.
func TestCutTurnsStayLinearizable(t *testing.T) {
	// 8 seeds from 6491 by default; a sim-campaign sets others.
	simrun.Seeds(t, 6491, 8, func(t *testing.T, seed uint64) struct{} {
		res := run(t, burstConfig(seed))
		checkers(t, res)
		t.Logf("seed %d: %d turns full with a connection to read again, %d times a call's frame waited for a later turn; %d calls succeeded, %d by clientv3", seed, res.Stats.CutTurns, res.Stats.DeferredFrames, res.Stats.OK, res.Stats.V3OK)
		if res.Stats.DeferredFrames == 0 {
			t.Fatalf("seed %d: no call's frame waited for a later turn", seed)
		}
		return struct{}{}
	}, nil)
}
