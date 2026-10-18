package sim

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestGrayFsyncOutlastsHealing builds the shape of #81 on purpose, so that
// it no longer depends on one seed's draws (#164): 1.5 s before healing
// the leader turns gray with slow fsyncs, and the one in flight at healing
// ends 1.5 to 3 s after it (Config.GrayLeaderSync). Its node's term and
// vote changes wait for it, so the liveness rule starts from its end
// (Sim.graySyncEnded). On every seed, every call that starts two election
// timeouts after healing and after that fsync must get a definite answer,
// at least 50 calls must, and the checkers must pass. Nearly every seed
// must have such an fsync; one whose gray leader had nothing to sync at
// healing has none (#262).
func TestGrayFsyncOutlastsHealing(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	var took effects
	for seed := uint64(1); seed <= 4; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt, cfg.GrayLeaderSync = cfg.Duration-1500*Millisecond, true
		a := run(t, cfg)
		checkers(t, a)
		took.add(map[string]bool{"gray fsync 1.5 s past healing": a.grayHealedAt >= cfg.Duration+1500*Millisecond})
		from := max(a.grayHealedAt, cfg.Duration) + 2*election
		for _, stop := range a.CommitStops {
			if stop >= from {
				// It would also answer calls with "server stopped".
				t.Fatalf("seed %d: the commit watchdog stopped a node at %d ms", seed, stop/Millisecond)
			}
		}
		calls := 0
		for _, r := range a.History {
			if r.Call < from {
				continue
			}
			if errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
				// With no other fault and no watchdog stop (above), only a
				// start from the log after a snapshot install answers
				// that.
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed after the gray fsync ended at %d ms: %v",
					seed, r.Node+1, r.Call/Millisecond, a.grayHealedAt/Millisecond, r.Err)
			}
			calls++
		}
		if calls < 50 {
			t.Fatalf("seed %d: only %d calls after %d ms", seed, calls, from/Millisecond)
		}
		t.Logf("seed %d: last gray fsync at %d ms; %d definite answers from %d ms", seed, a.grayHealedAt/Millisecond, calls, from/Millisecond)
	}
	took.check(t)
}
