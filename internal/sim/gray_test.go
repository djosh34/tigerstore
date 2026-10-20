package sim

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestGrayFsyncOutlastsHealing builds the shape of #81 on purpose, so that
// it no longer depends on one seed's draws (#164): 1.5 s before healing
// the leader turns gray with slow fsyncs, and the one in flight at healing
// ends 1.5 to 3 s after it (Config.GrayLeaderSync). Its node's term and
// vote changes wait for it, so the liveness rule starts from its end
// (Sim.graySyncEnded). On every seed, every call that starts two election
// timeouts after healing and after that fsync must get a definite answer,
// at least 50 calls must, and the checkers must pass. Every seed must
// have such an fsync (#262).
func TestGrayFsyncOutlastsHealing(t *testing.T) {
	if simrun.Drive(t) {
		return
	}
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	took := effects{all: true}
	for seed := uint64(1); seed <= 4; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt, cfg.GrayLeaderSync = cfg.Duration-1500*Millisecond, true
		a := run(t, cfg)
		checkers(t, a)
		took.add(seed, map[string]bool{"gray fsync 1.5 s past healing": a.grayHealedAt >= cfg.Duration+1500*Millisecond})
		from := max(a.grayHealedAt, cfg.Duration) + 2*election
		calls := 0
		for _, r := range a.History {
			if r.Call < from {
				continue
			}
			if errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
				// With no other fault, only a start from the log after a
				// snapshot install answers that.
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
