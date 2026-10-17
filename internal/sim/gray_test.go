package sim

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestGrayFsyncOutlastsHealing builds the shape of #81 on purpose, so that
// it no longer depends on one seed's draws (#164): 1.5 s before healing
// the leader turns gray with slow fsyncs, and one of them, started before
// healing, ends after it. Its node's term and vote changes wait for it, so
// the liveness rule starts from its end (Sim.graySyncEnded). On every
// seed, every call that starts two election timeouts after it must get a
// definite answer, at least 50 calls must, and the checkers must pass;
// the seeds run until two had a gray fsync end 1.5 s or more after
// healing, as in #81.
func TestGrayFsyncOutlastsHealing(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	late := 0
	for seed := uint64(1); seed <= 40 && late < 2; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt, cfg.GrayLeaderSync = cfg.Duration-1500*Millisecond, true
		a := run(t, cfg)
		checkers(t, a)
		if a.grayHealedAt <= cfg.Duration {
			t.Fatalf("seed %d: no gray fsync outlasted healing at %d ms", seed, cfg.Duration/Millisecond)
		}
		if a.grayHealedAt >= cfg.Duration+1500*Millisecond {
			late++
		}
		from := a.grayHealedAt + 2*election
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
	if late < 2 {
		t.Fatalf("only %d seeds had a gray fsync end 1.5 s after healing", late)
	}
}
