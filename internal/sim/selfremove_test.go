package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// The membership work adds and promotes a second voter, then removes it
// through whichever node leads. On main, seeds 9, 182, 228, 260, 279 and
// 295 of the 300-seed sweep stopped at three of four membership changes:
// a removing leader exited before its survivor learned the commit (#922).
// SIM_SEED=1 SIM_SEEDS=300 runs the qualification sweep under gosim.
func TestSelfRemovalOneVoterMembership(t *testing.T) {
	simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) Stats {
		cfg := wlMembershipConfig(seed)
		cfg.Nodes = 1
		res := run(t, cfg)
		for stage, count := range res.Stats.MembershipChanges {
			if count != 1 {
				t.Fatalf("seed %d: stage %d completed %d times", seed, stage, count)
			}
		}
		checkers(t, res)
		return res.Stats
	}, nil)
}
