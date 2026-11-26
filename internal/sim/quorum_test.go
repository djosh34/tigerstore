package sim

import (
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestReplacementLosesQuorum is TestCampaign seed 780 (#850): node 2
// was replaced, but node 1 had not applied the add, committed by node 3
// and itself, when node 3 was fenced too. No node can win an election by
// the voters it applied, so the run lost its quorum at that fence, and
// it must check safety and unavailability, not convergence.
func TestReplacementLosesQuorum(t *testing.T) {
	if simrun.Race {
		t.Skip("the replacement's unapplied add needs the seed's deterministic timing")
	}
	simrun.Each(t, []uint64{780}, func(t *testing.T, seed uint64) Stats {
		res := run(t, Scenario(seed))
		if st := res.Stats; st.Replaced != 1 || st.QuorumLostAt == 0 {
			t.Fatalf("seed %d: %d replaced, quorum lost at %s", seed, st.Replaced, seconds(st.QuorumLostAt))
		}
		if three := res.nodes[2]; !three.fenced || res.Stats.QuorumLostAt != three.since {
			t.Fatalf("quorum lost at %s, node 3 fenced %v at %s", seconds(res.Stats.QuorumLostAt), three.fenced, seconds(three.since))
		}
		// The cause, not only the fence: node 1 applied all it knows
		// committed, through the removal of node 2 at 55 and not the add
		// at 56, so its voters are itself and the fenced node 3; the
		// replacement applied nothing.
		for _, want := range []string{"node 1: applied 55, commit 55, voters [1 3]", "node 281751481: applied 0, commit 0, voters []"} {
			if !slices.ContainsFunc(res.Trace, func(l string) bool { return strings.HasSuffix(l, want) }) {
				t.Fatalf("the trace has no %q", want)
			}
		}
		checkers(t, res)
		return res.Stats
	}, nil)
}
