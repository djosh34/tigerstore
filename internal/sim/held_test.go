package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestHeldEntries runs clusters with the default faults: lost, late and
// reordered messages, partitions, crashes and restarts. The leader's
// appends name the entries its followers forwarded (core/held.go), so
// the faults hit side copies and named appends too. The checkers must
// pass on every seed, and across the set the leader must have named
// entries and a follower must have parked an append for a late side copy.
// A follower that asks for entries in full is rare since a follower Raft
// probes gets them in full anyway: 0 to 6 times in 10 seeds, so the test
// only counts it, as a set of 10 seeds in a campaign may have none.
// TestLostSideCopy, TestForwarderForgets and TestCutOffFollowerKeepsUp in
// internal/core take that path on purpose. A campaign runs it on
// $SIM_SEEDS seeds.
func TestHeldEntries(t *testing.T) {
	var named, parked, asked int
	if !simrun.Seeds(t, 6020, 4, func(t *testing.T, seed uint64) Stats {
		res := run(t, DefaultConfig(seed))
		checkers(t, res)
		return res.Stats
	}, func(_ uint64, st Stats) bool {
		named += st.HeldNamed
		parked += st.HeldParked
		asked += st.HeldAsked
		return false
	}) {
		return
	}
	t.Logf("%d entries named, %d appends parked, %d requests for entries in full", named, parked, asked)
	switch {
	case simrun.One() || t.Failed() || named > 0 && parked > 0:
	case simrun.Race:
		t.Logf("race coverage of held entries was partial this run: in %d runs under the race detector, %d entries named, %d appends parked", raceRuns, named, parked)
	default:
		t.Fatalf("%d entries named, %d appends parked; both must happen", named, parked)
	}
}
