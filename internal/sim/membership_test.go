package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestMembershipFaults uses the real membership proposals with the same
// disk, network, clock and process faults as the KV simulator, including
// its erase-one-disk restarts. The KV checkers still run; membership
// replies have a separate request-based oracle.
func TestMembershipFaults(t *testing.T) {
	n := 0
	var during [4]int
	var took effects
	if !simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) Stats {
		a := run(t, membershipConfig(seed))
		for i, count := range a.Stats.MembershipChanges {
			if count != 1 {
				t.Fatalf("stage %d completed %d times", i, count)
			}
		}
		checkers(t, a)
		h, err := a.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := agree(h); err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v", a.Stats)
		return a.Stats
	}, func(seed uint64, st Stats) bool {
		n++
		for i := range during {
			during[i] += st.MembershipFaults[i]
		}
		took.add(seed, map[string]bool{
			"crash": st.Crashes > 0, "link change": st.LinkChanges > 0,
			"disk fault": st.DiskFaults > 0, "cold restart": st.ColdRestarts > 0,
		})
		return false
	}) {
		return
	}
	took.check(t)
	// With every fault on, a stage completes during the faults in fewer
	// seeds, and 16 of them can miss one: the rule holds over the 64 seeds
	// of CI's job; race-sim runs 16.
	if n >= 48 {
		for i, count := range during {
			if count == 0 {
				t.Errorf("stage %d never completed during faults", i)
			}
		}
	}
}
