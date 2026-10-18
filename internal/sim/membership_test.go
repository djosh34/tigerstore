package sim

import (
	"strconv"
	"testing"
)

// TestMembershipFaults uses the real membership proposals with the same
// disk, network, clock and process faults as the KV simulator, including
// its erase-one-disk restarts. The KV checkers still run; membership
// replies have a separate request-based oracle.
func TestMembershipFaults(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 16
	}
	var during [4]int
	var took effects
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cfg := membershipConfig(seed)
			a := run(t, cfg)
			for i, count := range a.Stats.MembershipChanges {
				if count != 1 {
					t.Fatalf("stage %d completed %d times", i, count)
				}
				during[i] += a.Stats.MembershipFaults[i]
			}
			took.add(map[string]bool{
				"crash": a.Stats.Crashes > 0, "link change": a.Stats.LinkChanges > 0,
				"disk fault": a.Stats.DiskFaults > 0, "cold restart": a.Stats.ColdRestarts > 0,
			})
			checkers(t, a)
			h, err := a.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			if err := agree(h); err != nil {
				t.Fatal(err)
			}
			t.Logf("%+v", a.Stats)
		})
	}
	took.check(t, "seeds")
	if n >= 16 {
		for i, count := range during {
			if count == 0 {
				t.Errorf("stage %d never completed during faults", i)
			}
		}
	}
}
