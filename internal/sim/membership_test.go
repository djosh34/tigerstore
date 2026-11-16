package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestMembershipFaults runs wl nodes (wlMembershipConfig) through the
// real membership proposals with the same disk, network, clock and
// process faults as the KV simulator, including its erase-one-disk
// restarts. The KV checkers still run; membership
// replies have a separate request-based oracle.
func TestMembershipFaults(t *testing.T) {
	n := 0
	var during [4]int
	var took effects
	if !simrun.Seeds(t, 1, 16, runMembership, func(seed uint64, st Stats) bool {
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
	// With every fault on, a stage completes during the faults in few
	// seeds, stage 3 in a few percent, and 64 fresh seeds of CI's job can
	// miss one: the rule holds over the 250 seeds of a campaign process
	// (#380).
	if n >= 200 {
		for i, count := range during {
			if count == 0 {
				t.Errorf("stage %d never completed during faults", i)
			}
		}
	}
}

// runMembership runs seed of TestMembershipFaults and checks it.
func runMembership(t *testing.T, seed uint64) Stats {
	a := run(t, wlMembershipConfig(seed))
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
}

// TestMembershipFaultSeeds runs seeds of TestMembershipFaults that
// failed. On seed 881969382 a node's directory sync failed as it wrote a
// checkpoint, and its crash after kept the pointer to the checkpoint but
// lost the rename that gave the checkpoint its name: the node could not
// start again (#711). On seed 914212814 a leader sent a follower the same
// snapshot again, and the follower removed the file it had just received
// as the one before: its install found no file and stopped it (#713).
// On seed 958846293 a cold restart with node 1 erased had no leader: the
// fourth member held the newest entry, and node 3, whose log predated
// that member, refused its connection and could not answer its votes
// (#717).
func TestMembershipFaultSeeds(t *testing.T) {
	simrun.Each(t, []uint64{881969382, 914212814, 958846293}, runMembership, nil)
}
