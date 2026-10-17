package sim

import (
	"strconv"
	"testing"
)

// TestCampaignSeeds runs the seeds of the three failures of campaign
// 38928018896. The first two timed out in the revision witness; the third
// repeatedly fetched a corrupt snapshot from a sender that should have
// stopped, which TestSnapshotFromCorruptLeader now looks for. The
// simulator no longer plays them out the same way.
func TestCampaignSeeds(t *testing.T) {
	for _, seed := range []uint64{5068434, 5058843, 5038431} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			a := run(t, DefaultConfig(seed))
			checkers(t, a)
			t.Logf("%+v", a.Stats)
		})
	}
}
