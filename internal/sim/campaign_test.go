package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestCampaignSeeds runs the seeds of the three failures of campaign
// 38928018896. The first two timed out in the revision witness; the third
// repeatedly fetched a corrupt snapshot from a sender that should have
// stopped, which TestSnapshotFromCorruptLeader now looks for. The
// simulator no longer plays them out the same way.
func TestCampaignSeeds(t *testing.T) {
	simrun.Each(t, []uint64{5068434, 5058843, 5038431}, func(t *testing.T, seed uint64) struct{} {
		a := run(t, DefaultConfig(seed))
		checkers(t, a)
		t.Logf("%+v", a.Stats)
		return struct{}{}
	}, nil)
}
