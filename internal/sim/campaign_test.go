package sim

import (
	"strconv"
	"testing"
)

// TestCampaignSeeds covers the three failures of campaign 38928018896.
// The first two timed out in the revision witness; the third repeatedly
// fetched a corrupt snapshot from a sender that should have stopped.
func TestCampaignSeeds(t *testing.T) {
	for _, seed := range []uint64{5068434, 5058843, 5038431} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			a := run(t, DefaultConfig(seed))
			b := run(t, DefaultConfig(seed))
			if a.Trace != b.Trace {
				t.Fatalf("traces differ: %016x, %016x", a.Trace, b.Trace)
			}
			if seed == 5038431 && (a.Stats.ServeFailures == 0 || a.Stats.Fenced == 0) {
				t.Fatalf("no failed serve and fenced sender: %+v", a.Stats)
			}
			checkers(t, a)
			t.Logf("trace %016x: %+v", a.Trace, a.Stats)
		})
	}
}
