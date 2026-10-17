package check

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessedCampaignPrefixes keeps the histories independent of changes
// to the simulator. The first two seeds hide a long prefix of writes during
// faults; trying the newest ambiguous calls first times out on those gaps.
// In 77148 a lease's expiry is a candidate for every gap of an outage
// unless the search knows that an acked put used the lease after them.
func TestWitnessedCampaignPrefixes(t *testing.T) {
	for _, seed := range []string{"5068434", "5058843", "77148"} {
		t.Run(seed, func(t *testing.T) {
			data, err := os.ReadFile("testdata/seed-" + seed + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var h history.History
			if err := json.Unmarshal(data, &h); err != nil {
				t.Fatal(err)
			}
			mustPass(t, "revision witness", Witnessed(&h, FromRevisions, timeout))
		})
	}
}
