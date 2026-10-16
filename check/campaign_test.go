package check

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessedCampaignPrefixes keeps the histories independent of changes
// to the simulator. Both seeds hide a long prefix of writes during faults.
// Trying the newest ambiguous calls first times out on those gaps.
func TestWitnessedCampaignPrefixes(t *testing.T) {
	for _, seed := range []string{"5068434", "5058843"} {
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
