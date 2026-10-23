package check

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessedCampaignPrefixes keeps the histories independent of changes
// to the simulator. The first two seeds hide a long prefix of writes during
// faults; trying the newest ambiguous calls first times out on those gaps.
// In 77148 a lease's expiry is a candidate for every gap of an outage
// unless the search knows that an acked put used the lease after them.
// In 59113215 a lease's expiry, called at its grant, came first in every
// gap and fitted one that a delete wrote. In 888737 a put left a key one
// version short at the last write a read saw, and the search tried every
// order of the gaps up to that read before it went back (#386).
func TestWitnessedCampaignPrefixes(t *testing.T) {
	for _, seed := range []string{"5068434", "5058843", "77148", "59113215", "888737"} {
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

// TestRefutedPlantedPrefixes keeps histories with a violation that the
// revision witness must find, as a real-time contradiction rather than a
// timeout. In planted-B01a-1, a read answers revision 1 after a delete
// returned revision 5; the revision witness found that only once every
// gap was filled, after it tried every other order of the gaps (#387).
func TestRefutedPlantedPrefixes(t *testing.T) {
	for _, name := range []string{"planted-B01a-1"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var h history.History
			if err := json.Unmarshal(data, &h); err != nil {
				t.Fatal(err)
			}
			// Without the check before the search, the search runs out
			// of these steps on this cut; with it, it takes none.
			err = Witnessed(&h, FromRevisions, timeout, MaxSteps(100_000))
			if errors.Is(err, errWitnessTimeout) {
				t.Fatalf("revision witness: %v", err)
			}
			mustFail(t, "revision witness", err, "real time allows")
		})
	}
}
