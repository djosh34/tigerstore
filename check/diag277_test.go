package check

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestDiag277SavedHistory keeps only the writes through revision 9 and
// the reads/empty deletes around the ambiguous compaction of seed 8143538.
// All requests, answers and times are copied unchanged from run 39170367649,
// artifact sim-campaign-8, sim-out/TestSeeds_8143538/history.json.
// Original op indices: 54,55,56,63,66,67,70,71,72,75,76,81,82,84.
// Original log entries: [1:13]. Effects and watches are omitted.
func TestDiag277SavedHistory(t *testing.T) {
	data, err := os.ReadFile("testdata/diag277-seed-8143538.json")
	if err != nil {
		t.Fatal(err)
	}
	var h history.History
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	t.Run("porcupine", func(t *testing.T) {
		mustPass(t, "Linearizable", Linearizable(&h, timeout))
	})
	for _, mode := range bothModes {
		t.Run(mode.String(), func(t *testing.T) {
			mustPass(t, "Witnessed "+mode.String(), Witnessed(&h, mode, timeout))
		})
	}
}
