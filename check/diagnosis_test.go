package check

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// Opt-in step meter for the original, uncompressed control artifact. The
// normal regression in TestWitnessHardHistories exercises the public API.
func TestDiagnosis1529(t *testing.T) {
	path := os.Getenv("TIGERSTORE_DIAG_HISTORY")
	if path == "" {
		t.Skip("set TIGERSTORE_DIAG_HISTORY to the control seed 1529 JSON")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	var h history.History
	if err := json.NewDecoder(f).Decode(&h); err != nil {
		t.Fatal(err)
	}
	h.Log = nil
	for i := range h.Ops {
		h.Ops[i].Effect = nil
	}
	start := time.Now()
	w := newWitness(&h, start.Add(10*time.Minute))
	w.maxSteps = 50_000
	err = w.fromRevisions()
	t.Logf("ops=%d steps=%d time=%s backs=%d events=%d revision=%d err=%v", len(h.Ops), w.steps, time.Since(start), w.search.backs, len(w.events), w.live.rev, err)
	if err != nil {
		t.Fatal(err)
	}
}
