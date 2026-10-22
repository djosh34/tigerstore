package check

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestDiagnosis386 retains the campaign prefix independently of simulator
// changes. The log, watches and effects are absent from this fixture.
func TestDiagnosis386(t *testing.T) {
	data, err := os.ReadFile("testdata/seed-888737.json")
	if err != nil {
		t.Fatal(err)
	}
	var h history.History
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	w := newWitness(&h, time.Now().Add(2*time.Second))
	w.maxSteps = 100000
	err = w.fromRevisions()
	t.Logf("operations=%d steps=%d backs=%d events=%d revision=%d", len(h.Ops), w.steps, w.search.backs, len(w.events), w.live.rev)
	for e, ev := range w.events {
		if w.states[e+1].rev > 8 {
			break
		}
		t.Logf("event %d revision %d: %s", e+1, w.states[e+1].rev, w.describe(ev.op))
	}
	if err != nil {
		t.Fatalf("%v; %s", err, w.stoppedAt())
	}
	if err := Witnessed(&h, FromRevisions, 2*time.Second, MaxSteps(100000)); err != nil {
		t.Fatal(err)
	}
}
