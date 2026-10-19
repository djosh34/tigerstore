package check

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestIssue284Reduced is the five-call shape of seed 30120: a compaction
// starts before a grant, but applies after a historical read made after
// the grant returned. Both mutations have the same header revision.
func TestIssue284Reduced(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "old") // revision 2
	b.put(0, "a", "new") // revision 3
	grant := b.do(1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 3}})
	read := b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
	compact := b.do(3, history.Request{Compact: &history.CompactRequest{Revision: 3, Physical: true}})
	h := b.done()
	h.Ops[grant].Call, h.Ops[grant].Return = 60, 70
	h.Ops[read].Call, h.Ops[read].Return = 80, 90
	h.Ops[compact].Call, h.Ops[compact].Return = 50, 100

	mustPass(t, "Porcupine", Linearizable(h, timeout))
	for _, mode := range bothModes {
		t.Run(mode.String(), func(t *testing.T) {
			mustPass(t, "Witnessed", Witnessed(h, mode, timeout))
		})
	}

	// The same successful historical read really would be stale if the
	// compaction completed before it started. Do not weaken the oracle.
	h.Ops[compact].Return = 75
	mustFail(t, "Porcupine", Linearizable(h, timeout), "not linearizable")
	for _, mode := range bothModes {
		if err := Witnessed(h, mode, timeout); err == nil {
			t.Fatalf("%s accepted a read after completed compaction", mode)
		}
	}
}

// TestIssue284ArtifactHistory rechecks the exact failed campaign history,
// not a new simulator execution whose goroutine scheduling may differ.
func TestIssue284ArtifactHistory(t *testing.T) {
	path := os.Getenv("ISSUE284_HISTORY")
	if path == "" {
		t.Skip("set ISSUE284_HISTORY to sim-campaign-6/TestSeeds_30120/history.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var h history.History
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	for _, mode := range bothModes {
		t.Run(mode.String(), func(t *testing.T) {
			mustPass(t, "Witnessed", Witnessed(&h, mode, timeout))
		})
	}
}
