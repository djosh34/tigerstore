package check

import (
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestDiagnosis387 requires a real-time contradiction, not search exhaustion,
// from the B01a seed-1 cut. MaxSteps keeps the unfixed control short.
func TestDiagnosis387(t *testing.T) {
	data, err := os.ReadFile("testdata/planted-B01a-1.json")
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
	w := newWitness(&h, time.Now().Add(10*time.Second))
	w.maxSteps = 100_000
	err = w.fromRevisions()
	backs := 0
	if w.search != nil {
		backs = w.search.backs
	}
	t.Logf("%d ops, %d steps, %d backtracks: %v", len(h.Ops), w.steps, backs, err)
	if err == nil || errors.Is(err, errWitnessTimeout) {
		t.Fatalf("want a stale-read contradiction, got %v; %s", err, w.stoppedAt())
	}
	if !strings.Contains(err.Error(), "real time allows") {
		t.Fatalf("want a real-time contradiction, got %v", err)
	}
	mustFail(t, "revision witness", Witnessed(&h, FromRevisions, timeout, MaxSteps(100_000)), "real time allows")
}

// TestDiagnosis387Good guards the exclusions and the strict before-call
// boundary of the early revision check.
func TestDiagnosis387Good(t *testing.T) {
	for _, name := range []string{"overlap", "equal endpoints", "serializable stale read", "serializable predecessor", "historical read"} {
		t.Run(name, func(t *testing.T) {
			b := newBuilder(t, "a")
			old := b.get(0, "a")
			put := b.put(1, "a", "x")
			read := b.get(2, "a")
			h := b.done()
			switch name {
			case "overlap":
				// The old read may linearize before the concurrent put,
				// even though it returns later.
				h.Ops[put].Call, h.Ops[put].Return = 10, 20
				h.Ops[old].Call, h.Ops[old].Return = 15, 30
				h.Ops[read].Call, h.Ops[read].Return = 40, 50
			case "equal endpoints":
				h.Ops[put].Call, h.Ops[put].Return = 10, 20
				h.Ops[old].Call, h.Ops[old].Return = 20, 30
				h.Ops[read].Call, h.Ops[read].Return = 40, 50
			case "serializable stale read":
				h.Ops[read].Req.Range.Serializable = true
				h.Ops[read].Resp = h.Ops[old].Resp
			case "serializable predecessor":
				// Serializable answers are outside Witnessed's order,
				// whether they would raise or lower the revision bound.
				h.Ops[put].Call, h.Ops[put].Return = 10, 50
				h.Ops[old].Call, h.Ops[old].Return = 20, 25
				h.Ops[read].Call, h.Ops[read].Return = 30, 40
				h.Ops[old].Req.Range.Serializable = true
				h.Ops[old].Resp.Revision = 2
				h.Ops[read].Resp = &history.Response{Revision: 1, Range: &history.RangeResponse{}}
			case "historical read":
				h.Ops[read].Req.Range.Revision = 1
				h.Ops[read].Resp.Range = h.Ops[old].Resp.Range
			}
			// Input order is not call order.
			slices.Reverse(h.Ops)
			for _, mode := range bothModes {
				mustPass(t, mode.String(), Witnessed(h, mode, timeout))
			}
		})
	}
}
