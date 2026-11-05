package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// Diagnostic only: run on Actions, never generate a history on this host.
func TestDiag499Counts(t *testing.T) {
	file := os.Getenv("DIAG499_HISTORY")
	if file == "" {
		t.Skip("needs saved history")
	}
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close() //nolint:errcheck // read only
	var h history.History
	if err := json.NewDecoder(z).Decode(&h); err != nil {
		t.Fatal(err)
	}
	w := newWitness(&h, time.Now().Add(20*time.Minute))
	if err := w.fromRevisions(); err != nil {
		t.Fatal(err)
	}
	r := w.search
	var late, early, revokes, compacts int
	for _, o := range r.optional {
		if o.late {
			late++
		} else {
			early++
		}
		if h.Ops[o.op].Req.LeaseRevoke != nil {
			revokes++
		}
		if h.Ops[o.op].Req.Compact != nil {
			compacts++
		}
	}
	t.Logf("ops=%d keys=%d events=%d top=%d ambiguous=%d optional=%d late=%d early=%d expiry=%d compact=%d gaps=%d backs=%d used=%d", len(h.Ops), len(w.keys), len(w.events), r.top, len(r.ambiguous), len(r.optional), late, early, revokes, compacts, len(r.stack), r.backs, len(r.used))
	t.Logf("due calls=%d inspected=%d; insert calls=%d inspected=%d; fits calls=%d life visits=%d; may calls=%d revoke visits=%d; index nodes=%d", r.diagDueCalls, r.diagDueVisits, r.diagInsertCalls, r.diagInsertVisits, r.diagFitsCalls, r.diagLifeVisits, r.diagMayCalls, r.diagRevokeVisits, r.diagIndexVisits)
}
