package check

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestDiag281SavedHistory keeps the first 28 revisions of seed 8367529:
// original requests, answers and timestamps, without irrelevant failures,
// serializable reads or watches. In particular it retains the ambiguous
// delete of k2 and the phantom repair whose compare requires mod revision 6.
func TestDiag281SavedHistory(t *testing.T) {
	data, err := os.ReadFile("testdata/diag281-prefix.json")
	if err != nil {
		t.Fatal(err)
	}
	var h history.History
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	for _, mode := range bothModes {
		start := time.Now()
		mustPass(t, mode.String(), Witnessed(&h, mode, 3*time.Second))
		t.Logf("%s: %d ops in %s", mode, len(h.Ops), time.Since(start))
	}
	// The range covers only k0, k1, k2: deleting four keys is impossible.
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil && op.Resp.Revision == 21 && op.Req.Delete != nil {
			op.Resp.Delete.Deleted = 4
		}
	}
	for _, mode := range bothModes {
		if err := Witnessed(&h, mode, 3*time.Second); err == nil {
			t.Fatalf("%s accepted impossible delete count", mode)
		} else if errors.Is(err, errWitnessTimeout) {
			t.Fatalf("%s timed out instead of rejecting: %v", mode, err)
		} else {
			t.Logf("%s rejected bad history: %v", mode, err)
		}
	}
}

// The new pruning must not mistake a future/zero compare or a failure
// branch write for an impossible old-mod success branch.
func TestDiag281CountReachability(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mod        int64
		result     history.Result
		failurePut bool
		want       bool
	}{
		{"lost positive mod", 2, history.Equal, false, false},
		{"future mod", 4, history.Equal, false, true},
		{"missing-key mod", 0, history.Equal, false, true},
		{"failure branch can restore", 2, history.Equal, true, true},
		{"non-equality", 2, history.NotEqual, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := &history.TxnRequest{
				Compare: []history.Compare{{Key: "a", Target: history.TargetMod, Result: tc.result, Number: tc.mod}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a", Value: "repair"}}},
			}
			if tc.failurePut {
				txn.Failure = txn.Success
			}
			h := history.History{Ops: []history.Op{{Req: history.Request{Txn: txn}, Err: errUnavailable}}}
			w := newWitness(&h, time.Now().Add(time.Second))
			r := &revisionOrder{w: w, lb: map[int]int64{0: 1}, used: map[int]bool{}, counts: map[int64][]count{5: {{rev: 5, lo: 0, hi: 1, n: 1}}}}
			r.indexCounts()
			s := w.live.clone()
			s.step(history.Request{Put: &history.PutRequest{Key: "a", Value: "old"}}, false)
			s.step(history.Request{Delete: &history.DeleteRequest{Key: "a"}}, false)
			if got := r.reachable(s, -1); got != tc.want {
				t.Fatalf("reachable = %v, want %v", got, tc.want)
			}
		})
	}
}
