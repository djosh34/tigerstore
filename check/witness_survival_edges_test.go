package check

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func survivalIndex(h *history.History) *revisionOrder {
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	r.top = 10000
	r.limit = 10000
	r.indexCounts()
	r.indexSurvival()
	return r
}
func survivalRead(key, end string, at, header, n int64) history.Op {
	return history.Op{Client: 0, Call: 1, Return: 2, Req: history.Request{Range: &history.RangeRequest{Key: key, End: end, Revision: at, CountOnly: true}}, Resp: &history.Response{Revision: header, Range: &history.RangeResponse{Count: n}}}
}
func TestSurvivalRawDeadlinesAndBoundaries(t *testing.T) {
	for _, n := range []int{1, 128, 129} {
		h := survivalAstra(false, n, false)
		r := survivalIndex(h)
		want := survivalReference(h)
		if !maps.Equal(r.survives, want) {
			t.Fatalf("%d: got %v want %v", n, r.survives, want)
		}
		if (len(r.survives) > 0) != (n <= 128) {
			t.Fatal("width boundary not exercised")
		}
	}
	h := survivalAstra(false, 1, false)
	h.Ops = append(h.Ops, survivalRead("b", "c", 5, 68, 1))
	r := survivalIndex(h)
	if r.survives["b000"] != 68 {
		t.Fatal("missing maximum of delete prestate and historical read")
	}
	h.Ops = h.Ops[len(h.Ops)-1:]
	r = survivalIndex(h)
	if r.survives["b000"] != 5 {
		t.Fatal("used header rather than historical revision")
	}
	h.Ops[0].Req.Range.Serializable = true
	r = survivalIndex(h)
	if len(r.survives) != 0 {
		t.Fatal("serializable count became evidence")
	}
}
func TestSurvivalLogIndependence(t *testing.T) {
	h := survivalAstra(true, 1, false)
	base := survivalIndex(h)
	for i := range 200 {
		h.Log = append(h.Log, history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("b-log-%03d", i)}})
	}
	with := survivalIndex(h)
	if !maps.Equal(base.survives, with.survives) || with.survives["b000"] != 68 {
		t.Fatal("log-only keys changed range width/deadline")
	}
	// Log contents can of course make FromLog fail; revision evidence must not.
	a, b := runSurvival(h, true, true, 100000), runSurvival(h, false, true, 100000)
	if _, err := survivalCheck(a, b); err != nil {
		t.Fatal(err)
	}
	if b.err != "pass" || b.hits == 0 {
		t.Fatal("log-free rule lost its valid sibling")
	}
}
func TestSurvivalTxnFailurePutsAndPartial(t *testing.T) {
	h := survivalAstra(false, 2, false)
	h.Ops[len(h.Ops)-1].Resp.Delete.Deleted = 1
	if r := survivalIndex(h); len(r.survives) != 0 {
		t.Fatal("partial count identified individual survivors")
	}
	h = survivalAstra(false, 1, false)
	h.Ops = append(h.Ops, history.Op{Client: 10, Call: 1, Return: 2, Req: history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: "z", Target: history.TargetMod, Result: history.Equal, Number: 0}}, Success: []history.TxnOp{{Range: &history.RangeRequest{Key: "z"}}}, Failure: []history.TxnOp{{Put: &history.PutRequest{Key: "b000"}}}}}, Err: errUnavailable})
	r := survivalIndex(h)
	if len(r.survives) != 0 || len(r.putters[r.w.live.index("b000")]) == 0 {
		t.Fatal("ignored txn failure-branch creator")
	}
	if !maps.Equal(r.survives, survivalReference(h)) {
		t.Fatal("raw request rebuild differs")
	}
}
func TestSurvivalCutoffAndDuplicate(t *testing.T) {
	h := &history.History{Start: &history.Start{Rev: 2}}
	// Deliberately reverse observation revisions: cutoff must use countRevs,
	// not input/map order. All headers are later than requested revisions.
	for i := 512; i >= 0; i-- {
		key := fmt.Sprintf("x%03d", i)
		h.Start.KVs = append(h.Start.KVs, history.KeyValue{Key: key, CreateRevision: 2, ModRevision: 2, Version: 1})
		h.Ops = append(h.Ops, survivalRead(key, "", int64(3+i), 1000, 1))
	}
	h.Ops = append(h.Ops, survivalRead("x000", "", 999, 1000, 1))
	r := survivalIndex(h)
	if !maps.Equal(r.survives, survivalReference(h)) {
		t.Fatal("raw cutoff rebuild differs")
	}
	if len(r.survives) != 512 || r.survives["x000"] != 999 || r.survives["x512"] != 0 {
		t.Fatal("cutoff/late duplicate changed")
	}
	if !slices.IsSorted(r.countRevs) {
		t.Fatal("countRevs no longer sorted")
	}
}
func TestSurvivalOnlyPlainUnresolvedDeletes(t *testing.T) {
	h := survivalAstra(false, 1, true)
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	r.stack = []*frame{{rev: 3}}
	fits := 0
	r.testTrace = func(phase string, _ int) {
		if phase == "fit" {
			fits++
		}
	}
	if r.fitsGranted(0, 3) || fits != 1 {
		t.Fatal("survival delete must fail once without a grant retry")
	}
	r.resolved[3] = 0
	if !r.fitsGranted(0, 3) {
		t.Fatal("resolved writer was pruned")
	}
	delete(r.resolved, 3)
	// Revokes remain on the old path even though they delete protected keys.
	i := len(w.reqs)
	w.reqs = append(w.reqs, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}})
	if r.survivalBlocked(i, 3, "b000") {
		t.Fatal("rule applied to revoke")
	}
	w.reqs[i] = history.Request{Compact: &history.CompactRequest{Revision: 2}}
	if r.survivalBlocked(i, 3, "b000") {
		t.Fatal("rule applied to compaction")
	}
	// Inclusive deadline, exclusive following revision. Use the production
	// predicate, not an oracle-mutated copy of its output.
	r.writers = nil // These boundary probes are gap trials, not acked writers.
	r.stack[0].rev = 68
	if !r.survivalBlocked(0, 68, "b000") {
		t.Fatal("deadline not inclusive")
	}
	r.stack[0].rev = 69
	if r.survivalBlocked(0, 69, "b000") {
		t.Fatal("deadline extended to at+1")
	}
}
func TestSurvivalNestedExpiry(t *testing.T) {
	h := review898NestedLeaseHistory()
	h.Start.KVs = []history.KeyValue{{Key: "protected", CreateRevision: 2, ModRevision: 2, Version: 1}}
	read := survivalRead("protected", "", 3, 4, 1)
	read.Client = 100
	read.Call = 11
	read.Return = 12
	h.Ops = append(h.Ops, read)
	a, b := runSurvival(h, true, true, 200000), runSurvival(h, false, true, 200000)
	if _, err := survivalCheck(a, b); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range b.nodes {
		if n.state.phase == "fit" && n.state.op == 2 && n.accepted {
			found = true
		}
	}
	if !found || b.deadlines["protected"] != 3 {
		t.Fatal("nested expiry / active survival index not exercised")
	}
}
func TestSurvivalNoImageWorkAndConstructionStop(t *testing.T) {
	for _, st := range []*history.Start{nil, {}, {Rev: 2, Leases: []int64{9}}} {
		h := &history.History{Start: st}
		r := survivalIndex(h)
		if r.survivalItems != 0 || r.survives != nil {
			t.Fatal("no image keys must do no new work or allocation")
		}
	}
	h := survivalAstra(false, 1, false)
	r := survivalIndex(h)
	r.survives = nil
	r.indexStopped = false
	r.indexLeft = 0
	r.w.maxSteps = r.w.steps
	r.indexSurvival()
	if !r.indexStopped || !r.w.stopped || r.survives != nil {
		t.Fatal("construction ignored cancellation")
	}
}
