package check

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// startNearLimit is a store at revision rev, compacted there: a created at
// revision 3 and last written just below rev, on lease 5, b written once
// at 4, and lease 6 with no keys. Near the revision limit, as #502's
// synthetic images are.
func startNearLimit(rev int64) *history.Start {
	return &history.Start{Rev: rev, Leases: []int64{5, 6}, KVs: []history.KeyValue{
		{Key: "a", Value: "a2", CreateRevision: 3, ModRevision: rev - 3, Version: 1 << 40, Lease: 5},
		{Key: "b", Value: "b1", CreateRevision: 4, ModRevision: 4, Version: 1},
	}}
}

// historyNearLimit runs forkHistory's ops, but the read below the start,
// from a start rev: reads of its keys, a compare of a's mod revision, puts
// and deletes, a revoke of a's lease, and a watch of everything.
func historyNearLimit(t *testing.T, rev int64) *history.History {
	t.Helper()
	b := newBuilderFrom(t, startNearLimit(rev), "c")
	b.watch(9, "a", history.AllKeys, true)
	b.get(1, "a")
	b.get(2, "b")
	b.cas(1, "a", rev-3, "a3")
	b.put(2, "c", "c1")
	b.do(3, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
	b.del(1, "b")
	b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}})
	b.progress()
	return b.done()
}

// checkAll runs every checker on h, which must pass each.
func checkAll(t *testing.T, h *history.History) {
	t.Helper()
	mustPass(t, "Validate", h.Validate())
	mustPass(t, "Durable", Durable(h))
	mustPass(t, "Revisions", Revisions(h))
	mustPass(t, "Watches", Watches(h))
	mustPass(t, "Linearizable", Linearizable(h, time.Minute))
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
	}
}

// TestStartNearTheRevisionLimit checks histories from starts whose
// revision is near the int64 limit (#768). The checkers size nothing by
// the start's revision, and their bounds don't overflow: the last
// writes take the revision up to the limit itself, and unanswered writes
// that may not have taken effect could take it past.
func TestStartNearTheRevisionLimit(t *testing.T) {
	// Three writes after the start (the revoke deletes no key, as the cas
	// took a off its lease): the last takes the limit.
	top := int64(math.MaxInt64 - 3)
	h := historyNearLimit(t, top)
	if r := h.Ops[len(h.Ops)-1].Resp.Revision; r != math.MaxInt64 {
		t.Fatalf("the history ends at revision %d, want %d", r, int64(math.MaxInt64))
	}
	checkAll(t, h)

	// Further from the limit, a read of a value the start doesn't hold
	// is convicted.
	h = historyNearLimit(t, math.MaxInt64-1<<20)
	checkAll(t, h)
	h.Ops[0].Resp.Range.KVs[0].Value = "a1"
	mustFail(t, "Durable", Durable(h), "")
	mustFail(t, "Linearizable", Linearizable(h, time.Minute), "")
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustFail(t, "Witnessed", Witnessed(h, mode, time.Minute), "")
	}

	// Unanswered puts that did not take effect, besides one write that
	// takes the revision to the limit: the history's writes, counted, go
	// past it.
	b := newBuilderFrom(t, startNearLimit(math.MaxInt64-1), "c")
	b.put(1, "a", "a3")
	for i := range 4 {
		b.now += 10
		b.h.Ops = append(b.h.Ops, history.Op{
			Client: 3 + i, Call: b.now, Return: b.now + 5,
			Req: history.Request{Put: &history.PutRequest{Key: "c", Value: fmt.Sprint("c", i)}},
			Err: &history.Error{Code: "Unavailable", Message: "no answer", Ambiguous: true},
		})
	}
	b.get(1, "a")
	b.get(2, "c")
	h = b.done()
	if r := h.Ops[len(h.Ops)-1].Resp.Revision; r != math.MaxInt64 {
		t.Fatalf("the history ends at revision %d, want %d", r, int64(math.MaxInt64))
	}
	checkAll(t, h)
}
