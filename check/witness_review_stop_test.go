package check

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func assertTimeoutOnly(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("want timeout, got %v", err)
	}
	for _, invented := range []string{"cannot write it", "the furthest it got", "gets no state", "gets error"} {
		if strings.Contains(err.Error(), invented) {
			t.Fatalf("timeout invented a history failure: %v", err)
		}
	}
}

func TestStoppedFitPublicMessage(t *testing.T) {
	b := newBuilder(t, "k")
	put := b.put(0, "k", "v")
	for range 99 {
		b.get(0, "k")
	}
	h := b.done()
	h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
	err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(10))
	assertTimeoutOnly(t, err)
	if !strings.Contains(err.Error(), "the search was at revision 2") {
		t.Fatalf("did not stop in the fit: %v", err)
	}
}

// TestAckedTransactionReplayStop has two billion possible key visits in a
// single acked transaction. Its preflight is small relative to replay. The
// hook waits until it is polled inside a range, proving it is not just the
// index or transaction preflight that is stoppable. The stack is sampled
// only at tick's ordinary 1,024-step polls, not at each replay work item.
func TestAckedTransactionReplayStop(t *testing.T) {
	const keys, ranges = 100_000, 20_000
	h := &history.History{Start: &history.Start{Rev: 2}}
	for i := range keys {
		h.Start.KVs = append(h.Start.KVs, history.KeyValue{Key: fmt.Sprintf("%06d", i), CreateRevision: 2, ModRevision: 2, Version: 1})
	}
	txn := &history.TxnRequest{}
	got := &history.TxnResponse{Succeeded: true}
	for range ranges {
		txn.Success = append(txn.Success, history.TxnOp{Range: &history.RangeRequest{Key: "000000", End: history.AllKeys, CountOnly: true}})
		got.Results = append(got.Results, history.TxnOpResponse{Range: &history.RangeResponse{Count: keys}})
	}
	txn.Success = append(txn.Success, history.TxnOp{Put: &history.PutRequest{Key: "000000", Value: "after"}})
	got.Results = append(got.Results, history.TxnOpResponse{Put: &history.PutResponse{}})
	h.Ops = []history.Op{{Call: 1, Return: 2, Req: history.Request{Txn: txn}, Resp: &history.Response{Revision: 3, Txn: got}}}
	replayPolls := 0
	err := Witnessed(h, FromRevisions, time.Minute, StopWhen(func() bool {
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".(*state).read") {
				replayPolls++
				return true
			}
			if !more {
				return false
			}
		}
	}))
	assertTimeoutOnly(t, err)
	if replayPolls != 1 || !strings.Contains(err.Error(), "the order held 0 events") || !strings.Contains(err.Error(), "the search was at revision 3") {
		t.Fatalf("stop did not propagate before applying or matching the acked transaction: polls=%d, %v", replayPolls, err)
	}
}

func TestDueEmptySteadyState(t *testing.T) {
	const candidates, queries = 65_536, 4096
	r := dueShape(candidates)
	for i := 1; i < len(r.optional); i++ {
		r.optional[i].gone, r.optional[i].by = 100, 100
	}
	for range r.due(2, 1) {
	}
	r.use(0)
	_ = referenceDue(r, 2, 1)
	for _, old := range []bool{false, true} {
		r.w.steps, r.workLeft = 0, 0
		for range queries {
			if old {
				if len(referenceDue(r, 2, 1)) != 0 {
					t.Fatal("reference query was not empty")
				}
			} else {
				for range r.due(2, 1) {
					t.Fatal("retained query was not empty")
				}
			}
		}
		if r.w.steps > queries/64+1 {
			t.Fatalf("reference=%v: empty queries rescanned candidates: %d steps, want at most %d", old, r.w.steps, queries/64+1)
		}
		t.Logf("reference=%v: %d unchanged empty queries, %d steps", old, queries, r.w.steps)
	}
}

func TestAnsweredManyGrants(t *testing.T) {
	h := sameRevisionGrants(81_000)
	if err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(250_000)); err != nil {
		t.Fatal(err)
	}
	// The running maximum includes ambiguous calls too. A later acked
	// grant whose return is before any previous call cannot follow it.
	w := newWitness(sameRevisionGrants(3), time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.calls[0], w.returns[0] = 10, 11
	if r.answered([]int{0, 1, 2}) {
		t.Fatal("running call maximum lost a real-time violation")
	}
	if !r.answered([]int{1, 2, 0}) {
		t.Fatal("running call maximum rejected the legal order")
	}
}

func TestBulkChargeMatchesSteps(t *testing.T) {
	for left := range 64 {
		for _, n := range []int64{0, 1, 63, 64, 65, 4096} {
			bulk, items := dueShape(0), dueShape(0)
			bulk.workLeft, items.workLeft = uint8(left), uint8(left)
			bulk.charge(n)
			for range n {
				items.step()
			}
			if bulk.w.steps != items.w.steps || bulk.workLeft != items.workLeft {
				t.Fatalf("left=%d, work=%d: bulk %d/%d, items %d/%d", left, n, bulk.w.steps, bulk.workLeft, items.w.steps, items.workLeft)
			}
		}
	}
	r := dueShape(0)
	r.w.maxSteps = 10
	if !r.charge(1_000_000) || r.w.steps != 11 {
		t.Fatal("bulk charge did not stop at its step budget")
	}
}
