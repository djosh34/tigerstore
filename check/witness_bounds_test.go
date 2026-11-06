package check

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestEventTimesMatchScan pushes random events, acked or not, and takes
// some back, and compares after and before at random times with a scan of
// the events, from trees made empty and made with room for some events
// (newEventTimes), so that pushes fill them and go past them.
func TestEventTimesMatchScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 515))
	h := &history.History{}
	for range 200 {
		op := history.Op{Call: rng.Int64N(1000), Req: history.Request{Put: &history.PutRequest{Key: "a"}}}
		op.Return = op.Call + rng.Int64N(100)
		if rng.IntN(3) > 0 {
			op.Resp = &history.Response{}
		}
		h.Ops = append(h.Ops, op)
	}
	for _, size := range []int{0, 1, 2, 3, 50, 4096} {
		times := newEventTimes(size)
		var events []int
		for step := range 2000 {
			switch {
			case rng.IntN(5) == 0 && len(events) > 0:
				n := rng.IntN(len(events))
				events = events[:n]
				times.truncate(n)
			case rng.IntN(10) == 0:
				events = append(events, -1)
				times.push(h, -1)
			default:
				op := rng.IntN(len(h.Ops))
				events = append(events, op)
				times.push(h, op)
			}
			at := rng.Int64N(1100)
			after, before := 0, len(events)
			for k, op := range events {
				if op < 0 {
					continue
				}
				if o := &h.Ops[op]; o.Resp != nil && o.Return < at {
					after = k + 1
				}
			}
			for k, op := range events {
				if op >= 0 && h.Ops[op].Call > at {
					before = k
					break
				}
			}
			if got := times.after(at); got != after {
				t.Fatalf("room %d, step %d: after(%d) = %d, the scan says %d", size, step, at, got, after)
			}
			if got := times.before(at); got != before {
				t.Fatalf("room %d, step %d: before(%d) = %d, the scan says %d", size, step, at, got, before)
			}
		}
	}
}

// TestSizeReservesOnlyWritesThatMayHappen checks what the witness reserves
// for each key's versions: nothing for a rejected request, which writes
// nothing, and of an acked transaction only the branch that ran. A
// rejected transaction of many deletes, sent again and again, once made it
// reserve their product (review of #528).
func TestSizeReservesOnlyWritesThatMayHappen(t *testing.T) {
	var dels []history.TxnOp
	for range 1000 {
		dels = append(dels, history.TxnOp{Delete: &history.DeleteRequest{Key: "absent"}})
	}
	rejected := history.Request{Txn: &history.TxnRequest{Success: dels}}
	cas := history.Request{Txn: &history.TxnRequest{
		Success: []history.TxnOp{{Put: &history.PutRequest{Key: "won", Value: "v"}}},
		Failure: []history.TxnOp{{Put: &history.PutRequest{Key: "lost", Value: "v"}}},
	}}
	h := &history.History{}
	for i := range 100 {
		h.Ops = append(h.Ops, history.Op{Client: i, Call: int64(2 * i), Return: int64(2*i + 1), Req: rejected, Err: &history.Error{Code: "InvalidArgument", Message: "etcdserver: too many operations in txn request"}})
	}
	h.Ops = append(h.Ops, history.Op{Client: 100, Call: 300, Return: 301, Req: cas, Resp: &history.Response{Revision: 2, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Put: &history.PutResponse{}}}}}})
	w := newWitness(h, time.Now().Add(time.Minute))
	r := &revisionOrder{w: w, limit: 2}
	r.size()
	if v := w.past["absent"]; v != nil {
		t.Fatalf("a rejected request reserved %d versions of a key", cap(v.kvs))
	}
	if v := w.past["lost"]; v != nil {
		t.Fatalf("the branch an acked transaction did not run reserved %d versions", cap(v.kvs))
	}
	if v := w.past["won"]; v == nil || cap(v.kvs) != 1 {
		t.Fatalf("the branch an acked transaction ran reserved %v", v)
	}
}
