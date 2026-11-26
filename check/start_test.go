package check

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// startOf is an aged store at revision 10, compacted there: a on lease 5,
// written twice, b once, and lease 6 with no keys.
func startOf() *history.Start {
	return &history.Start{Rev: 10, Leases: []int64{5, 6}, KVs: []history.KeyValue{
		{Key: "a", Value: "a2", CreateRevision: 3, ModRevision: 7, Version: 2, Lease: 5},
		{Key: "b", Value: "b1", CreateRevision: 4, ModRevision: 4, Version: 1},
	}}
}

// newBuilderFrom is newBuilder whose history starts from st.
func newBuilderFrom(t *testing.T, st *history.Start, keys ...string) *builder {
	t.Helper()
	for _, kv := range st.KVs {
		keys = append(keys, kv.Key)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	b := &builder{t: t, s: newState(keys, st)}
	b.h.Start = st
	return b
}

// forkHistory runs ops on the start's keys and leases as a correct server
// would: reads of them, a compare of a's mod revision, puts and deletes,
// a revoke of the lease a was on, a read below the start's revision,
// which is compacted, and a watch of everything from the start on.
func forkHistory(t *testing.T) *history.History {
	t.Helper()
	b := newBuilderFrom(t, startOf(), "c")
	b.watch(9, "a", history.AllKeys, true)
	b.get(1, "a")
	b.get(2, "b")
	b.cas(1, "a", 7, "a3")
	b.put(2, "c", "c1")
	b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 8}})
	b.do(3, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
	b.del(1, "b")
	b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}})
	b.progress()
	return b.done()
}

// TestStartChecks checks a history that starts from an aged store with
// every checker: each must start from the store, not from an empty one.
func TestStartChecks(t *testing.T) {
	h := forkHistory(t)
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	if op := h.Ops[4]; op.Err == nil || !sameError(op.Err, errCompacted) {
		t.Fatalf("a read below the start's revision got %+v, want compacted", op)
	}
	mustPass(t, "Durable", Durable(h))
	mustPass(t, "Revisions", Revisions(h))
	mustPass(t, "Watches", Watches(h))
	mustPass(t, "Linearizable", Linearizable(h, time.Minute))
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
	}
}

// TestStartConvicts breaks a history that starts from an aged store where
// only the start tells it is wrong: each checker that reads the store
// must convict it.
func TestStartConvicts(t *testing.T) {
	// A read of a sees a value the start does not hold at its revision.
	h := forkHistory(t)
	h.Ops[0].Resp.Range.KVs[0].Value = "a1"
	mustFail(t, "Durable", Durable(h), "")
	mustFail(t, "Linearizable", Linearizable(h, time.Minute), "")
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustFail(t, "Witnessed", Witnessed(h, mode, time.Minute), "")
	}

	// A read below the start's revision succeeds.
	h = forkHistory(t)
	h.Ops[4].Err = nil
	h.Ops[4].Resp = &history.Response{Revision: 11, Range: &history.RangeResponse{}}
	mustFail(t, "Revisions", Revisions(h), "start's compaction")

	// The watch misses the delete of b.
	h = forkHistory(t)
	w := &h.Watches[0]
	for i := range w.Responses {
		w.Responses[i].Events = slices.DeleteFunc(w.Responses[i].Events, func(e history.Event) bool {
			return e.Type == history.EventDelete && e.KV.Key == "b"
		})
	}
	mustFail(t, "Watches", Watches(h), "misses")

	// A start whose key is on a lease it does not hold.
	h = forkHistory(t)
	h.Start.Leases = []int64{6}
	mustFail(t, "Validate", h.Validate(), "lease 5")
}

// TestStartFromRandomPrefix runs random requests on four keys and three
// leases, takes the store after them, compacted at its revision, as a
// start, and runs more random requests from it: every checker must pass
// the second part on its own, from the start.
func TestStartFromRandomPrefix(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	for seed := range uint64(40) {
		rng := rand.New(rand.NewPCG(seed, 1))
		random := func(b *builder) {
			for range 30 {
				k := keys[rng.IntN(len(keys))]
				id := 1 + rng.Int64N(3)
				switch rng.IntN(6) {
				case 0:
					b.put(rng.IntN(3), k, fmt.Sprint(seed, b.now))
				case 1:
					b.do(rng.IntN(3), history.Request{Put: &history.PutRequest{Key: k, Value: fmt.Sprint("l", b.now), Lease: id}})
				case 2:
					b.del(rng.IntN(3), k)
				case 3:
					b.do(rng.IntN(3), history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: id, TTL: 5}})
				case 4:
					b.do(rng.IntN(3), history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: id}})
				default:
					b.get(rng.IntN(3), k)
				}
			}
		}
		first := newBuilder(t, keys...)
		random(first)
		st := &history.Start{Rev: first.s.rev}
		for _, kv := range first.s.kvs {
			if kv.Version != 0 {
				st.KVs = append(st.KVs, kv)
			}
		}
		first.s.leases.each(func(id int64) { st.Leases = append(st.Leases, id) })
		b := newBuilderFrom(t, st, keys...)
		b.watch(9, "a", history.AllKeys, true)
		random(b)
		h := b.done()
		if len(h.Log) == 0 {
			continue
		}
		mustPass(t, "Validate", h.Validate())
		mustPass(t, "Durable", Durable(h))
		mustPass(t, "Revisions", Revisions(h))
		mustPass(t, "Watches", Watches(h))
		mustPass(t, "Linearizable", Linearizable(h, time.Minute))
		for _, mode := range []WitnessMode{FromLog, FromRevisions} {
			mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
		}
	}
}

// TestStartBoundaries checks a history from a start at the start's
// revision and around it: a watch from the start's revision cannot be
// checked, one from below must be canceled as compacted, an answer below
// the start's revision is convicted, and a history that committed
// nothing since its start passes.
func TestStartBoundaries(t *testing.T) {
	watchFrom := func(rev int64, resps ...history.WatchResponse) *history.History {
		h := forkHistory(t)
		h.Watches = append(h.Watches, history.Watch{Client: 8, Req: history.WatchRequest{Key: "b", StartRevision: rev}, Responses: resps})
		return h
	}
	mustFail(t, "Watches", Watches(watchFrom(10)), "at or below the start's revision")
	mustFail(t, "Watches", Watches(watchFrom(9)), "at or below the start's revision")
	mustPass(t, "Watches", Watches(watchFrom(9, history.WatchResponse{Time: 15, Revision: 10, Canceled: true, CompactRevision: 10})))
	// A member on an empty disk: progress at its revision 1, below the
	// watch's first, then canceled once the start reaches it. Progress at
	// the watch's first revision or above is a delivery the history
	// cannot check.
	mustPass(t, "Watches", Watches(watchFrom(2, history.WatchResponse{Time: 14, Revision: 1, Progress: true}, history.WatchResponse{Time: 15, Revision: 10, Canceled: true, CompactRevision: 10})))
	mustFail(t, "Watches", Watches(watchFrom(2, history.WatchResponse{Time: 14, Revision: 2, Progress: true}, history.WatchResponse{Time: 15, Revision: 10, Canceled: true, CompactRevision: 10})), "below the start's compaction")

	// A read answered below the start's revision.
	h := forkHistory(t)
	h.Ops[0].Resp.Revision = 9
	mustFail(t, "Durable", Durable(h), "below the start's revision")
	mustFail(t, "Revisions", Revisions(h), "got revision 9")

	// A serializable read answered below it, from a node that is behind,
	// cannot be checked, and passes.
	h = forkHistory(t)
	h.Ops[0].Req.Range.Serializable = true
	h.Ops[0].Resp.Revision = 1
	h.Ops[0].Resp.Range.KVs = nil
	mustPass(t, "Durable", Durable(h))

	// One answered at 9 must hold "a", which the start holds as last
	// changed at 7: at 9 it held the same.
	h = forkHistory(t)
	h.Ops[0].Req.Range.Serializable = true
	h.Ops[0].Resp.Revision = 9
	h.Ops[0].Resp.Range.KVs, h.Ops[0].Resp.Range.Count = nil, 0
	mustFail(t, "Durable", Durable(h), "counts 0 keys, but the start holds 1")
	h.Ops[0].Resp.Range.Count = 1
	mustFail(t, "Durable", Durable(h), `the start holds "a" as last changed at 7`)
	h = forkHistory(t)
	h.Ops[0].Req.Range.Serializable = true
	h.Ops[0].Resp.Revision = 9
	h.Ops[0].Resp.Range.KVs, h.Ops[0].Resp.Range.Count = []history.KeyValue{startOf().KVs[0]}, 1
	mustPass(t, "Durable", Durable(h))

	// A read of every key at 5 must hold "b", last changed at 4; of "a",
	// changed at 7, the history cannot say.
	h = forkHistory(t)
	all := h.Ops[len(h.Ops)-1]
	all.Req.Range.Serializable = true
	all.Resp.Revision = 5
	all.Resp.Range.KVs, all.Resp.Range.Count = []history.KeyValue{startOf().KVs[1]}, 1
	h.Ops[len(h.Ops)-1] = all
	mustPass(t, "Durable", Durable(h))
	all.Resp.Range.KVs, all.Resp.Range.Count = nil, 1
	mustFail(t, "Durable", Durable(h), `the start holds "b" as last changed at 4`)
	// With a limit, or of the count alone, it must still count "b".
	all.Req.Range.Limit = 1
	all.Resp.Range.Count = 0
	mustFail(t, "Durable", Durable(h), "counts 0 keys, but the start holds 1")
	all.Resp.Range.Count = 1
	mustPass(t, "Durable", Durable(h))

	// A history that only reads from its start.
	b := newBuilderFrom(t, startOf())
	b.get(1, "a")
	b.get(2, "b")
	h = b.done()
	mustPass(t, "Durable", Durable(h))
	mustPass(t, "Watches", Watches(h))
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
	}

	// Lease 0 means no lease.
	h = forkHistory(t)
	h.Start.Leases = append(h.Start.Leases, 0)
	mustFail(t, "Validate", h.Validate(), "lease 0")
}

// ambiguous makes op i of h a call whose client got no answer, though it
// took effect.
func ambiguous(h *history.History, i int) {
	h.Ops[i].Resp = nil
	h.Ops[i].Err = &history.Error{Code: "Unavailable", Message: "no answer", Ambiguous: true}
}

// TestStartWitnessCorners passes histories from a start that only the
// start explains, through every checker: an unanswered revoke of a lease
// the start holds, the lease granted again and used; and an unanswered
// put of the value a key already has at the start, before a read of
// another key at the revision it wrote.
func TestStartWitnessCorners(t *testing.T) {
	check := func(h *history.History) {
		t.Helper()
		mustPass(t, "Durable", Durable(h))
		mustPass(t, "Revisions", Revisions(h))
		mustPass(t, "Watches", Watches(h))
		mustPass(t, "Linearizable", Linearizable(h, time.Minute))
		for _, mode := range []WitnessMode{FromLog, FromRevisions} {
			mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
		}
	}

	b := newBuilderFrom(t, startOf(), "c")
	revoke := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
	b.do(2, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 5, TTL: 5}})
	b.do(2, history.Request{Put: &history.PutRequest{Key: "c", Value: "c1", Lease: 5}})
	b.get(3, "a")
	h := b.done()
	ambiguous(h, revoke)
	check(h)

	b = newBuilderFrom(t, startOf())
	put := b.put(1, "a", "a2")
	b.get(2, "b")
	h = b.done()
	ambiguous(h, put)
	check(h)
}

// TestStartCompactionCorners checks compactions next to a start: a
// transaction that reads below a compaction after the start's, unanswered,
// is explained by that compaction, not the start's; a watch from the
// start's revision with previous values may be canceled there; and a
// watch from below it must not deliver anything before its cancel.
func TestStartCompactionCorners(t *testing.T) {
	b := newBuilderFrom(t, startOf(), "c")
	b.put(1, "c", "c1")
	compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 11}})
	b.do(3, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Range: &history.RangeRequest{Key: "c", Revision: 10}}}}})
	h := b.done()
	ambiguous(h, compact)
	if op := h.Ops[2]; op.Err == nil || !sameError(op.Err, errCompacted) {
		t.Fatalf("the transaction's read below the compaction got %+v, want compacted", op)
	}
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		mustPass(t, "Witnessed", Witnessed(h, mode, time.Minute))
	}

	h = forkHistory(t)
	h.Watches = append(h.Watches, history.Watch{
		Client:    8,
		Req:       history.WatchRequest{Key: "b", StartRevision: 10, PrevKV: true},
		Responses: []history.WatchResponse{{Time: 15, Revision: 10, Canceled: true, CompactRevision: 10}},
	})
	mustPass(t, "Watches", Watches(h))

	h = forkHistory(t)
	h.Watches = append(h.Watches, history.Watch{Client: 8, Req: history.WatchRequest{Key: "c", StartRevision: 9}, Responses: []history.WatchResponse{
		{Time: 15, Revision: 11, Events: []history.Event{{Type: history.EventPut, KV: history.KeyValue{Key: "c", Value: "c1", CreateRevision: 12, ModRevision: 12, Version: 1}}}},
		{Time: 16, Revision: 13, Canceled: true, CompactRevision: 13},
	}})
	mustFail(t, "Watches", Watches(h), "before it was canceled")
}
