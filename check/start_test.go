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
