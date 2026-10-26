package check

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// Compare the complete model outcomes, not just pass/fail, at every
// stored state: ranges, counts, limits, missing keys, historical reads,
// both transaction branches, compaction, grants and revocations.
func TestScopedReplayMatchesFull(t *testing.T) {
	rng := rand.New(rand.NewPCG(441, 5))
	keys := []string{"a", "b", "c", "d", "e", "f"}
	var reqs []history.Request
	for _, k := range keys {
		reqs = append(reqs, history.Request{Put: &history.PutRequest{Key: k, Value: k}})
	}
	pickRange := func() *history.RangeRequest {
		q := &history.RangeRequest{Key: keys[rng.IntN(len(keys))]}
		switch rng.IntN(3) {
		case 1:
			q.End = history.AllKeys
		case 2:
			q.End = keys[rng.IntN(len(keys))]
		}
		q.CountOnly = rng.IntN(4) == 0
		q.KeysOnly = rng.IntN(4) == 0
		if rng.IntN(3) == 0 {
			q.Limit = 1
		}
		if rng.IntN(3) == 0 {
			q.Revision = int64(rng.IntN(20))
		}
		return q
	}
	for n := range 120 {
		k := keys[rng.IntN(len(keys))]
		req := history.Request{}
		switch rng.IntN(8) {
		case 0:
			req.Range = pickRange()
		case 1:
			req.Put = &history.PutRequest{Key: k, Value: k, Lease: int64(rng.IntN(3)), PrevKV: true}
		case 2:
			q := pickRange()
			req.Delete = &history.DeleteRequest{Key: q.Key, End: q.End, PrevKV: true}
		case 3:
			req.LeaseGrant = &history.LeaseGrantRequest{ID: int64(rng.IntN(3) + 1), TTL: 10}
		case 4:
			req.LeaseRevoke = &history.LeaseRevokeRequest{ID: int64(rng.IntN(3) + 1)}
		case 5:
			req.Compact = &history.CompactRequest{Revision: int64(rng.IntN(20) + 1)}
		default:
			q := pickRange()
			req.Txn = &history.TxnRequest{
				Compare: []history.Compare{{Key: k, Target: history.TargetMod, Result: history.Equal, Number: int64(n % 10)}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: k, Value: "txn"}}, {Range: q}},
				Failure: []history.TxnOp{{Range: q}, {Delete: &history.DeleteRequest{Key: k}}},
			}
		}
		reqs = append(reqs, req)
	}
	h := &history.History{}
	for _, r := range reqs {
		h.Ops = append(h.Ops, history.Op{Req: r})
	}
	w := newWitness(h, time.Now().Add(time.Minute))
	w.metrics = &WitnessStats{}
	for i, r := range reqs {
		w.apply(i, r)
	}
	for j := range w.states {
		for i, r := range reqs {
			full := w.stateAt(j).step(r, false)
			scoped := w.scopedState(i, j).step(r, false)
			if !reflect.DeepEqual(full, scoped) {
				t.Fatalf("state %d request %d: full=%s scoped=%s", j, i, outputOf(full), outputOf(scoped))
			}
			for _, events := range []bool{false, true} {
				s := w.stateAt(j)
				before := s.clone()
				want := s.clone().step(r, events)
				order := &revisionOrder{w: w}
				trial, restore := order.trial(s, r)
				got := trial.step(r, events)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("trial state %d request %d: want=%s got=%s", j, i, outputOf(want), outputOf(got))
				}
				restore()
				if !s.equal(before) {
					t.Fatalf("trial state %d request %d changed original", j, i)
				}
			}
		}
	}
}
