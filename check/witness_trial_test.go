package check

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// slowWitness makes Witnessed copy the whole store for every try and
// answer, as before #441's speedups.
func slowWitness() Option { return func(o *options) { o.slow = true } }

// TestTryAndScopeMatchFullReplay steps random requests through every state
// of a random history and compares the model's whole outcome, not only
// pass or fail: on the keys a request reads (scopedState) against the
// whole store, and through a try against a copy, which the try's undo must
// leave as it was. It covers ranges, counts, limits, missing keys, reads
// at past revisions, both transaction branches, compactions, grants and
// revokes.
func TestTryAndScopeMatchFullReplay(t *testing.T) {
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
				trial, undo := order.try(s, r)
				got := trial.step(r, events)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("trial state %d request %d: want=%s got=%s", j, i, outputOf(want), outputOf(got))
				}
				undo()
				if !s.equal(before) {
					t.Fatalf("trial state %d request %d changed original", j, i)
				}
			}
		}
	}
}

// TestFastWitnessMatchesSlow runs the witness from the revisions with its
// speedups and with a copy of the whole store for every try and answer, on
// 300 random histories, each also with a stale read put in, and requires
// the same verdict from both; at least 100 stale histories must fail.
func TestFastWitnessMatchesSlow(t *testing.T) {
	rng := rand.New(rand.NewPCG(441, 3))
	rejected := 0
	for i := range 300 {
		_, h := narrowingHistory(rng)
		for _, stale := range []bool{false, true} {
			if stale && !staleRead(rng, h) {
				continue
			}
			want := Witnessed(h, FromRevisions, time.Minute, slowWitness())
			got := Witnessed(h, FromRevisions, time.Minute)
			if (got == nil) != (want == nil) {
				t.Fatalf("history %d, stale %v: slow %v, fast %v", i, stale, want, got)
			}
			if !stale && got != nil {
				t.Fatalf("history %d: %v", i, got)
			}
			if got != nil {
				rejected++
			}
		}
	}
	if rejected < 100 {
		t.Fatalf("only %d stale histories failed", rejected)
	}
	t.Logf("%d stale histories failed with and without the speedups", rejected)
}

// leasedHistory runs random requests on four keys, one at a time in
// order, from four clients whose calls overlap their neighbours': puts,
// some on a lease, deletes of a key or a range, transactions that write in
// either branch, counts of every key, reads of one key, some at the
// revision the client saw last, lease grants and revokes, and compactions. A quarter of the
// writes time out; half of those took effect, and the log has them.
func leasedHistory(rng *rand.Rand) *history.History {
	keys := []string{"a", "b", "c", "d"}
	st := newState(keys)
	h := &history.History{}
	var leases []int64
	for i := range 40 {
		at := int64(i+1) * 10
		key := keys[rng.IntN(len(keys))]
		var req history.Request
		switch rng.IntN(9) {
		case 0:
			p := &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i), PrevKV: true}
			if len(leases) > 0 && rng.IntN(2) == 0 {
				p.Lease = leases[rng.IntN(len(leases))]
			}
			req.Put = p
		case 1:
			d := &history.DeleteRequest{Key: key, PrevKV: true}
			if rng.IntN(2) == 0 {
				d.End = history.AllKeys
			}
			req.Delete = d
		case 2:
			kv, _ := st.get(key)
			req.Txn = &history.TxnRequest{
				Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: kv.ModRevision - rng.Int64N(2)}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i)}}, {Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}},
				Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}, {Delete: &history.DeleteRequest{Key: keys[rng.IntN(len(keys))]}}},
			}
		case 3:
			req.Range = &history.RangeRequest{Key: "a", End: history.AllKeys, Limit: rng.Int64N(3), CountOnly: rng.IntN(3) == 0}
		case 4:
			req.LeaseGrant = &history.LeaseGrantRequest{ID: int64(len(leases) + 1), TTL: 10}
			leases = append(leases, int64(len(leases)+1))
		case 5:
			if len(leases) == 0 {
				req.Range = &history.RangeRequest{Key: key}
				break
			}
			req.LeaseRevoke = &history.LeaseRevokeRequest{ID: leases[rng.IntN(len(leases))]}
		case 6:
			req.Compact = &history.CompactRequest{Revision: 1 + rng.Int64N(st.rev)}
		case 7:
			req.Range = &history.RangeRequest{Key: key, Revision: st.rev}
		default:
			req.Range = &history.RangeRequest{Key: key}
		}
		op := history.Op{Client: i % 4, Call: at - rng.Int64N(16), Return: at + rng.Int64N(16), Req: req}
		if req.Mutates() && rng.IntN(4) == 0 {
			op.Err = &history.Error{Code: "Unavailable", Message: "timed out", Ambiguous: true}
			if rng.IntN(2) == 0 {
				if o := st.step(req, false); o.err == nil && changes(req, o) {
					h.Log = append(h.Log, req)
				}
			}
		} else {
			o := st.step(req, false)
			op.Resp, op.Err = o.resp, o.err
			if o.err == nil && changes(req, o) {
				h.Log = append(h.Log, req)
			}
		}
		h.Ops = append(h.Ops, op)
	}
	return h
}

// TestFastWitnessMatchesSlowWithLeases is TestFastWitnessMatchesSlow on
// leasedHistory, in both modes, so that the search tries transactions,
// revokes and compactions, with a stale answer put into every other one.
// It requires the same verdict with and without the speedups, and a pass
// of every true history: the witness from the revisions refused some whose
// ambiguous put used a lease that only an ambiguous grant made (#514).
func TestFastWitnessMatchesSlowWithLeases(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 513))
	rejected := 0
	for i := range 600 {
		h := leasedHistory(rng)
		stale := i%2 == 1
		if stale && !staleAnswer(rng, h) {
			continue
		}
		for _, mode := range []WitnessMode{FromLog, FromRevisions} {
			want := Witnessed(h, mode, time.Minute, slowWitness())
			got := Witnessed(h, mode, time.Minute)
			if (got == nil) != (want == nil) {
				t.Fatalf("history %d, stale %v, %s: slow %v, fast %v", i, stale, mode, want, got)
			}
			if !stale && got != nil {
				t.Fatalf("history %d, %s: a true history fails: %v", i, mode, got)
			}
			if stale && got != nil {
				rejected++
			}
		}
	}
	if rejected < 100 {
		t.Fatalf("only %d stale checks failed", rejected)
	}
	t.Logf("%d stale checks failed with and without the speedups", rejected)
}

// TestWitnessTrueLeasedHistories checks true leasedHistory histories that
// the witness from the revisions refused (#514), each the one leasedHistory
// draws at that place from a PCG seed. TestFastWitnessMatchesSlowWithLeases
// covers a fourth, its history 190: an ambiguous put on a lease that only
// an ambiguous grant made (fitsGranted).
//   - a grant and a revoke of its lease at one revision whose calls
//     overlap, among more acked ops of that revision than the orders tried
//     before (slotOrder): history 247 of PCG(10, 77);
//   - two compactions at one revision in the order of their revisions,
//     against their calls: history 113 of PCG(9, 77);
//   - an ambiguous compaction put in for a rejection that an acked one
//     explains, which pushed a later rejection's place past the revoke it
//     needs: history 460 of PCG(2, 77).
func TestWitnessTrueLeasedHistories(t *testing.T) {
	for _, c := range []struct{ seed, stream, index uint64 }{
		{10, 77, 247}, {9, 77, 113}, {2, 77, 460},
	} {
		rng := rand.New(rand.NewPCG(c.seed, c.stream))
		var h *history.History
		for i := uint64(0); i <= c.index; i++ {
			h = leasedHistory(rng)
		}
		if err := Witnessed(h, FromRevisions, time.Minute); err != nil {
			t.Errorf("history %d of PCG(%d, %d): %v", c.index, c.seed, c.stream, err)
		}
	}
}

// TestAckedCompactionDoesNotExplainEqualRead has an acked compaction to
// revision 2, an ambiguous one to 3 that took effect, and a read at 2
// rejected as compacted. A compaction to 2 rejects a compaction to 2 but
// not a read at 2, so only the ambiguous one explains the read, and it
// must go in (#514).
func TestAckedCompactionDoesNotExplainEqualRead(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "one")
	b.put(0, "a", "two")
	b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
	c := b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 3}})
	b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
	h := b.done()
	h.Ops[c].Resp, h.Ops[c].Err = nil, errUnavailable
	for _, mode := range bothModes {
		mustPass(t, mode.String(), Witnessed(h, mode, time.Minute))
	}
}

// TestWitnessRejectsWhatNoOrderReaches has histories that no order
// answers, which the indexes by revision must still reject, and without a
// panic or an allocation by what a client reported (review of #525): a
// watch that saw a put of a key no request puts, and a range answered at
// revision -1 and at the highest int64, and a read at a far future
// revision whose count the room index once took as a slice's length.
func TestWitnessRejectsWhatNoOrderReaches(t *testing.T) {
	for name, js := range map[string]string{
		"a key no request puts":           `{"ops":[{"call":1,"return":2,"req":{"put":{"key":"a","value":"v"}},"err":{"ambiguous":true}}],"watches":[{"responses":[{"events":[{"type":1,"kv":{"key":"z","value":"v","createRevision":2,"modRevision":2,"version":1}}]}]}]}`,
		"revision -1":                     `{"ops":[{"call":1,"return":2,"req":{"range":{"key":"a"}},"resp":{"revision":-1,"range":{"count":0}}}]}`,
		"the highest revision":            `{"ops":[{"call":1,"return":2,"req":{"range":{"key":"a"}},"resp":{"revision":9223372036854775807,"range":{"count":0}}}]}`,
		"a read at a far future revision": `{"ops":[{"call":1,"return":2,"req":{"put":{"key":"a","value":"v"}},"resp":{"revision":2,"put":{}}},{"call":3,"return":4,"req":{"range":{"key":"a","revision":4611686018427387904}},"resp":{"revision":2,"range":{"count":1,"kvs":[{"key":"a","value":"v","createRevision":2,"modRevision":2,"version":1}]}}}]}`,
		"a read at the highest revision":  `{"ops":[{"call":1,"return":2,"req":{"range":{"key":"a","revision":9223372036854775807}},"resp":{"revision":1,"range":{"count":0}}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var h history.History
			if err := json.Unmarshal([]byte(js), &h); err != nil {
				t.Fatal(err)
			}
			if err := Witnessed(&h, FromRevisions, time.Minute); err == nil {
				t.Fatal("the witness from the revisions passed a history that no order answers")
			}
		})
	}
}

// TestPlaceSeenRefusesAStaleFirstAnswer gives placeSeen a first answer of
// a state the order no longer has: a read's answer changed to a value no
// state holds, and its kept state id one that no state has. placeSeen
// must search, find no state that answers, and refuse; it must not take
// the kept place (review of #525).
func TestPlaceSeenRefusesAStaleFirstAnswer(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "one")
	i := b.do(0, history.Request{Range: &history.RangeRequest{Key: "a"}})
	h := b.done()
	w := newWitness(h, time.Now().Add(time.Minute))
	if err := w.fromRevisions(); err != nil {
		t.Fatal(err)
	}
	if w.firsts[i].id == 0 {
		t.Fatal("finish kept no first answer for the read")
	}
	h.Ops[i].Resp.Range.KVs[0].Value = "other"
	w.firsts[i].id = w.lastID + 1
	if err := w.placeSeen([]int{i}, false); err == nil {
		t.Fatal("placeSeen took a first answer whose state the order no longer has")
	}
}
