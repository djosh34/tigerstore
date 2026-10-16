package check

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

var bothModes = []WitnessMode{FromLog, FromRevisions}

// TestWitnessedGood passes linearizable histories in both modes.
func TestWitnessedGood(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T) *history.History
	}{
		{"sequential workload", func(t *testing.T) *history.History { return workload(t).done() }},
		{"read inside a concurrent write", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[put].Call, h.Ops[put].Return = 10, 50
			h.Ops[get].Call, h.Ops[get].Return = 20, 30
			return h
		}},
		{"indeterminate write lands after a later read saw it missing", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			first := b.get(1, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			// The put is called before the first read and times out; it
			// takes effect after that read.
			h.Ops[put].Call, h.Ops[put].Return = h.Ops[first].Call-5, h.Ops[first].Call-1
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			return h
		}},
		{"indeterminate write never lands", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			h.Log = nil
			h.Ops = append(h.Ops,
				history.Op{Client: 1, Call: 100, Return: 110, Req: history.Request{Put: &history.PutRequest{Key: "b", Value: "2"}}, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
				history.Op{Client: 1, Call: 120, Return: 130, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 2, Range: &history.RangeResponse{}}})
			h.Log = []history.Request{h.Ops[1].Req}
			return h
		}},
		{"rejected request had no effect", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 5}})
			b.get(0, "a")
			b.put(0, "a", "y")
			return b.done()
		}},
		{"lease expiry modelled as an ambiguous revoke", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.get(0, "a")
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Return+1, h.Ops[grant].Return+2
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			return h
		}},
		{"lease expiry without keys, found gone later", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.put(1, "a", "1")
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			return h
		}},
		{"historical reads, inside a txn too", func(*testing.T) *history.History { return historicalTxn() }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range bothModes {
				mustPass(t, "Witnessed "+mode.String(), Witnessed(c.build(t), mode, timeout))
			}
		})
	}
}

// historicalTxn puts a twice, then runs a txn that reads a at revision 2,
// puts it again and reads it at revision 3.
func historicalTxn() *history.History {
	put := func(v string) history.Request { return history.Request{Put: &history.PutRequest{Key: "a", Value: v}} }
	txn := history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Range: &history.RangeRequest{Key: "a", Revision: 2}},
		{Put: &history.PutRequest{Key: "a", Value: "3"}},
		{Range: &history.RangeRequest{Key: "a", Revision: 3}},
	}}}
	at := func(v string, mod, version int64) *history.RangeResponse {
		return &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: v, CreateRevision: 2, ModRevision: mod, Version: version}}}
	}
	return &history.History{
		Ops: []history.Op{
			{Client: 0, Call: 1, Return: 2, Req: put("1"), Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
			{Client: 0, Call: 3, Return: 4, Req: put("2"), Resp: &history.Response{Revision: 3, Put: &history.PutResponse{}}},
			{Client: 1, Call: 5, Return: 6, Req: txn, Resp: &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{
				{Range: at("1", 2, 1)}, {Put: &history.PutResponse{}}, {Range: at("2", 3, 2)},
			}}}},
		},
		Log: []history.Request{put("1"), put("2"), txn},
	}
}

// TestWitnessedBroken fails histories that are not linearizable, in both
// modes: #112's stale read, lost acked write and write applied twice, and
// Linearizable's broken cases that need no Effect.
func TestWitnessedBroken(t *testing.T) {
	cases := []struct {
		name  string
		want  string
		build func(t *testing.T) *history.History
	}{
		{"stale read", "answers it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0] = history.KeyValue{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 2, Version: 1}
			return h
		}},
		{"lost acked write", "answers it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp = &history.Response{Revision: 2, Range: &history.RangeResponse{}}
			return h
		}},
		{"write applied twice", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "1")
			b.put(1, "b", "2")
			h := b.done()
			// A replay of the first put after the second write.
			h.Log = append(h.Log, h.Log[0])
			h.Ops = append(h.Ops, history.Op{
				Client: 2, Call: 100, Return: 110, Req: history.Request{Range: &history.RangeRequest{Key: "a"}},
				Resp: &history.Response{Revision: 4, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 4, Version: 2}}}},
			})
			return h
		}},
		{"two CAS on one revision both win", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.cas(1, "a", 2, "x")
			second := b.cas(2, "a", 2, "y")
			h := b.done()
			h.Ops[second].Resp = &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Put: &history.PutResponse{}}}}}
			return h
		}},
		{"put reports a wrong revision", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp.Revision = 3
			return h
		}},
		{"rejected write took effect", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, &history.Error{Code: "InvalidArgument", Message: "etcdserver: request is too large"}
			return h
		}},
		{"read at the compaction revision fails as compacted", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			read := b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp, h.Ops[read].Err = nil, errCompacted
			return h
		}},
		{"read of a value nobody wrote", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0].Value = "2"
			return h
		}},
		{"read sees a write that starts after it returned", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Call, h.Ops[get].Return = 1, 2
			h.Ops[put].Call, h.Ops[put].Return = 5, 6
			return h
		}},
		{"historical read inside a txn sees a later value", "", func(*testing.T) *history.History {
			h := historicalTxn()
			h.Ops[2].Resp.Txn.Results[0].Range = h.Ops[2].Resp.Txn.Results[2].Range
			return h
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range bothModes {
				mustFail(t, "Witnessed "+mode.String(), Witnessed(c.build(t), mode, timeout), c.want)
			}
		})
	}
}

// TestWitnessedTimeout checks that running out of time is a failure.
func TestWitnessedTimeout(t *testing.T) {
	b := newBuilder(t, "a")
	for i := range 2000 {
		b.put(i%16, "a", strconv.Itoa(i))
	}
	for _, mode := range bothModes {
		mustFail(t, "Witnessed "+mode.String(), Witnessed(b.done(), mode, time.Nanosecond), "timed out")
	}
}

// randomHistory runs random puts, deletes, compare-and-swaps and reads on
// three keys, one at a time in order, from four clients whose calls
// overlap their neighbours'. A quarter of the writes time out; half of
// those took effect, and the log has them.
func randomHistory(rng *rand.Rand) *history.History {
	keys := []string{"a", "b", "c"}
	st := newState(keys)
	h := &history.History{}
	for i := range 40 {
		at := int64(i+1) * 10
		key := keys[rng.IntN(len(keys))]
		var req history.Request
		switch rng.IntN(5) {
		case 0:
			req.Put = &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i), PrevKV: true}
		case 1:
			req.Delete = &history.DeleteRequest{Key: key, PrevKV: true}
		case 2:
			kv, _ := st.get(key)
			req.Txn = &history.TxnRequest{
				Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: kv.ModRevision - rng.Int64N(2)}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i)}}},
				Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}},
			}
		case 3:
			req.Range = &history.RangeRequest{Key: "a", End: history.AllKeys, Limit: rng.Int64N(3)}
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

// TestWitnessedAgreesWithPorcupine checks Witnessed against Linearizable
// on random small histories, true ones and copies with a stale read: both
// modes must give Porcupine's verdict on every one.
func TestWitnessedAgreesWithPorcupine(t *testing.T) {
	rng := rand.New(rand.NewPCG(112, 148))
	rejected := 0
	for i := range 300 {
		h := randomHistory(rng)
		if i%2 == 1 && !staleAnswer(rng, h) {
			continue
		}
		lin := Linearizable(h, timeout)
		if i%2 == 0 && lin != nil {
			t.Fatalf("history %d: Porcupine rejects a true history: %v", i, lin)
		}
		if lin != nil {
			rejected++
		}
		for _, mode := range bothModes {
			if err := Witnessed(h, mode, timeout); (err == nil) != (lin == nil) {
				t.Fatalf("history %d: Porcupine says %v, Witnessed (%s) says %v", i, lin, mode, err)
			}
		}
	}
	if rejected < 50 {
		t.Fatalf("only %d histories were rejected", rejected)
	}
	t.Logf("%d histories rejected by all three", rejected)
}

// staleAnswer makes one successful read of a single key in h answer with
// an older value of it, from an acked put before it, and reports whether
// there was one.
func staleAnswer(rng *rand.Rand, h *history.History) bool {
	start := rng.IntN(len(h.Ops))
	for j := range h.Ops {
		i := (start + j) % len(h.Ops)
		op := &h.Ops[i]
		if op.Req.Range == nil || op.Req.Range.End != "" || op.Resp == nil || len(op.Resp.Range.KVs) == 0 {
			continue
		}
		got := op.Resp.Range.KVs[0]
		for k := i - 1; k >= 0; k-- {
			p := h.Ops[k]
			if p.Req.Put == nil || p.Req.Put.Key != got.Key || p.Resp == nil || p.Req.Put.Value == got.Value {
				continue
			}
			resp := *op.Resp
			rr := *resp.Range
			rr.KVs = []history.KeyValue{{Key: got.Key, Value: p.Req.Put.Value, CreateRevision: got.CreateRevision, ModRevision: p.Resp.Revision, Version: got.Version}}
			resp.Range = &rr
			op.Resp = &resp
			return true
		}
	}
	return false
}

// TestWitnessedTxnRevisions replays the difftest case txn-revisions with
// etcd v3.7.2's recorded answers (difftest/testdata/etcd-v3.7.2.json): a
// read at a past revision inside a txn, reads at the txn's start revision
// and the current one after its own write, and failure branches that
// write. The model must give each answer, in both modes, before a failure
// of the witness checker can be trusted (#112). The recording's revisions
// start one lower: its first put is revision 1 above the case's start.
func TestWitnessedTxnRevisions(t *testing.T) {
	kv := func(key, value string, create, mod, version int64) history.KeyValue {
		return history.KeyValue{Key: key, Value: value, CreateRevision: create + 1, ModRevision: mod + 1, Version: version}
	}
	one := func(k history.KeyValue) *history.RangeResponse {
		return &history.RangeResponse{Count: 1, KVs: []history.KeyValue{k}}
	}
	at := func(key string, rev int64) history.TxnOp {
		return history.TxnOp{Range: &history.RangeRequest{Key: key, Revision: rev + 1}}
	}
	get := func(key string) history.TxnOp { return history.TxnOp{Range: &history.RangeRequest{Key: key}} }
	put := func(key, value string) history.TxnOp {
		return history.TxnOp{Put: &history.PutRequest{Key: key, Value: value}}
	}
	del := func(key string) history.TxnOp { return history.TxnOp{Delete: &history.DeleteRequest{Key: key}} }
	mod := func(key string, rev int64) []history.Compare {
		return []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: rev}}
	}
	txn := func(cmp []history.Compare, success, failure []history.TxnOp) history.Request {
		return history.Request{Txn: &history.TxnRequest{Compare: cmp, Success: success, Failure: failure}}
	}
	results := func(rev int64, ok bool, rs ...history.TxnOpResponse) *history.Response {
		return &history.Response{Revision: rev + 1, Txn: &history.TxnResponse{Succeeded: ok, Results: rs}}
	}
	a1, a3, a4 := kv("t/a", "1", 1, 1, 1), kv("t/a", "3", 1, 4, 3), kv("t/a", "4", 1, 6, 4)
	b1, b2 := kv("t/b", "1", 3, 3, 1), kv("t/b", "2", 3, 5, 2)
	steps := []struct {
		req  history.Request
		resp *history.Response
	}{
		{history.Request{Put: &history.PutRequest{Key: "t/a", Value: "1"}}, &history.Response{Revision: 2, Put: &history.PutResponse{}}},
		{history.Request{Put: &history.PutRequest{Key: "t/a", Value: "2"}}, &history.Response{Revision: 3, Put: &history.PutResponse{}}},
		{history.Request{Put: &history.PutRequest{Key: "t/b", Value: "1"}}, &history.Response{Revision: 4, Put: &history.PutResponse{}}},
		{txn(nil, []history.TxnOp{at("t/a", 1)}, nil), results(3, true, history.TxnOpResponse{Range: one(a1)})},
		{
			txn(nil, []history.TxnOp{at("t/a", 1), put("t/a", "3"), at("t/a", 1), get("t/a")}, nil),
			results(4, true, history.TxnOpResponse{Range: one(a1)}, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(a1)}, history.TxnOpResponse{Range: one(a3)}),
		},
		{
			txn(nil, []history.TxnOp{put("t/b", "2"), at("t/b", 4), get("t/b")}, nil),
			results(5, true, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(b1)}, history.TxnOpResponse{Range: one(b2)}),
		},
		{
			txn(mod("t/a", 2), []history.TxnOp{get("t/a")}, []history.TxnOp{put("t/a", "4"), get("t/a")}),
			results(6, false, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(a4)}),
		},
		{
			txn(mod("t/b", 0), []history.TxnOp{get("t/b")}, []history.TxnOp{del("t/b"), get("t/b")}),
			results(7, false, history.TxnOpResponse{Delete: &history.DeleteResponse{Deleted: 1}}, history.TxnOpResponse{Range: &history.RangeResponse{}}),
		},
		{
			txn(mod("t/b", 6), []history.TxnOp{get("t/b")}, []history.TxnOp{del("t/none")}),
			results(7, false, history.TxnOpResponse{Delete: &history.DeleteResponse{}}),
		},
		{history.Request{Range: &history.RangeRequest{Key: "t/a", Revision: 5}}, &history.Response{Revision: 8, Range: one(a3)}},
		{history.Request{Range: &history.RangeRequest{Key: "t/b", Revision: 7}}, &history.Response{Revision: 8, Range: one(b2)}},
	}
	h := &history.History{}
	for i, s := range steps {
		h.Ops = append(h.Ops, history.Op{Client: 0, Call: int64(2*i + 1), Return: int64(2*i + 2), Req: s.req, Resp: s.resp})
		if s.resp.Revision > int64(len(h.Log)+1) {
			h.Log = append(h.Log, s.req)
		}
	}
	for _, mode := range bothModes {
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout))
	}
}
