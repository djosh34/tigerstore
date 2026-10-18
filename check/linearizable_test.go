package check

import (
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

const timeout = 10 * time.Second

var errUnavailable = &history.Error{Code: "Unavailable", Message: "etcdserver: request timed out", Ambiguous: true}

// workload runs a small kube-apiserver shaped workload: creates, CAS
// updates, failed CAS, reads, a lease, deletes and a compaction.
func workload(t *testing.T) *builder {
	t.Helper()
	b := newBuilder(t, "/r/a", "/r/b", "/r/c")
	b.watch(9, "/r/", "/r0", true)
	b.watch(8, "/r/a", "", false)
	create := func(client int, key, value string) {
		b.do(client, history.Request{Txn: &history.TxnRequest{
			Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: 0}},
			Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: value}}},
			Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}},
		}})
	}
	create(0, "/r/a", "a1")
	create(1, "/r/b", "b1")
	create(2, "/r/a", "lost race")
	b.cas(0, "/r/a", 2, "a2")
	b.cas(1, "/r/a", 2, "stale")
	b.progress()
	b.get(2, "/r/a")
	b.do(1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 7, TTL: 60}})
	b.do(1, history.Request{Put: &history.PutRequest{Key: "/r/c", Value: "c1", Lease: 7}})
	b.do(2, history.Request{Range: &history.RangeRequest{Key: "/r/", End: "/r0", Limit: 2}})
	b.del(0, "/r/b")
	b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 4}})
	b.do(2, history.Request{Range: &history.RangeRequest{Key: "/r/a", Revision: 4}})
	b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}})
	b.put(2, "/r/b", "b2")
	b.cas(1, "/r/b", 8, "b3")
	b.progress()
	b.do(0, history.Request{Range: &history.RangeRequest{Key: "/r/", End: "/r0"}})
	return b
}

func TestLinearizableGood(t *testing.T) {
	t.Run("sequential workload", func(t *testing.T) {
		mustPass(t, "Linearizable", Linearizable(workload(t).done(), timeout))
	})
	t.Run("read inside a concurrent write", func(t *testing.T) {
		b := newBuilder(t, "a")
		put := b.put(0, "a", "1")
		get := b.get(1, "a")
		h := b.done()
		h.Ops[put].Call, h.Ops[put].Return = 10, 50
		h.Ops[get].Call, h.Ops[get].Return = 20, 30
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
	t.Run("timed out write commits after its timeout", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.get(1, "a")
		put := b.put(0, "a", "1")
		b.get(1, "a")
		h := b.done()
		h.Ops[put].Call, h.Ops[put].Return = 1, 5
		h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
	t.Run("timed out write never commits", func(t *testing.T) {
		b := newBuilder(t, "a")
		put := b.put(0, "a", "1")
		h := b.done()
		h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
		h.Ops = append(h.Ops, history.Op{Client: 1, Call: 100, Return: 110, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 1, Range: &history.RangeResponse{}}})
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
	t.Run("timed out write took effect as the log says", func(t *testing.T) {
		b := newBuilder(t, "a")
		put := b.put(0, "a", "1")
		b.get(1, "a")
		h := b.done()
		h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
		h.Ops[put].Effect = &history.Effect{By: 25, Revision: 2}
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
	t.Run("rejected request had no effect", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 5}})
		b.get(0, "a")
		mustPass(t, "Linearizable", Linearizable(b.done(), timeout))
	})
	t.Run("lease expiry modelled as an ambiguous revoke", func(t *testing.T) {
		b := newBuilder(t, "a")
		grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
		b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
		expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
		b.get(0, "a")
		h := b.done()
		h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Return+1, h.Ops[grant].Return+2
		h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
	t.Run("server picks the lease ID", func(t *testing.T) {
		h := &history.History{Ops: []history.Op{
			{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{TTL: 60}}, Resp: &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 42, TTL: 60}}},
			{Client: 0, Call: 3, Return: 4, Req: history.Request{Put: &history.PutRequest{Key: "a", Lease: 42}}, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
		}}
		mustPass(t, "Linearizable", Linearizable(h, timeout))
	})
}

func TestLinearizableBroken(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T) *history.History
	}{
		{"stale read", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0] = history.KeyValue{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 2, Version: 1}
			return h
		}},
		{"lost acked write", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp = &history.Response{Revision: 1, Range: &history.RangeResponse{}}
			return h
		}},
		{"two CAS on one revision both win", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.cas(1, "a", 2, "x")
			second := b.cas(2, "a", 2, "y")
			h := b.done()
			h.Ops[second].Resp = &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Put: &history.PutResponse{}}}}}
			return h
		}},
		{"put reports a wrong revision", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			h := b.done()
			h.Ops[put].Resp.Revision = 3
			return h
		}},
		{"timed out write the log has is not read after it took effect", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			h.Ops[put].Effect = &history.Effect{By: 20, Revision: 2}
			h.Ops = append(h.Ops, history.Op{Client: 1, Call: 100, Return: 110, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 1, Range: &history.RangeResponse{}}})
			return h
		}},
		{"timed out write took effect at another revision than the log's", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			h.Ops[put].Effect = &history.Effect{By: 20, Revision: 3}
			return h
		}},
		{"rejected write took effect", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, &history.Error{Code: "InvalidArgument", Message: "etcdserver: request is too large"}
			return h
		}},
		{"read at the compaction revision fails as compacted", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			read := b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp, h.Ops[read].Err = nil, errCompacted
			return h
		}},
		{"read of a value nobody wrote", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0].Value = "2"
			return h
		}},
		{"txn writes two keys in two revisions", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.do(0, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
				{Put: &history.PutRequest{Key: "a", Value: "1"}},
				{Put: &history.PutRequest{Key: "b", Value: "1"}},
			}}})
			get := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}})
			h := b.done()
			h.Ops[0].Resp.Revision = 3
			kvs := h.Ops[get].Resp.Range.KVs
			kvs[1].CreateRevision, kvs[1].ModRevision = 3, 3
			h.Ops[get].Resp.Revision = 3
			return h
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustFail(t, "Linearizable", Linearizable(c.build(t), timeout), "not linearizable")
		})
	}
}

// TestLinearizableTimeout checks that running out of time is a failure
// that says how far the search got.
func TestLinearizableTimeout(t *testing.T) {
	b := newBuilder(t, "a", "b", "c", "d")
	for i := range 2000 {
		b.put(i%16, string(rune('a'+i%4)), strconv.Itoa(i))
	}
	h := b.done()
	// Make every op concurrent with every other one.
	for i := range h.Ops {
		h.Ops[i].Call, h.Ops[i].Return = int64(i), int64(i)+1_000_000
		h.Ops[i].Client = i
	}
	mustFail(t, "Linearizable", Linearizable(h, time.Nanosecond), "the longest order the search found holds")
}

func TestValidate(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	h := b.done()
	h.Ops[1].Call = h.Ops[0].Return - 1
	mustFail(t, "Validate", h.Validate(), "before its previous call returned")
	h = b.done()
	h.Ops[0].Err = errUnavailable
	mustFail(t, "Validate", h.Validate(), "exactly one of Resp and Err")
	h = b.done()
	h.Ops[0].Req.Range = &history.RangeRequest{Key: "a"}
	mustFail(t, "Validate", h.Validate(), "2 fields set")
	b = newBuilder(t, "a")
	b.put(0, "a", "1")
	h = b.done()
	h.Ops[0].Effect = &history.Effect{By: h.Ops[0].Return, Revision: 2}
	mustFail(t, "Validate", h.Validate(), "not an ambiguous mutation")
}
