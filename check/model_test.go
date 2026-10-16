package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestModel pins the model to etcd's rules, written out by hand so a model
// bug does not hide behind histories the model generated itself.
func TestModel(t *testing.T) {
	b := newBuilder(t, "a", "b", "c")
	put := b.put(0, "a", "1")
	if r := b.h.Ops[put].Resp; r.Revision != 2 || r.Put.PrevKV != nil {
		t.Fatalf("first put: %+v", r)
	}
	b.put(0, "a", "2")
	b.put(0, "b", "x")
	// One revision for a txn that writes two keys.
	two := b.do(0, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Put: &history.PutRequest{Key: "a", Value: "3", PrevKV: true}},
		{Put: &history.PutRequest{Key: "c", Value: "y"}},
		{Range: &history.RangeRequest{Key: "c"}},
	}}})
	r := b.h.Ops[two].Resp
	if r.Revision != 5 || !r.Txn.Succeeded {
		t.Fatalf("txn: %+v", r)
	}
	if p := r.Txn.Results[0].Put.PrevKV; p == nil || *p != (history.KeyValue{Key: "a", Value: "2", CreateRevision: 2, ModRevision: 3, Version: 2}) {
		t.Fatalf("txn prev value: %+v", p)
	}
	if kvs := r.Txn.Results[2].Range.KVs; len(kvs) != 1 || kvs[0] != (history.KeyValue{Key: "c", Value: "y", CreateRevision: 5, ModRevision: 5, Version: 1}) {
		t.Fatalf("txn reads its own write: %+v", kvs)
	}
	// A failed compare that only reads does not bump the revision.
	failed := b.cas(0, "a", 2, "z")
	if r := b.h.Ops[failed].Resp; r.Revision != 5 || r.Txn.Succeeded || r.Txn.Results[0].Range.KVs[0].Value != "3" {
		t.Fatalf("failed cas: %+v", r)
	}
	// A delete of a missing key does not bump it either.
	if r := b.h.Ops[b.del(0, "missing")].Resp; r.Revision != 5 || r.Delete.Deleted != 0 {
		t.Fatalf("delete of a missing key: %+v", r)
	}
	if r := b.h.Ops[b.del(0, "a")].Resp; r.Revision != 6 || r.Delete.Deleted != 1 {
		t.Fatalf("delete: %+v", r)
	}
	// Recreating resets version and create revision.
	b.put(0, "a", "4")
	kv := b.h.Ops[b.get(0, "a")].Resp.Range.KVs[0]
	if kv != (history.KeyValue{Key: "a", Value: "4", CreateRevision: 7, ModRevision: 7, Version: 1}) {
		t.Fatalf("recreated key: %+v", kv)
	}
	// Prefix range with a limit, count only and keys only.
	all := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys, Limit: 2}})].Resp.Range
	if all.Count != 3 || !all.More || len(all.KVs) != 2 || all.KVs[0].Key != "a" || all.KVs[1].Key != "b" {
		t.Fatalf("limited range: %+v", all)
	}
	count := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", End: "c", CountOnly: true}})].Resp.Range
	if count.Count != 2 || len(count.KVs) != 0 || count.More {
		t.Fatalf("count only: %+v", count)
	}
	keys := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "b", KeysOnly: true}})].Resp.Range
	if keys.KVs[0].Value != "" || keys.KVs[0].Version != 1 {
		t.Fatalf("keys only: %+v", keys)
	}
	// Compaction boundaries: a read at the compaction revision works, one
	// below fails, and so does one above the current revision.
	if o := b.h.Ops[b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 5}})]; o.Err != nil {
		t.Fatalf("compact: %+v", o.Err)
	}
	if o := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 5}})]; o.Err != nil || o.Resp.Range.KVs[0].Value != "3" || o.Resp.Revision != 7 {
		t.Fatalf("read at the compaction revision: %+v %+v", o.Resp, o.Err)
	}
	if o := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 4}})]; o.Err != errCompacted {
		t.Fatalf("read below the compaction revision: %+v", o.Err)
	}
	if o := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 8}})]; o.Err != errFutureRev {
		t.Fatalf("read above the current revision: %+v", o.Err)
	}
	// etcd reads at the current revision for a revision below zero.
	if o := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: -1}})]; o.Err != nil || o.Resp.Range.KVs[0].Value != "4" {
		t.Fatalf("read at revision -1: %+v %+v", o.Resp, o.Err)
	}
	txn := func(rev int64) *history.Error {
		return b.h.Ops[b.do(0, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Range: &history.RangeRequest{Key: "a", Revision: rev}}}}})].Err
	}
	if err := txn(4); err != errCompacted {
		t.Fatalf("txn read below the compaction revision: %+v", err)
	}
	if err := txn(8); err != errFutureRev {
		t.Fatalf("txn read above the current revision: %+v", err)
	}
	if o := b.h.Ops[b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 5}})]; o.Err != errCompacted {
		t.Fatalf("compact at the compaction revision again: %+v", o.Err)
	}
	if o := b.h.Ops[b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 8}})]; o.Err != errFutureRev {
		t.Fatalf("compact above the current revision: %+v", o.Err)
	}
	// Leases: a put with a missing lease fails, a revoke deletes the keys
	// of the lease in one revision.
	if o := b.h.Ops[b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "l", Lease: 9}})]; o.Err != errLeaseNotFound {
		t.Fatalf("put with a missing lease: %+v", o.Err)
	}
	b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 60}})
	b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "l", Lease: 9}})
	b.do(0, history.Request{Put: &history.PutRequest{Key: "c", Value: "l", Lease: 9}})
	if r := b.h.Ops[b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}})].Resp; r.Revision != 10 {
		t.Fatalf("revoke: %+v", r)
	}
	if r := b.h.Ops[b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}})].Resp.Range; r.Count != 1 {
		t.Fatalf("after revoke: %+v", r)
	}
}

func TestCompareTargets(t *testing.T) {
	b := newBuilder(t, "k")
	b.put(0, "k", "v")
	b.put(0, "k", "w")
	s := b.s
	for _, c := range []struct {
		cmp  history.Compare
		want bool
	}{
		{history.Compare{Key: "k", Target: history.TargetVersion, Result: history.Equal, Number: 2}, true},
		{history.Compare{Key: "k", Target: history.TargetVersion, Result: history.Greater, Number: 2}, false},
		{history.Compare{Key: "k", Target: history.TargetCreate, Result: history.Equal, Number: 2}, true},
		{history.Compare{Key: "k", Target: history.TargetMod, Result: history.Less, Number: 4}, true},
		{history.Compare{Key: "k", Target: history.TargetMod, Result: history.NotEqual, Number: 3}, false},
		{history.Compare{Key: "k", Target: history.TargetValue, Result: history.Equal, Value: "w"}, true},
		{history.Compare{Key: "k", Target: history.TargetValue, Result: history.Greater, Value: "v"}, true},
		{history.Compare{Key: "k", Target: history.TargetLease, Result: history.Equal, Number: 0}, true},
		// A missing key has version 0, which is how kube-apiserver
		// creates: MOD == 0.
		{history.Compare{Key: "missing", Target: history.TargetMod, Result: history.Equal, Number: 0}, true},
		{history.Compare{Key: "missing", Target: history.TargetVersion, Result: history.Equal, Number: 0}, true},
		// etcd fails every value compare on a missing key.
		{history.Compare{Key: "missing", Target: history.TargetValue, Result: history.Equal, Value: ""}, false},
		{history.Compare{Key: "missing", Target: history.TargetValue, Result: history.NotEqual, Value: "x"}, false},
	} {
		if got := s.compare(c.cmp); got != c.want {
			t.Errorf("compare %+v = %v, want %v", c.cmp, got, c.want)
		}
	}
}

// TestTxnEventPrevKV checks the previous values of the events of a txn
// that writes a key and then deletes it: the key as it was before the
// txn, as etcd v3.7.2 sends them (#203), and none for a key that did not
// exist then.
func TestTxnEventPrevKV(t *testing.T) {
	s := newState([]string{"a", "b"})
	s.step(history.Request{Put: &history.PutRequest{Key: "a", Value: "old"}}, true)
	o := s.step(history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Put: &history.PutRequest{Key: "a", Value: "new"}},
		{Put: &history.PutRequest{Key: "b", Value: "new"}},
		{Delete: &history.DeleteRequest{Key: "a", End: "c"}},
	}}}, true)
	old := history.KeyValue{Key: "a", Value: "old", CreateRevision: 2, ModRevision: 2, Version: 1}
	want := []history.Event{
		{Type: history.EventPut, KV: history.KeyValue{Key: "a", Value: "new", CreateRevision: 2, ModRevision: 3, Version: 2}, PrevKV: &old},
		{Type: history.EventPut, KV: history.KeyValue{Key: "b", Value: "new", CreateRevision: 3, ModRevision: 3, Version: 1}},
		{Type: history.EventDelete, KV: history.KeyValue{Key: "a", ModRevision: 3}, PrevKV: &old},
		{Type: history.EventDelete, KV: history.KeyValue{Key: "b", ModRevision: 3}},
	}
	if len(o.events) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(o.events), len(want), o.events)
	}
	for i := range want {
		if !sameEvent(o.events[i], want[i]) {
			t.Errorf("event %d: %s", i, eventDiff(want[i], o.events[i], true))
		}
	}
}
