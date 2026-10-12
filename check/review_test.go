package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// Regressions for the first review of the checkers.

// txnReadAtStart puts a=old at revision 2, then runs a txn that puts a=new
// and reads a at revision 2.
func txnReadAtStart(t *testing.T) (*history.History, int) {
	t.Helper()
	b := newBuilder(t, "a")
	b.put(0, "a", "old")
	txn := b.do(1, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Put: &history.PutRequest{Key: "a", Value: "new"}},
		{Range: &history.RangeRequest{Key: "a", Revision: 2}},
	}}})
	return b.done(), txn
}

func TestTxnReadAtItsStartRevision(t *testing.T) {
	h, txn := txnReadAtStart(t)
	kvs := h.Ops[txn].Resp.Txn.Results[1].Range.KVs
	if len(kvs) != 1 || kvs[0].Value != "old" || kvs[0].ModRevision != 2 {
		t.Fatalf("txn read at its start revision: %+v", kvs)
	}
	mustPass(t, "Linearizable", Linearizable(h, timeout))

	h, txn = txnReadAtStart(t)
	h.Ops[txn].Resp.Txn.Results[1].Range.KVs[0] = history.KeyValue{Key: "a", Value: "new", CreateRevision: 2, ModRevision: 3, Version: 2}
	mustFail(t, "Linearizable", Linearizable(h, timeout), "not linearizable")
}

func TestWatchWithoutStartAndCreated(t *testing.T) {
	b := newBuilder(t, "a")
	b.watch(0, "a", "", false)
	b.put(1, "a", "1")
	b.put(1, "a", "2")
	h := b.done()
	w := &h.Watches[0]
	// Drop the created response and the first event.
	w.Responses = w.Responses[2:]
	mustFail(t, "Watches", Watches(h), "no start revision")
}

func TestEventAboveItsHeader(t *testing.T) {
	b := newBuilder(t, "a")
	b.watch(0, "a", "", false)
	b.put(1, "a", "1")
	b.del(1, "a")
	h := b.done()
	w := &h.Watches[0]
	last := &w.Responses[len(w.Responses)-1]
	if last.Events[0].Type != history.EventDelete {
		t.Fatal("the last response must hold the delete")
	}
	last.Revision = 2
	mustFail(t, "Watches", Watches(h), "above its response's header revision")
	mustFail(t, "Revisions", Revisions(h), "above its response's header revision")
}

func TestEmptyValueIsNotKeysOnly(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "v")
	b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", KeysOnly: true}})
	get := b.get(0, "a")
	mustPass(t, "Revisions", Revisions(b.done()))

	h := b.done()
	h.Ops[get].Resp.Range.KVs[0].Value = ""
	mustFail(t, "Revisions", Revisions(h), "but op 0 put")
}

func TestDurableNoOpTxnWithDelete(t *testing.T) {
	b := newBuilder(t, "a", "absent")
	b.put(0, "a", "old")
	b.do(0, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Delete: &history.DeleteRequest{Key: "absent"}},
		{Range: &history.RangeRequest{Key: "a"}},
	}}})
	mustPass(t, "Durable", Durable(b.done()))
}
