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

// Regressions for the second review.

func TestLegalRejectionWhereTheModelRejectsToo(t *testing.T) {
	b := newBuilder(t, "a")
	put := b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 5}})
	h := b.done()
	h.Ops[put].Err = &history.Error{Code: "ResourceExhausted", Message: "etcdserver: too many requests"}
	mustPass(t, "Linearizable", Linearizable(h, timeout))
}

func TestSerializableReadsOnALaggingNode(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 3}})
	h := b.done()
	end := h.Ops[len(h.Ops)-1].Return
	h.Ops = append(h.Ops,
		// The node has not applied revision 3 yet.
		history.Op{Client: 1, Call: end + 1, Return: end + 2, Req: history.Request{Range: &history.RangeRequest{Key: "a", Revision: 3, Serializable: true}}, Err: errFutureRev},
		// Nor the compaction.
		history.Op{Client: 1, Call: end + 3, Return: end + 4, Req: history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2, Serializable: true}}, Resp: &history.Response{Revision: 2, Range: &history.RangeResponse{
			KVs: []history.KeyValue{{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 2, Version: 1}}, Count: 1,
		}}},
	)
	mustPass(t, "Revisions", Revisions(h))
}

func TestDurableNoOpAgainstTheLog(t *testing.T) {
	t.Run("txn reports the wrong branch", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.put(0, "a", "1")
		txn := b.do(0, history.Request{Txn: &history.TxnRequest{
			Compare: []history.Compare{{Key: "a", Target: history.TargetMod, Result: history.Equal, Number: 2}},
			Success: []history.TxnOp{{Delete: &history.DeleteRequest{Key: "missing"}}},
		}})
		h := b.done()
		mustPass(t, "Durable", Durable(h))
		h.Ops[txn].Resp.Txn = &history.TxnResponse{Succeeded: false}
		mustFail(t, "Durable", Durable(h), "its compares select succeeded=true")
	})
	t.Run("delete finds nothing while the key exists", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.put(0, "a", "1")
		h := b.done()
		h.Ops = append(h.Ops, history.Op{Client: 1, Call: 100, Return: 110, Req: history.Request{Delete: &history.DeleteRequest{Key: "a"}}, Resp: &history.Response{Revision: 2, Delete: &history.DeleteResponse{}}})
		mustFail(t, "Durable", Durable(h), "found no key while the log holds 1")
	})
}

func TestUnsupportedHistoricalTxnRead(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	h := b.done()
	h.Ops = append(h.Ops, history.Op{
		Client: 1, Call: 100, Return: 110,
		Req:  history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Range: &history.RangeRequest{Key: "a", Revision: 2}}}}},
		Resp: &history.Response{Revision: 3, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Range: &history.RangeResponse{}}}}},
	})
	mustFail(t, "Linearizable", Linearizable(h, timeout), "does not support")
}

func TestZeroTimeout(t *testing.T) {
	mustFail(t, "Linearizable", Linearizable(&history.History{}, 0), "positive timeout")
}

// Regressions for the fix checks.

func TestSerializableCompactedWithoutCompaction(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	read := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2, Serializable: true}})
	h := b.done()
	h.Ops[read].Resp, h.Ops[read].Err = nil, errCompacted
	mustFail(t, "Revisions", Revisions(h), "no compaction that could cause it")
}

func TestSameRevisionAcrossKeysOnlyReads(t *testing.T) {
	b := newBuilder(t, "a")
	put := b.put(0, "a", "nonempty")
	b.get(1, "a")
	b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", KeysOnly: true}})
	last := b.get(1, "a")
	h := b.done()
	// The client never learned that its put committed.
	h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
	mustPass(t, "Revisions", Revisions(h))
	h.Ops[last].Resp.Range.KVs[0].Value = ""
	mustFail(t, "Revisions", Revisions(h), "was seen as")
}
