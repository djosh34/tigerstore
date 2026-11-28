package wl

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// A held reply whose only, so least, record is a put its own Txn deleted
// at R holds R. Compaction then passes R, and a checkpoint takes the put
// in: its walk goes on past the deletion at R to the put beside it (the
// re-check of 6c8b0fcc, Opus's probe: it stopped at the deletion, and
// the reply was aborted after the log was cut).
func TestAHeldReplyOfAPutDeletedInItsRevisionOutlivesTheCut(t *testing.T) {
	c, s := flowClient(t)
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	txn := &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("/a"), Value: []byte("a3")}}},
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("/a"), RangeEnd: []byte{0}, PrevKv: true}}},
	}}
	req, err := proto.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRequest(kTxn, req, 0, maxRequestBytes); err != nil {
		t.Fatalf("checkRequest refused the Txn: %v", err)
	}
	if err := n.applyTxn(req, l.add(string(req)), nil, nil); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	id, _ := st.find([]byte("/a"))
	prev, err := st.putAt(id, 3) // the DeleteRange's prev_kv
	if err != nil || string(recValue(prev)) != "a3" {
		t.Fatalf("prev %q %v", recValue(prev), err)
	}
	// The Txn's reply held for the reserve: the delete's prev_kv only.
	res := []opRes{{kind: kDel, kvs0: 0, kvs1: 1, count: 1}}
	n.holdTxn(c, s, true, res, 0, 1, [][]byte{prev})
	hr := n.heldReplies[0]
	if hr.hold != 3 || !st.stateHeld(id, 3) {
		t.Fatalf("the held reply holds %d, /a at 3 held %v; want 3, held", hr.hold, st.stateHeld(id, 3))
	}
	putValue(t, st, l, "/b", "b1") // 4
	putValue(t, st, l, "/c", "c1") // 5
	st.compact(5)                  // another client compacts past R
	st.trimSteps(1 << 20)
	if rec, err := st.putAt(id, 3); err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("before the checkpoint, putAt(/a, 3) = %q, %v", recValue(rec), err)
	}
	writeCkpt(t, n, 10)
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut == 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	dropRecords(st)
	rec, err := st.putAt(id, 3) // stepHeld's read
	if err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("the held reply's prev_kv of /a at 3 reads %q, err %v", recValue(rec), err)
	}
	n.endHeld(hr)
}

// A held reply of two records, /a put at 2 and /b at 3, holds 2 for
// trimming and each record's state. /b is overwritten at 4, compaction
// passes it, and a checkpoint cuts the log: /b at 3 is seen at no held
// revision, so the state the reply names (stateHeld) takes it in (the
// re-check of 6c8b0fcc, Opus's probe).
func TestAHeldReplysNewerRecordOverwrittenOutlivesTheCut(t *testing.T) {
	c, s := flowClient(t)
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a2") // 2
	putValue(t, st, l, "/b", "b3") // 3
	a, _ := st.find([]byte("/a"))
	b, _ := st.find([]byte("/b"))
	ra, _ := st.live(a)
	rb, _ := st.live(b)
	res := []opRes{{kind: kRange, rev: st.rev, kvs0: 0, kvs1: 2, count: 2}}
	n.holdTxn(c, s, true, res, 0, 1, [][]byte{ra, rb})
	hr := n.heldReplies[0]
	putValue(t, st, l, "/b", "b4") // 4
	putValue(t, st, l, "/c", "c5") // 5
	st.compact(5)
	st.trimSteps(1 << 20)
	writeCkpt(t, n, 10)
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut == 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	dropRecords(st)
	for _, r := range hr.refs {
		rec, err := st.putAt(r.id, r.mod)
		if err != nil || rec == nil {
			t.Fatalf("the held reply's record %s at %d reads %q, err %v", st.key(r.id), r.mod, recValue(rec), err)
		}
	}
	n.endHeld(hr)
}
