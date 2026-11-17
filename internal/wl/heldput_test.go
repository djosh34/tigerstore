package wl

import (
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// A held reply whose record is a put its own revision deleted again (a
// DeleteRange's prev_kv after a put of its key in the same Txn) holds
// that revision (reserve.go), and trimming keeps the put: it keeps the
// states from the revision before the lowest held one (trimRev). From
// the held one, the deletion at it dropped the put (the re-check of
// e710ebb9, Opus's probe).
func TestAHeldPutDeletedInItsOwnRevisionOutlivesTrimming(t *testing.T) {
	st, l := newTestStore(t)
	defer st.close()               //nolint:errcheck // a test's store
	putValue(t, st, l, "/a", "a1") // 2
	id, _ := st.find([]byte("/a"))
	st.rev++
	if _, err := st.put(id, []byte("a3"), 0, st.rev, l.add("a3"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.del(id, st.rev, true); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	st.holdRead(3)                 // holdReply: max(3, compactRev 0)
	putValue(t, st, l, "/b", "b1") // 4
	st.compact(4)
	st.trimSteps(1 << 20)
	rec, err := st.putAt(id, 3)
	if err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("the held reply's record /a at 3 reads %q, err %v", recValue(rec), err)
	}
}

// A Txn may put a key and then delete it at the same revision: checkRequest
// admits a put of /a and a delete from /a on (range_end "\x00"), as
// etcd's checkIntervals does. A watch sent in pieces at that revision
// holds it and the one before, and reads the put (putAt). The put is
// seen at no revision after it, yet a checkpoint takes its value in, and
// the held watch reads it after the log is cut (the re-check of e710ebb9,
// Opus's probe: visible at no held revision, its value was cut away).
func TestATxnsPutDeletedInItsRevisionAndHeldOutlivesTheCut(t *testing.T) {
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
	if err := checkRequest(kTxn, req, 0); err != nil {
		t.Fatalf("checkRequest refused the Txn: %v", err)
	}
	if err := n.applyTxn(req, l.add(string(req)), nil, nil); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	if st.rev != 3 {
		t.Fatalf("rev %d after the Txn, want 3", st.rev)
	}
	id, _ := st.find([]byte("/a"))
	// A watch sent in pieces at 3 (bigwatch.go startBigWatch).
	st.holdRead(2)
	st.holdRead(3)
	putValue(t, st, l, "/b", "b1") // 4
	st.compact(4)
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
	rec, err := st.putAt(id, 3)
	if err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("the held watch's put event of /a at 3 reads %q, err %v", recValue(rec), err)
	}
}

// The same at the compacted revision: a watch catching up from the
// compacted revision 3 sends 3 in pieces, holding 2 and 3. ckptStates keeps
// the deletion at 3; the put at 3 beside it is taken in too.
func TestAPutDeletedAtTheCompactedRevisionAndHeldOutlivesTheCut(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	id, _ := st.find([]byte("/a"))
	st.rev++
	if _, err := st.put(id, []byte("a3"), 0, st.rev, l.add("a3"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.del(id, st.rev, false); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	st.compact(3)
	st.holdRead(2)
	st.holdRead(3)
	st.trimSteps(1 << 20)
	putValue(t, st, l, "/b", "b1") // 4
	if rec, err := st.putAt(id, 3); err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("before the checkpoint, putAt(/a, 3) = %q, %v", recValue(rec), err)
	}
	writeCkpt(t, n, 10)
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut == 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	dropRecords(st)
	rec, err := st.putAt(id, 3)
	if err != nil || string(recValue(rec)) != "a3" {
		t.Fatalf("the held watch's put of /a at 3 reads %q, err %v", recValue(rec), err)
	}
}

// A watch that comes to a compacted revision once a checkpoint scanned
// the key reads a put its own Txn deleted there: the checkpoint takes it
// in, as no hold could name it when the key was scanned (the re-check of
// 9b538554, Astra's probe: after the log was cut the watch's put event
// read nothing).
func TestALateWatchOfAPutDeletedAtTheCompactedRevisionOutlivesTheCut(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	n.sizes, n.connOut = sizeFor(256<<20), 64<<10
	oldValue, newValue := strings.Repeat("o", 1200<<10), strings.Repeat("n", 1200<<10)
	putValue(t, st, l, "/a", oldValue)
	id, _ := st.find([]byte("/a"))
	c, _ := flowClient(t)
	c.node = n
	rec, _ := st.live(id)
	n.holdTxn(c, c.newStream(3, kTxn), true, []opRes{{kind: kRange, rev: 2, kvs1: 1, count: 1}}, 0, 1, [][]byte{rec})
	defer n.endHeld(n.heldReplies[0])
	txn := &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("/a"), Value: []byte(newValue)}}},
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("/a"), RangeEnd: []byte{0}}}},
	}}
	req, err := proto.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRequest(kTxn, req, 0); err != nil {
		t.Fatal(err)
	}
	if err := n.applyTxn(req, l.add(string(req)), nil, nil); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	st.compact(3)
	st.trimSteps(1 << 20)
	putValue(t, st, l, "/b", "b4")
	n.ckptFrom = int64(len(l.b))
	if err := n.startCkpt(10, 1, int64(len(l.b))); err != nil {
		t.Fatal(err)
	}
	for n.ckpt.phase == ckptScanning {
		n.scanKeys(n.ckpt)
	}
	// A watch at 3 comes once the key was scanned.
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(5, kWatch)
	s.watch = true
	wi := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, id: 1, key: []byte("/a"), syncRev: 3, prevKV: true})
	s.wids = append(s.wids, wi)
	if size, err := n.revSize(&n.w.ws[wi], revStart(st, 3)); err != nil || int64(size) <= n.oneShot() {
		t.Fatalf("not a large watch: size %d, %v", size, err)
	}
	if !n.startBigWatch(wi, &n.w.ws[wi], c, s, revStart(st, 3)) {
		t.Fatal("late watch did not start")
	}
	defer n.endBigWatch(n.bigWatches[0])
	if rec, err := st.putAt(id, 3); err != nil || string(recValue(rec)) != newValue {
		t.Fatalf("before cut: %d bytes, %v", len(recValue(rec)), err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(true); err != nil {
			t.Fatal(err)
		}
	}
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut == 0 {
		t.Fatal("no cut")
	}
	dropRecords(st)
	rec, err = st.putAt(id, 3)
	if err != nil || string(recValue(rec)) != newValue {
		t.Fatalf("late watch put event: %d bytes, %v", len(recValue(rec)), err)
	}
}
