package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// roomOrder is a revision order of the ambiguous ops reqs on keys, at
// revision 2 with every key present at version 1, for roomReachable.
func roomOrder(keys []string, reqs ...history.Request) *revisionOrder {
	s := newState(keys)
	s.rev = 2
	for k, key := range keys {
		s.kvs[k] = history.KeyValue{Key: key, CreateRevision: 2, ModRevision: 2, Version: 1}
	}
	w := &witness{h: &history.History{}, live: s, reqs: reqs}
	r := &revisionOrder{w: w, writers: map[int64]int{}, revokes: map[int64][]int{}, resolved: map[int64]int{}, reserved: map[int]int64{}, lives: map[string]map[life]int64{}, counts: map[int64][]count{}, used: make([]bool, len(reqs)), lb: make([]int64, len(reqs))}
	for i, req := range reqs {
		w.h.Ops = append(w.h.Ops, history.Op{Req: req, Err: &history.Error{Ambiguous: true}})
		r.ambiguous = append(r.ambiguous, i)
	}
	return r
}

// index builds what roomReachable reads.
func (r *revisionOrder) indexForRoom() {
	r.indexCounts()
	r.indexRoom()
}

// TestRoomBoundsKeysTogether has two revisions to fill and needs that
// each key alone can meet but not both in the revisions there are, unless
// one op covers both.
func TestRoomBoundsKeysTogether(t *testing.T) {
	putA := history.Request{Put: &history.PutRequest{Key: "a"}}
	putB := history.Request{Put: &history.PutRequest{Key: "b"}}
	delA := history.Request{Delete: &history.DeleteRequest{Key: "a"}}
	for _, c := range []struct {
		name    string
		reqs    []history.Request
		deleteA bool
		want    bool
	}{
		{"two puts need two revisions", []history.Request{putA, putB}, false, false},
		{"one transaction puts both", []history.Request{{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: putA.Put}, {Put: putB.Put}}}}}, false, true},
		{"a delete and a put need two revisions", []history.Request{delA, putB}, true, false},
		{"one transaction deletes one and puts the other", []history.Request{{Txn: &history.TxnRequest{Success: []history.TxnOp{{Delete: delA.Delete}, {Put: putB.Put}}}}}, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := roomOrder([]string{"a", "b"}, c.reqs...)
			// One revision ahead, at 3: b at version 2, and a at version 2
			// or gone.
			r.lives["b"] = map[life]int64{{2, 3, 2}: 3}
			if c.deleteA {
				r.countRevs = []int64{3}
				r.counts[3] = []count{{rev: 3, lo: 0, hi: 1, n: 0}}
			} else {
				r.lives["a"] = map[life]int64{{2, 3, 2}: 3}
			}
			r.indexForRoom()
			if got := r.roomReachable(r.w.live, -1); got != c.want {
				t.Fatalf("roomReachable is %v, want %v", got, c.want)
			}
		})
	}
}

// TestRoomCountsPutsOfOneRevision has a transaction put one key twice, two
// versions in one revision, and checks that skip and a used op do not
// count.
func TestRoomCountsPutsOfOneRevision(t *testing.T) {
	put := &history.PutRequest{Key: "a"}
	r := roomOrder([]string{"a"}, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: put}, {Put: put}}}})
	r.lives["a"] = map[life]int64{{2, 3, 3}: 3}
	r.indexForRoom()
	if !r.roomReachable(r.w.live, -1) {
		t.Fatal("a transaction that puts a key twice adds two versions in one revision")
	}
	if r.roomReachable(r.w.live, 0) {
		t.Fatal("the op being placed counted")
	}
	r.used[0] = true
	if r.roomReachable(r.w.live, -1) {
		t.Fatal("a used op counted")
	}
}

// TestRoomTakesResolvedRevisions has the revision of a resolved writer:
// it is no free revision, and covers what it puts for free.
func TestRoomTakesResolvedRevisions(t *testing.T) {
	for _, key := range []string{"a", "b"} {
		t.Run(key, func(t *testing.T) {
			r := roomOrder([]string{"a", "b"}, history.Request{Put: &history.PutRequest{Key: key}}, history.Request{Put: &history.PutRequest{Key: "a"}})
			r.resolved[3] = 0
			r.reserved[0] = 3
			r.lives["a"] = map[life]int64{{2, 4, 3}: 4}
			r.indexForRoom()
			if got := r.roomReachable(r.w.live, -1); got != (key == "a") {
				t.Fatalf("roomReachable is %v with a resolved put of %s at 3", got, key)
			}
		})
	}
}

// TestRoomDeletesAndCreatesAtOneRevision has an acked transaction that
// deletes a key and creates it again in one revision, the creation of a
// life clients saw.
func TestRoomDeletesAndCreatesAtOneRevision(t *testing.T) {
	r := roomOrder([]string{"a"}, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Delete: &history.DeleteRequest{Key: "a"}}, {Put: &history.PutRequest{Key: "a"}}}}})
	r.w.h.Ops[0].Err, r.w.h.Ops[0].Resp = nil, &history.Response{Revision: 3, Txn: &history.TxnResponse{Succeeded: true}}
	r.writers[3] = 0
	r.ambiguous = nil
	r.lives["a"] = map[life]int64{{3, 3, 1}: 3}
	r.indexForRoom()
	if !r.roomReachable(r.w.live, -1) {
		t.Fatal("the writer that creates the key may delete its old life")
	}
}

// TestRoomRevokeDeletesKeysAPutMayAttach: a revoke may delete a key that
// does not hold its lease now, if a put may put the key on it later.
func TestRoomRevokeDeletesKeysAPutMayAttach(t *testing.T) {
	r := roomOrder([]string{"a"}, history.Request{Put: &history.PutRequest{Key: "a", Lease: 7}}, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}})
	r.indexForRoom()
	if got := r.room.deleted(r.w.live, r.w.reqs[1]); len(got) != 1 {
		t.Fatalf("a revoke of lease 7 deletes keys %v, want key 0, which a put may put on it", got)
	}
	if got := r.room.deleted(r.w.live, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 8}}); len(got) != 0 {
		t.Fatalf("a revoke of lease 8, which no key uses, deletes keys %v", got)
	}
}

// TestRoomRevokeOfALeaseAPutAttaches has key a with no lease now and a
// count that needs it gone at 4: an ambiguous put may put it on lease 7 at
// 3 and an ambiguous revoke of 7 delete it at 4, so the revoke covers the
// deletion though a holds no lease now.
func TestRoomRevokeOfALeaseAPutAttaches(t *testing.T) {
	r := roomOrder([]string{"a"}, history.Request{Put: &history.PutRequest{Key: "a", Lease: 7}}, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}})
	r.counts[4] = []count{{rev: 4, lo: 0, hi: 1, n: 0}}
	r.indexForRoom()
	if !r.roomReachable(r.w.live, -1) {
		t.Fatal("a put on lease 7 and a revoke of 7 can delete the key by 4")
	}
}
