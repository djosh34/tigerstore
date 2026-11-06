package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func capacityProblem(keys []string, reqs ...history.Request) *revisionOrder {
	s := newState(keys)
	s.rev = 2
	for k, key := range keys {
		s.kvs[k] = history.KeyValue{Key: key, CreateRevision: 2, ModRevision: 2, Version: 1}
	}
	w := &witness{h: &history.History{}, live: s, reqs: reqs}
	r := &revisionOrder{w: w, writers: map[int64]int{}, resolved: map[int64]int{}, reserved: map[int]int64{}, lives: map[string]map[life]int64{}, counts: map[int64][]count{}, used: make([]bool, len(reqs)), lb: make([]int64, len(reqs))}
	for i, req := range reqs {
		w.h.Ops = append(w.h.Ops, history.Op{Req: req, Err: &history.Error{Ambiguous: true}})
		r.ambiguous = append(r.ambiguous, i)
	}
	return r
}

func TestCapacityJointObligations(t *testing.T) {
	putA := history.Request{Put: &history.PutRequest{Key: "a"}}
	putB := history.Request{Put: &history.PutRequest{Key: "b"}}
	delA := history.Request{Delete: &history.DeleteRequest{Key: "a"}}
	for _, tc := range []struct {
		name    string
		reqs    []history.Request
		deleteA bool
		want    bool
	}{
		{"separate puts need two revisions", []history.Request{putA, putB}, false, false},
		{"one transaction can cover two puts", []history.Request{{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: putA.Put}, {Put: putB.Put}}}}}, false, true},
		{"a delete and put need two revisions", []history.Request{delA, putB}, true, false},
		{"a mixed transaction covers both", []history.Request{{Txn: &history.TxnRequest{Success: []history.TxnOp{{Delete: delA.Delete}, {Put: putB.Put}}}}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := capacityProblem([]string{"a", "b"}, tc.reqs...)
			r.lives["b"] = map[life]int64{{2, 3, 2}: 3}
			if tc.deleteA {
				r.countRevs = []int64{3}
				r.counts[3] = []count{{rev: 3, lo: 0, hi: 1, n: 0}}
			} else {
				r.lives["a"] = map[life]int64{{2, 3, 2}: 3}
			}
			r.indexCapacity()
			if got := r.capacityReachable(r.w.live, -1); got != tc.want {
				t.Fatalf("reachable=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestCapacityCountsMultiplePutsInOneRevision(t *testing.T) {
	r := capacityProblem([]string{"a"}, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a"}}, {Put: &history.PutRequest{Key: "a"}}}}})
	r.lives["a"] = map[life]int64{{2, 3, 3}: 3}
	r.indexCapacity()
	if !r.capacityReachable(r.w.live, -1) {
		t.Fatal("a transaction can increment a version twice at one revision")
	}
	if r.capacityReachable(r.w.live, 0) {
		t.Fatal("skip cannot be reused")
	}
	r.used[0] = true
	if r.capacityReachable(r.w.live, -1) {
		t.Fatal("a used operation cannot be reused")
	}
}

func TestCapacityResolvedRevisionsAreNotFree(t *testing.T) {
	for _, key := range []string{"a", "b"} {
		t.Run(key, func(t *testing.T) {
			r := capacityProblem([]string{"a", "b"}, history.Request{Put: &history.PutRequest{Key: key}}, history.Request{Put: &history.PutRequest{Key: "a"}})
			r.resolved[3] = 0
			r.reserved[0] = 3
			r.lives["a"] = map[life]int64{{2, 4, 3}: 4}
			r.indexCapacity()
			if got := r.capacityReachable(r.w.live, -1); got != (key == "a") {
				t.Fatalf("reachable=%v for fixed put of %s", got, key)
			}
		})
	}
}

func TestCapacityDeleteAndRecreateAtFixedRevision(t *testing.T) {
	r := capacityProblem([]string{"a"}, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Delete: &history.DeleteRequest{Key: "a"}}, {Put: &history.PutRequest{Key: "a"}}}}})
	r.writers[3] = 0
	r.ambiguous = nil
	r.lives["a"] = map[life]int64{{3, 3, 1}: 3}
	r.indexCapacity()
	if !r.capacityReachable(r.w.live, -1) {
		t.Fatal("the fixed creation can also delete the old life")
	}
}

func TestCapacityRevokeAllowsFutureReattachment(t *testing.T) {
	r := capacityProblem([]string{"a"}, history.Request{Put: &history.PutRequest{Key: "a", Lease: 7}}, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}})
	r.indexCapacity()
	if !r.capacity.deletesKey(r.w.live, r.w.reqs[1], 0) {
		t.Fatal("a future put may attach the currently unleased key")
	}
	if r.capacity.deletesKey(r.w.live, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 8}}, 0) {
		t.Fatal("an unrelated lease cannot delete the key")
	}
}
