package main

import (
	"sync/atomic"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvcc "go.etcd.io/etcd/api/v3/mvccpb"
)

func TestCASRequest(t *testing.T) {
	r := casRequest([]byte("k"), []byte("v"), 42)
	if len(r.Compare) != 1 || r.Compare[0].Target != pb.Compare_MOD || r.Compare[0].Result != pb.Compare_EQUAL || r.Compare[0].GetModRevision() != 42 || len(r.Success) != 1 || r.Success[0].GetRequestPut() == nil || len(r.Failure) != 1 || r.Failure[0].GetRequestRange() == nil {
		t.Fatalf("bad CAS: %v", r)
	}
}

func TestCASResponse(t *testing.T) {
	h := &pb.ResponseHeader{Revision: 43, RaftTerm: 1}
	passed := &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: h}}}}}
	if rev, err := checkCAS(passed, []byte("k"), []byte("v")); err != nil || rev != 43 {
		t.Fatalf("pass: %d %v", rev, err)
	}
	failed := &pb.TxnResponse{Header: h, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: h, Count: 1, Kvs: []*mvcc.KeyValue{{Key: []byte("k"), Value: []byte("v"), ModRevision: 42}}}}}}}
	if rev, err := checkCAS(failed, []byte("k"), []byte("v")); err != nil || rev != 42 {
		t.Fatalf("fallback: %d %v", rev, err)
	}
	failed.Responses[0].GetResponseRange().Kvs[0].Value = []byte("bad")
	if _, err := checkCAS(failed, []byte("k"), []byte("v")); err == nil {
		t.Fatal("accepted bad fallback value")
	}
	if _, err := checkCAS(&pb.TxnResponse{}, []byte("k"), []byte("v")); err == nil {
		t.Fatal("accepted missing response")
	}
}

func TestStoreRevision(t *testing.T) {
	var slot atomic.Int64
	slot.Store(42)
	storeRevision(&slot, 41)
	if slot.Load() != 42 {
		t.Fatal("late response rolled back revision")
	}
	storeRevision(&slot, 43)
	if slot.Load() != 43 {
		t.Fatal("new response did not advance revision")
	}
}

func TestParseIO(t *testing.T) {
	r, w, err := parseIO([]byte("rchar: 123\nsyscr: 45\nsyscw: 67\n"))
	if err != nil || r != 45 || w != 67 {
		t.Fatalf("bad io: %d %d %v", r, w, err)
	}
	for _, input := range []string{"", "syscr: 45", "syscr: x\nsyscw: 67", "syscr: 45\nsyscr: 45\nsyscw: 67"} {
		if _, _, err := parseIO([]byte(input)); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}
