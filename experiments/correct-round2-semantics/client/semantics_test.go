package main

import (
	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"testing"
	"time"
)

func TestStampedExpiryNormalization(t *testing.T) {
	n := newNormalizer()
	var log []history.Request
	for _, m := range []proto.Message{
		&pb.LeaseGrantRequest{ID: 42, TTL: 6},
		&pb.PutRequest{Key: []byte("a"), Value: []byte("one"), Lease: 42},
		&pb.PutRequest{Key: []byte("b"), Value: []byte("two"), Lease: 42},
		&pb.LeaseKeepAliveRequest{ID: 42},
		&pb.LeaseCheckpoint{ID: 42, Remaining_TTL: 1},
		&pb.LeaseCheckpoint{ID: 42, Remaining_TTL: 2},
		&pb.LeaseCheckpoint{ID: 42, Remaining_TTL: 2},
		&pb.LeaseGrantRequest{ID: 42, TTL: 6},
		&pb.PutRequest{Key: []byte("c"), Value: []byte("new-generation"), Lease: 42},
		&pb.LeaseCheckpoint{ID: 42, Remaining_TTL: 2},
		&pb.LeaseCheckpoint{ID: 42, Remaining_TTL: 3},
	} {
		qs, e := n.normalize(m, true)
		if e != nil {
			t.Fatal(e)
		}
		log = append(log, qs...)
	}
	rev, e := finalRevision(log)
	if e != nil || rev != 6 {
		t.Fatalf("revision %d, error %v, log %+v", rev, e, log)
	}
	revokes := 0
	for _, q := range log {
		if q.LeaseRevoke != nil {
			revokes++
		}
	}
	if revokes != 2 || len(n.diagnostics) != 11 {
		t.Fatalf("revokes %d diagnostics %d", revokes, len(n.diagnostics))
	}
}
func TestLeaseAndCompactionFence(t *testing.T) {
	log := []history.Request{
		{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 6}},
		{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a", Value: "a", Lease: 9}}, {Put: &history.PutRequest{Key: "b", Value: "b", Lease: 9}}}}},
		{Compact: &history.CompactRequest{Revision: 2}},
		{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}},
		{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}},
		{Put: &history.PutRequest{Key: "a", Lease: 9}},
	}
	rev, e := finalRevision(log)
	if e != nil || rev != 3 {
		t.Fatalf("revision %d error %v", rev, e)
	}
}
func TestFourVerdictsWithMissingLog(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "raw.json")
	out := filepath.Join(dir, "checks.json")
	h := history.History{Target: 2, Ops: []history.Op{{Client: 0, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "k", Value: "v"}}, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}}}, Watches: []history.Watch{{Client: 100, Req: history.WatchRequest{Key: "k", StartRevision: 1, PrevKV: true}}}}
	if e := save(in, h); e != nil {
		t.Fatal(e)
	}
	if e := runChecks(config{history: in, out: out, checkerTimeout: time.Second}); e == nil {
		t.Fatal("missing log passed")
	}
	var results []checkerResult
	if e := load(out, &results); e != nil {
		t.Fatal(e)
	}
	if len(results) != 4 {
		t.Fatalf("got %d checker results", len(results))
	}
	for i, name := range []string{"Linearizable", "Watches", "Durable", "Revisions"} {
		if results[i].Name != name || results[i].Passed {
			t.Fatalf("missing log verdict %+v", results[i])
		}
	}
}

func TestNormalizationFailsClosed(t *testing.T) {
	n := newNormalizer()
	if _, e := n.normalize(&pb.AlarmRequest{}, false); e == nil {
		t.Fatal("alarm silently ignored")
	}
	if _, e := n.normalize(&pb.LeaseCheckpointRequest{}, true); e == nil {
		t.Fatal("unexpected checkpoint form accepted")
	}
	if _, e := n.normalize(&pb.PutRequest{Key: []byte{255}}, true); e == nil {
		t.Fatal("non-UTF8 accepted")
	}
}
