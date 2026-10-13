package core

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/batchcmd"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func wireCommand(t *testing.T, op proto.Message) (batchcmd.Command, []byte) {
	t.Helper()
	body, err := proto.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	path := batchcmd.PutPath
	if _, ok := op.(*pb.TxnRequest); ok {
		path = batchcmd.TxnPath
	}
	c, err := batchcmd.Decode(path, body)
	if err != nil {
		t.Fatal(err)
	}
	return c, body
}

func wireCAS(expected int64, value string) *pb.TxnRequest {
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("k"), Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: expected}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k"), Value: []byte(value), PrevKv: true}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k")}}}},
	}
}

func TestWireBatchDurabilityAndAlias(t *testing.T) {
	disk := simfs.New()
	s := startSingle(t, disk, 1)
	put, putBody := wireCommand(t, &pb.PutRequest{Key: []byte("k"), Value: []byte("first")})
	txn, txnBody := wireCommand(t, wireCAS(2, "second"))
	s.n.SubmitBatch(100, []batchcmd.Command{put, txn})
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if len(eff.Replies) != 0 || s.n.kv.Rev() != 1 {
		t.Fatal("write applied or replied before local durability")
	}
	if got := writesIn(t, eff.Persist); len(got) != 1 || got[0] != 2 {
		t.Fatalf("not one whole batch: %v", got)
	}
	for _, p := range s.n.timeouts {
		if p.command != nil || p.req.Op != nil {
			t.Fatal("pending record still pins receive body")
		}
	}
	// The core has copied the input bodies once into the entry. Poison the
	// receive buffers only, never the GC-owned entry while MVCC references it.
	for _, body := range [][]byte{putBody, txnBody} {
		for i := range body {
			body[i] = 0xff
		}
	}
	var entryValue []byte
	for _, j := range eff.Persist {
		for _, e := range j.Entries {
			if err := DecodeBatch(e.GetData(), func(_, _, id uint64, op proto.Message) error {
				if id == 101 {
					entryValue = op.(*pb.TxnRequest).Success[0].GetRequestPut().Value
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if string(entryValue) != "second" {
		t.Fatalf("entry changed with receive buffer: %q", entryValue)
	}
	s.persistAll(eff.Persist)
	replies := s.run()
	if len(replies) != 2 || replies[0].ID != 100 || replies[1].ID != 101 {
		t.Fatalf("replies %v", replies)
	}
	for _, r := range replies {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	tr := replies[1].Resp.(*pb.TxnResponse)
	if !tr.Succeeded || tr.Header.Revision != 3 || string(tr.Responses[0].GetResponsePut().PrevKv.Value) != "first" {
		t.Fatalf("CAS reply: %v", tr)
	}
	r, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "second" || &r.Kvs[0].Value[0] != &entryValue[0] {
		t.Fatalf("MVCC does not alias entry: %v", r)
	}

	// Acknowledged values and revisions replay from synced bytes after crash.
	disk.Crash(rand.New(rand.NewPCG(7, 0)))
	s = startSingle(t, disk, 2)
	r, err = s.n.kv.Range(&pb.RangeRequest{Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "second" || r.Kvs[0].ModRevision != 3 {
		t.Fatalf("recovery lost acked CAS: %v", r)
	}
}

func TestWireCASFailureAndAtomicError(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.do(1, &pb.PutRequest{Key: []byte("k"), Value: []byte("old")})
	failed, _ := wireCommand(t, wireCAS(100, "wrong"))
	bad, _ := wireCommand(t, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("must not appear")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k"), Revision: 100}}},
	}})
	s.n.SubmitBatch(2, []batchcmd.Command{failed, bad})
	replies := s.run()
	if len(replies) != 2 {
		t.Fatalf("replies: %v", replies)
	}
	tr := replies[0].Resp.(*pb.TxnResponse)
	if tr.Succeeded || tr.Header.Revision != 2 || string(tr.Responses[0].GetResponseRange().Kvs[0].Value) != "old" {
		t.Fatalf("failed CAS: %v", tr)
	}
	if !errors.Is(replies[1].Err, rpctypes.ErrGRPCFutureRev) {
		t.Fatalf("atomic error: %v", replies[1].Err)
	}
	r, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("a")})
	if err != nil || len(r.Kvs) != 0 || s.n.kv.Rev() != 2 {
		t.Fatalf("partial transaction: %v %v", r, err)
	}
}

func TestWirePendingBodiesReleasedOnTimeoutAndNoLeader(t *testing.T) {
	n, err := New(DefaultConfig(1, []uint64{1}), &wal.Recovered{})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := wireCommand(t, &pb.PutRequest{Key: []byte("a"), Value: []byte("v")})
	n.SubmitBatch(1, []batchcmd.Command{c})
	eff, err := n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if len(eff.Replies) != 1 || !errors.Is(eff.Replies[0].Err, rpctypes.ErrGRPCNoLeader) {
		t.Fatalf("no leader: %v", eff.Replies)
	}
	if len(n.timeouts) != 1 || n.timeouts[0].command != nil || n.timeouts[0].req.Op != nil {
		t.Fatal("no-leader path pins input")
	}

	s := startSingle(t, simfs.New(), 1)
	s.do(1, &pb.PutRequest{Key: []byte("a")})
	s.n.SubmitBatch(2, []batchcmd.Command{c})
	if _, err := s.n.Process(); err != nil {
		t.Fatal(err)
	}
	s.n.SubmitBatch(3, []batchcmd.Command{c}) // Waits behind the unpersisted append.
	p := s.n.waiting[3]
	if p == nil || p.command == nil || len(p.command.Body()) == 0 {
		t.Fatal("request is not waiting")
	}
	s.n.now = p.deadline
	s.n.expire()
	if !p.done || p.command != nil || p.req.Op != nil {
		t.Fatal("timeout pins input")
	}
}

func TestWirePutFullFields(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.do(1, &pb.LeaseGrantRequest{ID: 7, TTL: 100})
	for i, op := range []*pb.PutRequest{
		{Key: []byte("k"), Value: []byte("leased"), Lease: 7},
		{Key: []byte("k"), IgnoreValue: true, IgnoreLease: true, PrevKv: true},
		{Key: []byte("k"), Value: []byte("new"), PrevKv: true},
	} {
		c, _ := wireCommand(t, op)
		s.n.SubmitBatch(uint64(2+i), []batchcmd.Command{c})
		rs := s.run()
		if len(rs) != 1 || rs[0].Err != nil {
			t.Fatalf("put: %v", rs)
		}
		if i > 0 && (rs[0].Resp.(*pb.PutResponse).PrevKv.Lease != 7 || string(rs[0].Resp.(*pb.PutResponse).PrevKv.Value) != "leased") {
			t.Fatalf("prev_kv: %v", rs[0])
		}
	}
	r, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("k")})
	if err != nil || !bytes.Equal(r.Kvs[0].Value, []byte("new")) || r.Kvs[0].Lease != 0 {
		t.Fatalf("detach: %v %v", r, err)
	}
	if len(s.n.state.leases[7].keys) != 0 {
		t.Fatal("old lease still owns key")
	}
}
