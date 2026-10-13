package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/batchcmd"
	"github.com/djosh34/tigerstore/internal/simfs"
)

func TestWireCleanupKeepsLegacyRetention(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	op := &pb.PutRequest{Key: []byte("legacy"), Value: []byte("v")}
	s.n.Submit(Request{ID: 1, Op: op})
	p := s.n.waiting[1]
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if p.req.Op != op {
		t.Fatal("encoding changed legacy retention")
	}
	s.persistAll(eff.Persist)
	rs := s.run()
	if len(rs) != 1 || rs[0].Err != nil {
		t.Fatalf("legacy reply: %v", rs)
	}
	if !p.done || p.req.Op != op {
		t.Fatal("finish changed legacy retention")
	}

	c, _ := wireCommand(t, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("legacy")}}},
	}})
	s.n.SubmitBatch(2, []batchcmd.Command{c})
	if len(s.n.reads) != 1 {
		t.Fatal("read-only compact Txn not waiting for ReadIndex")
	}
	p = s.n.reads[0]
	if p.command != nil || p.req.Op == nil || !p.compact {
		t.Fatal("compact read not adapted")
	}
	rs = s.run()
	if len(rs) != 1 || rs[0].Err != nil {
		t.Fatalf("compact read reply: %v", rs)
	}
	if !p.done || p.req.Op != nil {
		t.Fatal("compact read pins receive body after finish")
	}
}
