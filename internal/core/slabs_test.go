package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// TestSlabsDecodeTxns decodes more transactions than a slab holds into
// one connection's slabs: each decodes as protobuf does, and none
// changes once later ones are decoded into the same slabs.
func TestSlabsDecodeTxns(t *testing.T) {
	var s Slabs
	var want, got []*pb.TxnRequest
	for i := range 3 * slabSize {
		key := fmt.Appendf(nil, "/registry/pods/%d", i)
		txn := &pb.TxnRequest{
			Compare: []*pb.Compare{{Key: key, Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: int64(i)}}},
			Success: []*pb.RequestOp{
				{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: []byte("pod")}}},
				{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: key}}},
			},
			Failure: []*pb.RequestOp{
				{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key, Limit: int64(i)}}},
				{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: []*pb.RequestOp{
					{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}},
				}}}},
			},
		}
		wire, err := proto.Marshal(txn)
		if err != nil {
			t.Fatal(err)
		}
		req := Request{Op: s.Txn(), Wire: wire}
		if err := DecodeTxn(&req, &s); err != nil {
			t.Fatal(err)
		}
		want, got = append(want, txn), append(got, req.Op.(*pb.TxnRequest))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("transaction %d decoded from slabs as %v, want %v", i, got[i], want[i])
		}
	}
	if s.Range() == got[0].Failure[0].GetRequestRange() {
		t.Fatal("a slab handed out a message twice")
	}
}
