package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// TestTakesSpace checks which requests etcd v3.7.2 refuses while its
// NOSPACE alarm is active.
func TestTakesSpace(t *testing.T) {
	put := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k")}}}
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("k")}}}
	get := &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k")}}}
	nested := &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: []*pb.RequestOp{del}}}}
	for _, tc := range []struct {
		op   proto.Message
		want bool
	}{
		{nil, true}, // a put on the fast path
		{&pb.PutRequest{Key: []byte("k")}, true},
		{&pb.LeaseGrantRequest{TTL: 5}, true},
		{&pb.TxnRequest{Failure: []*pb.RequestOp{get, put}}, true},
		// The capped applier refuses deletes inside a transaction too.
		{&pb.TxnRequest{Success: []*pb.RequestOp{del}}, true},
		{&pb.TxnRequest{Success: []*pb.RequestOp{nested}}, true},
		{&pb.TxnRequest{Success: []*pb.RequestOp{get}, Failure: []*pb.RequestOp{get}}, false},
		{&pb.DeleteRangeRequest{Key: []byte("k")}, false},
		{&pb.CompactionRequest{Revision: 2}, false},
		{&pb.RangeRequest{Key: []byte("k")}, false},
	} {
		if got := TakesSpace(Request{Op: tc.op}); got != tc.want {
			t.Errorf("TakesSpace(%T %v) = %v", tc.op, tc.op, got)
		}
	}
}
