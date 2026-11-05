package server

import (
	"github.com/djosh34/tigerstore/internal/core"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"testing"
)

var floorReplySink []byte

func floorTxnReply() *pb.TxnResponse {
	return &pb.TxnResponse{Header: &pb.ResponseHeader{ClusterId: 0x323456789abcdef0, MemberId: 0x123456789abcdef0, Revision: 1000000, RaftTerm: 2}, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 1000000}}}}}}
}
func BenchmarkFloorReply(b *testing.B) {
	c := &conn{cluster: 0x323456789abcdef0, member: 0x123456789abcdef0}
	r := core.Reply{Rev: 1000000, Term: 2}
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			var err error
			floorReplySink, err = c.appendPutReply(make([]byte, 0, 5+putResponseSize), &r)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	txn := floorTxnReply()
	b.Run("txn", func(b *testing.B) {
		for b.Loop() {
			var err error
			floorReplySink, err = appendTxnReply(txn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
func TestFloorReplySizes(t *testing.T) {
	c := &conn{cluster: 0x323456789abcdef0, member: 0x123456789abcdef0}
	p, err := c.appendPutReply(nil, &core.Reply{Rev: 1000000, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	txn, err := appendTxnReply(floorTxnReply())
	if err != nil {
		t.Fatal(err)
	}
	for name, msg := range map[string][]byte{"put": p, "txn": txn} {
		t.Logf("%s protobuf=%d grpc=%d H2_headers=%d H2_trailers=%d H2_reply=%d", name, len(msg)-5, len(msg), len(grpcHeaders), len(okTrailers), len(msg)+27+len(grpcHeaders)+len(okTrailers))
	}
}
