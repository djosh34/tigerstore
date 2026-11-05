package server

// Experiment only. Not a correct store: no validation of CAS and no persistence.
import (
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"os"
	"sync/atomic"
)

var floorNull = os.Getenv("FLOOR_NULL") == "1"
var floorRevision atomic.Int64

func (c *conn) floorComplete(b opBatch) bool {
	for _, r := range b.reqs {
		if r.Op != nil {
			if _, ok := r.Op.(*pb.TxnRequest); !ok {
				return false
			}
		}
	}
	cs := make([]node.Completion, len(b.reqs))
	first := floorRevision.Add(int64(len(cs))) - int64(len(cs)) + 1
	for i, r := range b.reqs {
		rev := first + int64(i)
		reply := core.Reply{Rev: rev, Term: 2}
		if _, ok := r.Op.(*pb.TxnRequest); ok {
			h := &pb.ResponseHeader{ClusterId: c.cluster, MemberId: c.member, Revision: rev, RaftTerm: 2}
			reply.Resp = &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: h}}}}}
		}
		cs[i] = node.Completion{Tag: b.tags[i], Reply: reply}
	}
	c.Complete(cs)
	return true
}
