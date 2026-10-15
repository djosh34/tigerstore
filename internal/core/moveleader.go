package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MoveLeader follows etcd v3.7.2's (EtcdServer.MoveLeader): only the
// leader takes it, the transferee must be a voting member, and Raft's
// leadership transfer runs until this node sees the transferee lead.
// etcd waits for that as long as the client does; the node answers
// ErrTimeoutLeaderTransfer at the request's deadline.

// ErrTimeoutLeaderTransfer is etcd's answer to a transfer that did not
// finish in time, which etcd's gRPC layer has no code for.
var ErrTimeoutLeaderTransfer = status.Error(codes.Unknown, "etcdserver: request timed out, leader transfer took too long")

// moveLeader starts a leadership transfer for p.
func (n *Node) moveLeader(p *pending, r *pb.MoveLeaderRequest) {
	if !n.isLeader {
		n.finish(p, rpctypes.ErrGRPCNotLeader)
		return
	}
	if m := n.state.members[r.TargetID]; m == nil || m.IsLearner {
		n.finish(p, rpctypes.ErrGRPCBadLeaderTransferee)
		return
	}
	n.rn.TransferLeader(r.TargetID)
	n.transfers = append(n.transfers, p)
	n.timeouts = append(n.timeouts, p)
	n.checkTransfers()
}

// checkTransfers answers the leadership transfers whose transferee this
// node sees lead.
func (n *Node) checkTransfers() {
	if len(n.transfers) == 0 {
		return
	}
	lead := n.rn.BasicStatus().Lead
	kept := n.transfers[:0]
	for _, p := range n.transfers {
		switch {
		case p.done:
		case lead != raft.None && lead == p.req.Op.(*pb.MoveLeaderRequest).GetTargetID():
			n.answer(p, Reply{ID: p.req.ID, Resp: &pb.MoveLeaderResponse{}}, nil)
		default:
			kept = append(kept, p)
		}
	}
	n.transfers = kept
}
