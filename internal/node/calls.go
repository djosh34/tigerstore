package node

import (
	"context"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/peer"
)

// A node asks a peer with peer.Transport.Call: a kind byte, then the
// request in protobuf. The answer is the response in protobuf.
const (
	// callHashKV asks for the peer's HashKV answer (corrupt.go).
	callHashKV = 1
	// callPromote asks the leader to promote a learner, which only the
	// leader can check, as etcd's followers forward MemberPromote.
	callPromote = 2
	// callMembers asks for the members, which a node that joins a
	// running cluster needs to learn the Raft IDs, as etcd's do.
	callMembers = 3
)

// Answer answers a peer's Call. A node without a Raft ID yet may only
// ask for the members.
func (n *Node) Answer(from uint64, req []byte) ([]byte, error) {
	if len(req) == 0 || (from == 0 && req[0] != callMembers) {
		return nil, errors.New("node: a call this node does not answer")
	}
	var r proto.Message
	switch req[0] {
	case callHashKV:
		r = &pb.HashKVRequest{}
	case callPromote:
		r = &pb.MemberPromoteRequest{}
	case callMembers:
		r = &pb.MemberListRequest{}
	default:
		return nil, fmt.Errorf("node: unknown call %d", req[0])
	}
	if err := proto.Unmarshal(req[1:], r); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.requestTimeout())
	defer cancel()
	resp, err := n.Do(ctx, r)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp)
}

// call asks peer id with a Call of kind and decodes the answer into resp.
func call(ctx context.Context, t peer.Transport, id uint64, kind byte, req, resp proto.Message) error {
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	out, err := t.Call(ctx, id, append([]byte{kind}, b...))
	if err != nil {
		return err
	}
	return proto.Unmarshal(out, resp)
}

// Promote answers Cluster.MemberPromote. Only the leader can check that
// the learner caught up; another node asks the leader, as etcd does.
func (n *Node) Promote(ctx context.Context, r *pb.MemberPromoteRequest) (proto.Message, error) {
	resp, err := n.Do(ctx, r)
	if !errors.Is(err, rpctypes.ErrGRPCNotLeader) {
		return resp, err
	}
	t, ok := n.transport.Load().(peer.Transport)
	leader := n.Leader()
	if !ok || leader == 0 || leader == n.ID() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	out := &pb.MemberPromoteResponse{}
	if err := call(ctx, t, leader, callPromote, r, out); err != nil {
		return nil, err
	}
	// The header is this node's, as the answer comes from it.
	out.Header.MemberId = n.ID()
	return out, nil
}

// RemoteMembers asks the nodes at addrs for the cluster's members, as a
// node that joins a running cluster does (--initial-cluster-state
// existing) before it knows its Raft ID. cfg gives the certificates. It
// returns the first answer, with the cluster ID.
func RemoteMembers(ctx context.Context, cfg peer.Config, addrs []string) ([]*pb.Member, uint64, error) {
	b, err := proto.Marshal(&pb.MemberListRequest{Linearizable: true})
	if err != nil {
		return nil, 0, err
	}
	var errs error
	for _, addr := range addrs {
		out, err := peer.Ask(ctx, cfg, addr, append([]byte{callMembers}, b...))
		if err == nil {
			resp := &pb.MemberListResponse{}
			if err = proto.Unmarshal(out, resp); err == nil {
				return resp.Members, resp.GetHeader().GetClusterId(), nil
			}
		}
		errs = errors.Join(errs, fmt.Errorf("%s: %w", addr, err))
	}
	return nil, 0, fmt.Errorf("node: no peer gave the members: %w", errs)
}
