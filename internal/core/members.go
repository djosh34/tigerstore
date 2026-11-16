package core

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/djosh34/tigerstore/internal/planted"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Membership follows etcd v3.7.2 (server/etcdserver/api/membership and
// EtcdServer.AddMember and its kin). The members are replicated state,
// held in checkpoints: each has a Raft ID, peer URLs, whether it is a
// learner, and the name and client URLs it published once it started.
// A new cluster starts its log with one Raft configuration change per
// member of --initial-cluster (RawNode.Bootstrap), so every node, one
// that joins later included, rebuilds the members from the log or a
// checkpoint. A change of members is a Raft configuration change whose
// context carries the request's origin and the member. Like etcd, a
// change is checked when it applies and does nothing, but answers the
// error, when it no longer fits: a removed ID never comes back, an ID or
// a peer URL is taken once, and at most MaxLearners learners exist.

// ErrRemoved stops a node that the cluster removed.
var ErrRemoved = errors.New("core: the member has been permanently removed from the cluster")

// The operations a configuration change's context names.
const (
	confAdd     = 1
	confPromote = 2
	confRemove  = 3
	confUpdate  = 4
)

// encodeConfContext encodes the context of a configuration change: the
// origin node, boot and request ID, the operation and the member.
func encodeConfContext(origin, boot, id uint64, op byte, m *pb.Member) ([]byte, error) {
	b := binary.AppendUvarint(nil, origin)
	b = binary.AppendUvarint(b, boot)
	b = binary.AppendUvarint(b, id)
	b = append(b, op)
	return proto.MarshalOptions{Deterministic: true}.MarshalAppend(b, m)
}

func decodeConfContext(b []byte) (origin, boot, id uint64, op byte, m *pb.Member, err error) {
	var ok bool
	if origin, b, ok = uvarint(b); !ok {
		return 0, 0, 0, 0, nil, errBadBatch
	}
	if boot, b, ok = uvarint(b); !ok {
		return 0, 0, 0, 0, nil, errBadBatch
	}
	if id, b, ok = uvarint(b); !ok || len(b) == 0 {
		return 0, 0, 0, 0, nil, errBadBatch
	}
	op, m = b[0], &pb.Member{}
	if err := proto.Unmarshal(b[1:], m); err != nil {
		return 0, 0, 0, 0, nil, fmt.Errorf("%w: %w", errBadBatch, err)
	}
	return origin, boot, id, op, m, nil
}

// bootstrapPeers are the Raft configuration changes that start a new
// cluster's log: one per member of the initial cluster.
func bootstrapPeers(cfg Config) ([]raft.Peer, error) {
	var peers []raft.Peer
	for _, id := range cfg.Peers {
		m := &pb.Member{ID: id}
		for _, im := range cfg.Members {
			if im.ID == id {
				m = proto.CloneOf(im)
			}
		}
		ctx, err := encodeConfContext(0, 0, 0, confAdd, m)
		if err != nil {
			return nil, err
		}
		peers = append(peers, raft.Peer{ID: id, Context: ctx})
	}
	return peers, nil
}

// confState is the Raft configuration the members make.
func (st *State) confState() *raftpb.ConfState {
	cs := &raftpb.ConfState{}
	for _, m := range st.memberList() {
		if m.IsLearner {
			cs.Learners = append(cs.Learners, m.ID)
		} else {
			cs.Voters = append(cs.Voters, m.ID)
		}
	}
	return cs
}

// isVoter reports whether id is a voting member.
func (st *State) isVoter(id uint64) bool {
	m, ok := st.members[id]
	return ok && !m.IsLearner
}

// voters are the voting members' IDs in order.
func (st *State) voters() []uint64 { return st.confState().Voters }

// memberList is every member, in ID order, as etcd lists them.
func (st *State) memberList() []*pb.Member {
	ms := make([]*pb.Member, 0, len(st.members))
	for _, m := range st.members {
		ms = append(ms, m)
	}
	slices.SortFunc(ms, func(a, b *pb.Member) int { return cmp.Compare(a.ID, b.ID) })
	return ms
}

// cloneMembers copies the member list for an answer.
func (st *State) cloneMembers() []*pb.Member {
	ms := st.memberList()
	for i, m := range ms {
		ms[i] = proto.CloneOf(m)
	}
	return ms
}

// checkConfChange is etcd's ValidateConfigurationChange.
func (st *State) checkConfChange(op byte, m *pb.Member, maxLearners int) error {
	if st.removed[m.ID] {
		return rpctypes.ErrGRPCMemberNotFound
	}
	have := st.members[m.ID]
	urlTaken := func() bool {
		for _, o := range st.members {
			if o.ID == m.ID {
				continue
			}
			for _, u := range o.PeerURLs {
				if slices.Contains(m.PeerURLs, u) {
					return true
				}
			}
		}
		return false
	}
	switch op {
	case confAdd:
		if have != nil {
			return rpctypes.ErrGRPCMemberExist
		}
		if urlTaken() {
			return rpctypes.ErrGRPCPeerURLExist
		}
		if m.IsLearner {
			learners := 1
			for _, o := range st.members {
				if o.IsLearner {
					learners++
				}
			}
			if learners > maxLearners {
				return rpctypes.ErrGRPCTooManyLearners
			}
		}
	case confPromote:
		if have == nil {
			return rpctypes.ErrGRPCMemberNotFound
		}
		if !have.IsLearner {
			return rpctypes.ErrGRPCMemberNotLearner
		}
	case confRemove:
		if have == nil {
			return rpctypes.ErrGRPCMemberNotFound
		}
		// Raft cannot remove the last voter: it fails the change, and
		// the node would fail again on every start.
		if !have.IsLearner && len(st.voters()) == 1 {
			return rpctypes.ErrGRPCMemberNotEnoughStarted
		}
	case confUpdate:
		if have == nil {
			return rpctypes.ErrGRPCMemberNotFound
		}
		if urlTaken() {
			return rpctypes.ErrGRPCPeerURLExist
		}
	default:
		return fmt.Errorf("%w: configuration change %d", errBadBatch, op)
	}
	return nil
}

// applyMembers changes the members by a checked configuration change.
func (st *State) applyMembers(op byte, m *pb.Member, index uint64) {
	// Legacy logs and checkpoints have no confirmation epoch. Keep zero
	// until the first confirmation-aware announcement changes support.
	if st.versionEpoch != 0 {
		st.versionEpoch = index
	}
	clear(st.confirmed)
	switch op {
	case confAdd:
		st.members[m.ID] = &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs, IsLearner: m.IsLearner}
	case confPromote:
		st.members[m.ID].IsLearner = false
	case confRemove:
		delete(st.members, m.ID)
		delete(st.supports, m.ID)
		delete(st.runs, m.ID)
		st.removed[m.ID] = true
	case confUpdate:
		if !planted.On("B28") {
			st.members[m.ID].PeerURLs = m.PeerURLs
		}
	}
}

// applyConfChange applies a committed configuration change entry and
// answers the request waiting for it on this node.
func (n *Node) applyConfChange(e *raftpb.Entry) error {
	n.state.metaKnown = false
	var cc raftpb.ConfChange
	if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	origin, boot, id, op, m, err := decodeConfContext(cc.GetContext())
	if err != nil {
		return err
	}
	var p *pending
	if id != 0 && origin == n.cfg.ID && boot == n.cfg.Boot {
		p = n.waiting[id]
	}
	err = n.state.checkConfChange(op, m, n.maxLearners())
	if err != nil {
		// As in etcd, Raft still hears of it, as a change of nobody.
		cc.NodeId = new(raft.None)
	} else {
		n.state.applyMembers(op, m, e.GetIndex())
		n.membersChanged()
	}
	if n.rn != nil {
		n.rn.ApplyConfChange(&cc)
	}
	n.storage.setConf(e.GetIndex(), n.state.confState())
	// ApplyConfChange does not advance Raft's applied index. Wait for
	// runApply to deliver MsgStorageApplyResp before proposing another.
	if n.confGate == ^uint64(0) {
		n.confGate = e.GetIndex()
	}
	if p != nil {
		rep := Reply{ID: p.req.ID}
		if err == nil {
			rep.Resp = n.memberResponse(p.req.Op, m)
		}
		n.answer(p, rep, nil)
	}
	if err == nil && op == confRemove && m.ID == n.cfg.ID {
		if n.rn == nil {
			return ErrRemoved
		}
		// Stopping here closes the transport before a removed leader's
		// queued commit update reaches its followers. They can be left
		// needing the old quorum, including us. As in etcd, keep Raft
		// and the transport running for one second before shutting down.
		// Recovery still refuses a disk that already commits our removal.
		n.removeAt = n.now + n.cfg.ticksOf(1)
	}
	return nil
}

// membersChanged hands the driver the members after a change.
func (n *Node) membersChanged() {
	n.out.Members = n.state.cloneMembers()
}

// memberResponse answers a member change as etcd's Cluster service does:
// the members after it, and for an add, the new member.
func (n *Node) memberResponse(op proto.Message, m *pb.Member) proto.Message {
	h := n.memberHeader()
	ms := n.state.cloneMembers()
	switch op.(type) {
	case *pb.MemberAddRequest:
		return &pb.MemberAddResponse{Header: h, Member: &pb.Member{ID: m.ID, PeerURLs: m.PeerURLs, IsLearner: m.IsLearner}, Members: ms}
	case *pb.MemberRemoveRequest:
		return &pb.MemberRemoveResponse{Header: h, Members: ms}
	case *pb.MemberUpdateRequest:
		return &pb.MemberUpdateResponse{Header: h, Members: ms}
	case *pb.MemberPromoteRequest:
		return &pb.MemberPromoteResponse{Header: h, Members: ms}
	}
	return &pb.MemberListResponse{Header: h, Members: ms}
}

// memberHeader is the header of etcd's Cluster service: no revision.
func (n *Node) memberHeader() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: n.cfg.ClusterID, MemberId: n.cfg.ID, RaftTerm: n.term}
}

// maxLearners is etcd's --max-learners.
func (n *Node) maxLearners() int { return cmp.Or(n.cfg.MaxLearners, 1) }

// IsLearner reports whether this node is a learner.
func (n *Node) IsLearner() bool {
	m := n.state.members[n.cfg.ID]
	return m != nil && m.IsLearner
}

// applyAttributes sets a member's name and client URLs, a write of kind
// kindMember.
func (st *State) applyAttributes(a *pb.Member) bool {
	m := st.members[a.ID]
	if m == nil {
		return false
	}
	m.Name, m.ClientURLs = a.Name, a.ClientURLs
	return true
}
