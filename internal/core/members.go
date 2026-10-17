package core

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"

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
		rep := Reply{ID: p.req.ID, Err: err}
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

// Members are the members, in ID order.
func (n *Node) Members() []*pb.Member { return n.state.cloneMembers() }

// submitMember takes a member change: it checks it as etcd's server does
// before it proposes one, and queues its configuration change.
func (n *Node) submitMember(p *pending) {
	var op byte
	m := &pb.Member{}
	switch r := p.req.Op.(type) {
	case *pb.MemberAddRequest:
		// The server checked and sorted the URLs.
		op, m.PeerURLs, m.IsLearner = confAdd, r.PeerURLs, r.IsLearner
		if n.cfg.StrictReconfigCheck {
			if err := n.mayAddMember(m.IsLearner); err != nil {
				n.finish(p, err)
				return
			}
		}
		m.ID = n.newMemberID()
	case *pb.MemberRemoveRequest:
		op, m.ID = confRemove, r.ID
		if have := n.state.members[r.ID]; have != nil && !have.IsLearner && n.cfg.StrictReconfigCheck {
			if err := n.mayRemoveVoter(r.ID); err != nil {
				n.finish(p, err)
				return
			}
		}
	case *pb.MemberUpdateRequest:
		op, m.ID, m.PeerURLs = confUpdate, r.ID, r.PeerURLs
	case *pb.MemberPromoteRequest:
		op, m.ID = confPromote, r.ID
		if err := n.readyToPromote(r.ID); err != nil {
			n.finish(p, err)
			return
		}
	}
	ctx, err := encodeConfContext(n.cfg.ID, n.cfg.Boot, p.req.ID, op, m)
	if err != nil {
		n.finish(p, err)
		return
	}
	typ := raftpb.ConfChangeAddNode
	switch {
	case op == confAdd && m.IsLearner:
		typ = raftpb.ConfChangeAddLearnerNode
	case op == confRemove:
		typ = raftpb.ConfChangeRemoveNode
	case op == confUpdate:
		typ = raftpb.ConfChangeUpdateNode
	}
	p.conf = &raftpb.ConfChange{Type: typ.Enum(), NodeId: new(m.ID), Context: ctx}
	n.waiting[p.req.ID] = p
	// A member change is of featureVersion, as its entries are.
	if !n.capable() {
		p.early = true
		n.early = append(n.early, p)
		return
	}
	n.confs = append(n.confs, p)
}

// proposeConfs encodes local requests into the same queue as member
// changes forwarded by followers.
func (n *Node) proposeConfs() {
	if len(n.confs) == 0 || n.rn.BasicStatus().Lead == raft.None {
		return
	}
	for _, p := range n.confs {
		if p.done {
			continue
		}
		data, err := proto.Marshal(p.conf)
		if err != nil {
			n.finish(p, err)
			continue
		}
		n.confMessages = append(n.confMessages, &raftpb.Message{
			Type: raftpb.MsgProp.Enum(), From: new(n.cfg.ID),
			Entries: []*raftpb.Entry{{Type: raftpb.EntryConfChange.Enum(), Data: data}},
		})
	}
	n.confs = nil
}

// proposeConfMessages serializes configuration proposals at the leader.
// A follower can answer a change before the leader applies it (#210).
// Passing the next proposal straight to Raft then silently replaces it
// with an empty entry. Keep it queued until Raft acknowledges apply.
func (n *Node) proposeConfMessages() {
	if len(n.confMessages) == 0 {
		return
	}
	st := n.rn.BasicStatus()
	if st.Lead == raft.None {
		return
	}
	if st.GetTerm() != n.confTerm {
		n.confTerm, n.confGate = st.GetTerm(), 0
	}
	if st.RaftState == raft.StateLeader {
		if st.Applied < n.confGate || st.Applied == 0 {
			return
		}
		term, err := n.storage.Term(st.Applied)
		if err != nil {
			n.fail(err)
			return
		}
		// A new leader's pendingConfIndex covers its inherited log.
		// Applying an entry of this term clears that initial barrier.
		if term != st.GetTerm() {
			return
		}
	}
	// Raft replaces a change it ignores with an empty entry in place, so
	// it gets a copy and the queue keeps the change.
	m := proto.Clone(n.confMessages[0]).(*raftpb.Message)
	if err := n.rn.Step(m); err != nil {
		if !errors.Is(err, raft.ErrProposalDropped) {
			n.fail(err)
		}
		return
	}
	if m.Entries[0].GetType() != raftpb.EntryConfChange {
		// A proposal dropped for the uncommitted-size limit still moved
		// Raft's pending index past its log, so this retry became an
		// empty entry. Try again once more is applied.
		n.confGate = st.Applied + 1
		return
	}
	n.confMessages[0] = nil
	n.confMessages = n.confMessages[1:]
	if st.RaftState == raft.StateLeader {
		n.confGate = ^uint64(0)
	}
}

// newMemberID picks the ID of a new member: one above every ID the
// cluster has had or this node handed out.
func (n *Node) newMemberID() uint64 {
	next := n.lastMemberID
	for id := range n.state.members {
		next = max(next, id)
	}
	for id := range n.state.removed {
		next = max(next, id)
	}
	n.lastMemberID = next + 1
	return n.lastMemberID
}

// started counts the voting members that published their name, etcd's
// started members, other than skip.
func (n *Node) started(skip uint64) (members, started int) {
	for _, m := range n.state.members {
		if m.IsLearner || m.ID == skip {
			continue
		}
		members++
		if m.Name != "" {
			started++
		}
	}
	return members, started
}

// connected counts the voters this node has been connected to for
// etcd's health interval, itself included.
func (n *Node) connected() (voters, connected int) {
	for _, id := range n.state.voters() {
		voters++
		if id == n.cfg.ID || n.isConnected(id) {
			connected++
		}
	}
	return voters, connected
}

// healthSeconds is etcd's HealthInterval: a peer counts as connected
// once its connection is that old.
const healthSeconds = 5

func (n *Node) isConnected(id uint64) bool {
	return n.cfg.ConnectedTicks == nil || n.cfg.ConnectedTicks(id) > n.cfg.ticksOf(healthSeconds)
}

// isDown reports whether this node has no connection to peer id.
func (n *Node) isDown(id uint64) bool {
	return n.cfg.ConnectedTicks != nil && n.cfg.ConnectedTicks(id) < 0
}

// mayAddMember is etcd's mayAddMember: for a voter, enough started
// voters (IsReadyToAddVotingMember), and for a learner too, a connected
// quorum that counts the new member as down.
func (n *Node) mayAddMember(learner bool) error {
	members, started := n.started(0)
	members++
	if !learner && (started != 1 || members != 2) && started < members/2+1 {
		return rpctypes.ErrGRPCMemberNotEnoughStarted
	}
	if voters, connected := n.connected(); voters > 1 && connected < (voters+1)/2+1 {
		return rpctypes.ErrGRPCUnhealthy
	}
	return nil
}

// mayRemoveVoter is etcd's mayRemoveMember for a voter: enough started
// voters left (IsReadyToRemoveVotingMember), and, unless the voter is a
// down peer, a connected quorum left without it.
func (n *Node) mayRemoveVoter(id uint64) error {
	members, started := n.started(id)
	if started < members/2+1 {
		return rpctypes.ErrGRPCMemberNotEnoughStarted
	}
	if id != n.cfg.ID && n.isDown(id) {
		return nil
	}
	if voters, connected := n.connected(); connected-1 < 1+(voters-1)/2 {
		return rpctypes.ErrGRPCUnhealthy
	}
	return nil
}

// readyToPromote is etcd's check before it promotes a learner: only the
// leader knows how far a learner is, so another node answers
// ErrGRPCNotLeader and its driver asks the leader. The learner must have
// at least 90% of the leader's log.
func (n *Node) readyToPromote(id uint64) error {
	st := n.rn.Status()
	if st.RaftState != raft.StateLeader {
		return rpctypes.ErrGRPCNotLeader
	}
	m := n.state.members[id]
	if m == nil {
		return rpctypes.ErrGRPCMemberNotFound
	}
	if !m.IsLearner {
		return rpctypes.ErrGRPCMemberNotLearner
	}
	learner, ok := st.Progress[id]
	if !ok {
		return rpctypes.ErrGRPCLearnerNotReady
	}
	if float64(learner.Match) < 0.9*float64(st.Progress[n.cfg.ID].Match) {
		return rpctypes.ErrGRPCLearnerNotReady
	}
	if n.cfg.StrictReconfigCheck {
		// The learner counts as a started voter.
		members, started := n.started(0)
		if started+1 < (members+1)/2+1 {
			return rpctypes.ErrGRPCMemberNotEnoughStarted
		}
	}
	return nil
}

// publish proposes this node's name and client URLs, as etcd's members
// publish theirs once they start, until the members hold them; again
// every election timeout, as Raft may drop a proposal.
func (n *Node) publish() {
	m := n.state.members[n.cfg.ID]
	if m == nil || n.leader == raft.None || !n.capable() || n.cfg.Name == "" {
		return
	}
	if m.Name == n.cfg.Name && slices.Equal(m.ClientURLs, n.cfg.ClientURLs) {
		return
	}
	if n.publishedAt != 0 && n.now-n.publishedAt < int64(n.cfg.ElectionTicks) {
		return
	}
	n.publishedAt = n.now
	n.queueWrite(&pending{req: Request{Op: &pb.Member{ID: n.cfg.ID, Name: n.cfg.Name, ClientURLs: n.cfg.ClientURLs}}})
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

// RemovedMembers are the IDs the cluster removed.
func (n *Node) RemovedMembers() []uint64 {
	return slices.Sorted(maps.Keys(n.state.removed))
}

// Removed stops a node that a peer told the cluster removed it, as
// etcd's does.
func (n *Node) Removed() { n.fail(ErrRemoved) }

// ForceNewCluster is etcd's --force-new-cluster for a node that restarted
// from its log: the configuration changes that remove every other member,
// as entries right after the commit index, and the hard state that
// commits them. The driver appends both, which drops the entries after
// the commit index, and starts the node again from its log; the node is
// then the only member and elects itself. There is nothing to append for
// a node that is the only member already.
func (n *Node) ForceNewCluster() ([]*raftpb.Entry, wal.HardState, error) {
	hs := wal.HardState{Term: n.term, Vote: n.vote, Commit: n.applied}
	if n.state.members[n.cfg.ID] == nil {
		return nil, hs, errors.New("core: --force-new-cluster: this node is not a member of its cluster")
	}
	var ents []*raftpb.Entry
	for _, m := range n.state.cloneMembers() {
		if m.ID == n.cfg.ID {
			continue
		}
		ctx, err := encodeConfContext(n.cfg.ID, n.cfg.Boot, 0, confRemove, &pb.Member{ID: m.ID})
		if err != nil {
			return nil, hs, err
		}
		data, err := proto.Marshal(&raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: new(m.ID), Context: ctx})
		if err != nil {
			return nil, hs, err
		}
		hs.Commit++
		ents = append(ents, &raftpb.Entry{Term: new(n.term), Index: new(hs.Commit), Type: raftpb.EntryConfChange.Enum(), Data: data})
	}
	return ents, hs, nil
}
