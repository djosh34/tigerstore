package wl

import (
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A leader's Raft drops a membership change proposed while one it holds
// is not applied yet, or before it applied what it held when it became
// the leader (Raft's pendingConfIndex), and puts an empty entry in its
// place without a word to the proposer. The proposer's client then waits
// for its change until requestTimeout. A follower answers a change when
// it applies it, which may be before the leader has, so its client's next
// change could meet that (#705). The leader holds such a change back, a
// follower's proposal or its own client's request, until its Raft takes
// it, then lets one go a turn.

// confHold is what the leader holds back, and what it knows of the
// changes its Raft holds.
type confHold struct {
	// lastConf is the index of the last ConfChange the node appended as
	// the leader, and leadFirst the first entry it appended as the
	// leader in term leadTerm, 0 until then. stepped says it handed Raft
	// a change this turn that is not appended yet.
	lastConf, leadFirst, leadTerm uint64
	stepped                       bool
	props                         []heldProp
	reqs                          []heldMember
}

// heldProp is a follower's proposal of a change, held since at.
type heldProp struct {
	m  *pb.Message
	at time.Time
}

// heldMember is a client's request of a change, held since at.
type heldMember struct {
	h   handle
	msg []byte
	at  time.Time
}

func (h *confHold) waiting() bool { return len(h.props) > 0 || len(h.reqs) > 0 }

// confBlocked reports whether the leader's Raft would drop a membership
// change now. A follower's never does: its Raft hands the change to the
// leader.
func (n *node) confBlocked() bool {
	h := &n.confHold
	return n.leading() && (h.stepped || h.leadFirst == 0 || n.applied < max(h.lastConf, h.leadFirst))
}

// proposesConfChange reports whether proposal m carries a membership
// change.
func proposesConfChange(m *pb.Message) bool {
	for _, e := range m.GetEntries() {
		if t := e.GetType(); t == pb.EntryConfChange || t == pb.EntryConfChangeV2 {
			return true
		}
	}
	return false
}

// proposeConfChange steps a follower's proposal of a change, or holds it
// while the leader's Raft would drop it.
func (n *node) proposeConfChange(m *pb.Message, at time.Time) {
	if n.confBlocked() {
		// The entries' data lie in the receive ring, which this turn
		// lets go.
		n.confHold.props = append(n.confHold.props, heldProp{m: proto.CloneOf(m), at: at})
		return
	}
	n.confHold.stepped = n.leading()
	_ = n.rn.Step(m)
}

// confAppended notes, on the leader, its first entry and its last
// membership change among the entries it appends.
func (n *node) confAppended(ents []*pb.Entry) {
	h := &n.confHold
	if len(ents) == 0 || !n.leading() {
		return
	}
	// A leader that won a later term without the node seeing it lose
	// the first one appended that term's first entry now.
	if h.leadFirst == 0 || ents[0].GetTerm() > h.leadTerm {
		h.leadFirst, h.leadTerm = ents[0].GetIndex(), ents[0].GetTerm()
	}
	for _, e := range ents {
		if t := e.GetType(); t == pb.EntryConfChange || t == pb.EntryConfChangeV2 {
			h.lastConf = e.GetIndex()
		}
	}
}

// releaseConfChanges lets the changes held go, in order, while the
// leader's Raft takes them: one a turn, as each must be applied before the
// next. A node that is no longer the leader lets them all go, to the new
// leader.
func (n *node) releaseConfChanges() error {
	h := &n.confHold
	for h.waiting() && !n.confBlocked() {
		if len(h.props) > 0 {
			p := h.props[0]
			h.props = h.props[1:]
			n.proposeConfChange(p.m, p.at)
			continue
		}
		r := h.reqs[0]
		h.reqs = h.reqs[1:]
		if c, s := n.clientOf(r.h); c != nil {
			if err := n.memberRequest(c, s, r.msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// expireHeldConfChanges answers the requests held for requestTimeout as
// a change not applied in time, and drops the proposals held as long,
// whose clients were answered so.
func (n *node) expireHeldConfChanges() {
	h := &n.confHold
	k := 0
	for _, r := range h.reqs {
		if n.now.Sub(r.at) >= requestTimeout {
			n.replyErrorH(r.h, codeUnavailable, msgTimeout)
			continue
		}
		h.reqs[k] = r
		k++
	}
	clear(h.reqs[k:])
	h.reqs = h.reqs[:k]
	k = 0
	for _, p := range h.props {
		if n.now.Sub(p.at) < requestTimeout {
			h.props[k] = p
			k++
		}
	}
	clear(h.props[k:])
	h.props = h.props[:k]
}
