package wl

import (
	"encoding/binary"
	"errors"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// selfRemoval retains the already-owned LocalApply message until the old
// configuration has confirmed its committed removal. No later apply may pass
// this prefix, and Raft retains its ordinary apply/compaction ownership.
// After confirmation, the loop drains output before closing the sockets.
type selfRemoval struct {
	apply      *pb.Message
	index, seq uint64
	offset     int
	asked      time.Time
	ready      bool
	// notices contains only old voters; false means not yet acknowledged.
	notices map[uint64]bool
	quorum  int
	drainBy time.Time
}

func (n *node) deferSelfRemoval(m *pb.Message, offset int, cc *pb.ConfChange) bool {
	if r := &n.selfRemoval; r.apply != nil {
		return !r.ready || r.apply != m || r.offset != offset
	}
	if cc.GetType() != pb.ConfChangeRemoveNode || cc.GetNodeId() != n.id || !n.leading() || n.voters() < 2 || n.memberAt(n.id) < 0 {
		return false
	}
	if _, _, _, err := parseCCContext(cc.GetContext()); err != nil {
		return false // Preserve applyConfChange's ordinary corruption error.
	}
	n.selfRemoval = selfRemoval{apply: m, offset: offset, index: m.Entries[offset].GetIndex(), quorum: n.voters()/2 + 1}
	n.selfRemoval.notices = make(map[uint64]bool, n.voters())
	for _, member := range n.members {
		if !member.learner {
			n.selfRemoval.notices[member.id] = member.id == n.id
		}
	}
	n.askSelfRemovalRead()
	return true
}

func (n *node) askSelfRemovalRead() {
	r := &n.selfRemoval
	n.readSeq++
	r.seq, r.asked = n.readSeq, n.now
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], r.seq)
	// Ask before ApplyConfChange: a quorum of the NEW configuration is
	// insufficient. This needs the quorum's commit index, not application
	// through that index, since applying the removal is exactly the exit.
	n.stats.readIndex++
	n.rn.ReadIndex(ctx[:])
}

func (n *node) retrySelfRemovalRead() {
	if r := &n.selfRemoval; r.apply != nil && !r.ready && n.now.Sub(r.asked) > 5*tickEvery {
		n.askSelfRemovalRead()
	}
}

func (n *node) selfRemovalRead(seq, index uint64) bool {
	r := &n.selfRemoval
	if r.apply == nil || seq != r.seq {
		return false
	}
	r.ready = r.ready || index >= r.index
	return true
}

// selfRemovalNotice also accepts explicit commit knowledge: a peer only
// emits removedNotice after applying our committed removal. Survivors close
// the removed leader's connections at apply, so their notice on redial can
// arrive instead of a ReadIndex heartbeat response. Count distinct OLD voters
// (including ourselves), never a new quorum or two lanes of the same peer.
func (n *node) selfRemovalNotice(id uint64) {
	r := &n.selfRemoval
	if r.apply == nil {
		return
	}
	if _, voter := r.notices[id]; !voter {
		return
	}
	r.notices[id] = true
	acks := 0
	for _, ack := range r.notices {
		if ack {
			acks++
		}
	}
	if acks >= r.quorum {
		r.ready = true
	}
}

func (n *node) resumeSelfRemoval() error {
	r := &n.selfRemoval
	if r.apply == nil || !r.ready {
		return nil
	}
	err := n.storageApplyReady(r.apply, r.offset)
	*r = selfRemoval{}
	if errors.Is(err, ErrRemoved) {
		// etcd allows ten heartbeats for removal output. Here the quorum
		// barrier, not elapsed time, establishes commit knowledge; only
		// the output drain is time bounded, so a slow client cannot keep
		// the removed process alive forever. No timeout may bypass the
		// quorum barrier: with no old quorum it must remain available.
		r.drainBy = n.now.Add(10 * tickEvery)
		return nil
	}
	return err
}

// selfRemovalDrained runs only AFTER flush, including the MemberRemove reply.
// EAGAIN sockets get EPOLLOUT turns, but cannot extend the drain's deadline.
func (n *node) selfRemovalDrained() bool {
	if n.selfRemoval.drainBy.IsZero() {
		return false
	}
	if !n.now.Before(n.selfRemoval.drainBy) {
		return true
	}
	if len(n.dirty) > 0 || len(n.heldReplies) > 0 {
		return false
	}
	for _, x := range n.socks {
		if s := sockOf(x); s != nil && !s.dead && s.wantOut {
			return false
		}
	}
	return true
}
