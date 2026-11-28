package wl

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// selfRemoval retains the already-owned LocalApply message until the old
// configuration has confirmed its committed removal. No later apply may pass
// this prefix, and Raft retains its ordinary apply/compaction ownership.
// After confirmation, the loop drains output before closing the sockets.
type selfRemoval struct {
	apply      *pb.Message
	change     *pb.ConfChange // parsed before the apply prefix's buffers can be released
	index, seq uint64
	offset     int
	asked      time.Time
	ready      bool
	// Before local apply, notices retains evidence from known members.
	// At the barrier it is filtered to old voters; false means not yet acknowledged.
	notices map[uint64]bool
	quorum  int
	drainBy time.Time
}

// testSelfRemovalApply observes only the fenced removal, immediately before apply.
var testSelfRemovalApply atomic.Pointer[func(n *node)]

func (n *node) deferSelfRemoval(m *pb.Message, offset int, cc *pb.ConfChange) bool {
	if cc.GetType() != pb.ConfChangeRemoveNode || cc.GetNodeId() != n.id || !n.leading() || n.voters() < 2 || n.memberAt(n.id) < 0 {
		return false
	}
	if _, _, _, err := parseCCContext(cc.GetContext()); err != nil {
		return false // Preserve applyConfChange's ordinary corruption error.
	}
	early := n.selfRemoval.notices
	n.selfRemoval = selfRemoval{apply: m, change: cc, offset: offset, index: m.Entries[offset].GetIndex(), quorum: n.voters()/2 + 1}
	n.selfRemoval.notices = make(map[uint64]bool, n.voters())
	for _, member := range n.members {
		if !member.learner {
			n.selfRemoval.notices[member.id] = member.id == n.id || early[member.id]
		}
	}
	n.selfRemovalNotice(n.id)
	n.askSelfRemovalRead()
	return true
}

func (n *node) askSelfRemovalRead() {
	r := &n.selfRemoval
	n.readSeq++
	r.seq, r.asked = n.readSeq, n.now
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], r.seq)
	// Ask before ApplyConfChange to keep publishing the removal's commit
	// under the old configuration. A ReadIndex answer alone does NOT prove
	// the respondents know that commit: Raft caps each heartbeat's commit
	// at that follower's match index. Only explicit removal notices count.
	n.stats.readIndex++
	n.rn.ReadIndex(ctx[:])
}

func (n *node) retrySelfRemovalRead() {
	if r := &n.selfRemoval; r.apply != nil && !r.ready && n.now.Sub(r.asked) > 5*tickEvery {
		n.askSelfRemovalRead()
	}
}

func (n *node) selfRemovalRead(seq, _ uint64) bool {
	r := &n.selfRemoval
	if r.apply == nil || seq != r.seq {
		return false
	}
	// Even an answer in the original term at the removal index proves
	// leadership, not each respondent's commit knowledge. Consume it but
	// leave the barrier to distinct old voters' explicit removal notices.
	return true
}

// removalNotice never lets a transport notice stop a leader before its
// own removal applies. Followers retain their prompt removal exit; a fenced
// leader, even if it steps down, must finish its quorum and reply drain.
func (n *node) removalNotice(id uint64) {
	n.selfRemovalNotice(id)
	if !n.leading() && n.selfRemoval.apply == nil && n.selfRemoval.drainBy.IsZero() {
		n.stopErr = ErrRemoved
	}
}

// selfRemovalNotice accepts explicit commit knowledge: a peer only
// emits removedNotice after applying our committed removal. Survivors close
// the removed leader's connections at apply, so their notice on redial can
// arrive instead of a ReadIndex heartbeat response. Count distinct OLD voters
// (including ourselves), never a new quorum or two lanes of the same peer.
func (n *node) selfRemovalNotice(id uint64) {
	r := &n.selfRemoval
	if r.apply == nil {
		// A survivor can apply first while our local append is pending.
		// Keep its evidence without stopping the leader. Membership can
		// still change before local apply, so filter voters at the barrier,
		// not here (a current learner could be promoted before removal).
		if n.leading() && r.drainBy.IsZero() && n.memberAt(id) >= 0 {
			if r.notices == nil {
				r.notices = make(map[uint64]bool)
			}
			r.notices[id] = true
		}
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
	if f := testSelfRemovalApply.Load(); f != nil {
		(*f)(n)
	}
	// Do not unmarshal again: flushEvents/endApply may have released the
	// original entry data while this prefix waited for commit knowledge.
	err := n.applyConfChange(r.change, r.index)
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
		if s := sockOf(x); s != nil && !s.dead {
			if s.wantOut || len(s.out) > s.ow {
				return false
			}
			if c, ok := x.(*client); ok && len(c.flow.held) > 0 {
				// DATA and trailers behind a closed HTTP/2 window are
				// pending output even when the socket itself is writable.
				return false
			}
		}
	}
	return true
}
