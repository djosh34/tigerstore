package wl

import (
	"encoding/binary"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// selfRemoval keeps one already-owned LocalApply message, not copies of its
// entries, while a removing leader publishes its committed removal. It is
// fixed node control; no output, per-client plan, or auxiliary queue is built.
// Raft/storage keep their normal apply/compaction ownership until it completes.
type selfRemoval struct {
	apply      *pb.Message
	index, seq uint64
	offset     int
	asked      time.Time
	ready      bool
}

// deferSelfRemoval fences a leader's exit behind an old-configuration quorum
// round trip. Without it, applying a pending removal immediately after winning
// an election can stop the leader before any surviving voter learns the commit:
// the two-voter survivor cannot then elect itself to apply the removal.
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
	n.selfRemoval = selfRemoval{apply: m, offset: offset, index: m.Entries[offset].GetIndex()}
	n.askSelfRemovalRead()
	return true
}

func (n *node) askSelfRemovalRead() {
	r := &n.selfRemoval
	n.readSeq++
	r.seq, r.asked = n.readSeq, n.now
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], r.seq)
	// Unlike a normal client read, this barrier needs the quorum's index,
	// not local application through that index (which is exactly the exit).
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
	r.ready = index >= r.index
	return true
}

func (n *node) resumeSelfRemoval() error {
	r := &n.selfRemoval
	if r.apply == nil || !r.ready {
		return nil
	}
	err := n.storageApplyReady(r.apply, r.offset)
	*r = selfRemoval{}
	return err
}
