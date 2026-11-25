package wl

import (
	"encoding/binary"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A follower's write goes to the leader as a proposal, whose log may have
// no room for it, as when it keeps entries for a follower that is down
// (#814). The leader then drops it rather than step it: the log would wait
// for a segment that only a checkpoint's cut gives back, a checkpoint that
// waits for an apply behind that same write. It tells the follower at
// once, with a frame no release before this one knows, so it tells only a
// follower that says it knows the frame: one whose proposal carries
// refusalMark as its context. A release before this one reads a
// proposal's context and Raft ignores it, so a leader of that release
// steps a marked proposal as any other. A follower of that release marks
// none, and gets no frame: its write times out after requestTimeout, as
// before, and the leader's log goes on.

// peerRefusals is whether this build marks its proposals and knows the
// refusal frame. The rolling upgrade's old build turns it off, as a
// release before this one (rolling-upgrade.yml).
const peerRefusals = true

// refusalMark is the context of a proposal whose follower knows the
// refusal frame.
var refusalMark = []byte("refusable")

// frameRefused tells a follower that the leader dropped its batch, after
// [8 sequence], as its log had no room for it.
const (
	frameRefused   = 0xfd
	refusedFrameLn = 1 + 8
)

// refusals is whether this node marks its proposals and knows the refusal
// frame: a test's node may act as a release before this one.
func (n *node) refusals() bool { return peerRefusals && !n.oldPeerWire }

// propContext is the context a proposal m goes to the leader with.
func (n *node) propContext(m *pb.Message) []byte {
	if m.GetType() == pb.MsgProp && len(m.GetContext()) == 0 && n.refusals() && !proposesConfChange(m) {
		return refusalMark
	}
	return m.GetContext()
}

// leaderDrops reports whether this leader drops proposal m, of peer from,
// for want of room in its log's ring, and tells the peer if it knows the
// refusal frame. A proposal may take the ring's reserve, as a client's
// write here (ringRefuses): the deletes and compactions the reserve is
// for go on while the ring is short.
func (n *node) leaderDrops(m *pb.Message, from uint64) bool {
	w := n.wal
	if w == nil || w.ring == nil || n.lead != n.id || m.GetType() != pb.MsgProp {
		return false
	}
	var need int64
	for _, e := range m.GetEntries() {
		need += int64(len(e.GetData())) + walEntHdr + ringRecordSlack
	}
	if need+n.ringPledged <= w.ringRoom() {
		n.ringPledged += need
		return false
	}
	w.wanted = max(w.wanted, need)
	n.nudgeRing()
	n.stats.propsDropped++
	if string(m.GetContext()) != string(refusalMark) {
		return true
	}
	p := n.peersOut[from]
	if p == nil || p.dead {
		return true
	}
	for _, e := range m.GetEntries() {
		o, seq, ok := batchOrigin(e.GetData())
		if !ok || o != from {
			continue
		}
		var h [4 + refusedFrameLn]byte
		binary.LittleEndian.PutUint32(h[0:], refusedFrameLn)
		h[4] = frameRefused
		binary.LittleEndian.PutUint64(h[5:], seq)
		p.put(h[:])
		n.stats.refusalsSent++
	}
	n.markDirty(&p.sock)
	return true
}

// isRefusal reports whether peer frame f tells that the leader dropped a
// batch of this node's, if this node knows the frame.
func (n *node) isRefusal(f []byte) bool {
	return len(f) == refusedFrameLn && f[0] == frameRefused && n.refusals()
}

// refused answers the writes of the batch the leader dropped, which never
// apply, as too many requests: its clients may send them again.
func (n *node) refused(f []byte) {
	seq := binary.LittleEndian.Uint64(f[1:])
	b := &n.fwd[seq&(fwdSlots-1)]
	if b.seq != seq {
		return
	}
	for _, h := range b.handles {
		if c, s := n.clientOf(h); c != nil {
			n.reject(c, s, errTooManyRequests)
		}
	}
	b.handles = b.handles[:0]
	// The batch never comes to the log, so the room its writes were taken
	// for is free again (ringRefuses): kept, it refused every write the
	// size of the one refused for good (#814's test, through a follower).
	n.ringPledged = 0
	n.stats.refusalsGot++
}

// followerDrops reports whether this follower drops append m, whose
// entries its log's ring has no room for (#814). Written, they would wait
// for a segment the cut gives back, and the cut for a checkpoint that
// needs an apply behind them; and every append after them, the leader's
// snapshot too, would wait behind them. An append the ring has room for
// the first entries of keeps those and lets the rest go: Raft's follower
// answers the last it kept, commits no further, and the leader sends the
// rest after; without it, an append of more entries than the free room
// holds, as the leader sends to a follower that lags, was dropped and sent
// again for good. One whose first new entry has no room is dropped,
// unanswered: the leader sends it again, at its heartbeats once it
// probes, and this node goes on applying what it has, so that a checkpoint
// can cut, or the leader's snapshot can come. The room counts the appends
// taken and not yet in the log, and keeps appendMargin for the records an
// append without entries writes.
func (n *node) followerDrops(m *pb.Message) bool {
	w := n.wal
	if w == nil || w.ring == nil || m.GetType() != pb.MsgApp || len(m.GetEntries()) == 0 {
		return false
	}
	room := w.ringRoom() - n.appendPledged - w.waitingBytes() - appendMargin
	ents := m.GetEntries()
	keep, took, wanted := fitEntries(ents, n.storage.lastIndex(), room)
	if wanted > 0 {
		// The room it wants makes the ring low, so that a checkpoint is
		// due once the node applied past its newest: a ring with more
		// free segments than ringLow counts, and too few for the entry,
		// made none, and dropped the leader's appends for good.
		w.wanted = max(w.wanted, wanted+appendMargin)
	} else if took > 0 {
		w.wanted = 0
	}
	if keep == 0 {
		n.stats.appendsDropped++
		return true
	}
	if keep < len(ents) {
		m.Entries = ents[:keep]
		n.stats.appendsTrimmed++
	}
	n.appendPledged += took
	return false
}

// fitEntries picks the entries of an append that a follower's log, whose
// last index is last, has room for: how many of ents, from the first, it
// keeps, the log bytes those not in the log yet take, and the bytes of the
// first that has no room, 0 when all fit. Entries at last or before, as an
// append Raft sent again carries, write nothing and are kept: keep is 0
// only when the append's first entry is new and has no room.
func fitEntries(ents []*pb.Entry, last uint64, room int64) (keep int, took, wanted int64) {
	for k, e := range ents {
		if e.GetIndex() <= last {
			continue
		}
		need := int64(len(e.GetData())) + walEntHdr
		if took+need > room {
			return k, took, need
		}
		took += need
	}
	return len(ents), took, 0
}

// appendMargin is the room a follower keeps past the appends it takes, for
// the hard states of appends without entries.
const appendMargin = 2 * block

// waitingBytes is what the appends that wait for the stage will write.
func (w *wal) waitingBytes() int64 {
	var k int64
	for _, m := range w.waiting {
		for _, e := range m.GetEntries() {
			k += int64(len(e.GetData())) + walEntHdr
		}
	}
	return k
}
