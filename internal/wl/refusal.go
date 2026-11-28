package wl

import (
	"encoding/binary"
	"slices"

	"go.etcd.io/raft/v3"
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
// refusal frame. As a client's write here (ringRefuses), a proposal may
// take the ring's reserve while the ring is not short, and one that puts
// is refused while it is: the deletes and compactions the reserve is for
// go on.
func (n *node) leaderDrops(m *pb.Message, from uint64) bool {
	w := n.wal
	// Raft's role now: a node elected in this turn's input steps a
	// proposal as its leader before its Ready tells wl (n.lead).
	if w == nil || w.ring == nil || m.GetType() != pb.MsgProp || n.rn.BasicStatus().RaftState != raft.StateLeader {
		return false
	}
	var need int64
	puts := false
	for _, e := range m.GetEntries() {
		need += int64(len(e.GetData())) + walEntHdr + ringRecordSlack
		puts = puts || batchPuts(e.GetData())
	}
	switch {
	case w.ringShort() && puts:
		// A short ring refuses a client's put here (ringRefuses), and a
		// follower's the same: the reserve is for the deletes and
		// compactions that free space. With no checkpoint to free it, it
		// raises NOSPACE, as for a client's put here: else a cluster whose
		// clients all write through followers refused them as busy for
		// good. Once the alarm applies, the followers refuse puts with
		// NOSPACE themselves (alarmGate).
		if n.ckpt == nil && !n.ringDue() {
			n.raiseNoSpace()
		}
	case need+n.ringTaken() <= w.ringRoom():
		n.ringPledged += need
		return false
	default:
		w.wanted = max(w.wanted, need+n.ringTaken())
		n.nudgeRing()
	}
	n.stats.propsDropped++
	// Only a node that knows the frame sends it: the rolling upgrade's old
	// build sends none, as a release before this one.
	if string(m.GetContext()) != string(refusalMark) || !n.refusals() {
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
// apply, as too many requests: its clients may send them again. A write
// that the alarms still up forbid gets their error instead, CORRUPT first
// (fwdBatch.flags, alarmErr): a put through a follower whose leader's ring
// is short is refused with NOSPACE once the alarm is up, not as busy for
// good (#818's review). The batch's bytes may be gone by then, the ring's
// span taken back (dropForward), so its ops' flags were kept as it came.
func (n *node) refused(f []byte) {
	seq := binary.LittleEndian.Uint64(f[1:])
	b := &n.fwd[seq&(fwdSlots-1)]
	if b.seq != seq {
		return
	}
	for i, h := range b.handles {
		if c, s := n.clientOf(h); c != nil {
			err := errTooManyRequests
			if i < len(b.flags) {
				if e := n.alarmErr(b.flags[i]); e != nil {
					err = e
				}
			}
			n.reject(c, s, err)
		}
	}
	b.handles = b.handles[:0]
	if n.putTimings != nil {
		n.forgetPutTimings(seq)
	}
	// The batch never comes to the log, so the room its writes were taken
	// for is free again (ringRefuses): kept, it refused every write the
	// size of the one refused for good (#814's test, through a follower).
	n.releasePledge(b)
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
	if m.GetTerm() < n.rn.BasicStatus().GetTerm() {
		// Of a leader before: Raft ignores it and writes none of it.
		return false
	}
	ents := m.GetEntries()
	last := max(n.storage.lastIndex(), w.waitingLast(), n.appendLast)
	if m.GetIndex() > last || !n.holdsAt(m.GetIndex(), m.GetLogTerm()) {
		// The entry before them is not here, or is of another term: Raft
		// rejects the append and writes none of it, and its answer tells
		// the leader where this node's log parts from its own, so that it
		// sends what is missing, or a snapshot. Dropped, the leader,
		// probing, sent it again for good.
		return false
	}
	// Raft writes the append from its first entry that is new or of
	// another term than the one held at its index, truncating the log
	// there (findConflict), so the room counts those from it on (#817).
	held := last
	for _, e := range ents {
		if e.GetIndex() <= last && !n.holdsAt(e.GetIndex(), e.GetTerm()) {
			held = e.GetIndex() - 1
			break
		}
	}
	room := w.ringRoom() - n.appendPledged - w.waitingBytes() - appendMargin
	keep, took, wanted := fitEntries(ents, held, room)
	if wanted > 0 {
		// The room it wants, with what the ring holds besides, makes the
		// ring low, so that a checkpoint is due once the node applied past
		// its newest: a ring with more free segments than ringLow counts,
		// and too few for the entry, made none, and dropped the leader's
		// appends for good.
		w.wanted = max(w.wanted, took+wanted+n.appendPledged+w.waitingBytes()+appendMargin)
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
	// Raft takes the entries kept: they follow an entry it holds, of
	// their leader's term or later.
	if e := ents[keep-1]; e.GetIndex() > n.appendLast {
		n.appendLast, n.appendLastTerm = e.GetIndex(), e.GetTerm()
	}
	return false
}

// holdsAt reports whether this follower's log holds an entry at index i
// of term t, as Raft checks an append's predecessor: index 0 always; one
// Raft's storage no longer holds, at or before its snapshot, does not,
// and Raft answers such an append with its commit, writing none of it.
// One taken in this turn and not handed to the log yet (appendLast), or
// waiting for the stage, is held at the term it was taken at, or, past
// that, taken as held.
func (n *node) holdsAt(i, t uint64) bool {
	if i == 0 {
		return true
	}
	if i == n.appendLast && n.appendLast > n.storage.lastIndex() {
		return n.appendLastTerm == t
	}
	if i > n.storage.lastIndex() {
		return true
	}
	held, err := n.storage.Term(i)
	return err == nil && held == t
}

// waitingLast is the last index of the appends that wait for the stage.
func (w *wal) waitingLast() uint64 {
	for _, m := range slices.Backward(w.waiting) {
		if ents := m.GetEntries(); len(ents) > 0 {
			return ents[len(ents)-1].GetIndex()
		}
	}
	return 0
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

// batchPuts reports whether batch data holds an op that a short ring
// refuses (ringRefuses): a put, a lease grant, or a Txn that puts.
func batchPuts(data []byte) bool {
	if !isBatch(data) {
		return false
	}
	for p := batchHdr; p+opHdr <= len(data); {
		kind := data[p]
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		if p+opHdr+l > len(data) {
			return true // not a batch this node reads: as if it put
		}
		msg := data[p+opHdr : p+opHdr+l]
		switch kind {
		case kPut, kLeaseGrant:
			return true
		case kTxn:
			if txnCost(msg) != 0 {
				return true
			}
		}
		p += opHdr + l
	}
	return false
}
