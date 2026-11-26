package wl

import (
	"encoding/binary"
	"slices"
	"time"
)

// The turn's writes go into one batch, which Raft replicates as one
// entry. The batch is built in the proposal ring, where the log, the
// peers and the apply read it; when the ring has no room, on the heap.

// growBatch makes room in the open batch for n more bytes.
func (n *node) growBatch(k int) {
	b := n.batch
	if cap(b)-len(b) >= k {
		return
	}
	if n.batchInRing {
		if room := n.ring.reserve(len(b) + k); room != nil {
			// The same start, or the ring's start when its end had too
			// little: then the batch so far moves there.
			room = append(room, b...)
			n.batch = room
			return
		}
		n.ring.stats.heap++
		n.batchInRing = false
	}
	nb := make([]byte, len(b), 2*(len(b)+k))
	copy(nb, b)
	n.batch = nb
}

// addOp appends a write to this turn's batch. A caller op goes with the
// write after it, whose batch its caller chose before it (alarmApart).
func (n *node) addOp(h handle, kind uint8, msg []byte) {
	if kind != kCaller {
		n.alarmApart(kind, msg)
	}
	if n.batch == nil {
		n.batchInRing = true
		n.growBatch(batchHdr + opHdr + len(msg))
		var hdr [batchHdr]byte
		n.fwdSeq++
		binary.LittleEndian.PutUint32(hdr[0:], uint32(n.id))
		binary.LittleEndian.PutUint64(hdr[4:], n.fwdSeq)
		hdr[16] = n.format
		copy(hdr[17:], magic)
		n.batch = append(n.batch, hdr[:]...)
		n.batchOps = 0
		n.batchOwn = n.batchOwn[:0]
		n.batchFlags = n.batchFlags[:0]
	}
	n.growBatch(opHdr + len(msg))
	var oh [opHdr]byte
	oh[0] = kind
	binary.LittleEndian.PutUint32(oh[1:], uint32(len(msg)))
	n.batch = append(append(n.batch, oh[:]...), msg...)
	n.batchOps++
	n.batchOwn = append(n.batchOwn, h)
	var fl uint8
	if n.noSpace || n.corrupt {
		fl = alarmFlags(kind, msg)
	}
	n.batchFlags = append(n.batchFlags, fl)
	n.tracePutAdd(h, n.fwdSeq, n.batchOps-1)
}

// addTxn adds a Txn. A failure branch of reads only changes nothing,
// so only this node, which answers the client, needs it: it stays with
// the stream, and the batch carries the compares and the success branch.
func (n *node) addTxn(h handle, s *stream, msg []byte) {
	failBytes, readsOnly := 0, true
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			readsOnly = false
			break
		}
		if num == 3 {
			failBytes += len(m) - len(rest)
			if onum, _, _, _, _, err := field(p); err != nil || onum != 1 {
				readsOnly = false
			}
		}
		m = rest
	}
	// While auth is on, the apply checks the whole Txn.
	if !readsOnly || failBytes == 0 || n.auth.enabled {
		n.addOp(h, kTxn, msg)
		return
	}
	// Keep the failure branch in the side ring.
	fail := n.side.reserve(failBytes)
	if fail == nil {
		n.side.stats.heap++
		fail = make([]byte, 0, failBytes)
	}
	kept := n.scratch[:0]
	for m := msg; len(m) > 0; {
		_, _, _, _, rest, _ := field(m)
		f := m[:len(m)-len(rest)]
		if f[0] == 3<<3|2 {
			fail = append(fail, f...)
		} else {
			kept = append(kept, f...)
		}
		m = rest
	}
	s.fail = slices.Clip(fail)
	s.failAt = -1
	if n.side.offset(fail) >= 0 {
		s.failAt = n.side.commit(s.fail, span{kind: spanFail, st: s}).start
	}
	n.addOp(h, kTxn, kept)
	n.scratch = kept[:0]
}

// proposeBatch hands this turn's batch to Raft.
func (n *node) proposeBatch(now time.Time) {
	if n.batch == nil {
		return
	}
	data := slices.Clip(n.batch)
	n.batch = nil
	binary.LittleEndian.PutUint32(data[12:], uint32(n.batchOps))
	// A follower's batch comes back from the leader by name.
	seq := binary.LittleEndian.Uint64(data[4:])
	f := &n.fwd[seq&(fwdSlots-1)]
	n.forgetPutTimings(f.seq)
	f.seq, f.data = seq, data
	f.handles = append(f.handles[:0], n.batchOwn...)
	f.pledged, f.pledgeEpoch = n.batchPledge, n.pledgeEpoch
	n.batchPledge = 0
	f.flags = append(f.flags[:0], n.batchFlags...)
	n.batchAlarm = nil
	n.proposed = append(n.proposed, proposal{seq: seq, at: now})
	if n.batchInRing {
		// A leader's Raft copies the proposal as it takes it, so only the
		// forward table holds the leader's batch. A follower's waits for
		// the leader's append that names it.
		kind := spanUnnamed
		if n.lead == n.id {
			kind = spanFree
		}
		n.ring.commit(data, span{kind: kind, at: now, fwd: true, origin: n.id, seq: seq})
	}
	n.stats.proposals++
	if len(n.leases.deadline) > 0 {
		n.renewProposed(data)
	}
	n.tracePutBatch(data, "propose_begin", 0)
	if n.lead != n.id && n.lead != 0 {
		n.sendBatch(data)
	}
	if err := n.rn.Propose(data); err != nil {
		n.tracePutBatch(data, "proposal_rejected", 0)
		msg := n.droppedMsg()
		for _, h := range n.batchOwn {
			n.replyErrorH(h, codeUnavailable, msg)
		}
		f.handles = f.handles[:0]
		// The batch never comes to the log: the room its writes were
		// taken for is free again (ringRefuses).
		n.releasePledge(f)
	} else {
		n.tracePutBatch(data, "proposed", 0)
	}
}

// droppedMsg answers the writes of a batch Raft dropped, which never
// apply. A node that knows no leader, as while a cluster that started
// again elects one, answers etcd's "no leader", which says the write did
// not take effect; a call answered "leader changed" may have. With a
// leader known, Raft drops a proposal while it hands the leadership over.
func (n *node) droppedMsg() string {
	if n.lead == 0 {
		return "etcdserver: no leader"
	}
	return "etcdserver: leader changed"
}

// requestTimeout is how long a client waits for its write to apply before
// it is told the request timed out, as etcd's 5 s and two election
// timeouts: a proposal lost in a leader change never applies.
const requestTimeout = 7 * time.Second

// proposal is a batch this node proposed and when.
type proposal struct {
	seq uint64
	at  time.Time
}

// expireProposals answers the clients of batches that have not applied in
// requestTimeout with etcd's timeout error.
func (n *node) expireProposals() {
	k := 0
	for ; k < len(n.proposed) && n.now.Sub(n.proposed[k].at) >= requestTimeout; k++ {
		p := n.proposed[k]
		f := &n.fwd[p.seq&(fwdSlots-1)]
		if f.seq != p.seq {
			continue
		}
		for _, h := range f.handles {
			n.replyErrorH(h, codeUnavailable, msgTimeout)
		}
		f.handles = f.handles[:0]
		// A batch lost on the way to the log, as a follower's to its
		// leader, never comes there: the room its writes were taken for
		// is free again (ringRefuses).
		n.releasePledge(f)
	}
	if k > 0 {
		n.proposed = append(n.proposed[:0], n.proposed[k:]...)
	}
}

const msgTimeout = "etcdserver: request timed out"

// releasePledge frees the ring room batch f's writes were taken for, which
// never come to the log: only f's, and only if Raft handed the log no
// append since, at which ringPledged started again without it.
func (n *node) releasePledge(f *fwdBatch) {
	if f.pledgeEpoch == n.pledgeEpoch {
		n.ringPledged = max(0, n.ringPledged-f.pledged)
	}
	f.pledged = 0
}
