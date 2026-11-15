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

// addOp appends a write to this turn's batch.
func (n *node) addOp(h handle, kind uint8, msg []byte) {
	if n.batch == nil {
		n.batchInRing = true
		n.growBatch(batchHdr + opHdr + len(msg))
		var hdr [batchHdr]byte
		n.fwdSeq++
		binary.LittleEndian.PutUint32(hdr[0:], uint32(n.id))
		binary.LittleEndian.PutUint32(hdr[4:], n.fwdSeq)
		binary.LittleEndian.PutUint32(hdr[12:], magic)
		n.batch = append(n.batch, hdr[:]...)
		n.batchOps = 0
		n.batchOwn = n.batchOwn[:0]
	}
	n.growBatch(opHdr + len(msg))
	var oh [opHdr]byte
	oh[0] = kind
	binary.LittleEndian.PutUint32(oh[1:], uint32(len(msg)))
	n.batch = append(append(n.batch, oh[:]...), msg...)
	n.batchOps++
	n.batchOwn = append(n.batchOwn, h)
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
	if !readsOnly || failBytes == 0 {
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
	binary.LittleEndian.PutUint32(data[8:], uint32(n.batchOps))
	// A follower's batch comes back from the leader by name.
	seq := binary.LittleEndian.Uint32(data[4:])
	f := &n.fwd[seq&(fwdSlots-1)]
	f.seq, f.data = seq, data
	f.handles = append(f.handles[:0], n.batchOwn...)
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
	stats.proposals++
	if n.lead != n.id && n.lead != 0 {
		n.sendBatch(data)
	}
	if err := n.rn.Propose(data); err != nil {
		for _, h := range n.batchOwn {
			n.replyErrorH(h, codeUnavailable, "etcdserver: leader changed")
		}
	}
}
