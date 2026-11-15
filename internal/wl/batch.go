package wl

import "encoding/binary"

// addOp appends a write to this turn's batch, in the log ring.
func (n *node) addOp(h handle, kind uint8, msg []byte) {
	need := opHdr + len(msg)
	if n.batchStart < 0 {
		need += batchHdr
	}
	if n.ringPos+need > len(n.ring) {
		// Wrap: the batch so far moves to the start of the ring.
		if n.batchStart >= 0 {
			k := copy(n.ring, n.ring[n.batchStart:n.ringPos])
			n.batchStart, n.ringPos = 0, k
		} else {
			n.ringPos = 0
		}
	}
	if n.batchStart < 0 {
		n.batchStart = n.ringPos
		b := n.ring[n.ringPos : n.ringPos+batchHdr]
		n.fwdSeq++
		binary.LittleEndian.PutUint32(b, uint32(n.id))
		binary.LittleEndian.PutUint32(b[4:], n.fwdSeq)
		binary.LittleEndian.PutUint32(b[12:], magic)
		n.ringPos += batchHdr
		n.batchOps = 0
		n.batchOwn = n.batchOwn[:0]
	}
	b := n.ring[n.ringPos : n.ringPos+opHdr]
	b[0] = kind
	binary.LittleEndian.PutUint32(b[1:], uint32(len(msg)))
	copy(n.ring[n.ringPos+opHdr:], msg)
	n.ringPos += opHdr + len(msg)
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
	if n.sidePos+failBytes > len(n.side) {
		n.sidePos = 0
	}
	off := n.sidePos
	kept := n.scratch[:0]
	for m := msg; len(m) > 0; {
		_, _, _, _, rest, _ := field(m)
		f := m[:len(m)-len(rest)]
		if f[0] == 3<<3|2 {
			n.sidePos += copy(n.side[n.sidePos:], f)
		} else {
			kept = append(kept, f...)
		}
		m = rest
	}
	s.fail = n.side[off:n.sidePos:n.sidePos]
	n.addOp(h, kTxn, kept)
	n.scratch = kept[:0]
}

// proposeBatch hands this turn's batch to Raft.
func (n *node) proposeBatch() {
	if n.batchStart < 0 {
		return
	}
	data := n.ring[n.batchStart:n.ringPos:n.ringPos]
	binary.LittleEndian.PutUint32(data[8:], uint32(n.batchOps))
	n.batchStart = -1
	// A follower's batch comes back from the leader by name.
	seq := binary.LittleEndian.Uint32(data[4:])
	f := &n.fwd[seq&(fwdSlots-1)]
	f.seq, f.data = seq, data
	f.handles = append(f.handles[:0], n.batchOwn...)
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
