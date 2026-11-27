package wl

// W-backed canonical input cannot stay tied to a stalled socket. Transfer
// its funded finish commitment to a complete P destination, then retire W.
// Main's sort arrays, comparator, selected set and chunking are unchanged.
func (n *node) copyMaterialRange(b *legacyBatch, c *client, s *stream, r *rangeReq, kvs [][]byte, count int64, more bool) {
	m := n.newMaterialReply(b, s, r, kvs, count, more)
	if m == nil {
		b.cancelled = true
		n.abortBig(c, s)
		return
	}
	admission := materialAdmission(m, kvs, count, more)
	if n.cannotFit(admission) {
		n.startBig(c, s, b.owner.msg)
		return
	}
	wire := m.remaining
	room, covered := c.roomHeld, c.coveredRoom
	saved := b.promise
	b.promise = 0
	n.legacyPromises -= saved
	stage := n.admitStageQuota(c, int(wire), admission)
	if stage == nil {
		lost, lostCovered := room-c.roomHeld, covered-c.coveredRoom
		c.roomHeld += lost
		c.coveredRoom += lostCovered
		n.coveredRoom += lostCovered
		n.reserved += lost - lostCovered
		b.promise = saved
		n.legacyPromises += saved
		b.retryBlocked = true
		n.legacyWaitP = true
		return
	}
	for p := m.front(); len(p) > 0; p = m.front() {
		stage.append(p)
		m.consume(len(p))
	}
	c.replyStage = stage
	n.commitReplyStage(c, s)
	n.gs.rangesN++
	n.gs.calls[s.kind].sent += int64(m.frames)
	n.gs.sentBytes += wire - 5*int64(m.frames)
	n.stats.replies++
	n.callEnded(s, 0)
	n.endStreamOK(c, s)
	n.markDirty(&c.sock)
}

func materialAdmission(m *materialReply, kvs [][]byte, count int64, more bool) int64 {
	admission := int64(5 + m.responseN + sizeVarintField(4, uint64(count)))
	if more {
		admission += 2
	}
	for _, kv := range kvs {
		size, _, _ := projectedRecord(kv, m.keysOnly)
		admission += int64(sizeBytesField(2, size))
	}
	if m.stream {
		admission += 32 * (int64(len(kvs))/initialStreamChunk + 1)
	}
	return admission
}
