package wl

// fillResume is metadata only. Completed source bytes are owned by the
// published stage; the next piece re-resolves the same logical version.
type fillResume struct {
	record              fillRecord
	taken               int64
	outer               [32]byte
	outerPos, outerSize int
	watch               fillWatch
}

func (n *node) updateFillCredits() {
	if !n.cursorFetch || !n.fillCreditWake {
		return
	}
	n.fillCreditWake = false
	if !n.hasFillCreditWaiters() {
		return
	}
	n.legacyWaitP = false
	n.roomShort = false
	n.roomFreed = true
	for _, b := range n.bigs {
		b.waitP = false
	}
	for _, b := range n.bigWatches {
		b.waitP = false
	}
}

// Ordinary resident replies can return a covered loan with no fill waiter.
// That must not create an extra empty epoll turn or wake unrelated ranges.
func (n *node) hasFillCreditWaiters() bool {
	return n.legacyWaitP || len(n.fills) > 0 || len(n.bigs) > 0 || len(n.bigWatches) > 0 || len(n.roomWait) > 0 || len(n.heldReplies) > 0
}

func (n *node) newRawFill(c *client, h handle, st *store, left int) *replyFill {
	_, s := n.clientOf(h)
	if s == nil {
		return nil
	}
	left = min(left, n.directCapacity(c, s, s.kind == kWatch))
	if left <= 0 {
		return nil
	}
	n.stageCredits.limit = n.sizes.clientPool
	wake := n.fillCreditWake
	if !n.stageCredits.take(stageUnit, n.stageOther(0)) {
		return nil
	}
	c.growOut(stageUnit)
	promise := int64(0)
	if c.stages == 0 {
		promise = stageUnit
		if cap(c.out) < 64<<10 {
			promise += 64 << 10
		}
	}
	free := n.sizes.clientPool - n.stageCredits.used - n.stageOther(promise)
	logical := n.connOut + c.roomHeld - c.waiting() - c.flow.bytes - c.stagePending + max(0, n.sizes.reserve()-n.reserved-n.coveredRoom)
	want := min(left, int(max(smallMsg, logical)))
	lo, hi := 0, want
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if stageCapacity(mid) <= free {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo == 0 {
		c.growOut(-stageUnit)
		n.stageCredits.give(stageUnit)
		n.fillCreditWake = wake
		return nil
	}
	stage := n.admitStage(c, lo)
	if stage == nil {
		c.growOut(-stageUnit)
		n.stageCredits.give(stageUnit)
		n.fillCreditWake = wake
		return nil
	}
	n.fillSequence++
	q := &replyFill{h: h, c: c, st: st, stage: stage, raw: true, view: -1, header: st.rev, headerTerm: n.term, meta: stageUnit, op: n.fillSequence}
	c.heldCounted += stageUnit
	c.fillOwners++
	n.fills = append(n.fills, q)
	return q
}

func (n *node) startRawRange(b *bigRange) bool {
	if b.pieceLeft == 0 {
		for _, id := range b.piece {
			ks, _ := b.st.stateAt(id, b.rev)
			b.pieceLeft += sizeBytesField(2, b.recSize(id, &ks))
		}
		b.resume = fillResume{}
	}
	q := n.newRawFill(b.c, b.h, b.st, b.pieceLeft)
	if q == nil {
		b.waitP = true
		n.roomShort = true
		return true
	}
	q.bigRange = b
	q.ids = b.piece
	q.selected = int64(len(b.piece))
	q.req = b.req
	q.view = b.rev
	if b.resume.taken < 0 {
		full := 0
		for _, id := range b.piece {
			ks, _ := b.st.stateAt(id, b.rev)
			full += sizeBytesField(2, b.recSize(id, &ks))
		}
		if err := n.seekColdFill(q, full-b.pieceLeft, b.resume.record.crc); err != nil {
			q.cancelled = true
			n.failRawFill(q)
		}
	} else {
		q.record, q.taken = b.resume.record, b.resume.taken
		q.outer, q.outerPos, q.outerSize = b.resume.outer, b.resume.outerPos, b.resume.outerSize
	}
	b.fetching = true
	b.waitP = false
	return true
}

func (n *node) startRawWatch(b *bigWatch) bool {
	if b.pieceLeft == 0 {
		end, bytes := b.next, 0
		err := n.eachRevEvent(b.st, &b.x, revStart(b.st, b.rev), b.next, func(off int, ev bigEv) bool {
			if off >= b.end || bytes > 0 && bytes+ev.size > int(n.connOut) {
				return false
			}
			bytes += sizeBytesField(11, ev.size)
			end = off + 1
			return true
		})
		if err != nil || bytes == 0 {
			if c, s := n.clientOf(b.h); c != nil {
				n.abortBigWatch(c, s)
			}
			return false
		}
		b.pieceLeft = bytes
		b.resume = fillResume{watch: fillWatch{wi: b.wi, inc: b.inc, from: b.rev, to: b.rev + 1, rev: b.rev, compact: b.st.compactRev, offset: b.next, stopOffset: end}}
	}
	q := n.newRawFill(b.c, b.h, b.st, b.pieceLeft)
	if q == nil {
		b.waitP = true
		n.roomShort = true
		return true
	}
	w := b.resume.watch
	q.watch = &w
	q.bigWatch = b
	if b.resume.taken < 0 {
		full := 0
		_ = n.eachRevEvent(b.st, &b.x, revStart(b.st, b.rev), b.next, func(off int, ev bigEv) bool {
			if off >= b.resume.watch.stopOffset {
				return false
			}
			full += sizeBytesField(11, ev.size)
			return true
		})
		if err := n.seekColdFill(q, full-b.pieceLeft, b.resume.record.crc); err != nil {
			q.cancelled = true
			n.failRawFill(q)
		}
	} else {
		q.record, q.taken = b.resume.record, b.resume.taken
		q.outer, q.outerPos, q.outerSize = b.resume.outer, b.resume.outerPos, b.resume.outerSize
	}
	b.fetching = true
	b.waitP = false
	return true
}

func (n *node) publishRawFill(q *replyFill) bool {
	if q.stage.written != q.stage.size {
		q.cancelled = true
		n.failRawFill(q)
		return false
	}
	if !q.stage.commitPiece() {
		return false
	}
	stage := q.stage
	q.stage = nil
	resume := fillResume{record: q.record, taken: q.taken, outer: q.outer, outerPos: q.outerPos, outerSize: q.outerSize}
	if b := q.bigRange; b != nil {
		completed := q.taken - max(0, b.resume.taken)
		b.msgRecs -= completed
		b.taken += completed
		if b.sorts {
			b.next += int(completed)
		} else if completed > 0 {
			b.after = append(b.after[:0], b.st.key(q.ids[q.taken-1])...)
			b.begun = true
		}
		b.msgLeft -= stage.size
		b.pieceLeft -= stage.size
		b.resume = resume
		b.stage = stage
		b.fetching = false
		b.recount()
	} else if b := q.bigWatch; b != nil {
		resume.watch = *q.watch
		b.msgLeft -= stage.size
		b.pieceLeft -= stage.size
		b.resume = resume
		if b.pieceLeft == 0 {
			b.next = q.watch.stopOffset
		}
		b.stage = stage
		b.fetching = false
	}
	return true
}

// drainRawStage does not construct another whole-value buffer. Socket
// overlap/control capacity was funded at stage admission.
func rawSocketRoom(c *client) int {
	return (64 << 10) - int(c.waiting()) - (maxPlain + 2*recOverhead + 64)
}

func (n *node) drainRawStage(c *client, s *stream, stage *replyStage, watch bool) bool {
	c.stageSocket()
	for p := stage.front(); len(p) > 0; p = stage.front() {
		space := min(rawSocketRoom(c), cap(c.out)-len(c.out)-(maxPlain+2*recOverhead+64))
		win := s.win
		if watch {
			win = c.streamWin(s)
		}
		m := min(int64(len(p)), int64(space), n.connOut-c.waiting(), c.flow.conn, win)
		if m <= 0 {
			return false
		}
		w := msgW{s: &c.sock, sid: s.id, left: int(m)}
		w.write(p[:m])
		c.spendWin(s, m)
		stage.consume(int(m))
		n.markDirty(&c.sock)
	}
	return true
}

func (n *node) detachRawFill(q *replyFill) {
	n.endRawFill(q)
	q.ids = nil
	q.req = rangeReq{}
	q.bigRange = nil
	q.bigWatch = nil
}

func (n *node) endRawFill(q *replyFill) {
	if b := q.bigRange; b != nil {
		b.fetching = false
	}
	if b := q.bigWatch; b != nil {
		b.fetching = false
	}
}

func (n *node) failRawFill(q *replyFill) {
	c, s := n.clientOf(q.h)
	if c == nil {
		return
	}
	if q.bigWatch != nil {
		n.abortBigWatch(c, s)
	} else {
		n.abortBig(c, s)
	}
}

func (n *node) cancelRawRange(b *bigRange) {
	for _, q := range n.fills {
		if q.bigRange == b {
			q.cancelled = true
			q.stage.discard()
			q.st = nil
			q.ids = nil
			q.req = rangeReq{}
			q.bigRange = nil
		}
	}
}

func (n *node) cancelRawWatch(b *bigWatch) {
	for _, q := range n.fills {
		if q.bigWatch == b {
			q.cancelled = true
			q.stage.discard()
			q.st = nil
			q.bigWatch = nil
		}
	}
}
