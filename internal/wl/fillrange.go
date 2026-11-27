package wl

import (
	"bytes"
	"errors"
)

func (n *node) tryFillRange(c *client, s *stream, r *rangeReq, msg []byte) bool {
	if !n.cursorFetch || (s.kind != kRange && s.kind != kRangeStream) || r.sortsOther() {
		return false
	}
	protected := n.stageCredits.used > 0 || n.stagePromises > 0 || n.legacyPromises > 0
	if r.countOnly && !protected {
		return false
	}
	n.stats.rangeFillScans++
	st := n.st
	view := st.rev
	if r.rev > 0 {
		view = r.rev
	}
	var count, taken, matched, raw int64
	bytesOut := 0
	cold, big := false, false
	read := r.readLimit()
	if r.countOnly {
		read = 0
	}
	walk := func(id uint32) bool {
		if raw > n.oneShot() {
			big = true
			return false
		}
		ks, ok := st.stateAt(id, view)
		if !ok {
			return true
		}
		count++
		if taken >= read {
			return true
		}
		taken++
		k := &st.keys[id]
		size := kvSize(int(k.klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)
		rawSize := size
		if r.keysOnly {
			// Preserve keyAt's residency-dependent before-next-key operand;
			// only the returned projection omits value and lease.
			rawSize = kvSize(int(k.klen), 0, ks.create, ks.mod, ks.ver, ks.lease)
			if k.rec != 0 && k.mod <= view {
				rawSize = int(k.rlen)
			}
			size = kvSize(int(k.klen), 0, ks.create, ks.mod, ks.ver, 0)
		}
		raw += int64(rawSize)
		if !r.passes(&ks) {
			return true
		}
		matched++
		if r.limit > 0 && matched > r.limit {
			return true
		}
		bytesOut += sizeBytesField(2, size)
		cold = cold || !r.keysOnly && ks.vlen > 0 && (k.rec == 0 || k.mod != ks.mod)
		return true
	}
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			walk(id)
		}
	} else {
		end := r.end
		if len(end) == 1 && end[0] == 0 {
			end = nil
		}
		st.tree.scan(r.key, end, walk)
	}
	if big {
		n.startBig(c, s, msg)
		return true
	}
	more := r.limit > 0 && matched > r.limit
	body := sizeBytesField(1, n.hdrSizeAt(st.rev)) + bytesOut + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	selected := matched
	if r.limit > 0 {
		selected = min(selected, r.limit)
	}
	admission := body + 5
	if s.kind == kRangeStream {
		admission += 32 * (int(selected)/initialStreamChunk + 1)
	}
	if n.cannotFit(int64(admission)) {
		n.startBig(c, s, msg)
		return true
	}
	wire := body + 5
	if s.kind == kRangeStream {
		wire = n.streamFillWire(st, r, view, st.rev, selected, count, more)
	}
	// Resident values may use the ordinary within-share fast path. A
	// logical output loan still needs P backing even without disk I/O.
	if !cold && !protected && n.roomNeeded(c, int64(wire)) == 0 {
		return false
	}
	meta := stageUnit + int((int64(len(msg))+stageUnit-1)/stageUnit)*stageUnit
	n.stageCredits.limit = n.sizes.clientPool
	other := n.stageOther(0)
	if !n.stageCredits.take(int64(meta), other) {
		n.waitFillRoom(c, s, msg, body-sizeBytesField(1, n.hdrSizeAt(st.rev)), wire-body-5)
		return true
	}
	c.growOut(meta)
	stage := n.admitStageQuota(c, wire, int64(admission))
	if stage == nil {
		c.growOut(-meta)
		n.stageCredits.give(int64(meta))
		n.waitFillRoom(c, s, msg, body-sizeBytesField(1, n.hdrSizeAt(st.rev)), wire-body-5)
		return true
	}
	n.fillSequence++
	q := &replyFill{h: mkHandle(c, s.id), c: c, st: st, stage: stage, msg: cloneFillRequest(msg), view: view, header: st.rev, headerTerm: n.term, count: count, selected: matched, more: more, meta: meta, op: n.fillSequence}
	if r.limit > 0 {
		q.selected = min(q.selected, r.limit)
	}
	_ = parseRange(q.msg, &q.req)
	c.heldCounted += int64(meta)
	c.fillOwners++
	st.holdRead(view)
	if s.kind == kRangeStream {
		q.stream = &fillStream{chunk: min(initialStreamChunk, q.selected)}
	} else {
		q.prefix(body)
		q.length(n.hdrSizeAt(q.header))
		q.varint(1, n.clusterID)
		q.varint(2, n.id)
		q.varint(3, uint64(q.header))
		q.varint(4, n.term)
	}
	if !cold {
		err := n.planFill(q, nil)
		if err != nil && !errors.Is(err, errAway) {
			if n.st.bad == nil {
				n.st.bad = err
			}
			q.stage.discard()
			n.releaseFillRead(q)
			q.freeMeta(n)
			n.replyError(c, s.id, codeInternal, err.Error())
			return true
		}
		if q.ready && n.publishFill(q) {
			n.releaseFillRead(q)
			q.freeMeta(n)
			return true
		}
	}
	n.fills = append(n.fills, q)
	return true
}

func (n *node) nextFillRange(q *replyFill) bool {
	if q.taken >= q.selected {
		return false
	}
	st := q.st
	if q.ids != nil {
		id := q.ids[q.taken]
		ks, ok := st.stateAt(id, q.view)
		if !ok {
			return false
		}
		if q.req.keysOnly {
			ks.vlen = 0
			ks.lease = 0
		}
		q.record = fillRecord{active: true, field: 2, id: id, state: ks, size: kvSize(int(st.keys[id].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)}
		return true
	}
	visit := func(id uint32) bool {
		if q.hasLast && id == q.last {
			return true
		}
		ks, ok := st.stateAt(id, q.view)
		if !ok || !q.req.passes(&ks) {
			return true
		}
		q.last, q.hasLast = id, true
		if q.req.keysOnly {
			ks.vlen = 0
			ks.lease = 0
		}
		q.record = fillRecord{active: true, field: 2, id: id, state: ks, size: kvSize(int(st.keys[id].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)}
		return false
	}
	if len(q.req.end) == 0 {
		if !q.hasLast {
			if id, ok := st.find(q.req.key); ok {
				visit(id)
			}
		}
	} else {
		start := q.req.key
		if q.hasLast {
			start = st.key(q.last)
		}
		end := q.req.end
		if bytes.Equal(end, []byte{0}) {
			end = nil
		}
		st.tree.scan(start, end, visit)
	}
	return q.record.active
}

func (n *node) publishFillRange(q *replyFill) bool {
	c, s := n.clientOf(q.h)
	if c == nil {
		q.cancelled = true
		return false
	}
	c.stageSocket()
	if cap(c.out)-len(c.out) < maxPlain+recOverhead+64 {
		return false
	}
	if q.stage.written != q.stage.size {
		q.cancelled = true
		return false
	}
	stage := q.stage
	// Ownership leaves the private operation before stream completion
	// can cancel its old request. No syscall is needed for this commit.
	q.stage = nil
	n.openStream(c, s)
	if !c.holdStage(s, stage) {
		q.stage = stage
		q.cancelled = true
		return false
	}
	n.gs.rangesN++
	frames := 1
	if q.stream != nil {
		frames = q.stream.frames
	}
	n.gs.calls[s.kind].sent += int64(frames)
	n.gs.sentBytes += int64(stage.size - 5*frames)
	n.stats.replies++
	n.callEnded(s, 0)
	n.endStreamOK(c, s)
	n.markDirty(&c.sock)
	return true
}

func (n *node) cancelFills(c *client, sid uint32) {
	n.cancelLegacy(c, sid)
	for _, q := range n.fills {
		if q.c == c && (sid == 0 || q.h == mkHandle(c, sid)) && q.stage != nil {
			q.cancelled = true
			n.releaseFillRead(q)
			q.st = nil
			if q.raw {
				n.detachRawFill(q)
			}
			q.stage.discard()
		}
	}
}

func (n *node) removeFill(i int) {
	copy(n.fills[i:], n.fills[i+1:])
	n.fills[len(n.fills)-1] = nil
	n.fills = n.fills[:len(n.fills)-1]
}

func (q *replyFill) freeMeta(n *node) {
	q.c.growOut(-q.meta)
	q.c.heldCounted -= int64(q.meta)
	q.c.fillOwners--
	if q.c.fillOwners == 0 && q.c.stages == 0 && q.c.stageOrphan {
		q.c.stageOrphan = false
		n.stageOrphans--
	}
	n.stageCredits.give(int64(q.meta))
	q.msg = nil
	q.req = rangeReq{}
	q.st = nil
	q.stage = nil
	q.c = nil
	q.watch = nil
	q.stream = nil
	q.ids = nil
	q.bigRange = nil
	q.bigWatch = nil
}

func (n *node) invalidateFills(st *store) {
	n.invalidateLegacy(st)
	for _, q := range n.fills {
		if q.st == st && q.stage != nil {
			n.releaseFillRead(q)
			q.st = nil
			if q.raw {
				n.detachRawFill(q)
			}
			q.stage.discard()
		}
	}
}

func (n *node) fillsReady() bool {
	if n.cursorFetch && n.fillCreditWake {
		return true
	}
	if n.fillActive != nil || n.legacyRunning != nil {
		return false
	}
	if b := n.legacyMemo; b != nil && (b.cancelled || (b.held || b.material) && !b.needed || b.needed && !n.legacyWaitP) {
		return true
	}
	if !n.legacyWaitP && (n.legacyPending != nil || n.legacyAgain) {
		return true
	}
	for _, q := range n.fills {
		if q.cancelled || q.st != n.st || q.c.dead || q.watch != nil && (!n.validFillWatch(q) || !q.raw && q.watch.compact != n.st.compactRev) {
			return true
		}
		if !q.ready {
			return len(n.fetching) == 0 && len(n.st.fetched) == 0
		}
		if q.c.waiting() < 32<<10 {
			return true
		}
	}
	return false
}
