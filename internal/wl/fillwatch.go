package wl

// A small watch fill holds no revision. Its change cursor is logical
// (revision, sub-revision), never an index into a compacted slice.
type fillWatch struct {
	wi                                    int32
	inc                                   uint32
	from, to, rev, compact                int64
	offset, phase                         int
	ev                                    bigEv
	fragmented                            bool
	messageEvents, messages, payloadBytes int
	stopOffset                            int
}

func (n *node) fillWatcher(q *replyFill) *watcher {
	w := q.watch
	if q.raw && q.bigWatch != nil {
		return &q.bigWatch.x
	}
	if w == nil || w.wi < 0 || int(w.wi) >= len(n.w.ws) {
		return nil
	}
	x := &n.w.ws[w.wi]
	if !x.active || x.inc != w.inc || mkHandleRaw(x.sock, x.gen, x.sid) != q.h {
		return nil
	}
	return x
}

func (n *node) validFillWatch(q *replyFill) bool {
	if q.raw && q.bigWatch != nil {
		c, _ := n.clientOf(q.h)
		return c != nil && q.bigWatch.st == n.st
	}
	if n.fillWatcher(q) == nil {
		return false
	}
	for _, other := range n.fills {
		if other != q && !other.cancelled && other.watch != nil && other.watch.wi == q.watch.wi && other.watch.inc == q.watch.inc && other.op > q.op {
			return false
		}
	}
	return true
}

func (n *node) endFillWatch(q *replyFill) {
	if q.raw {
		return
	}
	if x := n.fillWatcher(q); x != nil && n.validFillWatch(q) {
		x.fetchWait = false
	}
}

func (n *node) releaseFillRead(q *replyFill) {
	if !q.raw && q.view >= 0 && q.st == n.st {
		q.st.releaseRead(q.view)
		q.view = -1
	}
}

func (n *node) nextFillRecord(q *replyFill) (bool, error) {
	if q.watch != nil {
		return n.nextFillWatch(q)
	}
	if q.stream != nil {
		return n.nextFillStream(q), nil
	}
	return n.nextFillRange(q), nil
}

func (n *node) publishFill(q *replyFill) bool {
	if q.raw {
		return n.publishRawFill(q)
	}
	if q.watch != nil {
		return n.publishFillWatch(q)
	}
	return n.publishFillRange(q)
}

// startWatchFill sizes complete revisions before reserving a destination.
// A dependency count never changes a small message into a large hold.
func (n *node) startWatchFill(wi int32, x *watcher, c *client, s *stream, b *syncBudget) (bool, error) {
	if x.fetchWait || !b.left() || c.slow() {
		return false, nil
	}
	all := b.bytes
	b.bytes = min(all, max(0, n.connOut-c.waiting()))
	allowed := b.bytes
	defer func() { b.bytes = all - (allowed - b.bytes) }()
	if !b.left() {
		return false, nil
	}
	st := n.st
	j := revStart(st, x.syncRev)
	if j >= len(st.changes) {
		return true, nil
	}
	hdr := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
	body, events := hdr, 0
	// Size fragment boundaries during selection, not with a second history
	// walk. Copying these scalars rolls back a revision that is too large.
	fragments := watchFillSize{header: hdr, part: hdr + 2}
	to := x.syncRev
	for j < len(st.changes) && (events == 0 || b.left()) {
		r := st.changes[j].rev
		bytes, count := 0, 0
		beforeFragments := fragments
		err := n.eachRevEvent(st, x, j, 0, func(_ int, ev bigEv) bool {
			add := sizeBytesField(11, ev.size)
			bytes += add
			fragments.add(add)
			count++
			return true
		})
		if err != nil {
			return false, err
		}
		if hdr+bytes > replyRoom {
			fragments = beforeFragments
			if events == 0 {
				n.startBigWatch(wi, x, c, s, j)
				return false, nil
			}
			break
		}
		body += bytes
		events += count
		next := revStart(st, r+1)
		b.changes -= next - j
		b.events -= count
		b.bytes -= int64(bytes)
		to = r + 1
		j = next
	}
	if j == len(st.changes) {
		to = st.rev + 1
	}
	if events == 0 {
		x.syncRev = to
		return to > st.rev, nil
	}
	fragmented := x.fragment && body >= maxRequestBytes && events > 1
	wire := 5 + body
	if fragmented {
		wire = fragments.wire()
	}
	q := n.newWatchFill(wi, x, c, s, wire, to)
	if q == nil {
		return false, nil
	}
	q.watch.fragmented = fragmented
	if !fragmented {
		// The first message can reuse selection's exact count and size.
		q.watch.messageEvents, q.watch.payloadBytes = events, body
	}
	return false, nil
}

func (n *node) newWatchFill(wi int32, x *watcher, c *client, s *stream, wire int, to int64) *replyFill {
	n.stageCredits.limit = n.sizes.clientPool
	wake := n.fillCreditWake
	if !n.stageCredits.take(stageUnit, n.stageOther(0)) {
		n.roomShort = true
		return nil
	}
	c.growOut(stageUnit)
	stage := n.admitStage(c, wire)
	if stage == nil {
		c.growOut(-stageUnit)
		n.stageCredits.give(stageUnit)
		n.fillCreditWake = wake
		n.roomShort = true
		return nil
	}
	n.fillSequence++
	q := &replyFill{
		h: mkHandle(c, s.id), c: c, st: n.st, stage: stage, view: -1, header: n.st.rev, headerTerm: n.term, meta: stageUnit, op: n.fillSequence,
		watch: &fillWatch{wi: wi, inc: x.inc, from: x.syncRev, to: to, rev: x.syncRev, compact: n.st.compactRev},
	}
	c.heldCounted += stageUnit
	c.fillOwners++
	x.fetchWait = true
	n.fills = append(n.fills, q)
	return q
}

func (n *node) nextFillWatch(q *replyFill) (bool, error) {
	w := q.watch
	x := n.fillWatcher(q)
	if x == nil {
		q.cancelled = true
		return false, nil
	}
	if w.phase == 1 && w.ev.prev {
		ks, ok := q.st.stateAt(w.ev.ch.key, w.ev.ch.rev-1)
		if !ok {
			return false, errPrevCompacted
		}
		q.record = fillRecord{active: true, field: 3, id: w.ev.ch.key, state: ks, size: kvSize(int(q.st.keys[w.ev.ch.key].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)}
		w.phase = 2
		return true, nil
	}
	if w.phase != 0 {
		w.offset++
		w.phase = 0
		w.messageEvents--
	}
	for w.rev < w.to {
		j := revStart(q.st, w.rev)
		if j >= len(q.st.changes) || q.st.changes[j].rev >= w.to {
			return false, nil
		}
		w.rev = q.st.changes[j].rev
		found := false
		var putSlot uint32
		err := n.eachRevEvent(q.st, x, j, w.offset, func(off int, ev bigEv) bool {
			// Consume the selection's history hint in this same turn;
			// never retain a physical slot across a yield or mutation.
			putSlot, ev.putSlot = ev.putSlot, 0
			w.offset, w.ev = off, ev
			found = true
			return false
		})
		if err != nil {
			return false, err
		}
		if !found {
			w.rev++
			w.offset = 0
			continue
		}
		if q.raw && w.offset >= w.stopOffset {
			return false, nil
		}
		ev := w.ev
		if !q.raw && (w.messages == 0 || w.messageEvents == 0) {
			if err := n.beginFillWatchMessage(q, x); err != nil {
				return false, err
			}
		}
		p := appendFillLen(q.outer[:0], 11, ev.size)
		if ev.ch.del {
			p = appendVarintField(p, 1, 1)
		}
		q.outerPos, q.outerSize = 0, len(p)
		var ks ckptState
		switch {
		case ev.ch.del:
			ks.mod = ev.ch.rev
		case putSlot != 0:
			// eachRevEvent just resolved this immutable metadata. No
			// mutation occurs between its callback and this use.
			e := &q.st.hist[putSlot]
			ks = ckptState{create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen, vsum: e.vsum}
		default:
			var ok bool
			ks, ok = q.st.putStateAt(ev.ch.key, ev.ch.rev)
			if !ok {
				return false, errPrevCompacted
			}
		}
		q.record = fillRecord{active: true, field: 2, id: ev.ch.key, state: ks, size: kvSize(int(q.st.keys[ev.ch.key].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)}
		w.phase = 1
		return true, nil
	}
	return false, nil
}

func (n *node) publishFillWatch(q *replyFill) bool {
	x := n.fillWatcher(q)
	if x == nil || !n.validFillWatch(q) {
		q.cancelled = true
		return false
	}
	if q.watch.compact != n.st.compactRev {
		return false
	}
	c, s := n.clientOf(q.h)
	if c == nil {
		q.cancelled = true
		return false
	}
	if q.stage.written != q.stage.size {
		q.cancelled = true
		return false
	}
	stage := q.stage
	q.stage = nil
	if !c.holdStage(s, stage) {
		q.stage = stage
		q.cancelled = true
		return false
	}
	// Queue ownership and the public revision move together, including
	// zero DATA windows. Installs must not replay this committed batch.
	x.syncRev = q.watch.to
	if x.syncRev > n.st.rev {
		x.syncRev = 0
	}
	x.deliveryPlant.retire(x.syncRev)
	x.fetchWait = false
	x.sent = true
	n.gs.calls[s.kind].sent += int64(q.watch.messages)
	n.gs.sentBytes += int64(q.watch.payloadBytes)
	return true
}

// startWatchData acquires only the history actually needed, in the
// same non-yielding transition that publishes the first DATA byte.
func (n *node) startWatchData(b *bigWatch, c *client, s *stream) bool {
	if b.held {
		return true
	}
	prev := false
	err := n.eachRevEvent(b.st, &b.x, revStart(b.st, b.rev), 0, func(_ int, ev bigEv) bool { prev = prev || ev.prev; return true })
	if b.rev < b.st.compactRev || err != nil {
		if x := n.watcherOf(b); x != nil {
			n.writeCompacted(c, s, x)
			n.w.remove(b.wi)
		}
		return false
	}
	if prev {
		b.st.holdRead(b.rev - 1)
	}
	b.st.holdRead(b.rev)
	b.held, b.heldPrev = true, prev
	return true
}

// walkFillEvents is metadata-only and retains no per-event plan.
func (n *node) walkFillEvents(st *store, x *watcher, from, to int64, offset int, f func(bigEv) bool) error {
	for j := revStart(st, from); j < len(st.changes) && st.changes[j].rev < to; {
		stop := false
		err := n.eachRevEvent(st, x, j, offset, func(_ int, ev bigEv) bool {
			if !f(ev) {
				stop = true
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		j = revStart(st, st.changes[j].rev+1)
		offset = 0
	}
	return nil
}

func (n *node) watchFillWire(st *store, x *watcher, from, to, header int64, term uint64, fragmented bool) (int, error) {
	hdr := sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(2, uint64(x.id))
	part, count, total := hdr, 0, 0
	if fragmented {
		part += 2
	}
	err := n.walkFillEvents(st, x, from, to, 0, func(ev bigEv) bool {
		add := sizeBytesField(11, ev.size)
		if fragmented && count > 0 && part+add >= maxRequestBytes {
			total += 5 + part
			part = hdr + 2
			count = 0
		}
		part += add
		count++
		return true
	})
	if count > 0 {
		if fragmented {
			part -= 2
		}
		total += 5 + part
	}
	return total, err
}

func (n *node) beginFillWatchMessage(q *replyFill, x *watcher) error {
	w := q.watch
	hdr := sizeBytesField(1, n.hdrSizeTerm(q.header, q.headerTerm)) + sizeVarintField(2, uint64(x.id))
	part, count := hdr, 0
	if w.fragmented {
		part += 2
	}
	more := false
	cached := !w.fragmented && w.messages == 0 && w.messageEvents > 0
	if cached {
		part, count = w.payloadBytes, w.messageEvents
	} else {
		err := n.walkFillEvents(q.st, x, w.rev, w.to, w.offset, func(ev bigEv) bool {
			add := sizeBytesField(11, ev.size)
			if w.fragmented && count > 0 && part+add >= maxRequestBytes {
				more = true
				return false
			}
			part += add
			count++
			return true
		})
		if err != nil {
			return err
		}
		if w.fragmented && !more {
			part -= 2
		}
		w.payloadBytes += part
	}
	w.messageEvents = count
	w.messages++
	q.prefix(part)
	q.length(n.hdrSizeTerm(q.header, q.headerTerm))
	q.varint(1, n.clusterID)
	q.varint(2, n.id)
	q.varint(3, uint64(q.header))
	q.varint(4, q.headerTerm)
	q.varint(2, uint64(x.id))
	if more {
		q.varint(7, 1)
	}
	return nil
}
