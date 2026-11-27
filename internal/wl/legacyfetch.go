package wl

import (
	"bytes"
	"encoding/binary"
	"math"
)

func roundedFetch(n int64) int64 { return (n + stageUnit - 1) &^ (stageUnit - 1) }

func (n *node) ensureFetchBook() {
	if n.fillBook == nil {
		b := newFetchWorkBook(n.st.budget)
		n.fillBook = &b
	}
}

func (n *node) beginLegacyRange(c *client, s *stream, r *rangeReq, msg []byte) {
	n.ensureFetchBook()
	n.st.fetchOwner = n
	if n.legacyRetry != nil {
		n.legacyReading = n.legacyRetry.owner
		return
	}
	view := n.st.rev
	if r.rev > 0 {
		view = r.rev
	}
	n.legacyReading = legacyOwner{c: c, h: mkHandle(c, s.id), req: *r, view: view, header: n.st.rev, term: n.term, msg: msg}
}

func (n *node) beginLegacyHeld(hr *heldReply) {
	n.ensureFetchBook()
	n.st.fetchOwner = n
	n.legacyReading = legacyOwner{
		c: hr.c, h: hr.h, hr: hr, view: hr.hold, header: hr.hdrRev, term: n.term,
		wire: 5 + sizeBytesField(1, n.hdrSizeTerm(hr.hdrRev, math.MaxUint64)) + hr.body,
	}
}

func (n *node) collectLegacy(id uint32, mod int64, size, sum uint32) {
	if n.legacyPending != nil || n.legacyRunning != nil {
		n.legacyAgain = true
		return
	}
	b := n.legacyCollect
	if b == nil {
		if n.legacyMemo != nil && !n.legacyMemo.needed {
			n.dropLegacyBatch(n.legacyMemo)
		}
		n.ensureFetchBook()
		lease := n.fillBook.reserve(stageUnit)
		if lease == nil {
			n.legacyAgain = true
			return
		}
		n.fillSequence++
		b = &legacyBatch{st: n.st, owner: n.legacyReading, lease: lease, op: n.fillSequence, needed: true}
		n.legacyCollect = b
	}
	b.add(id, mod, size, sum)
}

func (n *node) dropCollectingLegacy() {
	b := n.legacyCollect
	if b == nil || b.frozen {
		return
	}
	n.legacyCollect = nil
	n.dropLegacyBatch(b)
}

func (n *node) awaitLegacyRange(h handle, msg []byte) {
	c, _ := n.clientOf(h)
	if c == nil {
		return
	}
	if b := n.legacyCollect; b != nil && b.owner.h == h && b.owner.hr == nil {
		if b.owner.msgCharge == 0 {
			b.owner.msg = bytes.Clone(msg)
			b.owner.msgCharge = int(roundedFetch(int64(cap(b.owner.msg))))
			c.growOut(b.owner.msgCharge)
			c.heldCounted += int64(b.owner.msgCharge)
			_ = parseRange(b.owner.msg, &b.owner.req)
		}
	} else {
		for _, r := range n.awayReads {
			if r.h == h {
				return
			}
		}
		copyMsg := bytes.Clone(msg)
		charge := int(roundedFetch(int64(cap(copyMsg))))
		c.growOut(charge)
		c.heldCounted += int64(charge)
		n.awayReads = append(n.awayReads, awayRead{h: h, msg: copyMsg, c: c, charge: charge})
		n.legacyAgain = true
	}
	n.freezeLegacy()
}

func (n *node) freezeLegacy() {
	b := n.legacyCollect
	if b == nil {
		n.legacyAgain = true
		return
	}
	n.legacyCollect = nil
	b.freeze()
	b.created = n.now
	n.legacyReading = legacyOwner{}
	n.legacyPending = b
	n.legacyPrefer = true
	// This may reserve alongside a cursor only when the SAME book has
	// enough actual free credit. The worker slot remains exclusive.
	n.prepareLegacy(b)
}

func (n *node) legacyOutputBound(b *legacyBatch) int {
	if b.owner.wire > 0 {
		return b.owner.wire
	}
	r := &b.owner.req
	var count, candidates int64
	maxSize := 0
	visit := func(id uint32) bool {
		ks, ok := b.st.stateAt(id, b.owner.view)
		if !ok {
			return true
		}
		candidates++
		if !r.passes(&ks) {
			return true
		}
		count++
		vlen := int(ks.vlen)
		lease := ks.lease
		if r.keysOnly {
			vlen = 0
			if r.sortTarget != sortByValue {
				lease = 0
			}
		}
		maxSize = max(maxSize, kvSize(int(b.st.keys[id].klen), vlen, ks.create, ks.mod, ks.ver, lease))
		return true
	}
	if len(r.end) == 0 {
		if id, ok := b.st.find(r.key); ok {
			visit(id)
		}
	} else {
		end := r.end
		if len(end) == 1 && end[0] == 0 {
			end = nil
		}
		b.st.tree.scan(r.key, end, visit)
	}
	b.owner.candidates = candidates
	returned := count
	if r.limit > 0 {
		returned = min(returned, r.limit)
	}
	body := int64(5+sizeBytesField(1, n.hdrSizeTerm(b.owner.header, math.MaxUint64))+12) + returned*int64(sizeBytesField(2, maxSize))
	return int(min(body, n.connOut+n.sizes.reserve()))
}

func (n *node) legacyFinishBound(b *legacyBatch) int64 {
	wire := n.legacyOutputBound(b)
	material := int64(0)
	if b.owner.hr == nil {
		for i := range b.n {
			e := b.entry(i)
			ks, ok := b.st.putStateAt(e.key, e.mod)
			if !ok {
				continue
			}
			material += int64(kvSize(int(b.st.keys[e.key].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease))
		}
	}
	candidates := int64(b.n)
	if b.owner.hr == nil {
		candidates = b.owner.candidates
	}
	material += candidates * 24
	if b.owner.hr == nil && b.owner.req.sortsOther() {
		material += candidates * 96
	}
	f := int64(wire)
	// A flat held result and a growFor replacement are alternatives.
	// The old socket is still counted while its replacement is copied.
	need := f + (f/maxFrame+2)*9 + (f/maxPlain+2)*(recOverhead+8) + maxPlain + recOverhead + 8
	out := max(64<<10, max(roundedFetch(f), roundedFetch(int64(len(b.owner.c.out))+need)))
	return roundedFetch(material) + out + (64 << 10) + stageUnit
}

func (n *node) prepareLegacy(b *legacyBatch) bool {
	if b.prepared {
		return true
	}
	if b.full || b.st != n.st || b.owner.c == nil || b.owner.c.dead {
		return false
	}
	need := n.legacyFinishBound(b)
	physical := n.cpool + n.cout + int64(max(n.clients, n.active)+n.stageOrphans)*idleConn + n.stagePromises + n.legacyPromises - n.mainCaptureCount()
	if physical+need > n.sizes.clientPool {
		n.legacyWaitP = true
		return false
	}
	payload := roundedFetch(b.payload)
	if !b.lease.grow(payload) {
		return false
	}
	wire := n.legacyOutputBound(b)
	if !n.msgRoom(b.owner.c, int64(wire)) {
		// The payload has not been allocated. Return only this new
		// reservation; high-water metadata remains owned and counted.
		b.lease.book.used -= payload
		b.lease.bytes -= payload
		n.legacyWaitP = true
		return false
	}
	b.promise = need
	n.legacyPromises += need
	b.pending = int64(wire)
	b.owner.c.stagePending += b.pending
	// The complete finish commitment above is in P, not in I.
	c := b.owner.c
	covered := c.roomHeld - c.coveredRoom
	c.coveredRoom += covered
	n.coveredRoom += covered
	n.reserved -= covered
	if b.owner.hr == nil {
		b.st.holdRead(b.owner.view)
		b.readHeld = true
	}
	b.payloadCharge = payload
	b.data = make([]byte, int(payload))
	b.prepared = true
	if b.held && b.err == nil {
		b.initHeldData()
	}
	return true
}

func (n *node) retryLegacyOwner(b *legacyBatch) {
	if b.st != n.st || b.owner.c == nil || b.owner.c.dead {
		b.cancelled = true
		return
	}
	if b.pending > 0 {
		b.owner.c.stagePending -= b.pending
		b.pending = 0
	}
	n.legacyRetry = b
	b.owner.c.promiseLocal, b.owner.c.promiseTotal = &b.promise, &n.legacyPromises
	if hr := b.owner.hr; hr != nil {
		hr.fetching = false
		n.serveHeld()
	} else if c, s := n.clientOf(b.owner.h); c != nil {
		var req rangeReq
		if parseRange(b.owner.msg, &req) == nil {
			n.serveRange(c, s, &req, b.owner.msg)
		}
	}
	n.legacyRetry = nil
	b.owner.c.promiseLocal, b.owner.c.promiseTotal = nil, nil
	b.needed = false
	if b.promise > 0 {
		n.legacyPromises -= b.promise
		b.promise = 0
	}
	if b.readHeld && b.st == n.st {
		b.st.releaseRead(b.owner.view)
		b.readHeld = false
	}
	b.owner.c.node.giveRoom(b.owner.c, b.owner.c.dead)
}

func (n *node) finishLegacyPiece(b *legacyBatch, token fillToken, err error) error {
	if n.legacyRunning != b || token.op != b.op || token.piece != b.piece || !b.running {
		return nil
	}
	b.lease.acknowledge()
	b.running = false
	n.legacyRunning = nil
	n.releaseFetchSources(&b.fetchSources)
	if err == nil {
		if b.held {
			for i := b.cursor; i < b.resultCursor; i++ {
				size := int64(binary.LittleEndian.Uint32(b.data[b.recordOffset:b.recordOffset+4]) &^ legacyRecordReady)
				b.recordOffset += 4 + size
			}
		}
		b.cursor, b.valuePos, b.crc = b.resultCursor, b.resultPos, b.resultCRC
	} else {
		b.err = err
		b.cursor = b.n
	}
	if b.cursor == b.n {
		if n.legacyMemo != nil && n.legacyMemo != b {
			n.dropLegacyBatch(n.legacyMemo)
		}
		b.ready = true
		n.legacyPending = nil
		n.legacyMemo = b
		if b.st == n.st {
			b.st.legacyView = b
		}
	}
	n.legacyPrefer = false
	return nil
}

func (n *node) pumpLegacy() bool {
	if !n.cursorFetch {
		return false
	}
	if n.legacyRunning != nil {
		return true
	}
	if b := n.legacyMemo; b != nil {
		if b.cancelled || b.st != n.st || b.held && !b.needed {
			n.dropLegacyBatch(b)
		} else if b.needed {
			n.retryLegacyOwner(b)
			return true
		}
	}
	b := n.legacyPending
	if b == nil {
		if n.legacyAgain && !n.legacyWaitP {
			n.recollectLegacy()
		}
		b = n.legacyPending
		if b == nil {
			return false
		}
	}
	if b.cancelled || b.st != n.st || b.owner.c.dead {
		n.dropLegacyBatch(b)
		return true
	}
	if !b.prepared && b.owner.hr == nil && (b.owner.header != n.st.rev || b.owner.view < n.st.compactRev) {
		n.recollectLegacyBatch(b)
		return true
	}
	if b.full {
		if n.fillBook.active == nil && n.fillActive == nil {
			n.recollectLegacyBatch(b)
		}
		return false
	}
	if !n.prepareLegacy(b) {
		if n.legacyMemo != nil && !n.legacyMemo.needed {
			n.dropLegacyBatch(n.legacyMemo)
			return n.prepareLegacy(b)
		}
		return false
	}
	if err := n.planLegacyPiece(b); err != nil {
		b.err = err
		b.ready = true
		n.legacyPending = nil
		n.legacyMemo = b
		b.st.legacyView = b
		return true
	}
	b.piece++
	token := fillToken{op: b.op, piece: b.piece}
	if !b.lease.start() {
		n.releaseFetchSources(&b.fetchSources)
		return false
	}
	b.running = true
	n.legacyRunning = b
	err := b.executePiece(true)
	if err != nil {
		err = b.executePiece(true)
	}
	if err == nil {
		_ = n.finishLegacyPiece(b, token, nil)
		return true
	}
	if !n.queueFetchTask(&fetchTask{work: func() error { return b.executePiece(false) }, done: func(err error) error { return n.finishLegacyPiece(b, token, err) }}) {
		b.lease.acknowledge()
		b.running = false
		n.legacyRunning = nil
		n.releaseFetchSources(&b.fetchSources)
	}
	return true
}

func (n *node) dropLegacyBatch(b *legacyBatch) {
	if b.running {
		b.cancelled = true
		return
	}
	if b.st != nil && b.st.legacyView == b {
		b.st.legacyView = nil
	}
	if n.legacyMemo == b {
		n.legacyMemo = nil
	}
	if n.legacyPending == b {
		n.legacyPending = nil
	}
	if n.legacyCollect == b {
		n.legacyCollect = nil
	}
	if b.readHeld && b.st == n.st {
		b.st.releaseRead(b.owner.view)
	}
	if b.pending > 0 {
		b.owner.c.stagePending -= b.pending
	}
	if b.promise > 0 {
		n.legacyPromises -= b.promise
	}
	if b.owner.msgCharge > 0 {
		b.owner.c.growOut(-b.owner.msgCharge)
		b.owner.c.heldCounted -= int64(b.owner.msgCharge)
	}
	c := b.owner.c
	for index := &b.index; index != nil; {
		clear(index.pages[:])
		next := index.next
		index.next = nil
		index = next
	}
	b.data = nil
	b.st = nil
	b.owner = legacyOwner{}
	b.lease.release()
	if c != nil {
		n.giveRoom(c, c.dead)
	}
}

func (n *node) recollectLegacyBatch(b *legacyBatch) {
	owner := b.owner
	b.owner.msgCharge = 0
	n.dropLegacyBatch(b)
	if owner.hr != nil {
		owner.hr.fetching = false
		n.serveHeld()
	} else if c, s := n.clientOf(owner.h); c != nil {
		var r rangeReq
		if parseRange(owner.msg, &r) == nil {
			n.serveRange(c, s, &r, owner.msg)
		}
	}
	// A replacement collector adopts an immutable request copy above;
	// the previous raw request stays charged through that copy overlap.
	if owner.msgCharge > 0 {
		owner.c.growOut(-owner.msgCharge)
		owner.c.heldCounted -= int64(owner.msgCharge)
	}
}

func (n *node) recollectLegacy() {
	n.legacyAgain = false
	reads := n.awayReads
	n.awayReads = nil
	for _, r := range reads {
		if c, s := n.clientOf(r.h); c != nil {
			var req rangeReq
			if parseRange(r.msg, &req) == nil {
				n.serveRange(c, s, &req, r.msg)
			}
		}
		if r.c != nil {
			r.c.growOut(-r.charge)
			r.c.heldCounted -= int64(r.charge)
		}
	}
	for _, hr := range n.heldReplies {
		if hr.fetching {
			hr.fetching = false
		}
	}
	n.serveHeld()
}

func (n *node) cancelLegacy(c *client, sid uint32) {
	for _, b := range []*legacyBatch{n.legacyCollect, n.legacyPending, n.legacyRunning, n.legacyMemo} {
		if b != nil && b.owner.c == c && (sid == 0 || b.owner.h == mkHandle(c, sid)) {
			b.cancelled = true
		}
	}
}

func (n *node) invalidateLegacy(st *store) {
	for _, b := range []*legacyBatch{n.legacyCollect, n.legacyPending, n.legacyRunning, n.legacyMemo} {
		if b != nil && b.st == st {
			if b.readHeld {
				st.releaseRead(b.owner.view)
				b.readHeld = false
			}
			b.st = nil
			b.owner.hr = nil
			b.cancelled = true
		}
	}
}
