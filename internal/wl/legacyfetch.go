package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	if n.legacyReading.c == nil {
		return
	}
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
		b.owner.c.fillOwners++
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
	if b.owner.wire > 0 && (b.owner.hr != nil || b.owner.share == n.connOut) {
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
	if _, s := n.clientOf(b.owner.h); s != nil && s.kind == kRangeStream {
		body += 32 * (returned/initialStreamChunk + 1)
	}
	b.owner.share = n.connOut
	b.owner.wire = int(min(body, n.connOut+n.sizes.reserve()))
	return b.owner.wire
}

// Exact-size record construction avoids append growth/copy peaks. Small
// Go classes are bounded by the next power of two; large objects by 8 KiB.
func recordHeapBound(n int) int64 {
	if n <= 0 {
		return 0
	}
	if n > 32<<10 {
		return roundedFetch(int64(n))
	}
	size := 16
	for size < n {
		size *= 2
	}
	return int64(size)
}

func (n *node) legacyFinishBound(b *legacyBatch) int64 {
	wire := n.legacyOutputBound(b)
	// #908: only main's unchanged candidate/kvRecs scratch is outside
	// this proof. Canonical data, indices, retry/output and copies stay funded.
	if b.material {
		if b.materialW {
			return stageCapacity(wire) + (64 << 10) + 2*stageUnit
		}
		return roundedFetch(b.payload) + (64 << 10) + 2*stageUnit
	}
	material := int64(0)
	if b.owner.hr == nil {
		if b.materialNeed == 0 {
			b.walkMaterial(func(id uint32, ks ckptState) bool {
				b.materialNeed += recordHeapBound(kvSize(int(b.st.keys[id].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease))
				if b.owner.req.keysOnly {
					b.materialNeed += recordHeapBound(kvSize(int(b.st.keys[id].klen), 0, ks.create, ks.mod, ks.ver, 0))
				}
				return true
			}, nil)
		}
		material = b.materialNeed
	}
	candidates := int64(b.n)
	if b.owner.hr == nil {
		candidates = b.owner.candidates
	}
	if !b.material {
		if b.owner.hr != nil {
			material += candidates * 24
		}
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
	if !b.material && b.owner.hr == nil && fetchCoreWork+b.lease.bytes+roundedFetch(b.payload) > n.fillBook.limit {
		if !n.convertLegacyMaterial(b) {
			return false
		}
	}
	need := n.legacyFinishBound(b)
	if !b.material && b.owner.hr == nil {
		floor := n.sizes.clientInBufs()
		headers := max(1, int(floor/int64(clientInBuf+clientPtBuf+slotBytes)))
		if need+floor+int64(headers)*maxHeaderBlock+idleConn > n.sizes.clientPool {
			if !n.convertLegacyMaterial(b) {
				return false
			}
			return n.prepareLegacy(b)
		}
	}
	physical := n.stageCredits.used + n.stageOther(0)
	if physical+need > n.sizes.clientPool {
		n.legacyWaitP = true
		return false
	}
	payload := roundedFetch(b.payload)
	work := payload
	if b.material {
		work = roundedFetch(int64(b.materialWords()) * 8)
		if b.materialW {
			work += payload
		}
	}
	if !b.lease.grow(work) {
		return false
	}
	wire := n.legacyOutputBound(b)
	if !n.msgRoom(b.owner.c, int64(wire)) {
		// The payload has not been allocated. Return only this new
		// reservation; high-water metadata remains owned and counted.
		b.lease.book.used -= work
		b.lease.bytes -= work
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
	if b.material && !b.materialW {
		b.materialCharge = payload
		local, total := c.promiseLocal, c.promiseTotal
		c.promiseLocal, c.promiseTotal = &b.promise, &n.legacyPromises
		c.growOut(int(payload))
		c.promiseLocal, c.promiseTotal = local, total
	} else {
		b.payloadCharge = payload
	}
	if b.material {
		b.materialIndexCharge = roundedFetch(int64(b.materialWords()) * 8)
		b.materialIndex = make([]uint64, b.materialWords())
	}
	b.data = make([]byte, int(payload))
	b.prepared = true
	if b.err == nil {
		if b.held {
			b.initHeldData()
		} else if b.material {
			b.initMaterialData()
		}
	}
	return true
}

func (n *node) retryLegacyOwner(b *legacyBatch) {
	if b.st != n.st || b.owner.c == nil || b.owner.c.dead {
		b.cancelled = true
		return
	}
	wasPending := b.pending
	b.retryBlocked = false
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
	if b.retryBlocked {
		b.pending = wasPending
		b.owner.c.stagePending += wasPending
		return
	}
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
	if b.linked {
		if b.st.legacyView == b {
			b.st.legacyView = nil
		}
		if n.legacyMemo == b {
			n.legacyMemo = nil
		}
		c := b.owner.c
		if b.owner.msgCharge > 0 {
			c.growOut(-b.owner.msgCharge)
			c.heldCounted -= int64(b.owner.msgCharge)
		}
		c.fillOwners--
		b.owner.msg = nil
		b.owner.msgCharge = 0
		b.owner.req = rangeReq{}
		b.st = nil
		// P-backed material output still owns its W-funded transport
		// index until DATA/reset; W-backed material is copied at commit.
		if !b.material || b.materialW {
			b.materialIndex = nil
			b.lease.release()
		} else {
			// The output's charged P metadata root now owns b/m/lease.
			// Only the separately allocated transport index stays in W.
			b.lease.book.used -= stageUnit
			b.lease.bytes -= stageUnit
		}
	}
}

func (n *node) finishLegacyPiece(b *legacyBatch, token fillToken, err error) error {
	if n.legacyRunning != b || token.op != b.op || token.piece != b.piece || !b.running {
		return nil
	}
	b.lease.acknowledge()
	if err != nil && errors.Is(err, ErrBadValue) && !b.cancelled && b.st == n.st && n.st.bad == nil {
		n.st.bad = err
	}
	b.running = false
	n.legacyRunning = nil
	n.releaseFetchSources(&b.fetchSources)
	if err == nil {
		if b.material {
			for i := b.cursor; i < b.resultCursor; i++ {
				rec := b.materialRecord(i)
				if !b.cancelled && b.st == n.st {
					b.st.promoteCanonical(rec)
				}
				b.recordOffset += int64(len(rec))
			}
		} else if b.held {
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
		if b.restart {
			n.recollectLegacyBatch(b)
			return true
		}
		if b.cancelled || b.st != n.st || (b.held || b.material) && !b.needed {
			n.dropLegacyBatch(b)
		} else if b.needed {
			if n.legacyWaitP {
				return false
			}
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
	if b.restart {
		n.recollectLegacyBatch(b)
		return true
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
		if n.fillBook.used == fetchCoreWork+b.lease.bytes && n.fillBook.active == nil {
			if !b.fullAlone {
				n.recollectLegacyBatch(b)
				return true
			}
			if !n.convertLegacyMaterial(b) {
				return true
			}
		} else {
			return false
		}
	}
	if !n.prepareLegacy(b) {
		if n.legacyMemo != nil && !n.legacyMemo.needed {
			n.dropLegacyBatch(n.legacyMemo)
			return n.prepareLegacy(b)
		}
		return false
	}
	if err := n.planLegacyPiece(b); err != nil {
		n.releaseFetchSources(&b.fetchSources)
		if errors.Is(err, ErrBadValue) && n.st.bad == nil {
			n.st.bad = err
		}
		b.err = err
		b.ready = true
		n.legacyPending = nil
		if n.legacyMemo != nil && n.legacyMemo != b {
			n.dropLegacyBatch(n.legacyMemo)
		}
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
	values, readBytes := b.fetchShape()
	if !n.queueFetchTask(&fetchTask{work: func() error { return b.executePiece(false) }, done: func(err error) error { return n.finishLegacyPiece(b, token, err) }}) {
		b.lease.acknowledge()
		b.running = false
		n.legacyRunning = nil
		n.releaseFetchSources(&b.fetchSources)
	} else {
		n.recordFetch(values, readBytes)
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
	if b.materialCharge > 0 {
		b.owner.c.growOut(-int(b.materialCharge))
		b.materialCharge = 0
	}
	if b.owner.msgCharge > 0 {
		b.owner.c.growOut(-b.owner.msgCharge)
		b.owner.c.heldCounted -= int64(b.owner.msgCharge)
	}
	c := b.owner.c
	c.fillOwners--
	if c.fillOwners == 0 && c.stages == 0 && c.stageOrphan {
		c.stageOrphan = false
		n.stageOrphans--
	}
	for index := &b.index; index != nil; {
		clear(index.pages[:])
		next := index.next
		index.next = nil
		index = next
	}
	b.data = nil
	b.materialIndex = nil
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
	k := 0
	for _, r := range n.awayReads {
		if r.c == c && (sid == 0 || r.h == mkHandle(c, sid)) {
			c.growOut(-r.charge)
			c.heldCounted -= int64(r.charge)
			continue
		}
		n.awayReads[k] = r
		k++
	}
	clear(n.awayReads[k:])
	n.awayReads = n.awayReads[:k]
	for _, b := range []*legacyBatch{n.legacyCollect, n.legacyPending, n.legacyRunning, n.legacyMemo} {
		if b != nil && b != n.legacyRetry && b.owner.c == c && (sid == 0 || b.owner.h == mkHandle(c, sid)) {
			b.cancelled = true
			if !b.running {
				n.dropLegacyBatch(b)
			}
		}
	}
}

func (n *node) invalidateLegacy(st *store) {
	for _, b := range []*legacyBatch{n.legacyCollect, n.legacyPending, n.legacyRunning, n.legacyMemo} {
		if b != nil && b.st == st {
			n.legacyWaitP = false
			n.fillCreditWake = true
			if b.readHeld {
				st.releaseRead(b.owner.view)
				b.readHeld = false
			}
			b.restart = !b.held && b.needed && !b.cancelled
			b.st = nil
			b.owner.hr = nil
			if !b.restart {
				b.cancelled = true
			}
		}
	}
}
