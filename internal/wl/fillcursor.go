package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// A fill owns one destination and one strict wire cursor. Worker plans
// contain only immutable source leases and destination spans: no store.
type replyFill struct {
	h                   handle
	c                   *client
	st                  *store
	stage               *replyStage
	msg                 []byte
	req                 rangeReq
	view, header        int64
	headerTerm          uint64
	count, selected     int64
	more                bool
	last                uint32
	hasLast             bool
	taken               int64
	record              fillRecord
	watch               *fillWatch
	stream              *fillStream
	raw                 bool
	ids                 []uint32
	bigRange            *bigRange
	bigWatch            *bigWatch
	outer               [32]byte
	outerPos, outerSize int
	meta                int
	cancelled, ready    bool
	op, piece           uint64
	work                *fetchWorkLease
	arena               *fillArena
}

type fillToken struct{ op, piece uint64 }

type fillRecord struct {
	active            bool
	id                uint32
	state             ckptState
	phase             int
	field             int
	keyPos, valuePos  int
	crc               uint32
	start, size       int
	atom              [64]byte
	atomPos, atomSize int
	stageOp           uint64
}

const (
	fillReady = 1 << iota
	fillContinue
	fillFinal
	fillPromote
)

// Exactly 64 bytes on supported 64-bit builds. Allocation is one fixed
// arena, including both scratch windows and control, inside the 304-KiB
// reservation beyond the common worker stack/control.
type fillSpan struct {
	dst                                 []byte
	off                                 int64
	sum, expected                       uint32
	source, flags                       uint16
	id, start, size, valueLen, valuePos uint32
}

type fillSource struct {
	wal  *walSourceLease
	ckpt *ckptSourceLease
	log  logReader
	gen  int
}

type fetchSources struct {
	sources [2]fillSource
	ns      int
}

type fillArena struct {
	spans  [2048]fillSpan
	window [128 << 10]byte
	fetchSources
	n int
}

func (a *fetchSources) read(source int, p []byte, off int64, nowait bool) error {
	s := &a.sources[source]
	switch {
	case s.wal != nil:
		return s.wal.read(p, off, nowait)
	case s.ckpt != nil:
		if nowait {
			return readFileNoWait(s.ckpt.file, p, off&ckptOffMask)
		}
		_, err := s.ckpt.file.ReadAt(p, off&ckptOffMask)
		return err
	default:
		if r, ok := s.log.(noWaitReader); ok && nowait {
			return r.readNoWait(p, off)
		}
		return s.log.read(p, off)
	}
}

func (n *node) tryFillNowait(a *fillArena) bool {
	if n.fetchNoWaitRefused {
		for i := range a.n {
			if a.spans[i].flags&fillReady == 0 {
				return false
			}
		}
	}
	return a.tryNowait()
}

func (a *fillArena) tryNowait() bool {
	ready := true
	for i := range a.n {
		d := &a.spans[i]
		if d.flags&fillReady != 0 {
			continue
		}
		if err := a.read(int(d.source), d.dst, d.off, true); err == nil {
			d.flags |= fillReady
		} else {
			ready = false
		}
	}
	return ready
}

func (a *fillArena) execute() error {
	for i := 0; i < a.n; {
		d := &a.spans[i]
		if d.flags&fillReady != 0 {
			i++
			continue
		}
		// Coalesce nearby immutable reads; the bounded window is work,
		// never retained cache or client output.
		start, end := d.off, d.off+int64(len(d.dst))
		j := i + 1
		for j < a.n {
			x := &a.spans[j]
			if x.source != d.source || x.flags&fillReady != 0 || x.off < start-512 || x.off > end+512 || max(end, x.off+int64(len(x.dst)))-min(start, x.off) > int64(len(a.window)) {
				break
			}
			start = min(start, x.off)
			end = max(end, x.off+int64(len(x.dst)))
			j++
		}
		p := a.window[:end-start]
		if err := a.read(int(d.source), p, start, false); err != nil {
			return err
		}
		for k := i; k < j; k++ {
			x := &a.spans[k]
			copy(x.dst, p[x.off-start:x.off-start+int64(len(x.dst))])
			x.flags |= fillReady
		}
		i = j
	}
	for i := range a.n {
		d := &a.spans[i]
		seed := d.sum
		if d.flags&fillContinue != 0 {
			seed = a.spans[i-1].sum
		}
		d.sum = crc32.Update(seed, castagnoli, d.dst)
		if d.flags&fillFinal != 0 && d.sum != d.expected {
			return &ReadBackError{Err: ErrBadValue, Off: d.off - int64(d.valuePos), Len: int(d.valueLen)}
		}
	}
	return nil
}

func (n *node) releaseFetchSources(a *fetchSources) {
	for i := range a.ns {
		s := &a.sources[i]
		if s.wal != nil {
			n.removeAll(s.wal.release())
		}
		if s.ckpt != nil {
			_ = s.ckpt.release()
		}
		*s = fillSource{}
	}
	a.ns = 0
}

func (n *node) releaseFillSources(a *fillArena) {
	n.releaseFetchSources(&a.fetchSources)
	clear(a.spans[:])
}

func (q *replyFill) varint(field int, value uint64) {
	if value == 0 {
		return
	}
	var p [20]byte
	n := putUvarint(p[:], uint64(field)<<3)
	n += putUvarint(p[n:], value)
	q.stage.append(p[:n])
}

func (q *replyFill) length(size int) {
	var p [20]byte
	p[0] = 10
	n := 1 + putUvarint(p[1:], uint64(size))
	q.stage.append(p[:n])
}

func (n *node) fillSource(q *replyFill, a *fetchSources, off int64, want int) (int, int, error) {
	if inCkpt(off) {
		gen := ckptGenOf(off)
		for i := range a.ns {
			if a.sources[i].ckpt != nil && a.sources[i].gen == gen {
				return i, want, nil
			}
		}
		if a.ns == len(a.sources) {
			return -1, 0, nil
		}
		lease, err := q.st.lendCkpt(gen)
		if err != nil {
			return -1, 0, err
		}
		i := a.ns
		a.ns++
		a.sources[i] = fillSource{ckpt: lease, gen: gen}
		return i, want, nil
	}
	if w, ok := q.st.log.(*wal); ok {
		end := w.durableEnd()
		if off >= end {
			return -2, min(want, int(w.off-off)), nil
		}
		want = min(want, int(end-off), int(segSize-off%segSize))
		for i := range a.ns {
			if s := a.sources[i].wal; s != nil && s.off/segSize == off/segSize {
				return i, want, nil
			}
		}
		if a.ns == len(a.sources) {
			return -1, 0, nil
		}
		base := off / segSize * segSize
		lease, err := w.lendFetch(base, int(min(segSize, end-base)))
		if err != nil {
			return -1, 0, err
		}
		i := a.ns
		a.ns++
		a.sources[i] = fillSource{wal: lease}
		return i, want, nil
	}
	for i := range a.ns {
		if a.sources[i].log != nil {
			return i, want, nil
		}
	}
	if a.ns == len(a.sources) {
		return -1, 0, nil
	}
	i := a.ns
	a.ns++
	a.sources[i] = fillSource{log: q.st.log}
	return i, want, nil
}

func (n *node) planFill(q *replyFill, a *fillArena) error {
	if r := &q.record; r.active && r.state.vlen > 0 {
		fresh, ok := q.st.putStateAt(r.id, r.state.mod)
		if !ok {
			return errLogCut
		}
		if fresh.vlen != r.state.vlen || fresh.vsum != r.state.vsum {
			return &ReadBackError{Err: ErrBadValue, Off: r.state.voff, Len: int(r.state.vlen)}
		}
		r.state.voff = fresh.voff
	}
	for {
		r := &q.record
		if q.raw && q.stage.written == q.stage.size {
			if r.active && r.phase == 4 && r.valuePos == int(r.state.vlen) {
				r.leaseAtom()
				r.phase = 5
			}
			if r.active && r.phase == 5 && r.atomPos == r.atomSize {
				r.active = false
				q.taken++
			}
			q.ready = true
			return nil
		}
		if !r.active {
			next, err := n.nextFillRecord(q)
			if err != nil {
				return err
			}
			if !next {
				if q.watch == nil && q.stream == nil && !q.raw {
					if q.more {
						q.varint(3, 1)
					}
					q.varint(4, uint64(q.count))
				}
				q.ready = true
				return nil
			}
			r = &q.record
		}
		if !q.drainFillAtom(q.outer[:q.outerSize], &q.outerPos) {
			q.ready = q.raw
			return nil
		}
		s := &r.state
		key := q.st.key(r.id)
		switch r.phase {
		case 0:
			p := appendFillLen(r.atom[:0], r.field, r.size)
			r.start = q.stage.written + len(p)
			r.stageOp = q.op
			p = appendFillLen(p, 1, len(key))
			r.atomPos, r.atomSize = 0, len(p)
			r.phase = 1
		case 1:
			if !q.drainFillAtom(r.atom[:r.atomSize], &r.atomPos) {
				q.ready = q.raw
				return nil
			}
			r.phase = 2
		case 2:
			take := min(len(key)-r.keyPos, q.stage.size-q.stage.written)
			q.stage.append(key[r.keyPos : r.keyPos+take])
			r.keyPos += take
			if r.keyPos < len(key) {
				q.ready = q.raw
				return nil
			}
			p := r.atom[:0]
			for _, kv := range [...]struct {
				field int
				value int64
			}{{2, s.create}, {3, s.mod}, {4, s.ver}} {
				if kv.value != 0 {
					p = appendVarintField(p, kv.field, uint64(kv.value))
				}
			}
			if s.vlen > 0 {
				p = appendFillLen(p, 5, int(s.vlen))
			}
			r.atomPos, r.atomSize = 0, len(p)
			r.phase = 3
		case 3:
			if !q.drainFillAtom(r.atom[:r.atomSize], &r.atomPos) {
				q.ready = q.raw
				return nil
			}
			r.phase = 4
		case 4:
			if r.valuePos == int(s.vlen) {
				r.leaseAtom()
				r.phase = 5
				continue
			}
			if a == nil {
				k := &q.st.keys[r.id]
				if k.mod != s.mod || k.rec == 0 {
					return errAway
				}
				value := parseKV(q.st.slab.bytes(k.rec, int(k.rlen))).value
				if len(value) != int(s.vlen) {
					return &ReadBackError{Err: ErrBadValue, Off: s.voff, Len: int(s.vlen)}
				}
				dst := q.stage.span(q.stage.written, min(int(s.vlen)-r.valuePos, q.stage.size-q.stage.written))
				if len(dst) == 0 {
					return errLogCut
				}
				copy(dst, value[r.valuePos:r.valuePos+len(dst)])
				r.crc = crc32.Update(r.crc, castagnoli, dst)
				r.valuePos += len(dst)
				q.stage.written += len(dst)
				if r.valuePos == int(s.vlen) && r.crc != s.vsum {
					return &ReadBackError{Err: ErrBadValue, Off: s.voff, Len: int(s.vlen)}
				}
				continue
			}
			if a.n == len(a.spans) {
				return nil
			}
			dst := q.stage.span(q.stage.written, min(int(s.vlen)-r.valuePos, q.stage.size-q.stage.written))
			flags := uint16(0)
			off := s.voff + int64(r.valuePos)
			source, take := -1, len(dst)
			k := &q.st.keys[r.id]
			if k.mod == s.mod && k.rec != 0 {
				v := parseKV(q.st.slab.bytes(k.rec, int(k.rlen))).value
				copy(dst, v[r.valuePos:r.valuePos+take])
				flags |= fillReady
			} else {
				var err error
				source, take, err = n.fillSource(q, &a.fetchSources, off, take)
				if err != nil {
					return err
				}
				if source == -1 {
					return nil
				}
				if take <= 0 {
					return errLogCut
				}
				dst = dst[:take]
				if source == -2 {
					w := q.st.log.(*wal)
					if !w.copyPlannedStage(dst, off) {
						return errLogCut
					}
					flags |= fillReady
				}
			}
			if a.n > 0 && r.valuePos > 0 && a.spans[a.n-1].id == r.id && a.spans[a.n-1].flags&fillFinal == 0 {
				flags |= fillContinue
			}
			if r.valuePos+take == int(s.vlen) {
				flags |= fillFinal
				if r.stageOp == q.op {
					flags |= fillPromote
				}
			}
			a.spans[a.n] = fillSpan{dst: dst, off: off, sum: r.crc, expected: s.vsum, source: uint16(max(0, source)), flags: flags, id: r.id, start: uint32(r.start), size: uint32(r.size), valueLen: s.vlen, valuePos: uint32(r.valuePos)}
			a.n++
			r.valuePos += take
			q.stage.written += take
		case 5:
			if !q.drainFillAtom(r.atom[:r.atomSize], &r.atomPos) {
				q.ready = q.raw
				return nil
			}
			r.active = false
			q.taken++
			if q.stream != nil {
				q.stream.records--
			}
		}
	}
}

func (n *node) finishFillPiece(q *replyFill, token fillToken, err error) error {
	if n.fillActive != q || q.work == nil || q.arena == nil || q.op != token.op || q.piece != token.piece {
		return nil
	}
	if q.watch != nil && !n.validFillWatch(q) {
		q.cancelled = true
		q.stage.discard()
	}
	a := q.arena
	if err == nil && !q.cancelled && q.st == n.st {
		for i := range a.n {
			p := &a.spans[i]
			if p.flags&fillPromote != 0 {
				n.promoteFillRecord(q, p)
			}
		}
	}
	if q.work.running {
		q.work.acknowledge()
	}
	if err == nil && !q.cancelled && q.st == n.st && a.n > 0 {
		last := &a.spans[a.n-1]
		// Planning may already have selected the next version of the same
		// key when a source/descriptor limit ends the piece. Its zero-byte
		// cursor must not inherit the previous version's checksum seed.
		if r := &q.record; r.active && r.id == last.id && r.valuePos > 0 && r.valuePos == int(last.valuePos)+len(last.dst) && r.state.vsum == last.expected && r.state.vlen == last.valueLen {
			r.crc = last.sum
		}
	}
	n.releaseFillSources(a)
	q.arena = nil
	// Keep the credit until this callback's arena-bearing stack frame
	// has returned. Redispatch occurs in the same loop turn, afterward.
	n.fillRetired = q.work
	q.work = nil
	q.stage.acknowledge()
	n.fillActive = nil
	if err != nil && !q.cancelled && q.st == n.st {
		if errors.Is(err, ErrBadValue) && n.st.bad == nil {
			n.st.bad = err
		}
		q.cancelled = true
		if q.raw {
			n.failRawFill(q)
			return nil
		}
		if c, s := n.clientOf(q.h); c != nil {
			n.replyError(c, s.id, codeInternal, err.Error())
		}
	}
	return nil
}

func (n *node) pumpFills() {
	n.updateFillCredits()
	if n.fillRetired != nil {
		n.fillRetired.release()
		n.fillRetired = nil
	}
	if n.fillActive != nil {
		return
	}
	for i := 0; i < len(n.fills); {
		q := n.fills[i]
		c, _ := n.clientOf(q.h)
		staleWatch := q.watch != nil && (!n.validFillWatch(q) || !q.raw && q.watch.compact != n.st.compactRev)
		if q.cancelled || c == nil || q.st != n.st || staleWatch {
			msg := q.msg
			restart := !q.cancelled && c != nil && q.st != n.st
			n.removeFill(i)
			q.stage.discard()
			n.releaseFillRead(q)
			if q.watch != nil {
				n.endFillWatch(q)
				restart = false
			}
			if q.raw {
				n.endRawFill(q)
				restart = false
			}
			if restart {
				if _, s := n.clientOf(q.h); s != nil {
					var r rangeReq
					if parseRange(msg, &r) == nil {
						n.serveRange(c, s, &r, msg)
					}
				}
			}
			q.freeMeta(n)
			continue
		}
		if q.ready {
			if !n.publishFill(q) {
				i++
				continue
			}
			n.removeFill(i)
			n.releaseFillRead(q)
			q.freeMeta(n)
			continue
		}
		i++
	}
	if n.legacyRunning != nil {
		return
	}
	if (n.legacyPrefer || len(n.fills) == 0) && n.pumpLegacy() {
		return
	}
	if len(n.fills) == 0 || len(n.fetching) > 0 || len(n.st.fetched) > 0 {
		return
	}
	// Completed private destinations can wait for the socket. Planning
	// one again would append another footer to the complete message.
	index := -1
	for i, q := range n.fills {
		if !q.ready {
			index = i
			break
		}
	}
	if index < 0 {
		n.pumpLegacy()
		return
	}
	if n.fillBook == nil {
		b := newFetchWorkBook(n.st.budget)
		n.fillBook = &b
	}
	q := n.fills[index]
	lease := n.fillBook.reserve(fetchCursorWork - fetchCoreWork)
	if lease == nil && n.legacyMemo != nil && !n.legacyMemo.needed {
		n.dropLegacyBatch(n.legacyMemo)
		lease = n.fillBook.reserve(fetchCursorWork - fetchCoreWork)
	}
	if lease == nil {
		n.pumpLegacy()
		return
	}
	n.legacyPrefer = true
	q.work = lease
	q.arena = &fillArena{}
	q.piece++
	token := fillToken{op: q.op, piece: q.piece}
	if !q.stage.borrow() {
		lease.release()
		q.arena = nil
		q.work = nil
		return
	}
	n.fillActive = q
	beforeRecord, beforeLast, beforeHasLast := q.record, q.last, q.hasLast
	beforeOuter, beforeOuterPos, beforeOuterSize := q.outer, q.outerPos, q.outerSize
	var beforeWatch fillWatch
	var beforeStream fillStream
	if q.stream != nil {
		beforeStream = *q.stream
	}
	if q.watch != nil {
		beforeWatch = *q.watch
	}
	beforeTaken, beforeReady, beforeWritten := q.taken, q.ready, q.stage.written
	err := n.planFill(q, q.arena)
	if err != nil {
		_ = n.finishFillPiece(q, token, err)
		return
	}
	// Planning and immediately pre-dispatch each try NOWAIT.
	ready := n.tryFillNowait(q.arena)
	if !ready {
		ready = n.tryFillNowait(q.arena)
	}
	if ready {
		err = q.arena.execute()
		_ = n.finishFillPiece(q, token, err)
	} else {
		lease.start()
		a := q.arena
		values, readBytes := a.fetchShape()
		if !n.queueFetchTask(&fetchTask{work: a.execute, done: func(err error) error { return n.finishFillPiece(q, token, err) }}) {
			// A busy/stopping worker is a scheduling wait, not a new
			// client-visible error. The destination is still private.
			q.record, q.last, q.hasLast = beforeRecord, beforeLast, beforeHasLast
			q.outer, q.outerPos, q.outerSize = beforeOuter, beforeOuterPos, beforeOuterSize
			if q.watch != nil {
				*q.watch = beforeWatch
			}
			if q.stream != nil {
				*q.stream = beforeStream
			}
			q.taken, q.ready, q.stage.written = beforeTaken, beforeReady, beforeWritten
			a.n = 0
			lease.acknowledge()
			_ = n.finishFillPiece(q, token, nil)
		} else {
			n.recordFetch(values, readBytes)
		}
	}
	// Rotate after this bounded piece, including resident completion.
	if index < len(n.fills)-1 {
		copy(n.fills[index:], n.fills[index+1:])
		n.fills[len(n.fills)-1] = q
	}
}

func stageVarint(s *replyStage, at, end int) (uint64, int, bool) {
	var value uint64
	for shift := uint(0); shift < 64 && at < end; shift += 7 {
		page := s.page(at)
		b := page[stageHead+at%stagePayload]
		at++
		if shift == 63 && b > 1 {
			return 0, at, false
		}
		value |= uint64(b&127) << shift
		if b < 128 {
			return value, at, true
		}
	}
	return 0, at, false
}

func (n *node) promoteFillRecord(q *replyFill, p *fillSpan) {
	start, end := int(p.start), int(p.start+p.size)
	if end > q.stage.written || end > q.stage.size || p.id >= q.st.nkeys {
		return
	}
	at := start
	mod := int64(0)
	for at < end {
		tag, next, ok := stageVarint(q.stage, at, end)
		if !ok {
			return
		}
		at = next
		switch tag & 7 {
		case 0:
			value, next, ok := stageVarint(q.stage, at, end)
			if !ok {
				return
			}
			at = next
			if tag>>3 == 3 {
				mod = int64(value)
			}
		case 2:
			length, next, ok := stageVarint(q.stage, at, end)
			if !ok || length > uint64(end-next) {
				return
			}
			at = next + int(length)
		default:
			return
		}
		if mod != 0 {
			break
		}
	}
	st := q.st
	k := &st.keys[p.id]
	if mod == 0 || k.create == 0 || k.mod != mod || k.rec != 0 || k.vlen != p.valueLen || k.vsum != p.expected {
		return
	}
	size := int(p.size)
	st.loads++
	if _, cell := classOf(size); st.budget != 0 && (st.slab.used+int64(cell) > st.topMax() || !st.slab.reuses(size) && int64(st.slab.top+cell) > st.topMax()) {
		st.unkept++
		return
	}
	off, err := st.slab.alloc(size)
	if err != nil {
		st.unkept++
		return
	}
	dst := st.slab.bytes(off, size)
	for at := start; at < end; {
		take := min(end-at, stagePayload-at%stagePayload)
		page := q.stage.page(at)
		copy(dst[at-start:at-start+take], page[stageHead+at%stagePayload:stageHead+at%stagePayload+take])
		at += take
	}
	k.rec, k.rlen = off, uint32(size)
}

func appendFillLen(p []byte, field, size int) []byte {
	var b [20]byte
	n := putUvarint(b[:], uint64(field)<<3|2)
	n += putUvarint(b[n:], uint64(size))
	return append(p, b[:n]...)
}

func (r *fillRecord) leaseAtom() {
	p := r.atom[:0]
	if r.state.lease != 0 {
		p = appendVarintField(p, 6, uint64(r.state.lease))
	}
	r.atomPos, r.atomSize = 0, len(p)
}

func (q *replyFill) drainFillAtom(p []byte, pos *int) bool {
	take := min(len(p)-*pos, q.stage.size-q.stage.written)
	q.stage.append(p[*pos : *pos+take])
	*pos += take
	return *pos == len(p)
}

func cloneFillRequest(msg []byte) []byte { return bytes.Clone(msg) }

func (q *replyFill) prefix(size int) {
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(size))
	q.stage.append(p[:])
}
