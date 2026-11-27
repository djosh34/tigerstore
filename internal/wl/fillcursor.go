package wl

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// A fill owns one destination and one strict wire cursor. Worker plans
// contain only immutable source leases and destination spans: no store.
type replyFill struct {
	h                handle
	c                *client
	st               *store
	stage            *replyStage
	msg              []byte
	req              rangeReq
	view, header     int64
	count, selected  int64
	more             bool
	last             uint32
	hasLast          bool
	taken            int64
	record           fillRecord
	meta             int
	cancelled, ready bool
	op, piece        uint64
	work             *fetchWorkLease
	arena            *fillArena
}

type fillToken struct{ op, piece uint64 }

type fillRecord struct {
	active           bool
	id               uint32
	state            ckptState
	phase            int
	keyPos, valuePos int
	crc              uint32
	start, size      int
}

const (
	fillReady = 1 << iota
	fillContinue
	fillFinal
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

type fillArena struct {
	spans   [2048]fillSpan
	window  [128 << 10]byte
	sources [2]fillSource
	n, ns   int
}

func (a *fillArena) read(source int, p []byte, off int64, nowait bool) error {
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
			if x.source != d.source || x.flags&fillReady != 0 || x.off < start || x.off > end+512 || x.off+int64(len(x.dst))-start > int64(len(a.window)) {
				break
			}
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

func (n *node) releaseFillSources(a *fillArena) {
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

func (q *replyFill) length(field, size int) {
	var p [20]byte
	n := putUvarint(p[:], uint64(field)<<3|2)
	n += putUvarint(p[n:], uint64(size))
	q.stage.append(p[:n])
}

func (n *node) fillSource(q *replyFill, a *fillArena, off int64, want int) (int, int, error) {
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
	if r := &q.record; r.active {
		fresh, ok := q.st.putStateAt(r.id, r.state.mod)
		if !ok {
			return errLogCut
		}
		if fresh.vlen != r.state.vlen || fresh.vsum != r.state.vsum {
			return &ReadBackError{Err: ErrBadValue, Off: r.state.voff, Len: int(r.state.vlen)}
		}
		r.state.voff = fresh.voff
	}
	for a.n < len(a.spans) {
		r := &q.record
		if !r.active {
			if !n.nextFillRange(q) {
				if q.more {
					q.varint(3, 1)
				}
				q.varint(4, uint64(q.count))
				q.ready = true
				return nil
			}
			r = &q.record
		}
		s := &r.state
		key := q.st.key(r.id)
		switch r.phase {
		case 0:
			q.length(2, r.size)
			r.start = q.stage.written
			q.length(1, len(key))
			r.phase = 1
		case 1:
			q.stage.append(key[r.keyPos:])
			r.keyPos = len(key)
			q.varint(2, uint64(s.create))
			q.varint(3, uint64(s.mod))
			q.varint(4, uint64(s.ver))
			if s.vlen > 0 {
				q.length(5, int(s.vlen))
			}
			r.phase = 2
		case 2:
			if r.valuePos == int(s.vlen) {
				r.phase = 3
				continue
			}
			dst := q.stage.span(q.stage.written, int(s.vlen)-r.valuePos)
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
				source, take, err = n.fillSource(q, a, off, take)
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
			}
			a.spans[a.n] = fillSpan{dst: dst, off: off, sum: r.crc, expected: s.vsum, source: uint16(max(0, source)), flags: flags, id: r.id, start: uint32(r.start), size: uint32(r.size), valueLen: s.vlen, valuePos: uint32(r.valuePos)}
			a.n++
			r.valuePos += take
			q.stage.written += take
		case 3:
			q.varint(6, uint64(s.lease))
			r.active = false
			q.taken++
		}
	}
	return nil
}

func (n *node) finishFillPiece(q *replyFill, token fillToken, err error) error {
	if n.fillActive != q || q.work == nil || q.arena == nil || q.op != token.op || q.piece != token.piece {
		return nil
	}
	a := q.arena
	if q.work.running {
		q.work.acknowledge()
	}
	if err == nil && !q.cancelled && q.st == n.st && a.n > 0 {
		last := &a.spans[a.n-1]
		if q.record.active && q.record.id == last.id {
			q.record.crc = last.sum
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
		q.cancelled = true
		if c, s := n.clientOf(q.h); c != nil {
			n.replyError(c, s.id, codeInternal, err.Error())
		}
	}
	return nil
}

func (n *node) pumpFills() {
	if n.fillRetired != nil {
		n.fillRetired.release()
		n.fillRetired = nil
	}
	if len(n.fills) == 0 || n.fillActive != nil {
		return
	}
	for i := 0; i < len(n.fills); {
		q := n.fills[i]
		c, _ := n.clientOf(q.h)
		if q.cancelled || c == nil || q.st != n.st {
			msg := q.msg
			restart := !q.cancelled && c != nil && q.st != n.st
			n.removeFill(i)
			q.stage.discard()
			if q.st == n.st {
				q.st.releaseRead(q.view)
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
			if !n.publishFillRange(q) {
				i++
				continue
			}
			n.removeFill(i)
			q.st.releaseRead(q.view)
			q.freeMeta(n)
			continue
		}
		i++
	}
	if len(n.fills) == 0 || len(n.fetching) > 0 || len(n.st.fetched) > 0 {
		return
	}
	if n.fillBook == nil {
		b := newFetchWorkBook(n.st.budget)
		n.fillBook = &b
	}
	q := n.fills[0]
	lease := n.fillBook.reserve(fetchCursorWork - fetchCoreWork)
	if lease == nil {
		return
	}
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
	beforeTaken, beforeReady, beforeWritten := q.taken, q.ready, q.stage.written
	err := n.planFill(q, q.arena)
	if err != nil {
		_ = n.finishFillPiece(q, token, err)
		return
	}
	// Planning and immediately pre-dispatch each try NOWAIT.
	ready := q.arena.tryNowait()
	if !ready {
		ready = q.arena.tryNowait()
	}
	if ready {
		err = q.arena.execute()
		_ = n.finishFillPiece(q, token, err)
	} else {
		lease.start()
		a := q.arena
		if !n.queueFetchTask(&fetchTask{work: a.execute, done: func(err error) error { return n.finishFillPiece(q, token, err) }}) {
			// A busy/stopping worker is a scheduling wait, not a new
			// client-visible error. The destination is still private.
			q.record, q.last, q.hasLast = beforeRecord, beforeLast, beforeHasLast
			q.taken, q.ready, q.stage.written = beforeTaken, beforeReady, beforeWritten
			a.n = 0
			lease.acknowledge()
			_ = n.finishFillPiece(q, token, nil)
		}
	}
	// Rotate after this bounded piece, including resident completion.
	if len(n.fills) > 1 {
		copy(n.fills, n.fills[1:])
		n.fills[len(n.fills)-1] = q
	}
}

func cloneFillRequest(msg []byte) []byte { return bytes.Clone(msg) }

func (q *replyFill) prefix(size int) {
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(size))
	q.stage.append(p[:])
}
