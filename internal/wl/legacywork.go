package wl

import (
	"hash/crc32"
	"sort"
	"time"
)

const (
	legacyPageEntries = 341
	legacySlots       = 64
	legacyLenBits     = 21
	legacyLenMask     = 1<<legacyLenBits - 1
)

type legacyEntry struct {
	mod                int64
	key, len, sum, pos uint32
}

// No per-value Go objects: 341 pointer-free entries and internal padding
// occupy exactly one charged 8-KiB unit.
type legacyPage struct {
	entries [legacyPageEntries]legacyEntry
	_       [8]byte
}
type legacyIndex struct {
	pages [legacySlots]*legacyPage
	next  *legacyIndex
}
type legacySpan struct {
	off, dst                                 int64
	entry, pos, len, source, expected, total uint32
}

type legacyOwner struct {
	c            *client
	h            handle
	hr           *heldReply
	msg          []byte
	req          rangeReq
	view, header int64
	share        int64 // invalidates the cached answer bound on share changes
	candidates   int64
	term         uint64
	wire         int
	msgCharge    int
}

// The first index, owner, source plan and completion state fit in the
// same 8-KiB root. Metadata pages, payload and retained roots all spend W.
type legacyBatch struct {
	index               legacyIndex
	owner               legacyOwner
	st                  *store
	lease               *fetchWorkLease
	data                []byte
	materialIndex       []uint64
	materialIndexCharge int64
	materialNeed        int64
	materialW           bool
	fetchSources
	plan                                                      [128]legacySpan
	n, capacity, pages, planN                                 int
	cursor, valuePos, resultCursor, resultPos                 int
	plannedCursor, plannedPos                                 int
	crc, resultCRC                                            uint32
	recordOffset                                              int64
	lookupIndex                                               int
	lookupOffset                                              int64
	payload                                                   int64
	payloadCharge                                             int64
	promise                                                   int64
	pending                                                   int64
	created                                                   time.Time
	op, piece                                                 uint64
	full, frozen, prepared, running, ready, needed, cancelled bool
	readHeld                                                  bool
	held                                                      bool
	material                                                  bool
	fullAlone                                                 bool
	linked, retryBlocked                                      bool
	restart                                                   bool
	materialCharge                                            int64
	err                                                       error
}

func (b *legacyBatch) entry(i int) *legacyEntry {
	p := i / legacyPageEntries
	r := &b.index
	for p >= legacySlots {
		r = r.next
		p -= legacySlots
	}
	return &r.pages[p].entries[i%legacyPageEntries]
}

func (b *legacyBatch) add(key uint32, mod int64, n, sum uint32) bool {
	if b.n == b.capacity {
		extra := int64(stageUnit)
		newIndex := b.pages > 0 && b.pages%legacySlots == 0
		if newIndex {
			extra += stageUnit
		}
		if !b.lease.grow(extra) {
			b.full = true
			b.fullAlone = b.lease.book.used == fetchCoreWork+b.lease.bytes
			return false
		}
		r := &b.index
		p := b.pages
		for p >= legacySlots {
			if r.next == nil {
				r.next = &legacyIndex{}
			}
			r = r.next
			p -= legacySlots
		}
		r.pages[p] = &legacyPage{}
		b.pages++
		b.capacity += legacyPageEntries
	}
	*b.entry(b.n) = legacyEntry{key: key, mod: mod, len: n, sum: sum}
	b.n++
	return true
}

func (b *legacyBatch) Len() int { return b.n }
func (b *legacyBatch) Less(i, j int) bool {
	a, z := b.entry(i), b.entry(j)
	return a.key < z.key || a.key == z.key && a.mod < z.mod
}
func (b *legacyBatch) Swap(i, j int) { a, z := b.entry(i), b.entry(j); *a, *z = *z, *a }

func (b *legacyBatch) freeze() {
	if b.full {
		b.frozen = true
		return
	}
	if b.owner.hr != nil {
		b.freezeHeld()
		return
	}
	sort.Sort(b)
	out := 0
	for i := 0; i < b.n; i++ {
		e := *b.entry(i)
		if out > 0 {
			p := b.entry(out - 1)
			if p.key == e.key && p.mod == e.mod {
				continue
			}
		}
		*b.entry(out) = e
		out++
	}
	b.n = out
	b.payload = 0
	for i := range b.n {
		e := b.entry(i)
		n := e.len & legacyLenMask
		e.pos = uint32(b.payload)
		e.len = n | uint32(b.payload>>32)<<legacyLenBits
		b.payload += int64(n)
	}
	b.frozen = true
}

func (b *legacyBatch) lookup(id uint32, mod int64) ([]byte, error, bool) {
	if b.owner.hr != nil || b.material {
		rec, err, ok := b.canonicalRecord(id, mod)
		if err != nil || !ok {
			return nil, err, ok
		}
		return parseKV(rec).value, nil, true
	}
	if !b.ready {
		return nil, nil, false
	}
	lo, hi := 0, b.n
	for lo < hi {
		m := lo + (hi-lo)/2
		e := b.entry(m)
		if e.key < id || e.key == id && e.mod < mod {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == b.n {
		return nil, nil, false
	}
	e := b.entry(lo)
	if e.key != id || e.mod != mod {
		return nil, nil, false
	}
	if b.err != nil {
		return nil, b.err, true
	}
	at := int64(e.pos) | int64(e.len>>legacyLenBits)<<32
	return b.data[at : at+int64(e.len&legacyLenMask)], nil, true
}

func (b *legacyBatch) executePiece(nowait bool) error {
	crc := b.crc
	idx, pos := b.cursor, b.valuePos
	for i := range b.planN {
		p := &b.plan[i]
		dst := b.data[p.dst : p.dst+int64(p.len)]
		if p.source != ^uint32(1) {
			if err := b.read(int(p.source), dst, p.off, nowait); err != nil {
				return err
			}
		}
		if p.pos == 0 {
			crc = 0
		}
		crc = crc32.Update(crc, castagnoli, dst)
		idx, pos = int(p.entry), int(p.pos+p.len)
		if pos == int(p.total) {
			if crc != p.expected {
				return &ReadBackError{Err: ErrBadValue, Off: p.off - int64(p.pos), Len: pos}
			}
			idx++
			pos = 0
			crc = 0
		}
	}
	if b.held || b.material {
		idx, pos = b.plannedCursor, b.plannedPos
	}
	b.resultCursor, b.resultPos, b.resultCRC = idx, pos, crc
	return nil
}

func (n *node) planLegacyPiece(b *legacyBatch) error {
	if b.owner.hr != nil || b.material {
		return n.planLegacyHeld(b)
	}
	b.planN = 0
	for idx, pos := b.cursor, b.valuePos; idx < b.n && b.planN < len(b.plan); {
		e := b.entry(idx)
		ks, ok := b.st.putStateAt(e.key, e.mod)
		if !ok {
			return errLogCut
		}
		if ks.vlen != e.len&legacyLenMask || ks.vsum != e.sum {
			return &ReadBackError{Err: ErrBadValue, Off: ks.voff, Len: int(ks.vlen)}
		}
		want := int(ks.vlen) - pos
		q := replyFill{st: b.st}
		source, take, err := n.fillSource(&q, &b.fetchSources, ks.voff+int64(pos), want)
		if err != nil {
			return err
		}
		if source == -1 {
			break
		}
		if source == -2 {
			// Stage bytes are copied now, never retried through disk.
			at := int64(e.pos) | int64(e.len>>legacyLenBits)<<32
			w := b.st.log.(*wal)
			if !w.copyPlannedStage(b.data[at+int64(pos):at+int64(pos+take)], ks.voff+int64(pos)) {
				return errLogCut
			}
			// An immutable memory source is unnecessary: the plan marks
			// this span copied, and execute hashes it without rereading.
			source = -2
		}
		at := int64(e.pos) | int64(e.len>>legacyLenBits)<<32
		b.plan[b.planN] = legacySpan{off: ks.voff + int64(pos), dst: at + int64(pos), entry: uint32(idx), pos: uint32(pos), len: uint32(take), source: uint32(source), expected: e.sum, total: ks.vlen}
		b.planN++
		pos += take
		if pos == int(ks.vlen) {
			idx++
			pos = 0
		}
	}
	return nil
}
