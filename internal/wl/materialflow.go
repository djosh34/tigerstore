package wl

import (
	"encoding/binary"
	"math"
	"unsafe"
)

const (
	materialEnd     = ^uint64(0)
	materialLinkEnd = uint64(1)<<43 - 1
)

// materialReply transfers a COMPLETE immutable main materialization to
// ordered output. It is not a large history hold: no source, store or
// history dependency survives the queue commit. In-place next links use
// the materialization's already charged eight-byte record prefixes.
type materialReply struct {
	batch                                                      *legacyBatch
	first, slot                                                uint64
	offsetLinks                                                bool
	selected, count, sent, limit, chunk, packetLeft, nextChunk int64
	stream, keysOnly, more, last                               bool
	responsePending                                            bool
	header                                                     [128]byte
	footer                                                     [32]byte
	responseHeader                                             [64]byte
	atom                                                       [8]byte
	headerN, footerN, responseN, atomN                         int
	phase, pos, cut0, cut1                                     int
	remaining                                                  int64
	frames                                                     int
}

func materialVarint(p []byte, field int, value uint64) []byte {
	if value == 0 {
		return p
	}
	return appendVarintField(p, field, value)
}

func (m *materialReply) record(slot uint64) []byte {
	at := int(slot)
	size := int(binary.LittleEndian.Uint64(m.batch.data[at:at+8]) & legacyLenMask)
	return m.batch.data[at+materialPrefix : at+materialPrefix+size]
}

func (m *materialReply) ordinal(slot uint64) uint64 {
	var at uint64
	for ordinal := uint64(0); ; ordinal++ {
		if at == slot {
			return ordinal
		}
		at += materialPrefix + uint64(len(m.record(at)))
	}
}

func (m *materialReply) next(slot uint64) uint64 {
	next := binary.LittleEndian.Uint64(m.batch.data[int(slot):int(slot)+8]) >> legacyLenBits
	if next == materialLinkEnd {
		return materialEnd
	}
	if m.offsetLinks {
		return next
	}
	var at uint64
	for range next {
		at += materialPrefix + uint64(len(m.record(at)))
	}
	return at
}

func (m *materialReply) link(slot, next uint64) {
	at := int(slot)
	size := binary.LittleEndian.Uint64(m.batch.data[at:at+8]) & legacyLenMask
	binary.LittleEndian.PutUint64(m.batch.data[at:at+8], size|next<<legacyLenBits)
}

func projectedRecord(rec []byte, keysOnly bool) (size, cut0, cut1 int) {
	if !keysOnly {
		return len(rec), len(rec), len(rec)
	}
	for rest := rec; len(rest) > 0; {
		start := len(rec) - len(rest)
		num, _, _, _, tail, err := field(rest)
		if err != nil {
			break
		}
		if num == 5 {
			end := len(rec) - len(tail)
			return len(rec) - (end - start), start, end
		}
		rest = tail
	}
	return len(rec), len(rec), len(rec)
}

func (m *materialReply) packet(slot uint64, sent, chunk int64) (take, next int64, last, more bool, count int64, body int) {
	if !m.stream {
		take = m.selected
		last = true
		more = m.more
		count = m.count
	} else {
		left := int64(0)
		if m.more {
			left = m.count - m.selected
		}
		rest := m.selected - sent + left
		take = min(chunk, m.selected-sent)
		more = rest > chunk
		last = !more || sent+take == m.limit
		count = sent + take + max(rest-take, 0)*b2i(more)
	}
	recs := 0
	for range take {
		record := m.record(slot)
		size, _, _ := projectedRecord(record, m.keysOnly)
		recs += sizeBytesField(2, size)
		slot = m.next(slot)
	}
	body = recs
	if last {
		body += m.responseN + sizeVarintField(4, uint64(count))
		if more {
			body += 2
		}
	}
	if m.stream {
		left := int64(0)
		if m.more {
			left = m.count - m.selected
		}
		rest := m.selected - sent + left
		adaptive := sizeBytesField(1, sizeVarintField(3, uint64(m.batch.owner.header))) + 2 + sizeVarintField(4, uint64(min(rest, chunk+1))) + recs
		next = min(nextChunk(chunk, adaptive, maxRequestBytes), m.limit-(sent+take))
	}
	return
}

func (m *materialReply) preparePacket() {
	take, next, last, more, count, body := m.packet(m.slot, m.sent, m.chunk)
	m.packetLeft, m.nextChunk, m.last = take, next, last
	wire := body
	if m.stream {
		wire = sizeBytesField(1, body)
	}
	binary.BigEndian.PutUint32(m.header[1:5], uint32(wire))
	p := m.header[:5]
	if m.stream {
		p = appendFillLen(p, 1, body)
	}
	if last {
		p = append(p, m.responseHeader[:m.responseN]...)
	}
	m.headerN = len(p)
	p = m.footer[:0]
	if last {
		if more {
			p = appendVarintField(p, 3, 1)
		}
		p = materialVarint(p, 4, uint64(count))
	}
	m.footerN = len(p)
	m.phase, m.pos, m.atomN = 0, 0, 0
}

func (m *materialReply) front() []byte {
	for {
		switch m.phase {
		case 0:
			if m.pos < m.headerN {
				return m.header[m.pos:m.headerN]
			}
			m.phase, m.pos = 1, 0
		case 1:
			if m.packetLeft == 0 {
				m.phase, m.pos = 4, 0
				continue
			}
			if m.atomN == 0 {
				size, c0, c1 := projectedRecord(m.record(m.slot), m.keysOnly)
				m.cut0, m.cut1 = c0, c1
				p := appendFillLen(m.atom[:0], 2, size)
				m.atomN = len(p)
			}
			if m.pos < m.atomN {
				return m.atom[m.pos:m.atomN]
			}
			m.phase, m.pos = 2, 0
		case 2:
			rec := m.record(m.slot)
			if m.pos < m.cut0 {
				return rec[m.pos:m.cut0]
			}
			m.phase, m.pos = 3, 0
		case 3:
			rec := m.record(m.slot)
			if m.cut1+m.pos < len(rec) {
				return rec[m.cut1+m.pos:]
			}
			m.slot = m.next(m.slot)
			m.sent++
			m.packetLeft--
			m.phase, m.pos, m.atomN = 1, 0, 0
		case 4:
			if m.pos < m.footerN {
				return m.footer[m.pos:m.footerN]
			}
			m.phase, m.pos = 5, 0
		case 5:
			if m.last {
				return nil
			}
			m.chunk = m.nextChunk
			m.preparePacket()
		}
	}
}

func (m *materialReply) consume(n int) { m.pos += n; m.remaining -= int64(n) }

func (n *node) newMaterialReply(b *legacyBatch, s *stream, r *rangeReq, kvs [][]byte, count int64, more bool) *materialReply {
	m := &materialReply{batch: b, first: materialEnd, selected: int64(len(kvs)), count: count, limit: r.limit, stream: s.kind == kRangeStream, keysOnly: r.keysOnly, more: more, offsetLinks: uint64(len(b.data)) < materialLinkEnd}
	if m.limit <= 0 {
		m.limit = math.MaxInt64
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(b.data)))
	previous := materialEnd
	for _, kv := range kvs {
		ptr := uintptr(unsafe.Pointer(unsafe.SliceData(kv)))
		if ptr < base+materialPrefix || ptr-base+uintptr(len(kv)) > uintptr(len(b.data)) {
			return nil
		}
		slot := uint64(ptr - base - materialPrefix)
		next := slot
		if !m.offsetLinks {
			next = m.ordinal(slot)
		}
		if previous == materialEnd {
			m.first = slot
		} else {
			m.link(previous, next)
		}
		previous = slot
	}
	if previous != materialEnd {
		m.link(previous, materialLinkEnd)
	}
	p := appendFillLen(m.responseHeader[:0], 1, n.hdrSizeTerm(b.owner.header, b.owner.term))
	p = materialVarint(p, 1, n.clusterID)
	p = materialVarint(p, 2, n.id)
	p = materialVarint(p, 3, uint64(b.owner.header))
	p = materialVarint(p, 4, b.owner.term)
	m.responseN = len(p)
	chunk := min(int64(initialStreamChunk), m.selected)
	for slot, sent := m.first, int64(0); ; {
		take, next, last, _, _, body := m.packet(slot, sent, chunk)
		wire := body
		if m.stream {
			wire = sizeBytesField(1, body)
		}
		m.remaining += int64(5 + wire)
		m.frames++
		if last {
			break
		}
		for range take {
			slot = m.next(slot)
		}
		sent += take
		chunk = next
	}
	m.slot = m.first
	m.chunk = min(int64(initialStreamChunk), m.selected)
	m.preparePacket()
	return m
}

func (n *node) publishMaterialRange(b *legacyBatch, c *client, s *stream, r *rangeReq, kvs [][]byte, count int64, more bool) {
	m := n.newMaterialReply(b, s, r, kvs, count, more)
	if m == nil {
		b.cancelled = true
		n.abortBig(c, s)
		return
	}
	admission := int64(5 + m.responseN + sizeVarintField(4, uint64(count)))
	if more {
		admission += 2
	}
	for _, kv := range kvs {
		size, _, _ := projectedRecord(kv, r.keysOnly)
		admission += int64(sizeBytesField(2, size))
	}
	if m.stream {
		admission += 32 * (int64(len(kvs))/initialStreamChunk + 1)
	}
	if n.cannotFit(admission) {
		n.startBig(c, s, b.owner.msg)
		return
	}
	if !n.msgRoom(c, m.remaining) {
		b.retryBlocked = true
		n.legacyWaitP = true
		return
	}
	m.responsePending = !s.streaming
	s.streaming = true
	c.growOut(stageUnit)
	b.linked = true
	f := &c.flow
	if !s.holds {
		s.holds = true
		if f.streams == nil {
			f.streams, f.counts = map[uint32]int64{}, map[uint32]int{}
		}
		f.streams[s.id] = s.win
	}
	promise := int64(0)
	if c.stages == 0 {
		promise = stageUnit
		if cap(c.out) < 64<<10 {
			promise += 64 << 10
			c.socketPromise = 64 << 10
		}
	}
	c.takeOutputPromise(promise)
	c.stagePromise += promise
	n.stagePromises += promise
	c.stages++
	c.dropOut = true
	f.held = append(f.held, held{sid: s.id, material: m})
	f.bytes += m.remaining
	f.counts[s.id]++
	n.gs.rangesN++
	n.gs.calls[s.kind].sent += int64(m.frames)
	n.gs.sentBytes += m.remaining - 5*int64(m.frames)
	n.stats.replies++
	n.callEnded(s, 0)
	n.endStreamOK(c, s)
	n.markDirty(&c.sock)
}

func (c *client) drainMaterial(h *held) {
	c.stageSocket()
	m := h.material
	if m.responsePending {
		if rawSocketRoom(c) < 9+len(respHeaders) {
			return
		}
		p := c.frame(9 + len(respHeaders))
		putFrameHeader(p, len(respHeaders), frHeaders, flEndHeaders, h.sid)
		copy(p[9:], respHeaders)
		m.responsePending = false
		c.node.markDirty(&c.sock)
	}
	for p := m.front(); len(p) > 0; p = m.front() {
		take := min(int64(len(p)), int64(max(0, rawSocketRoom(c))), c.flow.conn, c.flow.streams[h.sid])
		if take <= 0 {
			return
		}
		w := msgW{s: &c.sock, sid: h.sid, left: int(take)}
		w.write(p[:take])
		m.consume(int(take))
		c.flow.bytes -= take
		c.flow.conn -= take
		c.flow.streams[h.sid] -= take
		c.node.markDirty(&c.sock)
	}
	c.releaseMaterial(h)
}

func (c *client) releaseMaterial(h *held) {
	b := h.material.batch
	c.growOut(-int(b.materialCharge) - stageUnit)
	b.materialCharge = 0
	b.data = nil
	h.material = nil
	c.stages--
	if c.stages == 0 {
		c.node.stagePromises -= c.stagePromise
		c.stagePromise = 0
		c.socketPromise = 0
	}
	if c.stages == 0 && c.fillOwners == 0 && c.stageOrphan {
		c.stageOrphan = false
		c.node.stageOrphans--
	}
}
