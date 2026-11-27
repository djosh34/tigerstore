package wl

import "math"

type fillStream struct {
	chunk, records, nextChunk int64
	started, last, more       bool
	count                     int64
	frames                    int
}

type fillStreamFrame struct {
	take, next  int64
	body, count int
	last, more  bool
}

func (n *node) fillStreamFrame(q *replyFill, chunk int64) fillStreamFrame {
	left := int64(0)
	if q.more {
		left = q.count - q.selected
	}
	rest := q.selected - q.taken + left
	take := min(chunk, q.selected-q.taken)
	more := rest > chunk
	total := q.req.limit
	if total <= 0 {
		total = math.MaxInt64
	}
	last := !more || q.taken+take == total
	meta := *q
	meta.record = fillRecord{}
	meta.stream = nil
	recs := 0
	for range take {
		if !n.nextFillRange(&meta) {
			break
		}
		recs += sizeBytesField(2, meta.record.size)
		meta.taken++
	}
	count := q.taken + take + max(rest-take, 0)*b2i(more)
	body := recs
	if last {
		body += sizeBytesField(1, n.hdrSizeTerm(q.header, q.headerTerm)) + sizeVarintField(4, uint64(count))
		if more {
			body += 2
		}
	}
	size := sizeBytesField(1, sizeVarintField(3, uint64(q.header))) + 2 + sizeVarintField(4, uint64(min(rest, chunk+1))) + recs
	next := min(nextChunk(chunk, size, maxRequestBytes), total-(q.taken+take))
	return fillStreamFrame{take: take, next: next, body: body, count: int(count), last: last, more: more}
}

func (n *node) streamFillWire(st *store, r *rangeReq, view, header, selected, count int64, more bool) int {
	q := replyFill{st: st, req: *r, view: view, header: header, headerTerm: n.term, selected: selected, count: count, more: more}
	chunk := min(int64(initialStreamChunk), selected)
	wire := 0
	for {
		f := n.fillStreamFrame(&q, chunk)
		wire += 5 + sizeBytesField(1, f.body)
		if f.last {
			return wire
		}
		for range f.take {
			if !n.nextFillRange(&q) {
				return wire
			}
			q.taken++
		}
		chunk = f.next
	}
}

func (n *node) nextFillStream(q *replyFill) bool {
	s := q.stream
	if s.started && s.records == 0 {
		if s.last {
			if s.more {
				q.varint(3, 1)
			}
			q.varint(4, uint64(s.count))
			return false
		}
		s.chunk = s.nextChunk
	}
	if !s.started || s.records == 0 {
		f := n.fillStreamFrame(q, s.chunk)
		q.prefix(sizeBytesField(1, f.body))
		q.length(f.body)
		if f.last {
			q.length(n.hdrSizeTerm(q.header, q.headerTerm))
			q.varint(1, n.clusterID)
			q.varint(2, n.id)
			q.varint(3, uint64(q.header))
			q.varint(4, q.headerTerm)
		}
		s.started = true
		s.records = f.take
		s.nextChunk = f.next
		s.last = f.last
		s.more = f.more
		s.count = int64(f.count)
		s.frames++
	}
	return n.nextFillRange(q)
}
