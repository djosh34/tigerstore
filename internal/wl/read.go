package wl

import (
	"encoding/binary"
	"math"
)

func (n *node) queueRead(h handle, kind uint8, msg []byte) {
	off := len(n.readBuf)
	n.readBuf = append(n.readBuf, msg...)
	n.reads = append(n.reads, pendingRead{h: h, kind: kind, off: off, len: len(msg)})
}

// askReads sends one ReadIndex for the reads that came in this turn.
func (n *node) askReads(now int64) {
	if len(n.reads) == 0 {
		return
	}
	n.readSeq++
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], n.readSeq)
	var b *readBatch
	if k := len(n.freeBatches); k > 0 {
		b = n.freeBatches[k-1]
		n.freeBatches = n.freeBatches[:k-1]
	} else {
		b = &readBatch{}
	}
	b.seq, b.known, b.asked = n.readSeq, false, now
	b.reads, n.reads = n.reads, b.reads[:0]
	b.buf, n.readBuf = n.readBuf, b.buf[:0]
	n.readBatches = append(n.readBatches, b)
	stats.readIndex++
	n.rn.ReadIndex(ctx[:])
}

// readState records the index of a ReadIndex.
func (n *node) readState(ctx []byte, index uint64) {
	if len(ctx) != 8 {
		return
	}
	seq := binary.BigEndian.Uint64(ctx)
	for _, b := range n.readBatches {
		if b.seq == seq {
			b.index, b.known = index, true
		}
	}
}

// serveReads answers the batches whose index is applied.
func (n *node) serveReads() {
	k := 0
	for i, b := range n.readBatches {
		if b.known && b.index <= n.applied {
			for _, r := range b.reads {
				c, s := n.clientOf(r.h)
				if c == nil {
					continue
				}
				var req rangeReq
				if err := parseRange(b.buf[r.off:r.off+r.len], &req); err != nil {
					n.replyError(c, s.id, codeInvalidArgument, "bad request")
					continue
				}
				n.serveRange(c, s, &req)
			}
			n.freeBatches = append(n.freeBatches, b)
			continue
		}
		n.readBatches[k] = n.readBatches[i]
		k++
	}
	n.readBatches = n.readBatches[:k]
}

// retryReads asks again for batches that waited too long, as after a
// leader change that dropped their ReadIndex.
func (n *node) retryReads(now int64) {
	for _, b := range n.readBatches {
		if !b.known && now-b.asked > 5 {
			n.readSeq++
			b.seq, b.asked = n.readSeq, now
			var ctx [8]byte
			binary.BigEndian.PutUint64(ctx[:], b.seq)
			n.rn.ReadIndex(ctx[:])
		}
	}
}

// serveRange answers a Range or RangeStream from the store as it is.
func (n *node) serveRange(c *client, s *stream, r *rangeReq) {
	st := n.st
	if r.rev > 0 && r.rev <= st.compactRev && r.rev < st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgCompacted)
		return
	}
	if r.rev > st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgFuture)
		return
	}
	kvs, count, err := n.rangeKVs(r, n.kvs[:0])
	if err != nil {
		n.replyError(c, s.id, codeInternal, err.Error())
		clear(kvs)
		n.kvs = kvs[:0]
		return
	}
	if r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = stripValue(kv)
		}
	}
	more := int64(len(kvs)) < count && !r.countOnly
	body := sizeBytesField(1, n.hdrSize()) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	size := body
	if s.kind == kRangeStream {
		size = sizeBytesField(1, body)
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	if s.kind == kRangeStream {
		w.lenField(1, body)
	}
	n.writeHdr(&w)
	for _, kv := range kvs {
		w.bytesField(2, kv)
	}
	if more {
		w.varintField(3, 1)
	}
	writeVarintNZ(&w, 4, uint64(count))
	n.endReply(c, s, &w)
	clear(kvs)
	n.kvs = kvs[:0]
}

// rangeKVs appends the records r asks for to kvs, and counts them.
func (n *node) rangeKVs(r *rangeReq, kvs [][]byte) ([][]byte, int64, error) {
	st := n.st
	k0 := len(kvs)
	if len(r.end) == 0 {
		id, ok := st.find(r.key)
		if !ok {
			return kvs, 0, nil
		}
		rec, err := st.at(id, r.rev)
		if err != nil || rec == nil {
			return kvs, 0, err
		}
		if !r.countOnly {
			kvs = append(kvs, rec)
		}
		return kvs, 1, nil
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	limit := r.limit
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var count int64
	var err error
	st.tree.scan(r.key, end, func(id uint32) bool {
		var rec []byte
		if rec, err = st.at(id, r.rev); err != nil {
			return false
		}
		if rec == nil {
			return true
		}
		count++
		if !r.countOnly && int64(len(kvs)-k0) < limit {
			kvs = append(kvs, rec)
		}
		return true
	})
	return kvs, count, err
}

func stripValue(kv []byte) []byte {
	var out []byte
	for m := kv; len(m) > 0; {
		num, typ, v, p, rest, err := field(m)
		if err != nil {
			return kv
		}
		switch {
		case num == 5:
		case typ == 0:
			out = appendVarintField(out, num, v)
		default:
			out = appendBytesField(out, num, p)
		}
		m = rest
	}
	return out
}
