package wl

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
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
	n.stats.readIndex++
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
	if r.rev > 0 && r.rev < st.compactRev {
		n.replyError(c, s.id, codeOutOfRange, msgCompacted)
		return
	}
	if r.rev > st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgFuture)
		return
	}
	kvs, count, more, err := n.rangeKVs(r, n.kvs[:0])
	if err != nil {
		n.replyError(c, s.id, codeInternal, err.Error())
		clear(kvs)
		n.kvs = kvs[:0]
		return
	}
	if r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = keysOnly(kv, r)
		}
	}
	if s.kind == kRangeStream {
		n.streamRange(c, s, r, kvs, count, more)
		clear(kvs)
		n.kvs = kvs[:0]
		return
	}
	body := sizeBytesField(1, n.hdrSize()) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	var w msgW
	n.beginReply(c, s, &w, body)
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

// streamRange answers a RangeStream with the records of its range as
// etcd sends them: in chunks of 10 keys first, then of twice or half as
// many as the last while its RangeResponse was under half or over twice
// --max-request-bytes, each a RangeStreamResponse; the last has the
// header, more and the count. count and more are the whole range's.
func (n *node) streamRange(c *client, s *stream, r *rangeReq, kvs [][]byte, count int64, more bool) {
	n.openStream(c, s)
	if r.countOnly {
		n.rangeChunk(c, s, nil, true, false, count)
		n.stats.replies++
		n.metrics.count(s.kind, 0)
		n.endStreamOK(c, s)
		return
	}
	total := r.limit // the keys the stream sends at most
	if total <= 0 {
		total = math.MaxInt64
	}
	// What etcd's chunk ranges read past the stream's limit: the keys left
	// in the range when the limit cut it.
	left := int64(0)
	if more {
		left = count - int64(len(kvs))
	}
	limit := min(int64(initialStreamChunk), total)
	sent := int64(0)
	for {
		rest := int64(len(kvs)) - sent + left // keys a chunk range from here sees
		take := min(limit, int64(len(kvs))-sent)
		chunkMore := rest > limit
		chunk := kvs[sent : sent+take]
		sent += take
		if done := !chunkMore || sent == total; done {
			n.rangeChunk(c, s, chunk, true, chunkMore, sent+max(rest-take, 0)*b2i(chunkMore))
			break
		}
		n.rangeChunk(c, s, chunk, false, false, 0)
		// The size of the RangeResponse etcd's chunk range returned: its
		// header with the revision, its records, more, and its count of
		// the keys it read, one past its limit.
		size := sizeBytesField(1, sizeVarintField(3, uint64(n.st.rev))) + 2 + sizeVarintField(4, uint64(min(rest, limit+1)))
		for _, kv := range chunk {
			size += sizeBytesField(2, len(kv))
		}
		limit = nextChunk(limit, size, maxRequestBytes)
		limit = min(limit, total-sent)
	}
	n.stats.replies++
	n.metrics.count(s.kind, 0)
	n.endStreamOK(c, s)
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// initialStreamChunk is etcd's initialStreamChunkLimit.
const initialStreamChunk = 10

// nextChunk is etcd's adjustChunkLimit.
func nextChunk(last int64, lastSize, target int) int64 {
	switch {
	case lastSize < target/2:
		last *= 2
	case lastSize > target*2:
		last /= 2
	}
	return max(last, 1)
}

// rangeChunk writes one RangeStreamResponse: kvs, and with last the
// header, more and count.
func (n *node) rangeChunk(c *client, s *stream, kvs [][]byte, last, more bool, count int64) {
	body := 0
	if last {
		body += sizeBytesField(1, n.hdrSize()) + sizeVarintField(4, uint64(count))
		if more {
			body += 2
		}
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	var w msgW
	w.begin(c, s, sizeBytesField(1, body))
	w.lenField(1, body)
	if last {
		n.writeHdr(&w)
	}
	for _, kv := range kvs {
		w.bytesField(2, kv)
	}
	if last {
		if more {
			w.varintField(3, 1)
		}
		writeVarintNZ(&w, 4, uint64(count))
	}
	w.end()
	n.markDirty(&c.sock)
}

// rangeKVs appends the records r asks for to kvs, counts the keys in
// its range, and reports whether its limit left some out. It takes
// etcd's steps (executeRange): read the range's keys, one more than the
// limit, or all when it sorts other than by key or filters by revision;
// filter, sort, and cut to the limit. The count is the range's,
// unfiltered.
func (n *node) rangeKVs(r *rangeReq, kvs [][]byte) ([][]byte, int64, bool, error) {
	st := n.st
	k0 := len(kvs)
	rev := max(r.rev, 0) // etcd reads at the current revision for any rev <= 0
	if len(r.end) == 0 {
		id, ok := st.find(r.key)
		if !ok {
			return kvs, 0, false, nil
		}
		rec, err := st.at(id, rev)
		if err != nil || rec == nil {
			return kvs, 0, false, err
		}
		if !r.countOnly {
			kvs = shapeKVs(append(kvs, rec), k0, r)
		}
		return kvs, 1, false, nil
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	read := int64(math.MaxInt64)
	if r.limit > 0 && r.limit < math.MaxInt64 && r.defaultOrder() && !r.filters() {
		read = r.limit + 1
	}
	var count int64
	var err error
	st.tree.scan(r.key, end, func(id uint32) bool {
		var rec []byte
		if rec, err = st.at(id, rev); err != nil {
			return false
		}
		if rec == nil {
			return true
		}
		count++
		if !r.countOnly && int64(len(kvs)-k0) < read {
			kvs = append(kvs, rec)
		}
		return true
	})
	if r.countOnly || err != nil {
		return kvs, count, false, err
	}
	kvs = shapeKVs(kvs, k0, r)
	more := false
	if r.limit > 0 && int64(len(kvs)-k0) > r.limit {
		clear(kvs[k0+int(r.limit):])
		kvs, more = kvs[:k0+int(r.limit)], true
	}
	return kvs, count, more, nil
}

// defaultOrder is etcd's IsDefaultOrdering: no order, or by key
// ascending.
func (r *rangeReq) defaultOrder() bool {
	return r.sortOrder == sortNone || (r.sortTarget == sortByKey && r.sortOrder == sortAscend)
}

// filters reports whether r filters by revision.
func (r *rangeReq) filters() bool {
	return r.minMod != 0 || r.maxMod != 0 || r.minCreate != 0 || r.maxCreate != 0
}

// order is the order etcd sorts r's records in: ascending by any target
// but the key when r names none.
func (r *rangeReq) order() uint64 {
	if r.sortTarget != sortByKey && r.sortOrder == sortNone {
		return sortAscend
	}
	return r.sortOrder
}

// Sort orders and targets, as etcd's RangeRequest names them.
const (
	sortNone, sortAscend, sortDescend                              = 0, 1, 2
	sortByKey, sortByVersion, sortByCreate, sortByMod, sortByValue = 0, 1, 2, 3, 4
)

// shapeKVs filters kvs[k0:] by r's revision filters and sorts them as
// etcd does, with the same sort, so that keys that tie come out in the
// same order. A range that does neither keeps them as they are.
func shapeKVs(kvs [][]byte, k0 int, r *rangeReq) [][]byte {
	order := r.order()
	sorts := order != sortNone && (r.sortTarget != sortByKey || order != sortAscend)
	if !sorts && !r.filters() {
		return kvs
	}
	recs := make(kvRecs, 0, len(kvs)-k0)
	for _, rec := range kvs[k0:] {
		kv := parseKV(rec)
		switch {
		case r.maxMod != 0 && kv.mod > r.maxMod,
			r.minMod != 0 && kv.mod < r.minMod,
			r.maxCreate != 0 && kv.create > r.maxCreate,
			r.minCreate != 0 && kv.create < r.minCreate:
			continue
		}
		recs = append(recs, kv)
	}
	if sorts {
		var s sort.Interface = kvSort{recs, r.sortTarget}
		if order == sortDescend {
			s = sort.Reverse(s)
		}
		sort.Sort(s)
	}
	clear(kvs[k0:])
	kvs = kvs[:k0]
	for _, kv := range recs {
		kvs = append(kvs, kv.rec)
	}
	return kvs
}

// kvRec is a record with the fields a sort or filter reads.
type kvRec struct {
	rec, key, value      []byte
	create, mod, version int64
}

type kvRecs []kvRec

func parseKV(rec []byte) kvRec {
	kv := kvRec{rec: rec}
	for m := rec; len(m) > 0; {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			break
		}
		switch num {
		case 1:
			kv.key = p
		case 2:
			kv.create = int64(v)
		case 3:
			kv.mod = int64(v)
		case 4:
			kv.version = int64(v)
		case 5:
			kv.value = p
		}
		m = rest
	}
	return kv
}

// kvSort is etcd's kvSortBy*, by target.
type kvSort struct {
	kvs    kvRecs
	target uint64
}

func (s kvSort) Len() int      { return len(s.kvs) }
func (s kvSort) Swap(i, j int) { s.kvs[i], s.kvs[j] = s.kvs[j], s.kvs[i] }

func (s kvSort) Less(i, j int) bool {
	a, b := &s.kvs[i], &s.kvs[j]
	switch s.target {
	case sortByVersion:
		return a.version-b.version < 0
	case sortByCreate:
		return a.create-b.create < 0
	case sortByMod:
		return a.mod-b.mod < 0
	case sortByValue:
		return bytes.Compare(a.value, b.value) < 0
	}
	return bytes.Compare(a.key, b.key) < 0
}

// keysOnly is a record as etcd answers it to a keys-only range: without
// its value, and without its lease too unless the range sorts by value
// (etcd's FastKeysOnly reads the index, which holds no lease).
func keysOnly(kv []byte, r *rangeReq) []byte {
	return stripValue(kv, r.sortTarget == sortByValue)
}

func stripValue(kv []byte, keepLease bool) []byte {
	var out []byte
	for m := kv; len(m) > 0; {
		num, typ, v, p, rest, err := field(m)
		if err != nil {
			return kv
		}
		switch {
		case num == 5, num == 6 && !keepLease:
		case typ == 0:
			out = appendVarintField(out, num, v)
		default:
			out = appendBytesField(out, num, p)
		}
		m = rest
	}
	return out
}
