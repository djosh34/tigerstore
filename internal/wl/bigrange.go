package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// Large range replies (#721, ruled: keep etcd's semantics and keep the
// budget). etcd builds a range's whole reply in memory: a range with no
// limit, a sort or a filter returns everything it matches, and its
// server sends a message of up to math.MaxInt32 bytes. This node returns
// the same, but a reply past oneShot is not built at once. Every record's
// size is known from its key's state, without its value, so the reply's
// length, its gRPC prefix and every RangeStream chunk's are known before
// any value is read. The reply then goes out in pieces: the revision it
// reads is held (store.holdRead), so that compaction keeps every state it
// needs while writes go on; each piece reads its records again by key and
// revision, fetches the values it lacks off the loop in one bounded
// batch, and is written as the connection's share (connOut) and its
// HTTP/2 windows take it. A reply past maxSendMsg is refused with
// ResourceExhausted before any value is read, as grpc-go refuses it.

// maxSendMsg is the largest message etcd's gRPC server sends, its default
// grpc.MaxSendMsgSize(math.MaxInt32).
const maxSendMsg = math.MaxInt32

// oneShot is the most of its records a range reply builds at once: the
// connection's share of what waits to be sent, and at least a request's
// worth, as one record of a key may take.
func (n *node) oneShot() int64 { return max(n.connOut, maxMsg) }

// errBig is a range whose reply passes oneShot: it is sent in pieces.
var errBig = errors.New("the range's reply is past what is built at once")

// bigRange is a range reply sent in pieces.
type bigRange struct {
	h   handle
	c   *client
	st  *store // the store it reads, which holds rev
	msg []byte // its request, which req's slices point into
	req rangeReq
	// planned: its records are counted and sized, and rev is held.
	planned bool
	stream  bool  // a RangeStream: each chunk is a message
	sorts   bool  // sorted other than by key ascending
	rev     int64 // the revision it reads
	hdrRev  int64 // the revision its header names
	// What it sends, n records in all: those after the key last sent
	// (after, once begun), in the tree's order, or sorted, from next on.
	after  []byte
	begun  bool
	sorted []sortedKey
	next   int
	n      int64
	taken  int64 // records written
	count  int64
	more   bool
	// The message being written: its bytes and records left, and its
	// closing fields (more and count; a RangeStream's only in its last).
	msgLeft  int
	msgRecs  int64
	msgClose bool
	msgMore  bool
	msgCount int64
	// RangeStream's chunks: the keys the next takes at most, the most the
	// stream sends (its limit), and the keys past it.
	chunk, total, past int64
	ended              bool // its last message is begun
	// buf is what it wrote and has not sent yet, from pos.
	buf []byte
	pos int
	// counted is what buf, sorted and after hold, in the client pool.
	counted  int
	fetching bool     // it waits for values read off the loop
	piece    []uint32 // the keys of the piece being read
	scan     []byte
}

// sortedKey is a key of a sorted range and its sort field.
type sortedKey struct {
	id uint32
	v  int64
}

// startBig sends the reply of range msg of stream s in pieces.
func (n *node) startBig(c *client, s *stream, msg []byte) {
	b := &bigRange{h: mkHandle(c, s.id), c: c, st: n.st, msg: bytes.Clone(msg), stream: s.kind == kRangeStream}
	if err := parseRange(b.msg, &b.req); err != nil {
		n.replyError(c, s.id, codeInvalidArgument, "bad request")
		return
	}
	n.bigs = append(n.bigs, b)
	n.pumpBig()
}

// pumpBig moves the large replies on as far as their connections take
// them.
func (n *node) pumpBig() {
	if len(n.bigs) == 0 {
		return
	}
	k := 0
	for _, b := range n.bigs {
		if n.stepBig(b) {
			n.bigs[k] = b
			k++
			continue
		}
		n.endBig(b)
	}
	clear(n.bigs[k:])
	n.bigs = n.bigs[:k]
}

// bigReady reports whether a large reply could move on now, so that the
// loop does not wait for an event first: one whose connection takes more
// and that waits for no value.
func (n *node) bigReady() bool {
	for _, b := range n.bigs {
		if b.fetching || !b.planned && !n.mayPlan(b) {
			continue
		}
		c, s := n.clientOf(b.h)
		if c == nil {
			return true
		}
		if c.waiting() >= n.connOut || b.pos < len(b.buf) && (c.flow.conn <= 0 || s.win <= 0 || s.holds) {
			continue
		}
		return true
	}
	return false
}

// mayPlan reports whether b, not planned yet, may start: one large reply
// at a time on a connection, and one sorted one at a time on the node,
// whose sorted keys take memory with the range's keys.
func (n *node) mayPlan(b *bigRange) bool {
	for _, o := range n.bigs {
		if o == b {
			return true
		}
		if o.planned && (o.c == b.c || o.sorts && b.req.sortsOther()) {
			return false
		}
	}
	return true
}

// stepBig moves b on. It reports false once b is done, or failed.
func (n *node) stepBig(b *bigRange) bool {
	c, s := n.clientOf(b.h)
	if c == nil {
		return false
	}
	if b.st != n.st {
		// A snapshot installed since replaced the store it read.
		n.abortBig(c, s)
		return false
	}
	if !b.planned {
		if !n.mayPlan(b) {
			return true
		}
		if !n.planBig(b, c, s) {
			return false
		}
	}
	for {
		// What waits to be sent to the client stays within its share.
		room := n.connOut - c.waiting()
		if room <= 0 {
			return true
		}
		if b.pos < len(b.buf) {
			m := min(int64(len(b.buf)-b.pos), c.flow.conn, s.win, room)
			if m <= 0 || s.holds {
				return true // it waits for window
			}
			w := msgW{s: &c.sock, sid: s.id, left: int(m)}
			w.write(b.buf[b.pos : b.pos+int(m)])
			c.flow.conn -= m
			s.win -= m
			b.pos += int(m)
			n.markDirty(&c.sock)
			continue
		}
		if b.fetching {
			return true
		}
		if b.ended && b.msgLeft == 0 {
			if !s.began.IsZero() && !b.stream {
				n.gs.ranges[1].observe(applyBuckets, n.now.Sub(s.began).Seconds())
			}
			n.stats.replies++
			n.callEnded(s, 0)
			n.endStreamOK(c, s)
			return false
		}
		if !n.buildPiece(b, c, s) {
			return false
		}
	}
}

// endBig lets go of what b holds.
func (n *node) endBig(b *bigRange) {
	if b.planned && b.st == n.st {
		b.st.releaseRead(b.rev)
	}
	b.planned = false
	b.c.growOut(-b.counted)
	b.counted = 0
	b.buf, b.sorted, b.after = nil, nil, nil
}

// abortBig ends a large reply's stream s with RST_STREAM, as grpc-go ends a call whose
// message it cannot finish.
func (n *node) abortBig(c *client, s *stream) {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], 2) // INTERNAL_ERROR
	f := c.frame(9 + 4)
	putFrameHeader(f, 4, frRST, 0, s.id)
	copy(f[9:], p[:])
	n.callEnded(s, codeInternal)
	c.endStream(s)
	n.markDirty(&c.sock)
}

// recount counts what b holds in its client's pool.
func (b *bigRange) recount() {
	now := cap(b.buf) + cap(b.sorted)*12 + cap(b.after) + cap(b.scan) + cap(b.piece)*4
	b.c.growOut(now - b.counted)
	b.counted = now
}

// sortsOther reports whether r sorts its records other than by key
// ascending, as shapeKVs sorts them.
func (r *rangeReq) sortsOther() bool {
	order := r.order()
	return order != sortNone && (r.sortTarget != sortByKey || order != sortAscend)
}

// passes reports whether a state passes r's revision filters.
func (r *rangeReq) passes(s *ckptState) bool {
	return (r.maxMod == 0 || s.mod <= r.maxMod) && (r.minMod == 0 || s.mod >= r.minMod) &&
		(r.maxCreate == 0 || s.create <= r.maxCreate) && (r.minCreate == 0 || s.create >= r.minCreate)
}

// recSize is the size of the record of key id in state s as b sends it.
func (b *bigRange) recSize(id uint32, s *ckptState) int {
	klen := int(b.st.keys[id].klen)
	if b.req.keysOnly {
		return kvSize(klen, 0, s.create, s.mod, s.ver, 0)
	}
	return kvSize(klen, int(s.vlen), s.create, s.mod, s.ver, s.lease)
}

// planBig counts b's keys and sizes its reply from their states, and
// refuses one past maxSendMsg. It reports false if it answered b.
func (n *node) planBig(b *bigRange, c *client, s *stream) bool {
	st, r := n.st, &b.req
	n.gs.rangesN++
	if r.rev > 0 && r.rev < st.compactRev {
		n.replyError(c, s.id, codeOutOfRange, msgCompacted)
		return false
	}
	if r.rev > st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgFuture)
		return false
	}
	b.rev, b.hdrRev = st.rev, st.rev
	if r.rev > 0 {
		b.rev = r.rev
	}
	b.sorts = r.sortsOther()
	if b.sorts && r.sortTarget == sortByValue {
		// Sorting by value needs every value at once, past what the node
		// builds at once: the one range it refuses (#721).
		n.replyError(c, s.id, codeResourceExhausted, "etcdserver: a range sorted by value past what the node reads at once")
		return false
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	limit := int64(math.MaxInt64)
	if r.limit > 0 {
		limit = r.limit
	}
	var matched int64
	recs := 0 // the size of the records' fields
	st.tree.scan(r.key, end, func(id uint32) bool {
		ks, ok := st.stateAt(id, b.rev)
		if !ok {
			return true
		}
		b.count++
		if !r.passes(&ks) {
			return true
		}
		matched++
		switch {
		case b.sorts:
			v := int64(0)
			switch r.sortTarget {
			case sortByVersion:
				v = ks.ver
			case sortByCreate:
				v = ks.create
			case sortByMod:
				v = ks.mod
			}
			b.sorted = append(b.sorted, sortedKey{id: id, v: v})
		case matched <= limit:
			recs += sizeBytesField(2, b.recSize(id, &ks))
		}
		return true
	})
	if b.sorts {
		var x sort.Interface = sortedKeys{b.sorted, st, r.sortTarget}
		if r.order() == sortDescend {
			x = sort.Reverse(x)
		}
		sort.Sort(x)
		b.sorted = b.sorted[:min(int64(len(b.sorted)), limit)]
		for _, k := range b.sorted {
			ks, _ := st.stateAt(k.id, b.rev)
			recs += sizeBytesField(2, b.recSize(k.id, &ks))
		}
	}
	b.n = min(matched, limit)
	b.more = r.limit > 0 && matched > r.limit
	if !b.stream {
		body := sizeBytesField(1, n.hdrSizeAt(b.hdrRev)) + recs + 2*int(b2i(b.more)) + sizeVarintField(4, uint64(b.count))
		if body > maxSendMsg {
			b.sorted = nil
			n.replyError(c, s.id, codeResourceExhausted, fmt.Sprintf("grpc: trying to send message larger than max (%d vs. %d)", body, maxSendMsg))
			return false
		}
		b.msgLeft, b.msgRecs = 5+body, b.n
		b.msgClose, b.msgMore, b.msgCount = true, b.more, b.count
		b.ended = true
		n.gs.calls[s.kind].sent++
		n.gs.sentBytes += int64(body)
		f := c.frame(9 + len(respHeaders))
		putFrameHeader(f, len(respHeaders), frHeaders, flEndHeaders, s.id)
		copy(f[9:], respHeaders)
		w := msgW{c: c, held: b.buf[:0], left: b.msgLeft}
		var p [5]byte
		binary.BigEndian.PutUint32(p[1:], uint32(body))
		w.write(p[:])
		n.writeHdrAt(&w, b.hdrRev)
		b.buf, b.msgLeft = w.held, w.left
	} else {
		n.openStream(c, s)
		b.total = r.limit
		if b.total <= 0 {
			b.total = math.MaxInt64
		}
		if b.more {
			b.past = b.count - b.n
		}
		b.chunk = min(int64(initialStreamChunk), b.total)
	}
	st.holdRead(b.rev)
	b.planned = true
	n.stats.bigRanges++
	b.recount()
	return true
}

// sortedKeys sorts a range's keys as kvSort sorts their records: the
// same comparisons on the same order give the same order, ties too.
type sortedKeys struct {
	ks     []sortedKey
	st     *store
	target uint64
}

func (s sortedKeys) Len() int      { return len(s.ks) }
func (s sortedKeys) Swap(i, j int) { s.ks[i], s.ks[j] = s.ks[j], s.ks[i] }

func (s sortedKeys) Less(i, j int) bool {
	if s.target == sortByKey {
		return bytes.Compare(s.st.key(s.ks[i].id), s.st.key(s.ks[j].id)) < 0
	}
	return s.ks[i].v-s.ks[j].v < 0
}

// walk calls f with the records b sends next, from where it is, in
// order, until f returns false or none is left.
func (n *node) walk(b *bigRange, f func(id uint32, size int) bool) {
	st, r := b.st, &b.req
	if b.sorts {
		for _, k := range b.sorted[b.next:] {
			ks, _ := st.stateAt(k.id, b.rev)
			if !f(k.id, b.recSize(k.id, &ks)) {
				return
			}
		}
		return
	}
	start := r.key
	if b.begun {
		b.scan = append(append(b.scan[:0], b.after...), 0)
		start = b.scan
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	st.tree.scan(start, end, func(id uint32) bool {
		ks, ok := st.stateAt(id, b.rev)
		if !ok || !r.passes(&ks) {
			return true
		}
		return f(id, b.recSize(id, &ks))
	})
}

// buildPiece writes b's next bytes into its buf: the next message's
// opening, or the next of its records, at most a share's worth, once
// their values are all in memory. It reports false if it ended b.
func (n *node) buildPiece(b *bigRange, c *client, s *stream) bool {
	b.buf, b.pos = b.buf[:0], 0
	if b.msgRecs == 0 && b.msgLeft == 0 {
		n.beginChunk(b, s)
		b.recount()
		return true
	}
	if b.msgRecs == 0 {
		w := msgW{c: c, held: b.buf, left: b.msgLeft}
		if b.msgClose {
			if b.msgMore {
				w.varintField(3, 1)
			}
			writeVarintNZ(&w, 4, uint64(b.msgCount))
		}
		b.buf, b.msgLeft = w.held, w.left
		if b.msgLeft != 0 {
			c.broken = true // a record came out another size than its state said
		}
		b.msgLeft = 0
		b.recount()
		return true
	}
	target := int(n.connOut)
	b.piece = b.piece[:0]
	size := 0
	n.walk(b, func(id uint32, sz int) bool {
		if int64(len(b.piece)) >= b.msgRecs || len(b.piece) > 0 && size+sz > target {
			return false
		}
		b.piece = append(b.piece, id)
		size += sz
		return true
	})
	if len(b.piece) == 0 {
		// Fewer records than it counted: never, as the revision is held.
		n.abortBig(c, s)
		return false
	}
	st := b.st
	recs := n.kvs[:0]
	away := false
	st.noWait = true
	for _, id := range b.piece {
		var rec []byte
		var err error
		if b.req.keysOnly {
			rec = st.keyAt(id, b.rev)
		} else {
			rec, err = st.at(id, b.rev)
		}
		if errors.Is(err, errAway) {
			away = true
			continue
		}
		if err != nil || rec == nil {
			st.noWait = false
			clear(recs)
			n.kvs = recs[:0]
			n.abortBig(c, s)
			return false
		}
		recs = append(recs, rec)
	}
	st.noWait = false
	if away {
		clear(recs)
		n.kvs = recs[:0]
		b.fetching = true
		n.fetchAway()
		b.recount()
		return true
	}
	w := msgW{c: c, held: b.buf, left: b.msgLeft}
	for _, rec := range recs {
		if b.req.keysOnly {
			rec = keysOnly(rec, &b.req)
		}
		w.bytesField(2, rec)
	}
	clear(recs)
	n.kvs = recs[:0]
	b.buf, b.msgLeft = w.held, w.left
	k := int64(len(b.piece))
	b.msgRecs -= k
	b.taken += k
	if b.sorts {
		b.next += int(k)
	} else {
		b.after = append(b.after[:0], st.key(b.piece[len(b.piece)-1])...)
		b.begun = true
	}
	b.recount()
	return true
}

// beginChunk writes the opening of a RangeStream's next chunk, as
// streamRange sends them: its message's prefix, its RangeResponse's
// length and, in the last, its header.
func (n *node) beginChunk(b *bigRange, s *stream) {
	rest := b.n - b.taken + b.past // keys a chunk range from here sees
	take := min(b.chunk, b.n-b.taken)
	more := rest > b.chunk
	last := !more || b.taken+take == b.total
	recs := 0
	k := int64(0)
	n.walk(b, func(id uint32, sz int) bool {
		if k == take {
			return false
		}
		recs += sizeBytesField(2, sz)
		k++
		return true
	})
	body := recs
	if last {
		body += sizeBytesField(1, n.hdrSizeAt(b.hdrRev)) + sizeVarintField(4, uint64(b.taken+take+max(rest-take, 0)*b2i(more)))
		if more {
			body += 2
		}
	}
	size := sizeBytesField(1, body)
	b.msgLeft, b.msgRecs = 5+size, take
	b.msgClose, b.msgMore, b.msgCount = last, more, b.taken+take+max(rest-take, 0)*b2i(more)
	b.ended = last
	n.gs.calls[s.kind].sent++
	n.gs.sentBytes += int64(size)
	w := msgW{c: b.c, held: b.buf, left: b.msgLeft}
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(size))
	w.write(p[:])
	w.lenField(1, body)
	if last {
		n.writeHdrAt(&w, b.hdrRev)
	}
	b.buf, b.msgLeft = w.held, w.left
	if !last {
		// The size of the RangeResponse etcd's chunk range returned (streamRange).
		sz := sizeBytesField(1, sizeVarintField(3, uint64(b.hdrRev))) + 2 + sizeVarintField(4, uint64(min(rest, b.chunk+1))) + recs
		b.chunk = min(nextChunk(b.chunk, sz, maxRequestBytes), b.total-(b.taken+take))
	}
	if take == 0 {
		// A chunk of no record: its closing follows at once.
		b.msgRecs = 0
	}
}
