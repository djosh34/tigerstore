package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sort"

	"go.etcd.io/raft/v3"

	"github.com/djosh34/tigerstore/internal/planted"
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
	var b *readBatch
	if k := len(n.freeBatches); k > 0 {
		b = n.freeBatches[k-1]
		n.freeBatches = n.freeBatches[:k-1]
	} else {
		b = &readBatch{}
	}
	b.seq, b.known, b.reask, b.asked, b.started = n.readSeq, false, false, now, now
	b.reads, n.reads = n.reads, b.reads[:0]
	b.buf, n.readBuf = n.readBuf, b.buf[:0]
	n.readBatches = append(n.readBatches, b)
	n.stats.readIndex++
	n.askReadIndex(b)
}

// readState records the index of a ReadIndex.
func (n *node) readState(ctx []byte, index uint64) {
	if len(ctx) != 8 {
		return
	}
	seq := binary.BigEndian.Uint64(ctx)
	if n.leadConfirmed(seq) {
		return
	}
	for _, b := range n.readBatches {
		if b.seq != seq {
			continue
		}
		b.index, b.known = index, true
	}
}

// readIndexUnsafe reports whether Raft would answer a ReadIndex asked
// now, the node's own or a peer's, with an index short of what the node
// committed and answered. That is so, by Raft's own state and not the
// loop's view of it, which a turn's steps outrun, while the node leads a
// cluster of one voter, whose ReadIndex Raft answers at once with the
// commit it knows (raft's IsSingleton), and that commit is not of its own
// term: after a restart it is the commit the hard state kept, and the
// entries after it, which the node committed, applied and answered before
// it crashed, are known committed again only with its term's first entry.
// A read served at that index missed a write already answered
// (TestOneVoterMembership, seed 4 under -race). A leader of more voters
// answers no ReadIndex before its term's first entry commits anyway.
func (n *node) readIndexUnsafe() bool {
	if n.voters() != 1 {
		return false
	}
	st := n.rn.BasicStatus()
	if st.RaftState != raft.StateLeader {
		return false
	}
	t, err := n.storage.Term(st.GetCommit())
	return err != nil || t != st.GetTerm()
}

// askReadIndex asks Raft for batch b's read index, or, while its answer
// could be short (readIndexUnsafe), holds the ask until it no longer
// could (askHeld).
func (n *node) askReadIndex(b *readBatch) {
	if n.readIndexUnsafe() {
		b.reask = true
		return
	}
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], b.seq)
	n.rn.ReadIndex(ctx[:])
}

// askHeld asks for the batches askReadIndex held, once Raft's answer can
// no longer be short.
func (n *node) askHeld() {
	if n.readIndexUnsafe() {
		return
	}
	for _, b := range n.readBatches {
		if b.reask {
			b.reask = false
			n.askReadIndex(b)
		}
	}
}

// termApplied reports whether the node leads and applied its own first
// entry of its current term (confHold.leadFirst, leadTerm). Until then
// the entries a lone voter committed before a restart, past the commit its
// hard state kept, may still revoke a lease it applied, or grant one again
// under the same ID (expireLeases).
func (n *node) termApplied() bool {
	h := &n.confHold
	return n.leading() && h.leadFirst != 0 && h.leadFirst <= n.applied && h.leadTerm == n.rn.BasicStatus().GetTerm()
}

// serveReads answers the batches whose index is applied, and asks for
// those whose ask was held (askHeld).
func (n *node) serveReads() {
	n.askHeld()
	k := 0
	for i, b := range n.readBatches {
		// Planted B01b: served once its read index is known, before the
		// node applied it.
		if b.known && (b.index <= n.applied || planted.On("B01b")) {
			for _, r := range b.reads {
				if r.done != nil {
					r.done()
					continue
				}
				c, s := n.clientOf(r.h)
				if c == nil {
					continue
				}
				msg := b.buf[r.off : r.off+r.len]
				// Auth is checked again with the store as applied up to the
				// read's index, as etcd checks a read after its barrier: a
				// permission revoked or auth enabled since the read came in
				// holds for it.
				if n.auth.enabled && !n.authAllows(c, s, msg) {
					continue
				}
				switch r.kind {
				case kMemberList:
					n.replyMembers(c, s)
					continue
				case kLeaseTTL:
					if err := n.leaseTimeToLive(c, s, msg); err != nil {
						n.replyError(c, s.id, codeInvalidArgument, "bad request")
					}
					continue
				}
				var req rangeReq
				if err := parseRange(msg, &req); err != nil {
					n.replyError(c, s.id, codeInvalidArgument, "bad request")
					continue
				}
				n.serveRange(c, s, &req, msg)
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
// leader change that dropped their ReadIndex, and tells the clients of a
// batch that waited past requestTimeout that their read timed out, as
// etcd's linearizable reads time out. Without a quorum no ReadIndex is
// answered, and the reads, which clients often give up on, would be kept
// for as long (#689's review).
func (n *node) retryReads(now int64) {
	k := 0
	for _, b := range n.readBatches {
		if now-b.started < int64(requestTimeout/tickEvery) {
			n.readBatches[k] = b
			k++
			continue
		}
		for _, r := range b.reads {
			// A read of the node's own (healthRead) times out on its own.
			if r.done == nil {
				n.replyErrorH(r.h, codeUnavailable, msgTimeout)
			}
		}
		n.freeBatches = append(n.freeBatches, b)
	}
	clear(n.readBatches[k:])
	n.readBatches = n.readBatches[:k]
	for _, b := range n.readBatches {
		if !b.known && !b.reask && now-b.asked > 5 {
			n.readSeq++
			b.seq, b.asked = n.readSeq, now
			n.askReadIndex(b)
		}
	}
}

// serveRange answers a Range or RangeStream, msg, from the store as it
// is. One that needs values the loop would wait on the disk for waits for
// them to be read off the loop, and is served again then (fetch.go).
func (n *node) serveRange(c *client, s *stream, r *rangeReq, msg []byte) {
	if n.cursorFetch && n.tryResidentPoint(c, s, r) {
		return
	}
	n.serveRangeSlow(c, s, r, msg)
}

// tryResidentPoint keeps the common no-fetch/no-loan path out of the large
// cursor/legacy retry frame and its defers. Selection and serialization are
// shared with main's path. Any protected output, loan, cold value, historical
// version, sort or large reply goes through the unchanged funded slow path.
func (n *node) tryResidentPoint(c *client, s *stream, r *rangeReq) bool {
	st := n.st
	if s.kind != kRange || len(r.end) != 0 || n.legacyRetry != nil || st.legacyView != nil ||
		n.stageCredits.used > 0 || n.stagePromises > 0 || n.legacyPromises > 0 || c.replyStage != nil ||
		r.sortsOther() || r.rev > st.rev || r.rev > 0 && r.rev < st.compactRev {
		return false
	}
	id, found := st.find(r.key)
	if found && (st.keys[id].rec == 0 || r.rev > 0 && st.keys[id].mod > r.rev) {
		return false
	}
	var kvs [][]byte
	var count int64
	var more bool
	var err error
	if !r.keysOnly && !r.countOnly && !r.filters() {
		// With no projection/filter/sort, this is exactly live's resident
		// record. Avoid entering historical lookup and no-op sort observers.
		kvs = n.kvs[:0]
		if found && st.keys[id].create != 0 {
			k := &st.keys[id]
			kvs = append(kvs, st.slab.bytes(k.rec, int(k.rlen)))
			count = 1
		}
	} else {
		kvs, count, more, err = n.keyRangeKVs(r, n.kvs[:0], id, found)
	}
	if r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = keysOnly(kv, r)
		}
	}
	header, term := st.rev, n.term
	body := sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	wire := int64(5 + body)
	// msgRoom returns true without changing any state when roomNeeded is
	// zero. This is that exact within-share admission, not an output loan.
	if err != nil || n.cannotFit(wire) || n.roomNeeded(c, wire) != 0 {
		clear(kvs)
		n.kvs = kvs[:0]
		return false
	}
	n.gs.rangesN++
	n.writeRangeReply(c, s, kvs, count, more, header, term, body)
	return true
}

func (n *node) serveRangeSlow(c *client, s *stream, r *rangeReq, msg []byte) {
	st := n.st
	header, term := st.rev, n.term
	legacyHeld := false
	if b := n.legacyRetry; b != nil && b.owner.hr == nil && b.owner.h == mkHandle(c, s.id) {
		copyReq := *r
		copyReq.rev = b.owner.view
		r = &copyReq
		header, term = b.owner.header, b.owner.term
		legacyHeld = b.readHeld
	}
	if r.rev > 0 && r.rev < st.compactRev && !legacyHeld {
		n.gs.rangesN++
		n.replyError(c, s.id, codeOutOfRange, msgCompacted)
		return
	}
	if r.rev > st.rev {
		n.gs.rangesN++
		n.replyError(c, s.id, codeOutOfRange, msgFuture)
		return
	}
	if n.tryFillRange(c, s, r, msg) {
		return
	}
	if n.cursorFetch {
		n.beginLegacyRange(c, s, r, msg)
		defer func() { n.legacyReading = legacyOwner{}; n.dropCollectingLegacy() }()
	}
	if n.legacyRetry != nil {
		defer func() { clear(n.kvs); n.kvs = nil }()
	}
	st.noWait = true
	kvs, count, more, err := n.rangeKVs(r, n.kvs[:0], n.oneShot())
	st.noWait = false
	if errors.Is(err, errBig) {
		clear(kvs)
		n.kvs = kvs[:0]
		n.startBig(c, s, msg)
		return
	}
	if errors.Is(err, errAway) {
		clear(kvs)
		n.kvs = kvs[:0]
		n.awaitValues(mkHandle(c, s.id), msg)
		return
	}
	if err == nil && n.legacyRetry != nil && n.legacyRetry.material {
		b := n.legacyRetry
		if b.materialW {
			defer func() { clear(kvs); n.kvs = nil }()
			n.copyMaterialRange(b, c, s, r, kvs, count, more)
			return
		}
		// Small selected output has a funded copy in the finish guard.
		// Retire the all-input W index rather than pinning it to DATA.
		wire := 5 + sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(4, uint64(count)) + 2
		for _, kv := range kvs {
			size, _, _ := projectedRecord(kv, r.keysOnly)
			wire += sizeBytesField(2, size)
		}
		if s.kind != kRange || wire > 512 || c.waiting() != 0 {
			defer func() { clear(kvs); n.kvs = nil }()
			n.publishMaterialRange(b, c, s, r, kvs, count, more)
			return
		}
		c.stageSocket()
	}
	if err == nil && r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = keysOnly(kv, r)
		}
	}
	body := sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	if s.kind == kRangeStream {
		// Its chunks' own fields: a few bytes each, of at least 10 keys.
		body += 32 * (len(kvs)/initialStreamChunk + 1)
	}
	wire := 5 + body
	if s.kind == kRangeStream && n.cursorFetch && n.legacyRetry == nil {
		wire = n.residentStreamWire(r, kvs, count, more, header, term)
	}
	defer n.dropReplyStage(c)
	if err == nil && !n.replyRoom(c, s, int64(wire), int64(5+body)) {
		clear(kvs)
		n.kvs = kvs[:0]
		if n.cannotFit(int64(5 + body)) {
			// It could never fit: it goes in pieces (reserve.go).
			n.startBig(c, s, msg)
			return
		}
		// The reserve has no room for it: it waits, unbuilt (reserve.go).
		n.waitRoom(c, s, msg, body-sizeBytesField(1, n.hdrSizeTerm(header, term)))
		return
	}
	n.gs.rangesN++
	if err != nil {
		n.replyError(c, s.id, codeInternal, err.Error())
		clear(kvs)
		n.kvs = kvs[:0]
		return
	}
	if s.kind == kRangeStream {
		n.streamRange(c, s, r, kvs, count, more)
		clear(kvs)
		n.kvs = kvs[:0]
		return
	}
	n.writeRangeReply(c, s, kvs, count, more, header, term, body)
}

func (n *node) writeRangeReply(c *client, s *stream, kvs [][]byte, count int64, more bool, header int64, term uint64, body int) {
	if !s.began.IsZero() {
		n.gs.ranges[1].observe(applyBuckets, n.now.Sub(s.began).Seconds())
	}
	var w msgW
	n.beginReply(c, s, &w, body)
	n.writeHdrTerm(&w, header, term)
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
	if c.replyStage == nil {
		n.openStream(c, s)
	}
	if r.countOnly {
		n.rangeChunk(c, s, nil, true, false, count)
		n.commitReplyStage(c, s)
		n.stats.replies++
		n.callEnded(s, 0)
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
	n.commitReplyStage(c, s)
	n.stats.replies++
	n.callEnded(s, 0)
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
	header, term := n.st.rev, n.term
	if b := n.legacyRetry; b != nil && b.owner.h == mkHandle(c, s.id) {
		header, term = b.owner.header, b.owner.term
	}
	body := 0
	if last {
		body += sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(4, uint64(count))
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
		n.writeHdrTerm(&w, header, term)
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
// unfiltered. Past most bytes of records read, 0 for no bound, it stops
// with errBig: the reply is sent in pieces (bigrange.go).
func (n *node) rangeKVs(r *rangeReq, kvs [][]byte, most int64) ([][]byte, int64, bool, error) {
	st := n.st
	k0 := len(kvs)
	a0 := len(st.away)
	rev := max(r.rev, 0) // etcd reads at the current revision for any rev <= 0
	// A range of keys only reads no value, unless it sorts by value.
	noValue := r.keysOnly && r.sortTarget != sortByValue
	if len(r.end) == 0 {
		id, ok := st.find(r.key)
		return n.keyRangeKVs(r, kvs, id, ok)
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	read := r.readLimit()
	if r.countOnly {
		read = 0
	}
	// count is the live keys of the range; taken, those of them it read or
	// found away.
	var count, taken, size int64
	var err error
	away, big := false, false
	st.tree.scan(r.key, end, func(id uint32) bool {
		if most > 0 && size > most {
			big = true
			return false
		}
		if taken >= read {
			// Past what the range returns, it only counts: the key's
			// state says whether it was live, and its value, which may
			// lie only on disk, is not read.
			count += b2i(st.liveAt(id, rev))
			return true
		}
		var rec []byte
		if noValue {
			rec = st.keyAt(id, rev)
		} else {
			a := st.awayBytes
			rec, err = st.at(id, rev)
			if errors.Is(err, errAway) {
				// The scan goes on, so that one fetch reads every value the
				// range lacks, up to most. Only a live state has a value to
				// read.
				away, err = true, nil
				count++
				taken++
				size += st.awayBytes - a
				return true
			}
			if err != nil {
				return false
			}
		}
		if rec == nil {
			return true
		}
		count++
		taken++
		size += int64(len(rec))
		kvs = append(kvs, rec)
		return true
	})
	if big {
		st.away = st.away[:a0]
		if n.cursorFetch {
			n.dropCollectingLegacy()
		}
		return kvs, 0, false, errBig
	}
	if away {
		return kvs, 0, false, errAway
	}
	if r.countOnly || err != nil {
		return kvs, count, false, err
	}
	kvs = n.shapeKVs(kvs, k0, r)
	more := false
	if r.limit > 0 && int64(len(kvs)-k0) > r.limit {
		clear(kvs[k0+int(r.limit):])
		kvs, more = kvs[:k0+int(r.limit)], true
	}
	return kvs, count, more, nil
}

// keyRangeKVs is the unchanged single-key selection shared with the
// resident point fast path. In particular count, revision filters, projection
// and sorting still follow shapeKVs; no alternate temporary representation.
func (n *node) keyRangeKVs(r *rangeReq, kvs [][]byte, id uint32, found bool) ([][]byte, int64, bool, error) {
	if !found {
		return kvs, 0, false, nil
	}
	st, k0, rev := n.st, len(kvs), max(r.rev, 0)
	if r.countOnly {
		return kvs, b2i(st.liveAt(id, rev)), false, nil
	}
	if r.keysOnly && r.sortTarget != sortByValue {
		if rec := st.keyAt(id, rev); rec != nil {
			return n.shapeKVs(append(kvs, rec), k0, r), 1, false, nil
		}
		return kvs, 0, false, nil
	}
	rec, err := st.at(id, rev)
	if err != nil || rec == nil {
		return kvs, 0, false, err
	}
	if !r.countOnly {
		kvs = n.shapeKVs(append(kvs, rec), k0, r)
	}
	return kvs, 1, false, nil
}

// readLimit is how many of its keys, in key order, range r reads, as
// etcd's rangeLimit asks mvcc for: one more than its limit in etcd's
// default ordering, which a sort order of none is whatever the target,
// with no revision filter; else all. A sort then orders those alone.
func (r *rangeReq) readLimit() int64 {
	if r.limit > 0 && r.limit < math.MaxInt64 && r.defaultOrder() && !r.filters() {
		return r.limit + 1
	}
	return math.MaxInt64
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
func (n *node) shapeKVs(kvs [][]byte, k0 int, r *rangeReq) [][]byte {
	if !r.sortsOther() && !r.filters() {
		return kvs
	}
	if n.measureSort == nil {
		return shapeKVs(kvs, k0, r)
	}
	return shapeKVsMeasured(kvs, k0, r, n.measureSort)
}

func shapeKVs(kvs [][]byte, k0 int, r *rangeReq) [][]byte { return shapeKVsMeasured(kvs, k0, r, nil) }

// Optional observation changes no storage, selection or sort operation.
func shapeKVsMeasured(kvs [][]byte, k0 int, r *rangeReq, observe func(int, int)) [][]byte {
	order := r.order()
	sorts := order != sortNone && (r.sortTarget != sortByKey || order != sortAscend)
	if !sorts && !r.filters() {
		return kvs
	}
	recs := make(kvRecs, 0, len(kvs)-k0)
	if observe != nil {
		observe(cap(kvs), cap(recs))
	}
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
