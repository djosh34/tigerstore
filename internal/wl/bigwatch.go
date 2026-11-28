package wl

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Large watch responses (#728, ruled on the line of #721's first ruling:
// keep etcd's semantics and keep the budget). etcd sends a watcher one
// WatchResponse with all of a revision's events, built whole: a DeleteRange
// with prev_kv over many watched keys, a revoke of a lease with many keys,
// or one put of a large value over another with prev_kv, as kube-apiserver
// watches, all give a revision of more than one message's worth. Its
// server sends up to math.MaxInt32 bytes, and clientv3 receives as much.
//
// A watcher's events of a revision are sized from their keys' states
// before any value is read. Within replyRoom they go out as any others.
// Past it the watcher catches up to the revision and sends it from here:
// the revision is held (store.holdRead), its events are walked in the
// change index, their values read and fetched a share's worth at a time,
// and written as the connection's share and HTTP/2 windows take them, the
// gRPC prefix known from the sizes. A watch that asked for fragments gets
// them as etcd's sendFragments splits a response: as many events a
// fragment as keep it under --max-request-bytes, at least one. One of
// more than maxSendMsg bytes cancels its watch with ResourceExhausted, as
// grpc-go refuses to send it.
//
// While a message of a watch stream is written in pieces, nothing else is
// written into the stream: its other messages wait in its client's held
// queue (client.take, client.drain), in order behind it.

// bigWatch is a revision's events of one watcher, sent in pieces.
type bigWatch struct {
	h      handle
	c      *client
	wi     int32   // the watcher
	inc    uint32  // its incarnation in the slot (watcher.inc)
	id     int64   // its watch ID
	x      watcher // a copy of it, its range and filters, at the start
	st     *store
	rev    int64 // the revision
	hdrRev int64 // the revision its responses' headers name
	// hdrTerm is the term they name: every fragment carries one header,
	// as etcd's sendFragments does.
	hdrTerm uint64
	// next is where the next event lies among the revision's changes,
	// counted from the revision's first; end is where the message being
	// written ends, and frag whether it is a fragment but the last.
	next, end      int
	frag           bool
	opened         bool // a message is begun: the stream holds it alone
	started        bool // at least one DATA byte has been published
	held, heldPrev bool
	msgLeft        int // bytes of that message still to build, its prefix included
	// buf is what it built and has not sent yet, from pos.
	buf                                []byte
	pos                                int
	fetching                           bool // it waits for values read off the loop
	canceled                           bool // its watch went: it ends the message under way
	counted                            int
	stage                              *replyStage
	waitP                              bool
	pieceLeft                          int
	resume                             fillResume
	directHead, directBody, directMeta int
	liveEvents                         []event // borrowed only during flushEvents, never across a turn
	liveFirst                          int32
}

// bigEv is one event of a bigWatch: its change and its Event's size.
type bigEv struct {
	ch      change
	putSlot uint32 // metadata-turn-only hint, in existing padding
	prev    bool   // it carries the value before
	size    int    // the Event message's size
}

// revSize is the size of a WatchResponse of watcher x with its events of
// the revision whose first change is changes[j], from their keys' states:
// no value is read. It reports errPrevCompacted where a value before x
// asks for is compacted, as catchUp does.
func (n *node) revSize(x *watcher, j int) (int, error) {
	size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
	err := n.eachRevEvent(n.st, x, j, 0, func(_ int, ev bigEv) bool {
		size += sizeBytesField(11, ev.size)
		return true
	})
	return size, err
}

// revStart is where the changes of revision rev begin in st's index.
func revStart(st *store, rev int64) int {
	j, _ := slices.BinarySearchFunc(st.changes, rev, func(ch change, r int64) int { return cmp.Compare(ch.rev, r) })
	return j
}

// eachRevEvent calls f with watcher x's events of the revision whose
// first change is st.changes[j], from the from-th change of it on, and
// each one's place among the revision's changes, until f returns false.
func (n *node) eachRevEvent(st *store, x *watcher, j, from int, f func(off int, ev bigEv) bool) error {
	if handled, err := x.deliveryPlant.each(n, st, x, j, from, f); handled {
		return err
	}
	return n.eachUnmutatedRevEvent(st, x, j, from, f)
}

func (n *node) eachUnmutatedRevEvent(st *store, x *watcher, j, from int, f func(off int, ev bigEv) bool) error {
	if j >= len(st.changes) {
		return nil
	}
	rev := st.changes[j].rev
	for k := j + from; k < len(st.changes) && st.changes[k].rev == rev; k++ {
		ch := st.changes[k]
		if (ch.del && x.noDel) || (!ch.del && x.noPut) || !x.covers(st.key(ch.key)) {
			continue
		}
		klen := int(st.keys[ch.key].klen)
		ev := bigEv{ch: ch}
		ver := int64(0)
		if ch.del {
			ev.size = 2 + sizeBytesField(2, sizeBytesField(1, klen)+sizeVarintField(3, uint64(rev)))
		} else {
			s, slot, ok := st.putStateAtSlot(ch.key, rev)
			ev.putSlot = slot
			if !ok && rev <= st.compactRev {
				// A put its own Txn deleted at the compacted revision,
				// which trimming dropped: etcd keeps only the deletion
				// there, and sends that alone (the re-check of 9b538554).
				continue
			}
			if !ok {
				return fmt.Errorf("the put of key %d at %d has no state", ch.key, rev)
			}
			ev.size = sizeBytesField(2, kvSize(klen, int(s.vlen), s.create, s.mod, s.ver, s.lease))
			ver = s.ver
		}
		if x.prevKV && (ch.del || !deletedIn(st.changes, k)) {
			ps, ok := st.stateAt(ch.key, rev-1)
			if !ok && rev <= st.compactRev && (ch.del || ver > 1) {
				// As catchUp: kube-apiserver rejects an event without the
				// value before it that it asked for.
				return errPrevCompacted
			}
			if ok && x.withPrev() {
				ev.prev = true
				ev.size += sizeBytesField(3, kvSize(klen, int(ps.vlen), ps.create, ps.mod, ps.ver, ps.lease))
			}
		}
		if !f(k-j, ev) {
			return nil
		}
	}
	return nil
}

// startBigWatch has watcher i, x, of stream s of client c send its events
// of the revision whose first change is st.changes[j] in pieces. It
// reports false if it did not: the watch was canceled instead, as
// compacted or past maxSendMsg.
func (n *node) startBigWatch(i int32, x *watcher, c *client, s *stream, j int) bool {
	st := n.st
	if !x.fragment {
		size, err := n.revSize(x, j)
		switch {
		case errors.Is(err, errPrevCompacted):
			n.writeCompacted(c, s, x)
			n.w.remove(i)
			return false
		case err != nil:
			n.abortBigWatch(c, s)
			return false
		case size > maxSendMsg:
			n.writeCanceled(c, s, x, fmt.Sprintf("rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (%d vs. %d)", size, maxSendMsg))
			n.w.remove(i)
			return false
		}
	} else if _, err := n.revSize(x, j); errors.Is(err, errPrevCompacted) {
		n.writeCompacted(c, s, x)
		n.w.remove(i)
		return false
	}
	meta := 0
	if n.cursorFetch {
		meta = n.takeDirectOwner(c, 0)
		if meta == 0 {
			n.roomShort = true
			return true
		}
	}
	b := &bigWatch{h: mkHandleRaw(x.sock, x.gen, x.sid), c: c, wi: i, inc: x.inc, id: x.id, x: *x, st: st, rev: st.changes[j].rev, hdrRev: st.rev, hdrTerm: n.term, directMeta: meta}
	// The states at the revision and before it, which its events show,
	// stay while it is sent.
	if !n.cursorFetch {
		st.holdRead(b.rev - 1)
		st.holdRead(b.rev)
		b.held, b.heldPrev = true, true
	}
	x.big = true
	n.bigWatches = append(n.bigWatches, b)
	n.stats.bigWatches++
	return true
}

// writeCanceled cancels watch x with reason, as etcd's watch stream
// cancels a watch.
func (n *node) writeCanceled(c *client, s *stream, x *watcher, reason string) {
	size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id)) + 2 + sizeBytesField(6, len(reason))
	mw := msgW{control: true}
	mw.begin(c, s, size)
	n.writeHdr(&mw)
	writeVarintNZ(&mw, 2, uint64(x.id))
	mw.varintField(4, 1)
	mw.bytesField(6, []byte(reason))
	mw.end()
	n.markDirty(&c.sock)
}

// pumpBigWatches moves the large watch responses on as far as their
// connections take them.
func (n *node) pumpBigWatches() {
	n.updateFillCredits()
	if len(n.bigWatches) == 0 {
		return
	}
	k := 0
	for _, b := range n.bigWatches {
		if n.stepBigWatch(b) {
			n.bigWatches[k] = b
			k++
			continue
		}
		n.endBigWatch(b)
	}
	clear(n.bigWatches[k:])
	n.bigWatches = n.bigWatches[:k]
}

// bigWatchReady reports whether a large watch response could move on now.
func (n *node) bigWatchReady() bool {
	for _, b := range n.bigWatches {
		if b.st != n.st {
			return true
		}
		if b.waitP && !n.fillCreditWake {
			continue
		}
		if b.fetching {
			continue
		}
		c, s := n.clientOf(b.h)
		if c == nil {
			return true
		}
		if b.st != n.st || n.watcherOf(b) == nil {
			return true
		}
		if n.cursorFetch && (c.flow.conn <= 0 || c.streamWin(s) <= 0 || rawSocketRoom(c) <= 0) {
			continue
		}
		if b.stage != nil && rawSocketRoom(c) <= 0 {
			continue
		}
		if c.bigW != nil && c.bigW != b || c.waiting() >= n.connOut || (b.stage != nil || b.pos < len(b.buf)) && (c.flow.conn <= 0 || c.streamWin(s) <= 0) || !b.opened && s.holds && !s.big {
			continue
		}
		return true
	}
	return false
}

// watcherOf is b's watcher, nil once its watch went.
func (n *node) watcherOf(b *bigWatch) *watcher {
	x := &n.w.ws[b.wi]
	if !x.active || x.inc != b.inc || mkHandleRaw(x.sock, x.gen, x.sid) != b.h || x.id != b.id {
		return nil
	}
	return x
}

// stepBigWatch moves b on. It reports false once b is done, or failed.
func (n *node) stepBigWatch(b *bigWatch) bool {
	c, s := n.clientOf(b.h)
	if c == nil {
		return false
	}
	x := n.watcherOf(b)
	if x == nil {
		// Its watch was canceled: the message under way is ended, so that
		// the stream's other watches go on, and no other is begun.
		b.canceled = true
	}
	if b.st != n.st {
		// A snapshot installed since replaced the store it reads. The
		// install has the watch catch up again (installSnapshot); a
		// stream that has part of the response, a fragment or more, ends.
		if b.started || !n.cursorFetch && (b.opened || b.next > 0) {
			n.abortBigWatch(c, s)
		}
		return false
	}
	if n.cursorFetch && !b.started {
		if b.canceled {
			return false
		}
		if c.flow.conn <= 0 || c.streamWin(s) <= 0 {
			return true
		}
	}
	if c.bigW != nil && c.bigW != b {
		// One large watch response at a time on a connection: another's
		// message, or its fragments, are under way (#689's re-check, C1).
		return true
	}
	for {
		room := n.connOut - c.waiting()
		if room <= 0 {
			return true
		}
		if n.cursorFetch && (c.flow.conn <= 0 || c.streamWin(s) <= 0 || rawSocketRoom(c) <= 0) {
			return true
		}
		if b.pos < len(b.buf) {
			m := min(int64(len(b.buf)-b.pos), c.flow.conn, c.streamWin(s), room)
			if m <= 0 {
				return true // it waits for window
			}
			if !b.started {
				if !n.startWatchData(b, c, s) {
					return false
				}
				b.started = true
			}
			w := msgW{s: &c.sock, sid: s.id, left: int(m)}
			w.write(b.buf[b.pos : b.pos+int(m)])
			c.spendWin(s, m)
			b.pos += int(m)
			n.markDirty(&c.sock)
			continue
		}
		if b.stage != nil {
			if !b.started {
				if !n.startWatchData(b, c, s) {
					return false
				}
				b.started = true
			}
			if !n.drainRawStage(c, s, b.stage, true) {
				return true
			}
			b.stage = nil
			continue
		}
		if b.fetching || b.waitP {
			return true
		}
		if b.opened && b.next == b.end {
			// The message is written. The stream stays held between
			// fragments: clientv3 joins a watch's fragments only when no
			// other response comes between them.
			if b.msgLeft != 0 {
				c.broken = true // an event came out another size than its state said
			}
			b.opened = false
			if !b.frag || b.canceled {
				n.endStreamPiece(c, s)
				if x != nil && !b.canceled {
					// Its catch-up goes on after the revision.
					x.syncRev = b.rev + 1
					x.deliveryPlant.retire(x.syncRev)
					x.sent = true
				}
				return false
			}
			continue
		}
		if !b.opened {
			if b.canceled {
				return false
			}
			// The stream's messages before the first go out first.
			if s.holds && !s.big {
				return true
			}
			if !n.beginBigWatchMsg(b, x, c, s) {
				return false
			}
			continue
		}
		if !n.buildWatchPiece(b, c, s) {
			return false
		}
	}
}

// abortBigWatch ends watch stream s with RST_STREAM, as abortBig ends a
// range's, and lets go of what the stream had: its watchers and the
// messages it held (#689's re-check, C3).
func (n *node) abortBigWatch(c *client, s *stream) {
	n.closeWatchStream(c, s)
	c.dropHeld(s.id)
	n.abortBig(c, s)
}

// endStreamPiece ends a watch stream's message written in pieces: the
// messages it held behind go out.
func (n *node) endStreamPiece(c *client, s *stream) {
	if s.big {
		s.big = false
		c.bigs--
	}
	if s.holds {
		c.drain()
	}
}

// beginBigWatchMsg begins b's next message: its events from b.next, all
// the revision's or, for a watch that asked for fragments, as many as
// etcd's sendFragments puts in one. It reports false if it ended b.
func (n *node) beginBigWatchMsg(b *bigWatch, x *watcher, c *client, s *stream) bool {
	st := b.st
	j := revStart(st, b.rev)
	hdr := sizeBytesField(1, n.hdrSizeTerm(b.hdrRev, b.hdrTerm)) + sizeVarintField(2, uint64(b.id))
	size, events, end := hdr, 0, b.next
	more := false
	err := n.eachRevEvent(st, &b.x, j, b.next, func(off int, ev bigEv) bool {
		add := sizeBytesField(11, ev.size)
		if b.x.fragment && events > 0 && size+2+add >= maxRequestBytes {
			more = true
			return false
		}
		size += add
		events++
		end = off + 1
		return true
	})
	if err != nil {
		n.abortBigWatch(c, s)
		return false
	}
	if events == 0 {
		// None left: a revision whose events ended with the last message.
		if x != nil {
			x.syncRev = b.rev + 1
			x.deliveryPlant.retire(x.syncRev)
		}
		return false
	}
	b.frag = more
	if b.frag {
		size += 2 // the fragment flag
	}
	if size > maxSendMsg {
		// Only a fragment of one event could: never.
		n.abortBigWatch(c, s)
		return false
	}
	b.end, b.opened = end, true
	c.bigW = b
	if !s.big {
		s.big = true
		c.bigs++
	}
	n.gs.calls[s.kind].sent++
	n.gs.sentBytes += int64(size)
	b.msgLeft = 5 + size
	if n.cursorFetch {
		b.directBody = size
		b.directHead = 5 + hdr + 2*int(b2i(b.frag))
		b.pos = 0
	} else {
		w := msgW{c: c, held: b.buf[:0], left: b.msgLeft}
		var p [5]byte
		binary.BigEndian.PutUint32(p[1:], uint32(size))
		w.write(p[:])
		n.writeHdrTerm(&w, b.hdrRev, b.hdrTerm)
		writeVarintNZ(&w, 2, uint64(b.id))
		if b.frag {
			w.varintField(7, 1)
		}
		b.buf, b.pos, b.msgLeft = w.held, 0, w.left
	}
	b.recount()
	return true
}

// buildWatchPiece writes b's next events into its buf, at most a share's
// worth, once their values are all in memory. It reports false if it
// ended b.
func (n *node) buildWatchPiece(b *bigWatch, c *client, s *stream) bool {
	if n.cursorFetch {
		return n.buildDirectWatch(b, c, s)
	}
	st := b.st
	j := revStart(st, b.rev)
	target := int(n.connOut)
	type pieceEv struct {
		off int
		ev  bigEv
	}
	var evs []pieceEv
	size := 0
	err := n.eachRevEvent(st, &b.x, j, b.next, func(off int, ev bigEv) bool {
		if off >= b.end || len(evs) > 0 && size+ev.size > target {
			return false
		}
		evs = append(evs, pieceEv{off, ev})
		size += ev.size
		return true
	})
	if err != nil || len(evs) == 0 {
		n.abortBigWatch(c, s)
		return false
	}
	recs := make([]event, len(evs))
	away := false
	st.noWait = true
	for i, pe := range evs {
		ch := pe.ev.ch
		recs[i] = event{del: ch.del, key: ch.key, rev: ch.rev}
		var err error
		if !ch.del {
			if recs[i].kv, err = st.putAt(ch.key, ch.rev); errors.Is(err, errAway) {
				away = true
				continue
			} else if err != nil || recs[i].kv == nil {
				st.noWait = false
				n.abortBigWatch(c, s)
				return false
			}
		}
		if pe.ev.prev {
			if recs[i].prev, err = st.at(ch.key, ch.rev-1); errors.Is(err, errAway) {
				away = true
				continue
			} else if err != nil || recs[i].prev == nil {
				st.noWait = false
				n.abortBigWatch(c, s)
				return false
			}
		}
	}
	st.noWait = false
	if away {
		b.fetching = true
		n.fetchAway()
		return true
	}
	w := msgW{c: c, held: b.buf[:0], left: b.msgLeft}
	for i := range recs {
		n.writeEvent(&w, &b.x, &recs[i])
	}
	b.buf, b.pos, b.msgLeft = w.held, 0, w.left
	b.next = evs[len(evs)-1].off + 1
	b.recount()
	return true
}

// recount counts what b holds in its client's pool.
func (b *bigWatch) recount() {
	now := cap(b.buf)
	b.c.growOut(now - b.counted)
	b.counted = now
}

// endBigWatch lets go of what b holds.
func (n *node) endBigWatch(b *bigWatch) {
	n.cancelRawWatch(b)
	if b.stage != nil {
		b.stage.discard()
		b.stage = nil
	}
	if b.st == n.st && b.held {
		if b.heldPrev {
			b.st.releaseRead(b.rev - 1)
		}
		b.st.releaseRead(b.rev)
	}
	b.held, b.heldPrev = false, false
	if x := n.watcherOf(b); x != nil {
		x.big = false
	}
	if b.c.bigW == b {
		// It wrote on its stream, a message or fragments: the stream's
		// other messages go on, however it ended (#689's re-check, C2).
		b.c.bigW = nil
		if c, s := n.clientOf(b.h); c != nil {
			n.endStreamPiece(c, s)
		}
	}
	b.c.growOut(-b.counted)
	b.counted = 0
	b.buf = nil
	n.freeDirectOwner(b.c, b.directMeta)
	b.directMeta = 0
}

// mayPassRoom reports whether watcher x's events of the revision whose
// first change is changes[j] may pass replyRoom: those of a revision of
// more than one change, or with the values before. One put's event
// alone is at most a request's value and its key.
func mayPassRoom(x *watcher, changes []change, j int) bool {
	return x.prevKV || j+1 < len(changes) && changes[j+1].rev == changes[j].rev
}
