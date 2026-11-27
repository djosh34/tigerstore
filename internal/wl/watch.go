package wl

import (
	"bytes"
	"cmp"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/djosh34/tigerstore/internal/planted"
)

// Watches. A watcher is matched to a write through the directory of its
// key: each directory caches the watchers whose prefix covers it, so an
// event costs a load of that list. Events of one apply are kept as
// references to the store's records, which stay valid until the apply
// ends, and each watcher gets one WatchResponse with all of them,
// written straight into its stream's records.

type watcher struct {
	active bool
	// inc tells this watcher from an earlier one in its slot, which a
	// large response under way may still name (bigWatch).
	inc      uint32
	sock     int
	gen      uint16
	sid      uint32
	id       int64
	key, end []byte
	prefix   bool // the range is every key with key as prefix
	prevKV   bool
	fragment bool  // split a response over --max-request-bytes, as etcd's
	minRev   int64 // a watch from a revision still to come gets no event before it
	noPut    bool
	noDel    bool
	first    int32 // events of this apply, a list through event.next
	last     int32
	queued   bool
	// syncRev, while not 0, is the revision a watch from the past catches
	// up from: it gets no live events until it reaches the head.
	syncRev int64
	// fetchWait: its catch-up waits for values read off the loop, and
	// goes on once they come (takeFetched).
	fetchWait bool
	// progressNotify asks for etcd's progress notifications; sent says
	// the watch got events since the last.
	progressNotify bool
	sent           bool
	// liveBytes is the size of its events of the apply; past replyRoom
	// it catches up instead (bigwatch.go). big: a revision of its events
	// is sent in pieces.
	liveBytes int
	big       bool
}

type event struct {
	next int32
	del  bool
	key  uint32
	rev  int64
	kv   []byte // the record written; nil for a delete
	prev []byte
}

type watches struct {
	ws      []watcher
	gen     uint32
	other   []int32 // watchers whose range is not a prefix
	events  []event
	touched []int32
	// unsynced are the watchers that catch up, and syncBuf the events of
	// one's catch-up.
	unsynced []int32
	syncBuf  []event
	// progressWait are the streams whose progress request waits for a
	// watch from a revision the store has not reached; the tick asks
	// again.
	progressWait []handle
}

func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}

// add registers a watcher and returns its index.
func (w *watches) add(x watcher) int32 {
	x.active = true
	x.first, x.last = -1, -1
	x.prefix = len(x.end) > 0 && len(x.key) > 0 && bytes.Equal(x.end, prefixEnd(x.key))
	i := int32(-1)
	for j := range w.ws {
		if !w.ws[j].active {
			i = int32(j)
			break
		}
	}
	if i < 0 {
		w.ws = append(w.ws, watcher{})
		i = int32(len(w.ws) - 1)
	}
	x.inc = w.gen
	w.ws[i] = x
	w.gen++
	w.rebuildOther()
	return i
}

func (w *watches) remove(i int32) {
	w.ws[i].active = false
	w.gen++
	w.rebuildOther()
}

func (w *watches) rebuildOther() {
	w.other = w.other[:0]
	for i := range w.ws {
		if w.ws[i].active && !w.ws[i].prefix {
			w.other = append(w.other, int32(i))
		}
	}
}

// cover makes sure dir d's list of watchers is current.
func (w *watches) cover(t *tree, d uint32) *dirEnt {
	e := &t.dirs[d]
	if e.wgen == w.gen+1 {
		return e
	}
	e.wfull = e.wfull[:0]
	e.wpart = false
	path := t.path(d)
	for i := range w.ws {
		x := &w.ws[i]
		if !x.active || !x.prefix {
			continue
		}
		if bytes.HasPrefix(path, x.key) {
			e.wfull = append(e.wfull, int32(i))
		} else if bytes.HasPrefix(x.key, path) {
			e.wpart = true
		}
	}
	e.wgen = w.gen + 1
	return e
}

func (x *watcher) covers(key []byte) bool {
	if len(x.end) == 0 {
		return bytes.Equal(key, x.key)
	}
	if bytes.Compare(key, x.key) < 0 {
		return false
	}
	return (len(x.end) == 1 && x.end[0] == 0) || bytes.Compare(key, x.end) < 0
}

// record adds the event of a write of key id to every watcher of it.
func (w *watches) record(st *store, id uint32, del bool, rev int64, kv, prev []byte) {
	if len(w.ws) == 0 {
		return
	}
	e := w.cover(&st.tree, st.keys[id].dir)
	klen := int(st.keys[id].klen)
	for _, i := range e.wfull {
		w.push(i, id, del, rev, kv, prev, klen)
	}
	if e.wpart || len(w.other) > 0 {
		key := st.key(id)
		if e.wpart {
			for i := range w.ws {
				x := &w.ws[i]
				if x.active && x.prefix && !bytes.HasPrefix(st.tree.path(st.keys[id].dir), x.key) && bytes.HasPrefix(key, x.key) {
					w.push(int32(i), id, del, rev, kv, prev, klen)
				}
			}
		}
		for _, i := range w.other {
			if w.ws[i].covers(key) {
				w.push(i, id, del, rev, kv, prev, klen)
			}
		}
	}
}

// wantsPrev reports whether a watcher the deletion (del) or the put of
// key id at rev reaches, as record reaches it, wants the key's previous
// value (prevKV): the change reads it only then.
func (w *watches) wantsPrev(st *store, id uint32, rev int64, del bool) bool {
	// A key that is not live has no value before to want, and one that
	// compaction took out of the tree names no directory to look in.
	if len(w.ws) == 0 || !st.exists(id) {
		return false
	}
	wants := func(i int32) bool {
		x := &w.ws[i]
		return x.active && x.prevKV && (del && !x.noDel || !del && !x.noPut) && x.syncRev == 0 && rev >= x.minRev
	}
	e := w.cover(&st.tree, st.keys[id].dir)
	if slices.ContainsFunc(e.wfull, wants) {
		return true
	}
	if !e.wpart && len(w.other) == 0 {
		return false
	}
	key := st.key(id)
	if e.wpart {
		dir := st.tree.path(st.keys[id].dir)
		for i := range w.ws {
			if x := &w.ws[i]; x.active && x.prefix && !bytes.HasPrefix(dir, x.key) && bytes.HasPrefix(key, x.key) && wants(int32(i)) {
				return true
			}
		}
	}
	for _, i := range w.other {
		if w.ws[i].covers(key) && wants(i) {
			return true
		}
	}
	return false
}

func (w *watches) push(i int32, id uint32, del bool, rev int64, kv, prev []byte, klen int) {
	x := &w.ws[i]
	if (del && x.noDel) || (!del && x.noPut) || x.syncRev != 0 || rev < x.minRev {
		return
	}
	// Past one message's room, the apply's events go out in pieces from
	// the change index: the watcher catches up to them, and the apply
	// reads it no more values before (wantsPrev; #728).
	if x.liveBytes += len(kv) + len(prev) + klen + 32; x.liveBytes > replyRoom {
		x.syncRev = rev
		if x.first >= 0 {
			x.syncRev = w.events[x.first].rev
		}
		x.first, x.last, x.liveBytes = -1, -1, 0
		w.unsynced = append(w.unsynced, i)
		return
	}
	firstOfWrite := x.first < 0 || w.events[x.last].rev != rev
	w.link(x, i, event{next: -1, del: del, key: id, rev: rev, kv: kv, prev: prev})
	if planted.On("B06b") && firstOfWrite {
		// Planted: the first event of a write goes to the watcher twice.
		w.link(x, i, event{next: -1, del: del, key: id, rev: rev, kv: kv, prev: prev})
	}
}

// link appends ev to watcher x's (number i) events of the apply.
func (w *watches) link(x *watcher, i int32, e event) {
	ev := int32(len(w.events))
	w.events = append(w.events, e)
	if x.first < 0 {
		x.first = ev
	} else {
		w.events[x.last].next = ev
	}
	x.last = ev
	if !x.queued {
		x.queued = true
		w.touched = append(w.touched, i)
	}
}

func (n *node) watchRequest(c *client, s *stream, msg []byte) error {
	if !s.watch {
		s.watch = true
		b := c.frame(9 + len(respHeaders))
		putFrameHeader(b, len(respHeaders), frHeaders, flEndHeaders, s.id)
		copy(b[9:], respHeaders)
	}
	if len(msg) == 0 {
		// A request of no kind: etcd ignores it.
		return nil
	}
	num, _, _, p, _, err := field(msg)
	if err != nil {
		return err
	}
	switch num {
	case 1:
		x := watcher{sock: c.idx, gen: c.gen, sid: s.id}
		var filters []byte
		startRev := int64(0)
		for m := p; len(m) > 0; {
			fn, typ, v, fp, rest, err := field(m)
			if err != nil {
				return err
			}
			switch fn {
			case 1:
				x.key = bytes.Clone(fp)
			case 2:
				x.end = bytes.Clone(fp)
			case 5:
				if typ == 2 {
					filters = append(filters, fp...)
				} else {
					filters = append(filters, byte(v))
				}
			case 3:
				startRev = int64(v)
			case 4:
				x.progressNotify = v != 0
			case 6:
				x.prevKV = v != 0
			case 8:
				x.fragment = v != 0
			case 7:
				if v != 0 {
					x.id = int64(v)
				}
			}
			m = rest
		}
		for _, f := range filters {
			switch f {
			case 0:
				x.noPut = true
			case 1:
				x.noDel = true
			}
		}
		// etcd's: an empty key is "\x00", the smallest; a start revision
		// below 0 is refused as compacted.
		if len(x.key) == 0 {
			x.key = []byte{0}
		}
		if n.auth.enabled {
			// etcd checks a watch from a key as the key alone.
			end := x.end
			if len(end) == 1 && end[0] == 0 {
				end = nil
			}
			if reason := n.watchAllowed(c, s, x.key, end); reason != "" {
				n.writeRefused(c, s, reason)
				n.markDirty(&c.sock)
				return nil
			}
		}
		if planted.On("B07") && startRev > 1 {
			// Planted: the watch starts one revision early.
			startRev--
		}
		if startRev < 0 {
			n.writeRefused(c, s, msgCompacted)
			n.markDirty(&c.sock)
			return nil
		}
		// etcd's watch stream: an empty range or an ID the stream has is
		// refused; ID 0 asks for the next one free.
		toEnd := len(x.end) == 1 && x.end[0] == 0
		switch {
		case len(x.end) != 0 && !toEnd && bytes.Compare(x.key, x.end) >= 0:
			n.writeRefused(c, s, "mvcc: watcher range is empty")
			n.markDirty(&c.sock)
			return nil
		case x.id == 0:
			for n.hasWatch(c, s, s.nextWatch) {
				s.nextWatch++
			}
			x.id = s.nextWatch
			s.nextWatch++
		case n.hasWatch(c, s, x.id):
			n.writeRefused(c, s, "mvcc: duplicate watch ID provided on the WatchStream")
			n.markDirty(&c.sock)
			return nil
		}
		i := n.w.add(x)
		n.pruneWatches(c, s)
		s.wids = append(s.wids, i)
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id)) + 2
		w.begin(c, s, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(x.id))
		w.varintField(3, 1)
		w.end()
		// A start revision in the past: the events since, then the live
		// ones. Nothing applies in between, so none is lost or doubled.
		if startRev > n.st.rev {
			n.w.ws[i].minRev = startRev
		}
		if startRev > 0 && startRev <= n.st.rev {
			if startRev < n.st.compactRev {
				n.writeCompacted(c, s, &x)
				n.w.remove(i)
			} else {
				n.w.ws[i].syncRev = startRev
				n.w.unsynced = append(n.w.unsynced, i)
			}
		}
	case 2:
		var wid int64
		for m := p; len(m) > 0; {
			fn, _, v, _, rest, err := field(m)
			if err != nil {
				return err
			}
			if fn == 1 {
				wid = int64(v)
			}
			m = rest
		}
		found, first := false, false
		for _, i := range s.wids {
			if n.owns(c, s, i) && n.w.ws[i].id == wid {
				if b := c.bigW; b != nil && b.frag && b.wi == i && b.inc == n.w.ws[i].inc && b.h == mkHandleRaw(c.idx, c.gen, s.id) {
					// Its fragments are under way: clientv3 drops any
					// other response while it joins them, until a canceled
					// one. The cancel goes first of what waits behind them
					// (#689's re-check, D3).
					first = true
				}
				n.w.remove(i)
				found = true
			}
		}
		n.pruneWatches(c, s)
		if !found {
			// etcd answers no cancel of a watch the stream does not have.
			return nil
		}
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(wid)) + 2
		w.begin(c, s, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(wid))
		w.varintField(4, 1)
		w.end()
		if first {
			c.holdFirst(s.id)
		}
	case 3:
		// As etcd's, the progress waits until every watch of the stream
		// caught up: no event it then sends is older.
		s.progressDue = true
		n.progressIfSynced(c, s)
	}
	n.markDirty(&c.sock)
	return nil
}

// hasWatch reports whether watch stream s of client c has a watch of ID
// id.
func (n *node) hasWatch(c *client, s *stream, id int64) bool {
	for _, i := range s.wids {
		if n.owns(c, s, i) && n.w.ws[i].id == id {
			return true
		}
	}
	return false
}

// owns reports whether watcher i is a live watch of stream s of client
// c. A slot s.wids names may since hold another stream's watch: a watch
// removed (canceled, compacted) frees its slot for the next, and another
// stream may give that one the same ID.
func (n *node) owns(c *client, s *stream, i int32) bool {
	x := &n.w.ws[i]
	return x.active && x.sock == c.idx && x.gen == c.gen && x.sid == s.id
}

// pruneWatches keeps in s.wids only the stream's live watches, once each.
func (n *node) pruneWatches(c *client, s *stream) {
	k := 0
	for _, i := range s.wids {
		if n.owns(c, s, i) && !slices.Contains(s.wids[:k], i) {
			s.wids[k] = i
			k++
		}
	}
	s.wids = s.wids[:k]
}

// writeRefused answers a watch create that etcd refuses: created and
// canceled at once, with watch ID -1 and the reason.
func (n *node) writeRefused(c *client, s *stream, reason string) {
	size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, math.MaxUint64) + 2 + 2 + sizeBytesField(6, len(reason))
	var w msgW
	w.begin(c, s, size)
	n.writeHdr(&w)
	w.varintField(2, math.MaxUint64)
	w.varintField(3, 1)
	w.varintField(4, 1)
	w.bytesField(6, []byte(reason))
	w.end()
}

func (n *node) closeWatchStream(c *client, s *stream) {
	if planted.On("B18") {
		// Planted: the stream's watchers stay.
		s.wids = s.wids[:0]
		return
	}
	for _, i := range s.wids {
		if n.w.ws[i].active && n.w.ws[i].sock == c.idx && n.w.ws[i].sid == s.id {
			n.w.remove(i)
		}
	}
	s.wids = s.wids[:0]
}

// flushEvents writes each watcher's events of the apply as one
// WatchResponse.
func (n *node) flushEvents() {
	w := &n.w
	for _, i := range w.touched {
		x := &w.ws[i]
		x.queued = false
		first := x.first
		x.first, x.last, x.liveBytes = -1, -1, 0
		if !x.active || first < 0 {
			continue
		}
		c, s := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid))
		if c == nil {
			continue
		}
		if planted.On("B06a") {
			dropLastOfWrite(w, first)
		}
		if planted.On("B06d") && x.progressNotify {
			// Planted: a progress notification at the current revision
			// goes out ahead of the events it covers.
			n.writeProgress(c, s, x.id)
		}
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			size += sizeBytesField(11, n.eventSize(x, &w.events[e]))
		}
		if c.slow() || !n.msgRoom(c, int64(5+size)) {
			// Its client takes nothing now, or the reserve has no room
			// for what passes its share: the watch catches up from these
			// events once it does.
			x.syncRev = w.events[first].rev
			w.unsynced = append(w.unsynced, i)
			continue
		}
		if x.fragment {
			// Rare: a watch that asked for fragments goes the long way.
			var evs []event
			for e := first; e >= 0; e = w.events[e].next {
				evs = append(evs, w.events[e])
			}
			n.writeEvents(c, s, x, evs)
			continue
		}
		var mw msgW
		mw.begin(c, s, size)
		n.writeHdr(&mw)
		writeVarintNZ(&mw, 2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			n.writeEvent(&mw, x, &w.events[e])
		}
		mw.end()
		n.markDirty(&c.sock)
	}
	clear(w.events)
	w.events = w.events[:0]
	w.touched = w.touched[:0]
}

// dropLastOfWrite is planted bug B06a: the list of events from first
// loses its last one when the write before it changed several keys.
func dropLastOfWrite(w *watches, first int32) {
	prev := int32(-1)
	e := first
	for w.events[e].next >= 0 {
		prev, e = e, w.events[e].next
	}
	if prev >= 0 && w.events[prev].rev == w.events[e].rev {
		w.events[prev].next = -1
	}
}

// withPrev reports whether x's events carry the value before them.
func (x *watcher) withPrev() bool { return x.prevKV && !planted.On("B06c") }

// writeEvent writes ev as an Event, field 11 of a WatchResponse.
func (n *node) writeEvent(mw *msgW, x *watcher, ev *event) {
	mw.lenField(11, n.eventSize(x, ev))
	if ev.del {
		mw.varintField(1, 1)
		key := n.st.key(ev.key)
		mw.lenField(2, sizeBytesField(1, len(key))+sizeVarintField(3, uint64(ev.rev)))
		mw.bytesField(1, key)
		mw.varintField(3, uint64(ev.rev))
	} else {
		mw.bytesField(2, ev.kv)
	}
	if x.withPrev() && ev.prev != nil {
		mw.bytesField(3, ev.prev)
	}
}

// A watch from a past revision catches up from the change index, a
// bounded amount each turn, as etcd's unsynced watchers do: a watch of
// a long history never holds the loop up. It gets no live events until it
// reaches the head; that happens between applies, so none is lost or sent
// twice.
// defaultProgressNotify is etcd's default
// --experimental-watch-progress-notify-interval.
const defaultProgressNotify = 10 * time.Minute

const (
	syncChanges = 1 << 14 // changes the catch-up looks at in a turn, all watchers together
	syncEvents  = 256     // events it sends in a turn, all watchers together
)

// syncBytes is the most of records and values the catch-up reads in a
// turn, all watchers together, those it fetches off the loop too: a
// share's worth (oneShot). Counted in events alone, a turn's catch-up of
// large values fetched all of 16,384 changes' values at once, and wrote
// 256 events of up to the request limit each (#689's area-3 review,
// finding 22).
func (n *node) syncBytes() int64 { return n.oneShot() }

// syncWatchers moves the watchers that catch up on, for one turn.
func (n *node) syncWatchers() error {
	w := &n.w
	if len(w.unsynced) == 0 {
		return nil
	}
	budget := syncBudget{changes: syncChanges, events: syncEvents, bytes: n.syncBytes(), bigAt: -1}
	k := 0
	for _, i := range w.unsynced {
		x := &w.ws[i]
		if !x.active || x.syncRev == 0 {
			continue
		}
		if x.big {
			// A revision of its events is sent in pieces (bigwatch.go).
			w.unsynced[k] = i
			k++
			continue
		}
		c, s := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid))
		if c == nil {
			continue
		}
		if x.syncRev < n.st.compactRev {
			n.writeCompacted(c, s, x)
			w.remove(i)
			continue
		}
		if n.cursorFetch {
			done, err := n.startWatchFill(i, x, c, s, &budget)
			if errors.Is(err, errPrevCompacted) {
				n.writeCompacted(c, s, x)
				w.remove(i)
				continue
			}
			if err != nil {
				return err
			}
			if done {
				x.syncRev = 0
				n.progressIfSynced(c, s)
				continue
			}
			w.unsynced[k] = i
			k++
			continue
		}
		if room := n.connOut - c.waiting(); budget.left() && room > 0 && !c.slow() && !x.fetchWait {
			// It writes what its client's share has room for, at least a
			// revision's events (#725).
			budget.away = false
			all := budget.bytes
			budget.bytes = min(all, room)
			done, err := n.catchUp(x, &budget, func(evs []event) bool {
				size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
				for i := range evs {
					size += sizeBytesField(11, n.eventSize(x, &evs[i]))
				}
				// What passes the share takes the reserve's room, or waits
				// for the next turn. A revision within one message always
				// fits the reserve once it has room; one past it goes in
				// pieces (bigwatch.go, #689's re-check, B7).
				if !n.msgRoom(c, int64(5+size)) {
					return false
				}
				n.writeEvents(c, s, x, evs)
				return true
			})
			budget.bytes = all - (min(all, room) - budget.bytes)
			if errors.Is(err, errPrevCompacted) {
				n.writeCompacted(c, s, x)
				w.remove(i)
				continue
			}
			if err != nil {
				return err
			}
			if j := budget.bigAt; j >= 0 {
				// Its next revision passes one message's room: it goes
				// out in pieces.
				budget.bigAt = -1
				if !n.startBigWatch(i, x, c, s, j) {
					continue
				}
			}
			if done {
				x.syncRev = 0
				n.progressIfSynced(c, s)
				continue
			}
			x.fetchWait = budget.away
		}
		w.unsynced[k] = i
		k++
	}
	w.unsynced = w.unsynced[:k]
	return nil
}

// syncReady reports whether a watch's catch-up could go on now, so that
// the loop does not wait for an event first: one that stopped at its
// turn's budget, or whose client took what was sent to it, and that
// waits for no value. Waiting for the next tick, a catch-up of large
// values went at a share every 100 ms.
func (n *node) syncReady() bool {
	if n.roomShort {
		return false
	}
	for _, i := range n.w.unsynced {
		x := &n.w.ws[i]
		if !x.active || x.syncRev == 0 || x.fetchWait || x.big {
			// A large response moves on by bigWatchReady.
			continue
		}
		if c, _ := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid)); c == nil || !c.slow() && c.waiting() < n.connOut {
			return true
		}
	}
	return false
}

type syncBudget struct {
	changes, events int
	bytes           int64
	away            bool // a value the turn read was away
	// bigAt is where in the change index a revision begins whose events
	// pass replyRoom, for a bigWatch; -1 for none.
	bigAt int
}

func (b *syncBudget) left() bool { return b.changes > 0 && b.events > 0 && b.bytes > 0 }

// deletedIn reports whether change j's key was deleted earlier in the
// same revision.
func deletedIn(changes []change, j int) bool {
	for k := j - 1; k >= 0 && changes[k].rev == changes[j].rev; k-- {
		if changes[k].key == changes[j].key && changes[k].del {
			return true
		}
	}
	return false
}

// errPrevCompacted is a catch-up that met an event whose previous value,
// which its watcher asks for, is compacted.
var errPrevCompacted = errors.New("the value before an event is compacted")

// catchUp hands emit watcher x's events from x.syncRev on, until the
// head or until the budget is spent, and then only at a revision's end.
// It reports whether x reached the head. emit reports whether it sent
// the events; if not, x goes on from where it was, as if it had none.
//
// A value the loop would wait on the disk for is read off the loop: x
// then sends nothing this turn, and catches up from where it was once the
// values came (fetch.go).
func (n *node) catchUp(x *watcher, b *syncBudget, emit func([]event) bool) (bool, error) {
	st := n.st
	st.noWait = true
	away := false
	defer func() {
		st.noWait = false
		if away {
			n.fetchAway()
		}
	}()
	j, _ := slices.BinarySearchFunc(st.changes, x.syncRev, func(ch change, rev int64) int { return cmp.Compare(ch.rev, rev) })
	evs := n.w.syncBuf[:0]
	defer func() {
		clear(evs)
		n.w.syncBuf = evs[:0]
	}()
	last := int64(-1)
	for ; j < len(st.changes); j++ {
		ch := st.changes[j]
		if !b.left() && ch.rev != last {
			if away {
				return false, nil
			}
			if len(evs) == 0 || emit(evs) {
				x.syncRev = ch.rev
			}
			return false, nil
		}
		if ch.rev != last && mayPassRoom(x, st.changes, j) {
			// A revision is sized from its keys' states before any of its
			// values is read: one past one message's room goes out in
			// pieces (bigwatch.go, #728).
			size, err := n.revSize(x, j)
			if err != nil {
				return false, err
			}
			if size > replyRoom {
				if away || len(evs) > 0 && !emit(evs) {
					return false, nil
				}
				x.syncRev, b.bigAt = ch.rev, j
				return false, nil
			}
		}
		b.changes--
		last = ch.rev
		if (ch.del && x.noDel) || (!ch.del && x.noPut) || !x.covers(st.key(ch.key)) {
			continue
		}
		ev := event{del: ch.del, key: ch.key, rev: ch.rev}
		var err error
		a := st.awayBytes
		// What the event's values take, those found away too: the fetch
		// reads them.
		spend := func() {
			b.bytes -= st.awayBytes - a
			a = st.awayBytes
		}
		if !ch.del {
			if ev.kv, err = st.putAt(ch.key, ch.rev); errors.Is(err, errAway) {
				away, b.away = true, true
				spend()
				continue
			} else if err != nil {
				return false, err
			}
			if ev.kv == nil && ch.rev <= st.compactRev {
				// A put its own Txn deleted at the compacted revision,
				// which trimming dropped: as etcd, the deletion alone
				// (eachRevEvent).
				continue
			}
		}
		// Only a put can follow a delete of its key in the revision: a
		// delete's own scan, quadratic in a large revision, is skipped.
		if x.prevKV && !ch.del && deletedIn(st.changes, j) {
			// A put after a delete of its key in the same Txn: as etcd's
			// sub-revisions have it, the value before is the tombstone.
		} else if x.prevKV {
			if ev.prev, err = st.at(ch.key, ch.rev-1); errors.Is(err, errAway) {
				away, b.away = true, true
				spend()
				continue
			} else if err != nil {
				return false, err
			}
			// At the compacted revision, the value before may be gone:
			// etcd would send the event without it, which kube-apiserver
			// rejects, so the watch is canceled as compacted instead.
			if ev.prev == nil && ch.rev <= st.compactRev && (ch.del || recVersion(ev.kv) > 1) {
				return false, errPrevCompacted
			}
		}
		evs = append(evs, ev)
		b.events--
		b.bytes -= int64(len(ev.kv) + len(ev.prev))
		if ev.del {
			// A delete's event writes its key, which a put's record has:
			// uncounted, deletes of large keys at revisions apart made one
			// message no share or reserve took (#689's re-check, D4).
			b.bytes -= int64(st.keys[ch.key].klen)
		}
	}
	if away || len(evs) > 0 && !emit(evs) {
		return false, nil
	}
	return true, nil
}

// progressIfSynced answers the progress request of watch stream s, if one
// waits, once none of its watches catches up and the store reached every
// one's start revision, as etcd's: a progress at a revision before a
// watch's start would tell a client that resumed the watch here, as
// clientv3 does on another node, that it has seen less than it has.
func (n *node) progressIfSynced(c *client, s *stream) {
	if !s.progressDue {
		return
	}
	for _, i := range s.wids {
		x := &n.w.ws[i]
		if !n.owns(c, s, i) {
			continue
		}
		if x.syncRev != 0 {
			return
		}
		if n.st.rev < x.minRev {
			if !s.progressWaits {
				s.progressWaits = true
				n.w.progressWait = append(n.w.progressWait, mkHandle(c, s.id))
			}
			return
		}
	}
	s.progressDue = false
	n.writeProgress(c, s, -1)
}

// retryProgress asks again for the progress of the streams that waited
// for a watch's start revision.
func (n *node) retryProgress() {
	waits := n.w.progressWait
	n.w.progressWait = waits[:0]
	// progressIfSynced appends at most one handle per handle read, so
	// it overwrites only handles read already.
	for _, h := range waits {
		if c, s := n.clientOf(h); c != nil {
			s.progressWaits = false
			n.progressIfSynced(c, s)
		}
	}
}

// writeProgress writes a progress notification of watch id, -1 for the
// stream's, at the current revision.
func (n *node) writeProgress(c *client, s *stream, id int64) {
	var w msgW
	w.begin(c, s, sizeBytesField(1, n.hdrSize())+sizeVarintField(2, uint64(id)))
	n.writeHdr(&w)
	writeVarintNZ(&w, 2, uint64(id))
	w.end()
	n.markDirty(&c.sock)
}

// notifyProgress sends etcd's periodic progress notification to each
// caught-up watch that asked for them and got no event since the last.
func (n *node) notifyProgress() {
	if n.progressEvery <= 0 || n.now.Before(n.progressAt) {
		return
	}
	n.progressAt = n.now.Add(n.progressEvery)
	for i := range n.w.ws {
		x := &n.w.ws[i]
		if !x.active || !x.progressNotify || x.syncRev != 0 || n.st.rev < x.minRev {
			continue
		}
		if x.sent {
			x.sent = false
			continue
		}
		if c, s := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid)); c != nil {
			n.writeProgress(c, s, x.id)
		}
	}
}

// writeEvents writes x's evs as one WatchResponse, or, if x asked for
// fragments and the response would reach --max-request-bytes, as etcd's
// sendFragments splits it: as many events a fragment as keep it under
// the limit, at least one, each but the last marked as a fragment.
func (n *node) writeEvents(c *client, s *stream, x *watcher, evs []event) {
	x.sent = true
	hdr := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
	size := hdr
	for i := range evs {
		size += sizeBytesField(11, n.eventSize(x, &evs[i]))
	}
	if !x.fragment || size < maxRequestBytes || len(evs) < 2 {
		n.writeResponse(c, s, x, evs, size, false)
		return
	}
	for i := 0; i < len(evs); {
		part, j := hdr+2, i // a fragment's flag takes 2 bytes
		for ; j < len(evs); j++ {
			add := sizeBytesField(11, n.eventSize(x, &evs[j]))
			if j > i && part+add >= maxRequestBytes {
				break
			}
			part += add
		}
		last := j == len(evs)
		if last {
			part -= 2
		}
		n.writeResponse(c, s, x, evs[i:j], part, !last)
		i = j
	}
}

// writeResponse writes one WatchResponse of x of size bytes with evs.
func (n *node) writeResponse(c *client, s *stream, x *watcher, evs []event, size int, fragment bool) {
	var mw msgW
	mw.begin(c, s, size)
	n.writeHdr(&mw)
	writeVarintNZ(&mw, 2, uint64(x.id))
	if fragment {
		mw.varintField(7, 1)
	}
	for i := range evs {
		n.writeEvent(&mw, x, &evs[i])
	}
	mw.end()
	n.markDirty(&c.sock)
}

// writeCompacted cancels watcher x, whose start revision is compacted.
// As etcd's, the header names revision 0.
func (n *node) writeCompacted(c *client, s *stream, x *watcher) {
	size := sizeBytesField(1, n.hdrSizeAt(0)) + sizeVarintField(2, uint64(x.id)) + 2 + sizeVarintField(5, uint64(n.st.compactRev))
	var mw msgW
	mw.begin(c, s, size)
	n.writeHdrAt(&mw, 0)
	writeVarintNZ(&mw, 2, uint64(x.id))
	mw.varintField(4, 1)
	writeVarintNZ(&mw, 5, uint64(n.st.compactRev))
	mw.end()
	n.markDirty(&c.sock)
}

func (n *node) eventSize(x *watcher, ev *event) int {
	b := 0
	if ev.del {
		key := n.st.key(ev.key)
		b += 2 + sizeBytesField(2, sizeBytesField(1, len(key))+sizeVarintField(3, uint64(ev.rev)))
	} else {
		b += sizeBytesField(2, len(ev.kv))
	}
	if x.withPrev() && ev.prev != nil {
		b += sizeBytesField(3, len(ev.prev))
	}
	return b
}

func mkHandleRaw(idx int, gen uint16, sid uint32) handle {
	return handle(uint64(idx)<<48 | uint64(gen)<<32 | uint64(sid))
}
