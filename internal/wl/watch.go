package wl

import (
	"bytes"
	"cmp"
	"errors"
	"math"
	"slices"
)

// Watches. A watcher is matched to a write through the directory of its
// key: each directory caches the watchers whose prefix covers it, so an
// event costs a load of that list. Events of one apply are kept as
// references to the store's records, which stay valid until the apply
// ends, and each watcher gets one WatchResponse with all of them,
// written straight into its stream's records.

type watcher struct {
	active   bool
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
	for _, i := range e.wfull {
		w.push(i, id, del, rev, kv, prev)
	}
	if e.wpart || len(w.other) > 0 {
		key := st.key(id)
		if e.wpart {
			for i := range w.ws {
				x := &w.ws[i]
				if x.active && x.prefix && !bytes.HasPrefix(st.tree.path(st.keys[id].dir), x.key) && bytes.HasPrefix(key, x.key) {
					w.push(int32(i), id, del, rev, kv, prev)
				}
			}
		}
		for _, i := range w.other {
			if w.ws[i].covers(key) {
				w.push(i, id, del, rev, kv, prev)
			}
		}
	}
}

func (w *watches) push(i int32, id uint32, del bool, rev int64, kv, prev []byte) {
	x := &w.ws[i]
	if (del && x.noDel) || (!del && x.noPut) || x.syncRev != 0 || rev < x.minRev {
		return
	}
	ev := int32(len(w.events))
	w.events = append(w.events, event{next: -1, del: del, key: id, rev: rev, kv: kv, prev: prev})
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
			for n.hasWatch(s, s.nextWatch) {
				s.nextWatch++
			}
			x.id = s.nextWatch
			s.nextWatch++
		case n.hasWatch(s, x.id):
			n.writeRefused(c, s, "mvcc: duplicate watch ID provided on the WatchStream")
			n.markDirty(&c.sock)
			return nil
		}
		i := n.w.add(x)
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
		found := false
		for _, i := range s.wids {
			if n.w.ws[i].active && n.w.ws[i].id == wid {
				n.w.remove(i)
				found = true
			}
		}
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
	case 3:
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, math.MaxUint64)
		w.begin(c, s, size)
		n.writeHdr(&w)
		w.varintField(2, math.MaxUint64)
		w.end()
	}
	n.markDirty(&c.sock)
	return nil
}

// hasWatch reports whether watch stream s has a watch of ID id.
func (n *node) hasWatch(s *stream, id int64) bool {
	for _, i := range s.wids {
		if x := &n.w.ws[i]; x.active && x.id == id {
			return true
		}
	}
	return false
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
		x.first, x.last = -1, -1
		if !x.active {
			continue
		}
		c, s := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid))
		if c == nil {
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
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			size += sizeBytesField(11, n.eventSize(x, &w.events[e]))
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
	if x.prevKV && ev.prev != nil {
		mw.bytesField(3, ev.prev)
	}
}

// A watch from a past revision catches up from the change index, a
// bounded amount each turn, as etcd's unsynced watchers do: a watch of
// a long history never holds the loop up. It gets no live events until it
// reaches the head; that happens between applies, so none is lost or sent
// twice.
const (
	syncChanges = 1 << 14 // changes the catch-up looks at in a turn, all watchers together
	syncEvents  = 256     // events it sends in a turn, all watchers together
)

// syncWatchers moves the watchers that catch up on, for one turn.
func (n *node) syncWatchers() error {
	w := &n.w
	if len(w.unsynced) == 0 {
		return nil
	}
	budget := syncBudget{changes: syncChanges, events: syncEvents}
	k := 0
	for _, i := range w.unsynced {
		x := &w.ws[i]
		if !x.active || x.syncRev == 0 {
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
		if budget.left() {
			done, err := n.catchUp(x, &budget, func(evs []event) { n.writeEvents(c, s, x, evs) })
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
				continue
			}
		}
		w.unsynced[k] = i
		k++
	}
	w.unsynced = w.unsynced[:k]
	return nil
}

type syncBudget struct{ changes, events int }

func (b *syncBudget) left() bool { return b.changes > 0 && b.events > 0 }

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
// It reports whether x reached the head.
func (n *node) catchUp(x *watcher, b *syncBudget, emit func([]event)) (bool, error) {
	st := n.st
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
			x.syncRev = ch.rev
			if len(evs) > 0 {
				emit(evs)
			}
			return false, nil
		}
		b.changes--
		last = ch.rev
		if (ch.del && x.noDel) || (!ch.del && x.noPut) || !x.covers(st.key(ch.key)) {
			continue
		}
		ev := event{del: ch.del, key: ch.key, rev: ch.rev}
		var err error
		if !ch.del {
			if ev.kv, err = st.putAt(ch.key, ch.rev); err != nil {
				return false, err
			}
		}
		if x.prevKV && deletedIn(st.changes, j) {
			// A put after a delete of its key in the same Txn: as etcd's
			// sub-revisions have it, the value before is the tombstone.
		} else if x.prevKV {
			if ev.prev, err = st.at(ch.key, ch.rev-1); err != nil {
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
	}
	if len(evs) > 0 {
		emit(evs)
	}
	return true, nil
}

// writeEvents writes x's evs as one WatchResponse, or, if x asked for
// fragments and the response would reach --max-request-bytes, as etcd's
// sendFragments splits it: as many events a fragment as keep it under
// the limit, at least one, each but the last marked as a fragment.
func (n *node) writeEvents(c *client, s *stream, x *watcher, evs []event) {
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
	if x.prevKV && ev.prev != nil {
		b += sizeBytesField(3, len(ev.prev))
	}
	return b
}

func mkHandleRaw(idx int, gen uint16, sid uint32) handle {
	return handle(uint64(idx)<<48 | uint64(gen)<<32 | uint64(sid))
}
