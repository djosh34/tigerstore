package wl

import (
	"bytes"
	"math"
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
	noPut    bool
	noDel    bool
	first    int32 // events of this apply, a list through event.next
	last     int32
	queued   bool
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
	if (del && x.noDel) || (!del && x.noPut) {
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
	num, _, _, p, _, err := field(msg)
	if err != nil {
		return err
	}
	switch num {
	case 1:
		x := watcher{sock: c.idx, gen: c.gen, sid: s.id, id: int64(len(s.wids))}
		var filters []byte
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
			case 6:
				x.prevKV = v != 0
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
		i := n.w.add(x)
		s.wids = append(s.wids, i)
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id)) + 2
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(x.id))
		w.varintField(3, 1)
		w.end()
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
		for _, i := range s.wids {
			if n.w.ws[i].active && n.w.ws[i].id == wid {
				n.w.remove(i)
			}
		}
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(wid)) + 2
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(wid))
		w.varintField(4, 1)
		w.end()
	case 3:
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, math.MaxUint64)
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		w.varintField(2, math.MaxUint64)
		w.end()
	}
	n.markDirty(&c.sock)
	return nil
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
	st := n.st
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
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			size += sizeBytesField(11, n.eventSize(x, &w.events[e]))
		}
		var mw msgW
		mw.begin(&c.sock, s.id, size)
		n.writeHdr(&mw)
		writeVarintNZ(&mw, 2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			ev := &w.events[e]
			mw.lenField(11, n.eventSize(x, ev))
			if ev.del {
				mw.varintField(1, 1)
				key := st.key(ev.key)
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
		mw.end()
		n.markDirty(&c.sock)
	}
	clear(w.events)
	w.events = w.events[:0]
	w.touched = w.touched[:0]
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
