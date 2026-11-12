package wl

import "bytes"

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
