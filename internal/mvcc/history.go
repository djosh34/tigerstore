package mvcc

import (
	"cmp"
	"slices"
	"sort"

	"google.golang.org/protobuf/encoding/protowire"
)

// history is every change since the compaction revision, in revision
// order, by its key: a watch catches up from a position in it a piece at
// a time, as etcd's unsynced watchers read the backend by revision (#505).
// It takes 8 bytes a change and 8 a revision. Positions count every
// change since the store began, so a position stays valid as the history
// drops its oldest revisions.
type history struct {
	// keys[i] is the key of the change at position first+i. starts[j] is
	// the position of the first change of revision base+j.
	first  int64
	base   int64
	starts []int64
	keys   []*keyIndex
}

// add logs the change of k at revision main, the newest of the store.
func (h *history) add(k *keyIndex, main int64) {
	if len(h.starts) == 0 {
		h.base = main
	}
	for h.base+int64(len(h.starts)) <= main {
		h.starts = append(h.starts, h.first+int64(len(h.keys)))
	}
	h.keys = append(h.keys, k)
}

// end is the position after the last change logged.
func (h *history) end() int64 { return h.first + int64(len(h.keys)) }

// drop forgets the revisions before rev. The arrays shrink once less than
// half of them is in use, so that the memory goes.
func (h *history) drop(rev int64) {
	n := rev - h.base
	if n <= 0 || len(h.starts) == 0 {
		return
	}
	cut := h.end()
	if n < int64(len(h.starts)) {
		cut = h.starts[n]
	} else {
		n = int64(len(h.starts))
	}
	// The dropped slots are cleared, so they keep no removed key alive.
	clear(h.keys[:cut-h.first])
	h.keys = h.keys[cut-h.first:]
	h.starts = h.starts[n:]
	h.first, h.base = cut, rev
	if cap(h.keys) > 2*len(h.keys)+64 {
		h.keys = slices.Clone(h.keys)
	}
	if cap(h.starts) > 2*len(h.starts)+64 {
		h.starts = slices.Clone(h.starts)
	}
}

// revision returns the revision of the change at position pos.
func (h *history) revision(pos int64) int64 {
	j := sort.Search(len(h.starts), func(j int) bool { return h.starts[j] > pos })
	return h.base + int64(j) - 1
}

// rebuild logs every version of the index from revision from on, as Load
// finds them, in revision order.
func (h *history) rebuild(s *Store, from int64) {
	type logged struct {
		mod rev
		k   *keyIndex
	}
	var all []logged
	s.keys.Ascend(func(k *keyIndex) bool {
		for _, v := range k.vers {
			if v.mod.main >= from {
				all = append(all, logged{v.mod, k})
			}
		}
		return true
	})
	slices.SortFunc(all, func(a, b logged) int {
		if c := cmp.Compare(a.mod.main, b.mod.main); c != 0 {
			return c
		}
		return cmp.Compare(a.mod.sub, b.mod.sub)
	})
	*h = history{}
	for _, l := range all {
		h.add(l.k, l.mod.main)
	}
}

// HistoryFrom is the position of the first change at revision rev or
// after. rev must not be below the compaction revision.
func (s *Store) HistoryFrom(rev int64) int64 {
	h := &s.hist
	if j := rev - h.base; j >= 0 && j < int64(len(h.starts)) {
		return h.starts[j]
	}
	if rev < h.base && len(h.starts) > 0 {
		return h.first
	}
	return h.end()
}

// HistoryEnd is the position after the last change of the store's
// revision. A write applied in pieces, as a lease revoke is, logs its
// changes before the store moves to its revision; they are not readable
// until it does.
func (s *Store) HistoryEnd() int64 { return s.HistoryFrom(s.rev + 1) }

// Changed returns the change at position pos, which must be at or after
// the history's start and before HistoryEnd.
func (s *Store) Changed(pos int64) Change {
	h := &s.hist
	k := h.keys[pos-h.first]
	main := h.revision(pos)
	return Change{s: s, k: k, i: k.at(main)}
}

// HistoryStart is the position of the oldest change the history holds.
func (s *Store) HistoryStart() int64 { return s.hist.first }

// ChangesFrom is the history position of the first change of the last
// write, Changes()[0].
func (s *Store) ChangesFrom() int64 { return s.hist.end() - int64(len(s.changes)) }

// EventSize is the size of the event of the change as one element of a
// WatchResponse's events, with the previous version if withPrev is set,
// its values counted by their length, as Event builds it.
func (c Change) EventSize(withPrev bool) int {
	v := &c.k.vers[c.i]
	kv := KV{Key: c.k.key, ModRevision: v.mod.main}
	n := 0
	valueLen := 0
	if v.ver != 0 {
		kv = c.k.kvValue(v)
		valueLen = int(v.value.Len)
	} else {
		// The type field: DELETE is 1, PUT the default 0.
		n += 2
	}
	n += 1 + protowire.SizeBytes(kv.Size(valueLen))
	if p := c.prev(withPrev); p != nil {
		prev := c.k.kvValue(p)
		n += 1 + protowire.SizeBytes(prev.Size(int(p.value.Len)))
	}
	return 1 + protowire.SizeBytes(n)
}
