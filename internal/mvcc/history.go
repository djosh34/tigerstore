package mvcc

import (
	"cmp"
	"fmt"
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
// finds them, in revision order. A version a compaction removed leaves an
// empty slot where a later one of its revision follows, so that each
// change's slot stays at its sub-revision (Changed). Only a key changed
// twice in one write leaves such a gap, behind its kept change, so a
// checkpoint whose gaps need more empty slots than it has versions is
// malformed.
func (h *history) rebuild(s *Store, from int64) error {
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
	main, sub := int64(0), int64(0)
	empty := int64(0)
	for _, l := range all {
		if l.mod.main != main {
			main, sub = l.mod.main, 0
		}
		if empty += l.mod.sub - sub; empty > int64(len(all)) {
			return fmt.Errorf("%w: revision %d has a change at sub-revision %d after %d", ErrBadCheckpoint, main, l.mod.sub, sub)
		}
		for ; sub < l.mod.sub; sub++ {
			h.add(nil, main)
		}
		h.add(l.k, main)
		sub++
	}
	return nil
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
// the history's start and before HistoryEnd. A write logs its changes in
// the order of their sub-revisions, from 0, so a change's slot is its
// revision's first plus its sub-revision: a key that changed more than
// once in a revision has a slot for each change. A slot whose version a
// compaction removed, or drops once the checkpoint copy it waits for is
// done, is Gone, as etcd's backend no longer holds it.
func (s *Store) Changed(pos int64) Change {
	h := &s.hist
	k := h.keys[pos-h.first]
	main := h.revision(pos)
	if k == nil {
		return Change{s: s, i: -1, main: main}
	}
	sub := pos - h.starts[main-h.base]
	i := k.at(main)
	for i >= 0 && k.vers[i].mod.main == main && k.vers[i].mod.sub > sub {
		i--
	}
	if i < 0 || k.vers[i].mod != (rev{main: main, sub: sub}) {
		return Change{s: s, k: k, i: -1, main: main}
	}
	// A compaction that waits for a checkpoint copy has dropped the
	// version for every reader but the copy.
	if c := s.pendingCompact(); c > 0 && i < k.dropped(c) {
		return Change{s: s, k: k, i: -1, main: main}
	}
	return Change{s: s, k: k, i: i}
}

// Gone reports whether a compaction removed the version of the change.
func (c Change) Gone() bool { return c.i < 0 }

// HistoryStart is the position of the oldest change the history holds.
func (s *Store) HistoryStart() int64 { return s.hist.first }

// Deletes reports whether the change deletes its key.
func (c Change) Deletes() bool { return c.k.vers[c.i].ver == 0 }

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
