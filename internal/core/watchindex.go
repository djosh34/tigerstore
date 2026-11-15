package core

import (
	"bytes"
	"slices"
	"sort"
)

// watchIndex finds the watchers whose range holds a key, so that each
// change of a write looks at those only: matchWatches looked at every
// watcher for each change, and with Kubernetes' 90 or so prefixes that
// took 1.5% of the leader's CPU on crd-heavy (#602).
//
// It holds the watchers by their range's start, and for each the furthest
// end of the ranges up to it: a lookup takes the watchers that start at
// or before the key, from the last back, as long as one of them may still
// reach it. Prefixes do not nest much, so that is a few. It is made again
// after the list of watchers changed.
type watchIndex struct {
	valid   bool
	entries []watchEntry
	// pos and out are the lookup's answer, by position in Node.watchers.
	pos []int
	out []*watcher
}

// watchEntry is a watcher at position pos of Node.watchers, with its
// range's end as reach takes it, and reach the furthest of the entries up
// to it; inf is an end past every key.
type watchEntry struct {
	key, end []byte
	inf      bool
	reach    []byte
	reachInf bool
	pos      int
}

// invalidate makes the next lookup build the index again.
func (x *watchIndex) invalidate() { x.valid = false }

// build makes the index over watchers.
func (x *watchIndex) build(watchers []*watcher) {
	x.entries = x.entries[:0]
	for i, w := range watchers {
		if w.dead {
			continue
		}
		e := watchEntry{key: w.key, pos: i}
		switch {
		case len(w.end) == 0:
			e.end = append(slices.Clip(w.key), 0)
		case len(w.end) == 1 && w.end[0] == 0:
			e.inf = true
		default:
			e.end = w.end
		}
		x.entries = append(x.entries, e)
	}
	sort.SliceStable(x.entries, func(i, j int) bool { return bytes.Compare(x.entries[i].key, x.entries[j].key) < 0 })
	for i := range x.entries {
		e := &x.entries[i]
		e.reach, e.reachInf = e.end, e.inf
		if i > 0 {
			p := &x.entries[i-1]
			if p.reachInf || !e.reachInf && bytes.Compare(p.reach, e.reach) > 0 {
				e.reach, e.reachInf = p.reach, p.reachInf
			}
		}
	}
	x.valid = true
}

// lookup returns the watchers whose range holds key, in their order in
// watchers, the dead ones included. The slice is the index's own until
// the next lookup.
func (x *watchIndex) lookup(watchers []*watcher, key []byte) []*watcher {
	if !x.valid {
		x.build(watchers)
	}
	x.pos = x.pos[:0]
	j := sort.Search(len(x.entries), func(i int) bool { return bytes.Compare(x.entries[i].key, key) > 0 })
	for i := j - 1; i >= 0; i-- {
		e := &x.entries[i]
		if !e.reachInf && bytes.Compare(e.reach, key) <= 0 {
			break
		}
		if e.inf || bytes.Compare(key, e.end) < 0 {
			x.pos = append(x.pos, e.pos)
		}
	}
	slices.Sort(x.pos)
	x.out = x.out[:0]
	for _, p := range x.pos {
		x.out = append(x.out, watchers[p])
	}
	return x.out
}
