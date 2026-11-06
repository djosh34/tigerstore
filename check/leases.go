package check

import (
	"slices"
	"sort"
)

// leaseSet is a sorted set of lease IDs that never changes in place: with
// and without return a new set that shares every chunk but one with the
// old. A state, the state a try gives, and the witness's summary of every
// state share their leases for the cost of copying the list of chunks and
// one chunk. A busy history holds thousands of live leases, and copying
// them all at each grant and revoke grew the check's cost per op with
// the history's length: 374 million IDs copied for a history of 825,606
// ops, against 11 million for 191,599 (#499).
type leaseSet struct {
	// chunks are sorted, in order, each nonempty and at most leaseChunk
	// long. Neither a chunk nor the list changes once made.
	chunks [][]int64
	n      int
}

const leaseChunk = 64

// find returns the chunk id is in or would go in, its place there, and
// whether it is there.
func (l leaseSet) find(id int64) (c, i int, ok bool) {
	c = sort.Search(len(l.chunks), func(c int) bool {
		ch := l.chunks[c]
		return ch[len(ch)-1] >= id
	})
	if c == len(l.chunks) {
		return c, 0, false
	}
	i, ok = slices.BinarySearch(l.chunks[c], id)
	return c, i, ok
}

func (l leaseSet) has(id int64) bool {
	_, _, ok := l.find(id)
	return ok
}

// with returns l with id.
func (l leaseSet) with(id int64) leaseSet {
	c, i, ok := l.find(id)
	if ok {
		return l
	}
	if c == len(l.chunks) {
		if c == 0 {
			return leaseSet{chunks: [][]int64{{id}}, n: 1}
		}
		// Past every ID: the end of the last chunk.
		c--
		i = len(l.chunks[c])
	}
	old := l.chunks[c]
	ch := make([]int64, 0, len(old)+1)
	ch = append(append(append(ch, old[:i]...), id), old[i:]...)
	chunks := slices.Clone(l.chunks)
	if len(ch) > leaseChunk {
		h := len(ch) / 2
		chunks[c] = ch[:h:h]
		chunks = slices.Insert(chunks, c+1, ch[h:])
	} else {
		chunks[c] = ch
	}
	return leaseSet{chunks: chunks, n: l.n + 1}
}

// without returns l without id.
func (l leaseSet) without(id int64) leaseSet {
	c, i, ok := l.find(id)
	if !ok {
		return l
	}
	old := l.chunks[c]
	chunks := slices.Clone(l.chunks)
	if len(old) == 1 {
		chunks = slices.Delete(chunks, c, c+1)
	} else {
		ch := make([]int64, 0, len(old)-1)
		chunks[c] = append(append(ch, old[:i]...), old[i+1:]...)
	}
	return leaseSet{chunks: chunks, n: l.n - 1}
}

// each calls f with every ID of l, in order.
func (l leaseSet) each(f func(int64)) {
	for _, ch := range l.chunks {
		for _, id := range ch {
			f(id)
		}
	}
}

func (l leaseSet) equal(o leaseSet) bool {
	if l.n != o.n {
		return false
	}
	c, i := 0, 0
	for _, ch := range l.chunks {
		for _, id := range ch {
			for i == len(o.chunks[c]) {
				c, i = c+1, 0
			}
			if o.chunks[c][i] != id {
				return false
			}
			i++
		}
	}
	return true
}
