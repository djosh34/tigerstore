package check

import (
	"slices"
	"sort"
)

// leaseSet is a sorted set of lease IDs that never changes in place: with
// and without return a new set that shares all but a few small slices
// with the old. A state, the state a try gives, and the witness's summary
// of every state share their leases. A busy history holds thousands of
// live leases, and copying them all at each grant and revoke grew the
// check's cost per op with the history's length: 374 million IDs copied
// for a history of 825,606 ops, against 11 million for 191,599 (#499).
//
// The IDs are in chunks of at most leaseChunk, and the chunks in pages of
// at most leaseChunk, so a change copies the list of pages, one page and
// one chunk: a single list of chunks still grew with the live leases, as
// their number grew over a busy run (#499).
type leaseSet struct {
	// pages are in order, each nonempty, of nonempty chunks in order,
	// each sorted. Neither a chunk, a page nor the list changes once
	// made.
	pages [][][]int64
	n     int
}

const leaseChunk = 64

// last is the last ID of page.
func last(page [][]int64) int64 {
	ch := page[len(page)-1]
	return ch[len(ch)-1]
}

// find returns the page and chunk id is in or would go in, its place
// there, and whether it is there. A page or chunk past the last means id
// is above every ID.
func (l leaseSet) find(id int64) (p, c, i int, ok bool) {
	p = sort.Search(len(l.pages), func(p int) bool { return last(l.pages[p]) >= id })
	if p == len(l.pages) {
		return p, 0, 0, false
	}
	page := l.pages[p]
	c = sort.Search(len(page), func(c int) bool {
		ch := page[c]
		return ch[len(ch)-1] >= id
	})
	i, ok = slices.BinarySearch(page[c], id)
	return p, c, i, ok
}

func (l leaseSet) has(id int64) bool {
	_, _, _, ok := l.find(id)
	return ok
}

// with returns l with id.
func (l leaseSet) with(id int64) leaseSet {
	p, c, i, ok := l.find(id)
	if ok {
		return l
	}
	if len(l.pages) == 0 {
		return leaseSet{pages: [][][]int64{{{id}}}, n: 1}
	}
	if p == len(l.pages) {
		// Past every ID: the end of the last chunk of the last page.
		p = len(l.pages) - 1
		c = len(l.pages[p]) - 1
		i = len(l.pages[p][c])
	}
	old := l.pages[p][c]
	ch := make([]int64, 0, len(old)+1)
	ch = append(append(append(ch, old[:i]...), id), old[i:]...)
	page := slices.Clone(l.pages[p])
	page[c] = ch
	if len(ch) > leaseChunk {
		h := len(ch) / 2
		page[c] = ch[:h:h]
		page = slices.Insert(page, c+1, ch[h:])
	}
	pages := slices.Clone(l.pages)
	pages[p] = page
	if len(page) > leaseChunk {
		h := len(page) / 2
		pages[p] = page[:h:h]
		pages = slices.Insert(pages, p+1, page[h:])
	}
	return leaseSet{pages: pages, n: l.n + 1}
}

// without returns l without id.
func (l leaseSet) without(id int64) leaseSet {
	p, c, i, ok := l.find(id)
	if !ok {
		return l
	}
	page := slices.Clone(l.pages[p])
	if old := page[c]; len(old) == 1 {
		page = slices.Delete(page, c, c+1)
	} else {
		ch := make([]int64, 0, len(old)-1)
		page[c] = append(append(ch, old[:i]...), old[i+1:]...)
	}
	pages := slices.Clone(l.pages)
	if len(page) == 0 {
		pages = slices.Delete(pages, p, p+1)
	} else {
		pages[p] = page
	}
	return leaseSet{pages: pages, n: l.n - 1}
}

// each calls f with every ID of l, in order.
func (l leaseSet) each(f func(int64)) {
	for _, page := range l.pages {
		for _, ch := range page {
			for _, id := range ch {
				f(id)
			}
		}
	}
}

func (l leaseSet) equal(o leaseSet) bool {
	if l.n != o.n {
		return false
	}
	// A cursor into o, page p, chunk c, place i.
	p, c, i := 0, 0, 0
	for _, page := range l.pages {
		for _, ch := range page {
			for _, id := range ch {
				for i == len(o.pages[p][c]) {
					i, c = 0, c+1
					if c == len(o.pages[p]) {
						c, p = 0, p+1
					}
				}
				if o.pages[p][c][i] != id {
					return false
				}
				i++
			}
		}
	}
	return true
}
