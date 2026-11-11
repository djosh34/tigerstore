package mvcc

import "slices"

// leaves is the ordered index of the keys as sorted arrays of items of up
// to leafMax each, in order: a sorted batch of new keys merges into it
// leaf by leaf, with a binary search inside a leaf per key and no walk
// from a root (#602, prototype).
type leaves struct {
	ls   [][]uint64
	n    int
	less func(a, b uint64) bool
}

const leafMax = 512

// Len is the number of items.
func (x *leaves) Len() int { return x.n }

// leafOf returns the index of the leaf whose range holds it: the last leaf
// whose first item is not after it, or 0.
func (x *leaves) leafOf(it uint64) int {
	lo, hi := 1, len(x.ls)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if x.less(it, x.ls[m][0]) {
			hi = m
		} else {
			lo = m + 1
		}
	}
	return lo - 1
}

// search returns the first index in l whose item is not before it.
func (x *leaves) search(l []uint64, it uint64) int {
	lo, hi := 0, len(l)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if x.less(l[m], it) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}

// InsertSorted adds items, sorted and none of them in x yet.
func (x *leaves) InsertSorted(items []uint64) {
	if len(items) == 0 {
		return
	}
	x.n += len(items)
	if len(x.ls) == 0 {
		x.ls = append(x.ls, make([]uint64, 0, leafMax))
	}
	out := make([][]uint64, 0, len(x.ls)+len(items)/leafMax+1)
	first := x.leafOf(items[0])
	out = append(out, x.ls[:first]...)
	i := 0
	for li := first; li < len(x.ls); li++ {
		l := x.ls[li]
		if i == len(items) {
			out = append(out, x.ls[li:]...)
			break
		}
		// The items before the next leaf's first one go into this leaf.
		j := i
		if li+1 < len(x.ls) {
			next := x.ls[li+1][0]
			for j < len(items) && x.less(items[j], next) {
				j++
			}
		} else {
			j = len(items)
		}
		if j == i {
			out = append(out, l)
			continue
		}
		merged := make([]uint64, 0, len(l)+j-i)
		at := 0
		for _, it := range items[i:j] {
			p := at + x.search(l[at:], it)
			merged = append(merged, l[at:p]...)
			merged = append(merged, it)
			at = p
		}
		merged = append(merged, l[at:]...)
		i = j
		for len(merged) > leafMax {
			half := leafMax / 2
			out = append(out, slices.Clip(merged[:half:half]))
			merged = merged[half:]
		}
		out = append(out, merged)
	}
	x.ls = out
}

// ReplaceOrInsert adds it, which is not in x yet.
func (x *leaves) ReplaceOrInsert(it uint64) { x.InsertSorted([]uint64{it}) }

// build makes x hold items, which are sorted, alone.
func (x *leaves) build(items []uint64) {
	x.ls, x.n = nil, len(items)
	for len(items) > 0 {
		k := min(len(items), leafMax*3/4)
		x.ls = append(x.ls, slices.Clone(items[:k]))
		items = items[k:]
	}
}

// Delete removes it, if x holds it.
func (x *leaves) Delete(it uint64) {
	if len(x.ls) == 0 {
		return
	}
	li := x.leafOf(it)
	l := x.ls[li]
	p := x.search(l, it)
	if p == len(l) || x.less(it, l[p]) {
		return
	}
	l = slices.Delete(l, p, p+1)
	x.n--
	if len(l) == 0 {
		x.ls = slices.Delete(x.ls, li, li+1)
		return
	}
	x.ls[li] = l
}

// AscendFrom calls fn for each item from the first not before from on, and
// before stop if stop is set, until fn returns false.
func (x *leaves) ascendFrom(from uint64, stop *uint64, fn func(uint64) bool) {
	if len(x.ls) == 0 {
		return
	}
	li := x.leafOf(from)
	p := x.search(x.ls[li], from)
	for ; li < len(x.ls); li, p = li+1, 0 {
		for _, it := range x.ls[li][p:] {
			if stop != nil && !x.less(it, *stop) {
				return
			}
			if !fn(it) {
				return
			}
		}
	}
}

// Ascend calls fn for each item in order until fn returns false.
func (x *leaves) Ascend(fn func(uint64) bool) {
	for _, l := range x.ls {
		for _, it := range l {
			if !fn(it) {
				return
			}
		}
	}
}

// AscendGreaterOrEqual calls fn for each item not before pivot.
func (x *leaves) AscendGreaterOrEqual(pivot uint64, fn func(uint64) bool) {
	x.ascendFrom(pivot, nil, fn)
}

// AscendRange calls fn for each item in [from, stop).
func (x *leaves) AscendRange(from, stop uint64, fn func(uint64) bool) {
	x.ascendFrom(from, &stop, fn)
}
