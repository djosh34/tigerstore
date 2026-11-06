package check

import (
	"cmp"
	"slices"
)

// timeSorter sorts ops by a time and then by their number, as
// slices.SortFunc with cmp.Or(cmp.Compare(at(a), at(b)), cmp.Compare(a,
// b)) does, by radix over records that hold each op's time beside it, so
// that the sort does not reach into the history at every comparison: on a
// busy history the comparisons of those sorts grew with its length
// (#499). It sorts only the ops that do not already stand in order. It
// keeps its scratch records from one sort to the next.
type timeSorter struct {
	a, b, c []timedOp
}

type timedOp struct {
	key uint64
	op  int
}

// minRadix is the fewest ops a radix sort pays for.
const minRadix = 64

func (t *timeSorter) sort(ops []int, at func(int) int64) {
	if len(ops) < minRadix {
		slices.SortFunc(ops, func(a, b int) int { return cmp.Or(cmp.Compare(at(a), at(b)), cmp.Compare(a, b)) })
		return
	}
	if cap(t.a) < len(ops) {
		t.a, t.b, t.c = make([]timedOp, len(ops)), make([]timedOp, len(ops)), make([]timedOp, len(ops))
	}
	a, b := t.a[:len(ops)], t.b[:len(ops)]
	// A history's ops come nearly in the order of their returns, so most
	// of them already stand in order: those stay in a, in place, and only
	// the rest, in b, are sorted, then merged with them. The radix passes
	// scatter over arrays as long as the history, which cost more per op
	// the longer it is (#499).
	kept, rest := 0, 0
	for _, i := range ops {
		v := timedOp{uint64(at(i)) ^ 1<<63, i}
		if kept == 0 || !v.less(a[kept-1]) {
			a[kept] = v
			kept++
		} else {
			b[rest] = v
			rest++
		}
	}
	sorted := radix(b[:rest], t.c[:rest])
	// Merge, by time and then number.
	k := 0
	for x, y := a[:kept], sorted; len(x) > 0 || len(y) > 0; k++ {
		if len(y) == 0 || len(x) > 0 && !y[0].less(x[0]) {
			ops[k], x = x[0].op, x[1:]
		} else {
			ops[k], y = y[0].op, y[1:]
		}
	}
}

func (v timedOp) less(w timedOp) bool { return v.key < w.key || v.key == w.key && v.op < w.op }

// radix sorts a by key and then op, using b, of a's length, as scratch,
// and returns whichever of the two holds the result.
func radix(a, b []timedOp) []timedOp {
	if len(a) < 2 {
		return a
	}
	var differ uint64
	for _, v := range a {
		differ |= v.key ^ a[0].key
	}
	for shift := uint(0); shift < 64; shift += 8 {
		if byte(differ>>shift) == 0 {
			continue
		}
		var counts [256]int
		for _, v := range a {
			counts[byte(v.key>>shift)]++
		}
		n := 0
		for k, c := range counts {
			counts[k], n = n, n+c
		}
		for _, v := range a {
			k := byte(v.key >> shift)
			b[counts[k]] = v
			counts[k]++
		}
		a, b = b, a
	}
	// Ops of one time, by number.
	for lo := 0; lo < len(a); {
		hi := lo + 1
		for hi < len(a) && a[hi].key == a[lo].key {
			hi++
		}
		slices.SortFunc(a[lo:hi], func(v, w timedOp) int { return cmp.Compare(v.op, w.op) })
		lo = hi
	}
	return a
}
