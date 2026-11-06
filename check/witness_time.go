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
// (#499). It keeps its scratch records from one sort to the next.
type timeSorter struct {
	a, b []timedOp
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
		t.a, t.b = make([]timedOp, len(ops)), make([]timedOp, len(ops))
	}
	a, b := t.a[:len(ops)], t.b[:len(ops)]
	var differ uint64
	for k, i := range ops {
		// The sign bit flipped orders signed times as unsigned ones.
		a[k] = timedOp{uint64(at(i)) ^ 1<<63, i}
		differ |= a[k].key ^ a[0].key
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
	for k, v := range a {
		ops[k] = v.op
	}
	// Ops of one time, by number.
	for lo := 0; lo < len(a); {
		hi := lo + 1
		for hi < len(a) && a[hi].key == a[lo].key {
			hi++
		}
		slices.Sort(ops[lo:hi])
		lo = hi
	}
}
