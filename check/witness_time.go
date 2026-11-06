package check

import (
	"cmp"
	"slices"
)

type timedOp struct {
	stamp uint64
	op    int
}

// sortOpsByTime sorts by timestamp then op number. Scratch records hold
// timestamps next to operation numbers, so sorting never repeatedly
// dereferences the parsed history. Scratch storage is reused.
func (w *witness) sortOpsByTime(ops []int, at func(int) int64) {
	if len(ops) < 64 || w.slow {
		slices.SortFunc(ops, func(a, b int) int { return cmp.Or(cmp.Compare(at(a), at(b)), cmp.Compare(a, b)) })
		return
	}
	if cap(w.sortA) < len(ops) {
		w.sortA = make([]timedOp, len(ops))
		w.sortB = make([]timedOp, len(ops))
	}
	a, b := w.sortA[:len(ops)], w.sortB[:len(ops)]
	var different uint64
	for k, i := range ops {
		a[k] = timedOp{uint64(at(i)) ^ (1 << 63), i}
		different |= a[k].stamp ^ a[0].stamp
	}
	for shift := uint(0); shift < 64; shift += 8 {
		if byte(different>>shift) == 0 {
			continue
		}
		var counts [256]int
		for _, v := range a {
			counts[byte(v.stamp>>shift)]++
		}
		n := 0
		for k, c := range counts {
			counts[k], n = n, n+c
		}
		for _, v := range a {
			k := byte(v.stamp >> shift)
			b[counts[k]] = v
			counts[k]++
		}
		a, b = b, a
	}
	for k, v := range a {
		ops[k] = v.op
	}
	for lo := 0; lo < len(a); {
		hi := lo + 1
		for hi < len(a) && a[hi].stamp == a[lo].stamp {
			hi++
		}
		slices.Sort(ops[lo:hi])
		lo = hi
	}
}
