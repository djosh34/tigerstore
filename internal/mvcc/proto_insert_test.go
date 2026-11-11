package mvcc

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// BenchmarkInsertNewKeys creates new keys in a store of a million keys,
// with deltas of two sizes (#602).
func BenchmarkInsertNewKeys(b *testing.B) {
	for _, dm := range []int{2048, 32768} {
		b.Run(fmt.Sprintf("delta=%d", dm), func(b *testing.B) {
			old := deltaMax
			deltaMax = dm
			defer func() { deltaMax = old }()
			r := rand.New(rand.NewPCG(1, 2))
			key := func() []byte {
				return fmt.Appendf(nil, "/registry/events/ns-%d/pod-%016x.%016x", r.IntN(300), r.Uint64(), r.Uint64())
			}
			s := New(&Heap{})
			for range 1_000_000 {
				s.insert(key(), nil)
			}
			s.mergeDelta()
			keys := make([][]byte, b.N)
			for i := range keys {
				keys[i] = key()
			}
			b.ResetTimer()
			for i := range b.N {
				s.insert(keys[i], nil)
			}
			b.StopTimer()
			s.mergeDelta()
		})
	}
}

// TestLeavesOrder checks that batches large and small, and deletes, keep
// the leaves in order (#602).
func TestLeavesOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	x := &leaves{less: func(a, b uint64) bool { return a < b }}
	want := map[uint64]bool{}
	for round := range 400 {
		n := 1 + r.IntN(3000)
		if round%3 == 0 {
			n = 1 + r.IntN(4)
		}
		var batch []uint64
		for range n {
			it := r.Uint64() >> 8
			if !want[it] {
				want[it] = true
				batch = append(batch, it)
			}
		}
		slices.Sort(batch)
		batch = slices.Compact(batch)
		x.InsertSorted(batch)
		for it := range want {
			if r.IntN(20) == 0 {
				x.Delete(it)
				delete(want, it)
			}
		}
	}
	var got []uint64
	x.Ascend(func(it uint64) bool { got = append(got, it); return true })
	if len(got) != len(want) || x.Len() != len(want) || !slices.IsSorted(got) {
		t.Fatalf("%d items, Len %d, want %d, sorted %v", len(got), x.Len(), len(want), slices.IsSorted(got))
	}
	for _, l := range x.ls {
		if len(l) == 0 || len(l) > leafMax {
			t.Fatalf("a leaf of %d items", len(l))
		}
	}
}
