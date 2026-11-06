package check

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestLeaseSetMatchesSortedSlice changes a lease set at random and
// compares every set it made, the old ones too, with a sorted slice.
func TestLeaseSetMatchesSortedSlice(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	type kept struct {
		set  leaseSet
		want []int64
	}
	var all []kept
	var set leaseSet
	var want []int64
	for step := range 20_000 {
		id := rng.Int64N(3000)
		if rng.IntN(3) == 0 {
			set = set.without(id)
			if i, ok := slices.BinarySearch(want, id); ok {
				want = slices.Delete(slices.Clone(want), i, i+1)
			}
		} else {
			set = set.with(id)
			if i, ok := slices.BinarySearch(want, id); !ok {
				want = slices.Insert(slices.Clone(want), i, id)
			}
		}
		if step%97 == 0 {
			all = append(all, kept{set, want})
		}
	}
	all = append(all, kept{set, want})
	for k, c := range all {
		var got []int64
		c.set.each(func(id int64) { got = append(got, id) })
		if !slices.Equal(got, c.want) || c.set.n != len(c.want) {
			t.Fatalf("set %d holds %d IDs (n %d), want %d", k, len(got), c.set.n, len(c.want))
		}
		for _, ch := range c.set.chunks {
			if len(ch) == 0 || len(ch) > leaseChunk {
				t.Fatalf("set %d has a chunk of %d", k, len(ch))
			}
		}
		for range 50 {
			id := rng.Int64N(3100)
			_, ok := slices.BinarySearch(c.want, id)
			if c.set.has(id) != ok {
				t.Fatalf("set %d has %d: %v, want %v", k, id, c.set.has(id), ok)
			}
		}
		if k > 0 {
			prev := all[k-1]
			if c.set.equal(prev.set) != slices.Equal(c.want, prev.want) {
				t.Fatalf("sets %d and %d: equal %v, want %v", k-1, k, c.set.equal(prev.set), slices.Equal(c.want, prev.want))
			}
		}
		if !c.set.equal(c.set) {
			t.Fatalf("set %d is not equal to itself", k)
		}
	}
	// Equal sets made by different changes, so with different chunks.
	var a, b leaseSet
	for id := range int64(200) {
		a = a.with(id)
		b = b.with(199 - id)
	}
	if !a.equal(b) || !b.equal(a) {
		t.Fatal("two sets of the same 200 IDs are not equal")
	}
	if a.without(5).equal(b) || b.equal(a.without(5).with(500)) {
		t.Fatal("different sets are equal")
	}
}
