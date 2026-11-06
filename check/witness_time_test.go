package check

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestTimeSorterMatchesSortFunc compares timeSorter with slices.SortFunc
// by time and number, over sizes on both sides of minRadix, times with
// ties, negative ones and the extremes of int64, reusing one sorter.
func TestTimeSorterMatchesSortFunc(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 499))
	var sorter timeSorter
	for round := range 400 {
		n := rng.IntN(300)
		times := make([]int64, n)
		for i := range times {
			switch rng.IntN(5) {
			case 0:
				times[i] = math.MinInt64 + rng.Int64N(3)
			case 1:
				times[i] = math.MaxInt64 - rng.Int64N(3)
			case 2:
				times[i] = rng.Int64N(8) - 4
			default:
				times[i] = rng.Int64() - math.MaxInt64/2
			}
		}
		at := func(i int) int64 { return times[i] }
		ops := rng.Perm(n)
		want := slices.Clone(ops)
		slices.SortFunc(want, func(a, b int) int { return cmp.Or(cmp.Compare(at(a), at(b)), cmp.Compare(a, b)) })
		sorter.sort(ops, at)
		if !slices.Equal(ops, want) {
			t.Fatalf("round %d, %d ops: the radix sort gives %v, SortFunc %v", round, n, ops, want)
		}
	}
}

// TestPlaceKeepsLargeStateNumbers checks that a place keeps its state
// number, past what 4 bytes hold, and its flags, and orders as its parts
// do (review of #527).
func TestPlaceKeepsLargeStateNumbers(t *testing.T) {
	if (place{}).set() {
		t.Fatal("the zero place is set")
	}
	for _, at := range []int{0, 1, 1<<31 - 1, 1 << 31, 1<<31 + 5, 1 << 40, 1<<62 - 1} {
		for _, seen := range []bool{false, true} {
			p := placeAt(at, seen, 7)
			if !p.set() || p.seen() != seen || p.at() != at || p.rank != 7 {
				t.Fatalf("placeAt(%d, %v, 7) gives set %v, seen %v, at %d, rank %d", at, seen, p.set(), p.seen(), p.at(), p.rank)
			}
			if at > 0 && placeAt(at-1, true, 9).compare(p) >= 0 {
				t.Fatalf("a place at %d does not come before one at %d", at-1, at)
			}
		}
	}
}
