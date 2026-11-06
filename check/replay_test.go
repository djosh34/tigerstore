package check

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// TestFirstAboveMatchesSearch checks the galloping search against a binary
// search of the whole list, from every start, over lists with repeats.
func TestFirstAboveMatchesSearch(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 300 {
		revs := make([]int64, rng.IntN(40))
		r := int64(0)
		for i := range revs {
			r += int64(rng.IntN(3))
			revs[i] = r
		}
		for rev := int64(-1); rev <= r+1; rev++ {
			want := sort.Search(len(revs), func(j int) bool { return revs[j] > rev })
			for from := 0; from <= len(revs); from++ {
				if got := firstAbove(revs, rev, from); got != want {
					t.Fatalf("firstAbove(%v, %d, %d) = %d, want %d", revs, rev, from, got, want)
				}
			}
		}
	}
}
