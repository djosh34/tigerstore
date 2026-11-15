package core

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestWatchIndexFindsEveryWatcher checks the index against a look at
// every watcher, for random single keys, ranges and open ends that nest
// and overlap, some of the watchers dead, and keys on and between their
// bounds.
func TestWatchIndexFindsEveryWatcher(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	key := func() []byte {
		b := make([]byte, 1+rng.IntN(3))
		for i := range b {
			b[i] = 'a' + byte(rng.IntN(4))
		}
		return b
	}
	for round := range 200 {
		var ws []*watcher
		for range 1 + rng.IntN(20) {
			w := &watcher{key: key(), dead: rng.IntN(5) == 0}
			switch rng.IntN(3) {
			case 0:
			case 1:
				w.end = []byte{0}
			default:
				w.end = key()
			}
			ws = append(ws, w)
		}
		var x watchIndex
		for range 50 {
			k := key()
			var want []*watcher
			for _, w := range ws {
				if !w.dead && mvcc.InRange(k, w.key, w.end) {
					want = append(want, w)
				}
			}
			var got []*watcher
			for _, w := range x.lookup(ws, k) {
				if !w.dead {
					got = append(got, w)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("round %d, key %q: %d watchers found, want %d", round, k, len(got), len(want))
			}
		}
	}
}
