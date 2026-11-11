package mvcc

import (
	"fmt"
	"math/rand/v2"
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
