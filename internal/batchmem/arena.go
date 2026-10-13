// Package batchmem owns bounded transient batch storage. It never owns Raft
// entries or database values. Callers retain each asynchronous user explicitly.
package batchmem

import (
	"errors"
	"sort"
	"sync/atomic"
)

const ChunkBytes = 4 << 20

type Mode string

const (
	Ring Mode = "ring"
	Pool Mode = "pool"
)

// Bank has a fixed number of chunks. Ring allocates all bytes at construction;
// Pool allocates a chunk on its first use and keeps it for subsequent batches.
// Both reject acquisition when all suitable chunks are pinned. Neither waits.
// There is no unbounded fallback and no sync.Pool.
type Bank struct {
	mode      Mode
	arenas    []Arena
	classes   []sizeClass
	next      atomic.Uint64
	retained  atomic.Int64
	used      atomic.Int64
	highwater atomic.Int64
	rejected  atomic.Uint64
}

type sizeClass struct{ start, end, capacity int }

// Arena has one bump-allocation owner and any number of reference holders.
// Alloc and Retain require a live reference. Only the owner calls Alloc.
// Release may run on any goroutine. After the final release, no caller may
// access the arena or any borrowed slice until a new acquisition.
type Arena struct {
	bank     *Bank
	index    int
	capacity int
	bytes    []byte
	offset   int
	refs     atomic.Int64
}

type Stats struct {
	BudgetBytes, RetainedBytes, InUseBytes, HighwaterBytes int64
	Rejected                                               uint64
}

// New reserves metadata at startup. Capacities also permit a bounded large
// response class without changing the public response-size limit.
func New(mode Mode, capacities []int) (*Bank, error) {
	if mode != Ring && mode != Pool {
		return nil, errors.New("batchmem: mode must be ring or pool")
	}
	sizes := append([]int(nil), capacities...)
	sort.Ints(sizes)
	b := &Bank{mode: mode, arenas: make([]Arena, len(sizes))}
	for i, n := range sizes {
		if len(b.classes) == 0 || b.classes[len(b.classes)-1].capacity != n {
			b.classes = append(b.classes, sizeClass{start: i, capacity: n})
		}
		b.classes[len(b.classes)-1].end = i + 1
		if n < 0 {
			return nil, errors.New("batchmem: negative capacity")
		}
		a := &b.arenas[i]
		a.bank, a.index, a.capacity = b, i, n
		if mode == Ring {
			a.bytes = make([]byte, n)
			b.retained.Add(int64(n))
		}
	}
	return b, nil
}

func (b *Bank) Acquire(minBytes int) *Arena {
	if minBytes < 0 || len(b.arenas) == 0 {
		b.rejected.Add(1)
		return nil
	}
	start := b.next.Add(1) - 1
	for _, class := range b.classes {
		if class.capacity < minBytes {
			continue
		}
		count := class.end - class.start
		for j := range count {
			a := &b.arenas[class.start+(int(start%uint64(count))+j)%count]
			if !a.refs.CompareAndSwap(0, 1) {
				continue
			}
			if a.bytes == nil && a.capacity != 0 {
				a.bytes = make([]byte, a.capacity)
				b.retained.Add(int64(a.capacity))
			}
			a.offset = 0
			used := b.used.Add(int64(a.capacity))
			for old := b.highwater.Load(); used > old; old = b.highwater.Load() {
				if b.highwater.CompareAndSwap(old, used) {
					break
				}
			}
			return a
		}
	}
	b.rejected.Add(1)
	return nil
}

func (a *Arena) Index() int { return a.index }
func (a *Arena) Alloc(n int) []byte {
	if n < 0 || n > a.capacity-a.offset {
		return nil
	}
	start := a.offset
	a.offset += n
	return a.bytes[start:a.offset:a.offset]
}
func (a *Arena) Retain() { a.refs.Add(1) }
func (a *Arena) Release() {
	// Publish availability only after all release bookkeeping is done. A CAS
	// from the last live ref to -1 excludes acquisition during this update.
	for {
		n := a.refs.Load()
		if n > 1 {
			if a.refs.CompareAndSwap(n, n-1) {
				return
			}
			continue
		}
		if n < 1 {
			return
		}
		if !a.refs.CompareAndSwap(1, -1) {
			continue
		}
		a.bank.used.Add(-int64(a.capacity))
		a.refs.Store(0)
		return
	}
}

func (b *Bank) Stats() Stats {
	var budget int64
	for i := range b.arenas {
		budget += int64(b.arenas[i].capacity)
	}
	return Stats{BudgetBytes: budget, RetainedBytes: b.retained.Load(), InUseBytes: b.used.Load(), HighwaterBytes: b.highwater.Load(), Rejected: b.rejected.Load()}
}
