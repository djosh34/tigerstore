package gosimruntime

import (
	"container/heap"
)

// timers implements heap.Interface
type timers []*Timer

//go:norace
func (h timers) Len() int { return len(h) }

//go:norace
func (h timers) Less(i, j int) bool { return h[i].when < h[j].when }

//go:norace
func (h timers) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].pos = i
	h[j].pos = j
}

//go:norace
func (h *timers) Push(x any) {
	t := x.(*Timer)
	if t.pos != -1 {
		panic(t.pos)
	}
	t.pos = len(*h)
	*h = append(*h, x.(*Timer))
}

//go:norace
func (h *timers) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	x.pos = -1
	return x
}

type timerHeap struct {
	timers timers
}

//go:norace
func newTimerHeap() *timerHeap {
	return &timerHeap{}
}

//go:norace
func (h *timerHeap) add(t *Timer) {
	heap.Push(&h.timers, t)
}

//go:norace
func (h *timerHeap) adjust(t *Timer, when int64) {
	if t.pos == -1 || h.timers[t.pos] != t {
		panic(t)
	}
	t.when = when
	heap.Fix(&h.timers, t.pos)
}

//go:norace
func (h *timerHeap) remove(t *Timer) {
	if t.pos == -1 || h.timers[t.pos] != t {
		panic(t)
	}
	heap.Remove(&h.timers, t.pos)
}

//go:norace
func (h *timerHeap) len() int {
	return len(h.timers)
}

//go:norace
func (h *timerHeap) pop() *Timer {
	return heap.Pop(&h.timers).(*Timer)
}

//go:norace
func (h *timerHeap) peek() *Timer {
	return h.timers[0]
}

//go:norace
func (h *timerHeap) removemachine(m *Machine) {
	// XXX: don't use a helper because it upsets the race detector
	i, j := 0, 0
	changed := false
	for i < len(h.timers) {
		if h.timers[i].Machine == m {
			changed = true
			i++
			continue
		}
		if changed {
			h.timers[j] = h.timers[i]
			h.timers[j].pos = j
		}
		i++
		j++
	}
	i = j
	for i < len(h.timers) {
		h.timers[i] = nil
		i++
	}
	h.timers = h.timers[:j]
	heap.Init(&h.timers)
}
