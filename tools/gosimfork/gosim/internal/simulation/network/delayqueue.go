package network

import (
	"container/heap"
	"sync"
	"time"
)

type timeIndex struct {
	// XXX: can we pack these together somehow?
	time  int64
	index int
}

func (ti timeIndex) Less(other timeIndex) bool {
	if ti.time != other.time {
		return ti.time < other.time
	}
	return ti.index < other.index
}

type timedHeap []*Packet

func (h timedHeap) Len() int           { return len(h) }
func (h timedHeap) Less(i, j int) bool { return h[i].ArrivalTime.Less(h[j].ArrivalTime) }
func (h timedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *timedHeap) Push(x any) {
	panic("don't use")
	// *h = append(*h, x.(timedPacket))
}

func (h *timedHeap) Pop() any {
	panic("don't use")
	// old := *h
	// n := len(old)
	// x := old[n-1]
	// *h = old[0 : n-1]
	// return x
}

type delayQueue struct {
	mu    sync.Mutex
	items timedHeap

	timer *time.Timer
	cond  *sync.Cond
}

func newDelayQueue() *delayQueue {
	q := &delayQueue{
		items: make(timedHeap, 0, 16),
	}
	q.cond = sync.NewCond(&q.mu)
	q.timer = time.AfterFunc(time.Hour, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.cond.Broadcast()
	})
	q.timer.Stop()
	return q
}

func (q *delayQueue) updateTimerLocked() {
	if len(q.items) == 0 {
		q.timer.Stop()
		return
	}
	delay := time.Until(time.Unix(0, q.items[0].ArrivalTime.time))
	if delay > 0 {
		q.timer.Reset(delay)
	} else {
		q.timer.Stop()
		q.cond.Broadcast()
	}
}

func (q *delayQueue) enqueue(item *Packet) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// xxx: skip an allocation
	// heap.Push(&q.items, item)
	n := len(q.items)
	q.items = append(q.items, item)
	heap.Fix(&q.items, n)
	q.updateTimerLocked()
}

func (q *delayQueue) dequeue() *Packet {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		now := time.Now().UnixNano()
		if len(q.items) > 0 && q.items[0].ArrivalTime.time <= now { //.After(time.Now()) {
			// xxx: skip an allocation
			// next := heap.Pop(&q.items).(timedPacket)

			next := q.items[0]
			n := len(q.items) - 1
			q.items[0] = q.items[n]
			q.items = q.items[:n]
			heap.Fix(&q.items, 0)

			q.updateTimerLocked()
			return next
		}
		q.cond.Wait()
	}
}
