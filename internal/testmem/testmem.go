// Package testmem measures the peak heap of a test while something runs,
// for the tests that hold a consumer back and bound what the node builds
// meanwhile (#505).
package testmem

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"time"
)

// heapObjects is the runtime metric of the bytes in live and dead heap
// objects not yet swept, and heapAllocs that of the bytes ever allocated.
const (
	heapObjects = "/memory/classes/heap/objects:bytes"
	heapAllocs  = "/gc/heap/allocs:bytes"
)

// step is how far the heap may grow past the most found live before Peak
// collects the garbage to tell what is held.
const step = 1 << 20

// Peak watches the heap from its call until the returned stop, which
// returns the most the heap held live above the heap live at the call,
// and the heap live at the stop above it: what the code built and kept,
// such as new state.
//
// Garbage that a collection can free does not count. Peak samples the
// heap every millisecond, and whenever it is more than step past the most
// found live so far, it runs a collection and takes the heap left as
// live. Counting garbage made the peak depend on the collector's timing:
// with three nodes' 186 MiB live, the garbage of a second's cluster
// traffic counted for up to 7 MiB on a loaded runner, where an answer
// held 1.5 MiB (#561).
//
// What it misses: a live heap that rises and falls back within step,
// between two samples, or within one collection, and objects allocated
// and held while its own collection runs, until the next. Both results
// are net: what was live at the call and is freed meanwhile lowers them.
// The collector runs at GOGC 1 meanwhile, so that collections come often
// anyway.
func Peak() (stop func() (peak, kept int64)) {
	old := debug.SetGCPercent(1)
	base := liveHeap()
	held := int64(0)
	done := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if objects, _ := heap(); objects-base > held+step {
					held = max(held, liveHeap()-base)
				}
			}
		}
	}()
	return func() (int64, int64) {
		close(done)
		<-ended
		kept := liveHeap() - base
		debug.SetGCPercent(old)
		return max(held, kept, 0), kept
	}
}

// heap returns the bytes of live and dead heap objects not yet swept, and
// the bytes ever allocated.
func heap() (objects, allocs int64) {
	sample := []metrics.Sample{{Name: heapObjects}, {Name: heapAllocs}}
	metrics.Read(sample)
	return int64(sample[0].Value.Uint64()), int64(sample[1].Value.Uint64())
}

// liveHeap collects the garbage and returns the heap that was live when
// the collection began. A collection keeps every object allocated while
// it runs, garbage or not, so the bytes allocated from before it until
// the heap is read do not count: with the cluster's traffic going on, they
// counted for megabytes (#623).
func liveHeap() int64 {
	_, before := heap()
	runtime.GC()
	objects, after := heap()
	return objects - (after - before)
}
