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
// objects not yet swept, heapAllocs that of the bytes ever allocated,
// gcCycles that of the collections ended, and gcStops that of the times
// a collection stopped the world, which the start of its mark is one of.
const (
	heapObjects = "/memory/classes/heap/objects:bytes"
	heapAllocs  = "/gc/heap/allocs:bytes"
	gcCycles    = "/gc/cycles/total:gc-cycles"
	gcStops     = "/sched/pauses/stopping/gc:seconds"
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
// anyway, and only when Peak asks while Peak collects: a caller that sets
// the GC percent meanwhile may find Peak's setting instead of its own.
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
				if objects, _, _ := heap(); objects-base > held+step {
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

// heap returns the bytes of live and dead heap objects not yet swept, the
// bytes ever allocated, and the collections ended.
func heap() (objects, allocs int64, cycles uint64) {
	sample := []metrics.Sample{{Name: heapObjects}, {Name: heapAllocs}, {Name: gcCycles}}
	metrics.Read(sample)
	return int64(sample[0].Value.Uint64()), int64(sample[1].Value.Uint64()), sample[2].Value.Uint64()
}

// liveHeap runs a collection and returns the heap that was live when its
// mark began. The collection keeps every object allocated from then on,
// garbage or not, so the bytes allocated from the mark's start until the
// heap is read do not count: with the cluster's traffic going on, they
// counted for megabytes (#623).
//
// Bytes allocated before the mark began must count, or what of them the
// collection frees would be taken off twice, and what is held would read
// as less (#640's review). So a goroutine waits for the collection to
// stop the world to start its mark, and reads the bytes allocated as the
// world goes on. What is allocated before that read still counts: the
// error is that much too high, never too low, and at most what the heap
// left reads, which Peak took before #623.
//
// No automatic collection starts meanwhile, and one that runs ends its
// mark first (debug.SetGCPercent). A measurement is taken again if its
// goroutine read only after the mark ended, or another collection crossed
// it, as one a test started; after a few tries, the heap left counts.
func liveHeap() int64 {
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	for try := 0; ; try++ {
		_, _, cycles := heap()
		stops := gcStopped()
		polling := make(chan struct{})
		type read struct {
			allocs int64
			late   bool
		}
		marking := make(chan read)
		go func() {
			close(polling)
			n := gcStopped()
			for n == stops {
				n = gcStopped()
			}
			_, allocs, _ := heap()
			// The mark ends with the world stopped once more.
			marking <- read{allocs, n > stops+1}
		}()
		<-polling
		runtime.GC()
		r := <-marking
		objects, after, ended := heap()
		if ended == cycles+1 && !r.late {
			return objects - (after - r.allocs)
		}
		if try == 4 {
			return objects
		}
	}
}

// gcStopped returns how often a collection stopped the world.
func gcStopped() uint64 {
	sample := []metrics.Sample{{Name: gcStops}}
	metrics.Read(sample)
	n := uint64(0)
	for _, c := range sample[0].Value.Float64Histogram().Counts {
		n += c
	}
	return n
}
