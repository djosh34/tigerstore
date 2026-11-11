// Package testmem measures the peak heap of a test while something runs,
// for the tests that hold a consumer back and bound what the node builds
// meanwhile (#505).
package testmem

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"time"
)

// heapObjects is the runtime metric of the bytes in live and dead heap
// objects not yet swept.
const heapObjects = "/memory/classes/heap/objects:bytes"

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
// between two samples, or within one collection; and objects allocated
// during its own collection count as live until the next. Both results
// are net: what was live at the call and is freed meanwhile lowers them.
// The collector runs at GOGC 1 meanwhile, so that collections come often
// anyway.
func Peak() (stop func() (peak, kept int64)) {
	old := debug.SetGCPercent(1)
	base := liveHeap()
	held := int64(0)
	captured := false
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
				if heap()-base > held+step {
					before := heap()
					start := time.Now()
					n := liveHeap() - base
					if dir := os.Getenv("TS_PEAK_DIR"); dir != "" && n > held {
						fmt.Fprintf(os.Stderr, "PEAK base=%d before=%d new=%d gc=%s\n", base, before-base, n, time.Since(start))
						if n > 8<<20 && !captured {
							// Collect again before the profiler itself can allocate.
							fmt.Fprintf(os.Stderr, "PEAK secondGC=%d thirdGC=%d\n", liveHeap()-base, liveHeap()-base)
							dumpHeap(dir, fmt.Sprintf("peak-%d", n))
							captured = true
						}
					}
					held = max(held, n)
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

func dumpHeap(dir, name string) {
	f, err := os.Create(dir + "/" + name + ".pb.gz")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	if err := f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

// heap returns the bytes of live and dead heap objects not yet swept.
func heap() int64 {
	sample := []metrics.Sample{{Name: heapObjects}}
	metrics.Read(sample)
	return int64(sample[0].Value.Uint64())
}

// liveHeap collects the garbage and returns the heap left.
func liveHeap() int64 {
	runtime.GC()
	return heap()
}
