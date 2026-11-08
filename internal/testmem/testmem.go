// Package testmem measures the peak heap of a test while something runs,
// for the tests that hold a consumer back and bound what the node builds
// meanwhile (#505).
package testmem

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// heapObjects is the runtime metric of the bytes in live and dead heap
// objects not yet swept.
const heapObjects = "/memory/classes/heap/objects:bytes"

// Peak samples the heap every millisecond from its call until the
// returned stop, which returns the highest heap seen above the heap live
// at the call, and the heap live at the stop above it: what the code
// built and kept, such as new state. The collector runs at GOGC 1
// meanwhile, so that garbage, up to 1% of the live heap, does not pass
// for what the code holds.
func Peak() (stop func() (peak, kept int64)) {
	old := debug.SetGCPercent(1)
	runtime.GC()
	sample := []metrics.Sample{{Name: heapObjects}}
	read := func() int64 {
		metrics.Read(sample)
		return int64(sample[0].Value.Uint64())
	}
	base := read()
	var peak atomic.Int64
	done := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		local := make([]metrics.Sample, 1)
		local[0].Name = heapObjects
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				metrics.Read(local)
				if v := int64(local[0].Value.Uint64()); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()
	return func() (int64, int64) {
		close(done)
		<-ended
		runtime.GC()
		kept := read() - base
		debug.SetGCPercent(old)
		return max(peak.Load()-base, 0), kept
	}
}
