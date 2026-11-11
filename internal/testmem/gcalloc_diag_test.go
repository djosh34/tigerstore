//go:build diagnosis

package testmem

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

type diagLink struct {
	next *diagLink
	pad  [7]uintptr
}

// At most the old and new 64 KiB buffers are reachable together. The
// pointer chain slows marking so garbage allocations land inside Peak's
// own collection. This is a diagnosis probe, not a passing regression.
func TestGarbageAllocatedDuringPeakGC(t *testing.T) {
	procs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(procs)
	gc := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gc)
	var head *diagLink
	for range 1 << 20 {
		head = &diagLink{next: head}
	}
	stop := Peak()
	debug.SetGCPercent(-1)
	until := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(until) {
		sink = append(sink[:0], make([]byte, 64<<10))
		runtime.Gosched()
	}
	sink = nil
	peak, kept := stop()
	t.Logf("at most 128 KiB reachable: peak=%d KiB kept=%d KiB", peak>>10, kept>>10)
	runtime.KeepAlive(head)
	if peak > 1<<20 {
		t.Fatalf("garbage counted as held: %d KiB", peak>>10)
	}
}
