package testmem

import (
	"runtime/debug"
	"testing"
	"time"
)

// sink keeps what the test holds live.
var sink [][]byte

// TestGarbageDoesNotCount makes 8 MiB of garbage, 64 KiB a millisecond,
// while the collector falls behind, as it did on a loaded runner, where
// a long mark of three nodes' heaps let the cluster's garbage pile up
// beside the answer a test bounds (#561): the peak counts none of it.
// Then the test holds 8 MiB for a while and frees it before the stop: the
// peak is the 8 MiB held, within step.
func TestGarbageDoesNotCount(t *testing.T) {
	t.Cleanup(func() { sink = nil })
	peak := Peak()
	// No collection runs but the ones Peak starts.
	gc := debug.SetGCPercent(-1)
	for range 128 {
		sink = append(sink[:0], make([]byte, 64<<10))
		time.Sleep(time.Millisecond)
	}
	sink = nil
	debug.SetGCPercent(gc)
	garbage, _ := peak()
	if garbage > 1<<20 {
		t.Fatalf("8 MiB of garbage peaks at %d KiB", garbage>>10)
	}
	peak = Peak()
	for range 8 {
		sink = append(sink, make([]byte, 1<<20))
		time.Sleep(10 * time.Millisecond)
	}
	// Freed before the stop, the 8 MiB counts only through the samples.
	sink = nil
	held, kept := peak()
	if held < 8<<20-step || kept > 1<<20 {
		t.Fatalf("8 MiB held and freed peaks at %d KiB, %d KiB kept", held>>10, kept>>10)
	}
}
