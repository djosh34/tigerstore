package testmem

import (
	"runtime"
	"testing"
	"time"
)

// sink keeps what the test holds live.
var sink [][]byte

// TestGarbageDoesNotCount makes 16 MiB of garbage, 64 KiB a millisecond,
// beside a live heap of 384 MiB, as a cluster's traffic does around the
// answer a test bounds, and then holds 8 MiB: the peak is about the 8 MiB
// held, not the garbage. At GOGC 1 the collector leaves up to 1% of the
// live heap as garbage, 3.8 MiB here, which counted when the peak was the
// heap sampled (#561).
func TestGarbageDoesNotCount(t *testing.T) {
	t.Cleanup(func() { sink = nil })
	base := make([][]byte, 384)
	for i := range base {
		base[i] = make([]byte, 1<<20)
	}
	peak := Peak()
	for range 256 {
		sink = append(sink[:0], make([]byte, 64<<10))
		time.Sleep(time.Millisecond)
	}
	sink = sink[:0]
	garbage, _ := peak()
	if garbage > 2<<20 {
		t.Fatalf("16 MiB of garbage peaks at %d KiB", garbage>>10)
	}
	peak = Peak()
	for range 8 {
		sink = append(sink, make([]byte, 1<<20))
		time.Sleep(10 * time.Millisecond)
	}
	held, kept := peak()
	if held < 7<<20 || kept < 7<<20 {
		t.Fatalf("8 MiB held peaks at %d KiB, %d KiB kept", held>>10, kept>>10)
	}
	runtime.KeepAlive(base)
}
