package testmem

import (
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

// sink keeps what the test holds live.
var sink [][]byte

// TestHeldSmallObjectsCount holds 32 small buffers, 1 to 32 KiB, made
// right after Peak's call, in a fresh process: Peak finds all of them
// kept. A P counts its small allocations only when its cache flushes,
// which the collection does, so bytes allocated read before the mark
// missed them, and they were taken off as allocated during it; and the
// first measurement in a process reads some KiB of its own setup that
// later ones do not, which as Peak's base would take them off again
// (#640's review). It comes first in the file, so that it runs first.
func TestHeldSmallObjectsCount(t *testing.T) {
	procs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(procs)
	peak := Peak()
	held := make([][]byte, 32)
	want := 0
	for i := range held {
		held[i] = make([]byte, (i+1)<<10)
		want += len(held[i])
	}
	_, kept := peak()
	runtime.KeepAlive(held)
	if kept < int64(want) {
		t.Fatalf("%d bytes of small buffers held, %d found kept", want, kept)
	}
}

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
	// The 8 MiB stays held until a sample found it, within step: a
	// collection of Peak's on a loaded runner outlasted the time each MiB
	// was held before, and the peak missed 2 MiB.
	found := make(chan struct{})
	var once sync.Once
	measured = func(held int64) {
		if held >= 8<<20-step-128<<10 {
			once.Do(func() { close(found) })
		}
	}
	peak = Peak()
	stopped := false
	// On every exit the sampler stops before the hook goes, so it never
	// reads it meanwhile or outlives the test.
	t.Cleanup(func() {
		if !stopped {
			peak()
		}
		measured = nil
	})
	for range 8 {
		sink = append(sink, make([]byte, 1<<20))
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-found:
	case <-time.After(10 * time.Second):
		t.Fatal("no sample found the 8 MiB held within 10 s")
	}
	// Freed before the stop, the 8 MiB counts only through the samples,
	// and the result is net of what of the heap at the call was freed
	// meanwhile, some KiB.
	sink = nil
	held, kept := peak()
	stopped = true
	if held < 8<<20-step-128<<10 || kept > 1<<20 {
		t.Fatalf("8 MiB held and freed peaks at %d KiB, %d KiB kept", held>>10, kept>>10)
	}
}

// link is a node of a long pointer chain that slows a collection's mark.
type link struct {
	next *link
	_    [7]uintptr
}

// TestGarbageDuringPeakGCDoesNotCount makes garbage, 64 KiB about every
// tenth of a millisecond, while Peak's own collections mark a 64 MiB
// pointer chain: at most two buffers are reachable at once, and the
// garbage allocated during a collection, which that collection keeps,
// does not count (#623). What is allocated before Peak's goroutine reads
// at the mark's start still counts, a few buffers unless it runs late.
func TestGarbageDuringPeakGCDoesNotCount(t *testing.T) {
	t.Cleanup(func() { sink = nil })
	procs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(procs)
	var head *link
	for range 1 << 20 {
		head = &link{next: head}
	}
	peak := Peak()
	// No collection runs but the ones Peak starts.
	gc := debug.SetGCPercent(-1)
	until := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(until) {
		sink = append(sink[:0], make([]byte, 64<<10))
		time.Sleep(100 * time.Microsecond)
	}
	sink = nil
	debug.SetGCPercent(gc)
	held, _ := peak()
	runtime.KeepAlive(head)
	if held > 1<<20 {
		t.Fatalf("garbage made during Peak's collections peaks at %d KiB", held>>10)
	}
}

// TestHeldUnderChurnCounts holds 16 MiB while another goroutine makes
// garbage, 64 KiB at a time, with automatic collections on, and a 64 MiB
// pointer chain slows each mark: no measurement finds less than the
// 16 MiB. The garbage that a collection frees must not be taken off as
// allocated during it, which read 1-2.5 MiB less here (#640's review). A
// measurement may count some garbage, more on a loaded host, as
// TestGarbageDuringPeakGCDoesNotCount bounds.
func TestHeldUnderChurnCounts(t *testing.T) {
	procs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(procs)
	gc := debug.SetGCPercent(1)
	defer debug.SetGCPercent(gc)
	var head *link
	for range 1 << 20 {
		head = &link{next: head}
	}
	base := liveHeap()
	held := make([]byte, 16<<20)
	stop, ended := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(ended)
		var b []byte
		for {
			select {
			case <-stop:
				runtime.KeepAlive(b)
				return
			default:
			}
			b = make([]byte, 64<<10)
			runtime.Gosched()
		}
	}()
	measured := make([]int64, 20)
	for i := range measured {
		measured[i] = liveHeap() - base
	}
	close(stop)
	<-ended
	runtime.KeepAlive(head)
	runtime.KeepAlive(held)
	for _, live := range measured {
		if live < 16<<20-256<<10 {
			t.Fatalf("16 MiB held beside garbage: measured %v KiB", kib(measured))
		}
	}
}

// kib converts byte counts to KiB.
func kib(b []int64) []int64 {
	k := make([]int64, len(b))
	for i, v := range b {
		k[i] = v >> 10
	}
	return k
}
