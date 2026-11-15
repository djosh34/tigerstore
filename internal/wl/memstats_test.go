package wl

import (
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"unsafe"
)

// Go's memory limit counts only Go's own memory. A node whose budget came
// from GOMEMLIMIT left Go's limit at the whole budget, so Go's heap grew
// on top of the store's tables: the k8s bench's nodes held 4 to 5.4 GB
// with a 3 GiB budget, swapped, and stalled for seconds (#602). The node
// gives Go only what the memory outside its heap leaves.
func TestGoGetsWhatTheMemoryOutsideItsHeapLeaves(t *testing.T) {
	const budget = 1 << 30
	// As GOMEMLIMIT gives the budget, Go's limit starts at all of it.
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(budget))
	st, l := newTestStore(t)
	st.setBudget(budget, 100<<20)
	value := strings.Repeat("v", 4096)
	for i := range 20000 {
		putValue(t, st, l, fmt.Sprintf("/k/%05d", i), value)
	}
	n := &node{st: st}
	n.limitGo()
	// Counted here as the kernel maps it: the rings and stages, the key
	// table, the hash index, the history, the key arena and the slab.
	outside := int64(100<<20) + int64(st.nkeys)*int64(unsafe.Sizeof(keyEnt{})) + int64(len(st.hash))*8 +
		int64(st.nhist)*int64(unsafe.Sizeof(histEnt{})) + int64(st.karena.top) + int64(st.slab.top)
	if got := debug.SetMemoryLimit(-1); got+outside > budget {
		t.Fatalf("Go's memory limit is %d MiB, and %d MiB lie outside Go's heap: %d MiB in all, past the budget of %d MiB",
			got>>20, outside>>20, (got+outside)>>20, budget>>20)
	}
}

// Go never gets less than its live heap and room to collect in: at the
// k8s bench's 4 million keys, the leader's live heap of 523 MB met a Go
// limit of 552 MB, and the collector ran without end. The slab gives way
// instead.
func TestGoNeverGetsLessThanItsLiveHeap(t *testing.T) {
	st, l := newTestStore(t)
	const budget = 1 << 30
	st.setBudget(budget, 0)
	putValue(t, st, l, "/k", "v")
	before := st.limit
	st.goLive = 600 << 20
	st.relimit()
	if got := st.goLimit(); got < 750<<20 {
		t.Fatalf("Go gets %d MiB with a live heap of 600 MiB", got>>20)
	}
	if st.limit != before-(750<<20-goShare(budget)) {
		t.Fatalf("the slab's limit went from %d to %d MiB, want it to give Go the %d MiB it needs past its share",
			before>>20, st.limit>>20, (750<<20-goShare(budget))>>20)
	}
}

// Letting records go never unmaps the slab's slots. A limit that counted
// the freed slots against the records could never be met once the slab
// had grown past it, and the clock hand let every record go: the k8s
// bench's leader dropped from 1,263 to 757 MB of records in seconds, and
// its applies read them back from the disk (#602).
func TestTheSlabStopsLettingGoWithinItsLimit(t *testing.T) {
	st, l := newTestStore(t)
	const budget = 1 << 30
	st.setBudget(budget, 0)
	value := strings.Repeat("v", 1000)
	for i := range 20000 {
		putValue(t, st, l, fmt.Sprintf("/k/%05d", i), value)
	}
	used := st.slab.used
	// Go needs enough more that half the records must go.
	st.goLive = (st.limit - used/2 + goShare(budget)) * 4 / 5
	for i := range 100 {
		putValue(t, st, l, fmt.Sprintf("/k/%05d", i), value)
	}
	if st.slab.used > st.limit {
		t.Fatalf("the slab holds %d KiB of records past its limit of %d KiB", st.slab.used>>10, st.limit>>10)
	}
	if st.slab.used < st.limit-st.limit/8 {
		t.Fatalf("the slab let go down to %d KiB of records, far below its limit of %d KiB", st.slab.used>>10, st.limit>>10)
	}
}
