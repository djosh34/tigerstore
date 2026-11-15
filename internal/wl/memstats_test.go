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
