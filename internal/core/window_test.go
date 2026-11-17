package core

import (
	"runtime"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// TestWindowCountsWhatEntriesHold fills the window with small entries: the
// bytes it counts must be about the heap they take, not only their data
// (#431).
func TestWindowCountsWhatEntriesHold(t *testing.T) {
	st := newStorage()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for b := range 200 {
		ents := make([]*raftpb.Entry, 1000)
		for i := range ents {
			ents[i] = &raftpb.Entry{Index: new(uint64(b*1000 + i + 1)), Term: new(uint64(1)), Data: make([]byte, 30)}
		}
		if err := st.add(ents); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if counted := st.total(); heap > 2*counted {
		t.Fatalf("the window counts %d bytes for 200,000 entries of 30 bytes, which take %d bytes of heap", counted, heap)
	}
	runtime.KeepAlive(st)
}
