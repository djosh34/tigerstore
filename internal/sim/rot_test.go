package sim

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestRotNeedsReportedWitness checks which entries of the newest segment
// the simulator may rot: one with entry index+2 after it, durable and
// reported durable. Entry 5 is on the disk, but the node reported entries
// to 4 only, so a failed sync could cut it off, and damage to entry 3 would
// then look like a torn write (#375). A short test, not a scenario: the
// failure needs a sync that wrote an entry of a later group along, a rot
// two entries before it, and a failed sync whose cut removes it, which no
// fault arranges on purpose. Seed 483056 of the campaign met it, and the
// fix of #372 moved its schedule.
func TestRotNeedsReportedWitness(t *testing.T) {
	d := simfs.New()
	w, _, err := wal.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		w.Append(&wal.HardState{Term: 1, Commit: i}, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(i), Data: []byte("value")}})
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	seq, _, _ := w.ValuePlace(1, 0, 0)
	n := &node{disk: d, durableLast: 4}
	for index, want := range map[uint64]bool{1: true, 2: true, 3: false, 4: false} {
		if got := witnessed(n, w, wal.SegmentName(seq), seq, index); got != want {
			t.Errorf("entry %d may rot: %v, want %v", index, got, want)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
