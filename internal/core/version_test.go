package core

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

func TestAnnounceRaisesVersionOnce(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	for i, step := range []struct {
		id      uint64
		v, want uint32
		run     uint64
	}{
		{1, 2, 1, 1}, {2, 2, 1, 1}, // node 3 has not announced
		{3, 1, 1, 1},               // node 3 is older
		{2, 1, 1, 2}, {3, 2, 1, 2}, // node 2 rolls back before node 3 upgrades
		{2, 2, 1, 1},               // node 2's announcement of run 1 arrives late (#174)
		{4, 3, 1, 1},               // not a voter
		{2, 2, 2, 3},               // node 2 upgrades again: every voter supports 2
		{3, 1, 2, 3}, {1, 3, 2, 2}, // never goes down
	} {
		st.announce(step.id, step.v, step.run, true, voters)
		if st.version != step.want {
			t.Fatalf("step %d: node %d announced %d, version %d, want %d", i, step.id, step.v, st.version, step.want)
		}
	}
}

// TestLateAnnouncementAppliesFirst is #174's other order: node 2's
// announcement of version 2 from run 1 arrives late but applies before
// that of its rollback to version 1 in run 2. The rollback still counts,
// and node 3's upgrade does not activate version 2.
func TestLateAnnouncementAppliesFirst(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	st.announce(1, 2, 1, true, voters)
	st.announce(3, 1, 1, true, voters)
	st.announce(2, 2, 1, true, voters) // run 1, late
	st.announce(2, 1, 2, true, voters) // run 2, the rollback
	st.announce(3, 2, 2, true, voters)
	if st.version != 1 || st.supports[2] != 1 {
		t.Fatalf("version %d, node 2 supports %d; want 1 and 1", st.version, st.supports[2])
	}
}

// TestLegacyAnnouncements replays announcements without a run, from a
// build before #174: each counts, as it did there, so versions rise and a
// rollback holds the cluster back; a later one with a run still works.
func TestLegacyAnnouncements(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	for i, step := range []struct {
		id      uint64
		v, want uint32
		legacy  bool
	}{
		{1, 2, 1, true}, {2, 2, 1, true}, {3, 1, 1, true}, // node 3 is older
		{2, 1, 1, true}, {3, 2, 1, true}, // node 2 rolls back
		{2, 2, 2, false}, // node 2 upgrades again with a new build
	} {
		st.announce(step.id, step.v, 1, !step.legacy, voters)
		if st.version != step.want {
			t.Fatalf("step %d: version %d, want %d", i, st.version, step.want)
		}
	}
}
