package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// A log index gives a new epoch the same name even when recovery started
// from a checkpoint that did not store confirmation state.
func TestVersionEpochAfterLegacyRecovery(t *testing.T) {
	voters := []uint64{1, 2, 3}
	states := []*State{NewState(2, &mvcc.Heap{}), NewState(2, &mvcc.Heap{})}
	for _, st := range states {
		for _, id := range voters {
			st.applyMembers(confAdd, &pb.Member{ID: id}, id)
			v := uint32(2)
			if id == 3 {
				v = 1
			}
			st.announce(id, v, 1, true, voters)
		}
		if st.versionEpoch != 0 {
			t.Fatal("legacy replay invented confirmation state absent from old checkpoints")
		}
	}
	// Also recover safely from an earlier confirmation epoch. The next
	// run must replace it with the entry index, not add to a local count.
	states[0].versionEpoch = 17
	for _, st := range states {
		st.confirmVersion(100, 3, versionAnnouncement{version: 2, run: 2, hasRun: true, hasEpoch: true}, voters)
		if st.versionEpoch != 100 || st.version != 1 {
			t.Fatalf("new run: epoch %d, version %d", st.versionEpoch, st.version)
		}
	}
	if states[0].Hash() != states[1].Hash() {
		t.Fatal("new run did not reconcile recovered confirmation state")
	}
	// Epoch zero is not unanimous confirmation by missing map entries.
	st := NewState(2, &mvcc.Heap{})
	st.members[1], st.supports[1], st.runs[1] = &pb.Member{ID: 1}, 2, 1
	st.confirmVersion(101, 1, versionAnnouncement{version: 2, run: 1, hasRun: true, hasEpoch: true}, []uint64{1})
	if st.version != 1 || st.versionEpoch != 101 {
		t.Fatalf("zero epoch activated version %d, epoch %d", st.version, st.versionEpoch)
	}
}

func TestVersionMembershipInvalidatesConfirmations(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	st.versionEpoch = 4
	st.confirmed[1], st.confirmed[2] = 4, 4
	st.applyMembers(confAdd, &pb.Member{ID: 3}, 9)
	if st.versionEpoch != 9 || len(st.confirmed) != 0 {
		t.Fatalf("membership kept epoch %d and confirmations %v", st.versionEpoch, st.confirmed)
	}
}
