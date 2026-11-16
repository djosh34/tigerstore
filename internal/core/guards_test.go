package core

import (
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// The tests in this file check the guards etcd v3.7.2 keeps around Raft
// that #232 lists and tigerstore copies.

// TestApplyRefusesGap hands apply an entry two past the applied one, which
// etcd stops on (row 30).
func TestApplyRefusesGap(t *testing.T) {
	s := newMember(t, []uint64{1})
	err := s.n.apply([]*raftpb.Entry{{Index: new(s.n.applied + 2), Term: new(uint64(1)), Type: raftpb.EntryNormal.Enum()}})
	if err == nil || !strings.Contains(err.Error(), "does not follow") {
		t.Fatalf("an entry after a gap: %v", err)
	}
}
