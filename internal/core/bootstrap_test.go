package core

import (
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestBootstrapWithoutCommitCampaigns is #247's case. A new member's log
// starts with the cluster's members, which Raft's Bootstrap writes at
// term 1 as committed, but a crash kept them, and a later entry from the
// leader, without the commit index that follows them on disk. Its next
// start must still apply them, so that it has voters and campaigns: with
// the longest log, it is the only node the others can elect.
func TestBootstrapWithoutCommitCampaigns(t *testing.T) {
	disk := simfs.New()
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(3, []uint64{1, 2, 3})
	n, err := New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	eff, err := n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if len(eff.Persist) != 1 || len(eff.Persist[0].Entries) != 3 {
		t.Fatalf("the first append holds %d jobs, want the 3 members in one", len(eff.Persist))
	}
	// What a crash keeps: the term, the members and the leader's first
	// entry, but no commit.
	w.Append(&wal.HardState{Term: 1}, eff.Persist[0].Entries)
	w.Append(&wal.HardState{Term: 2}, []*raftpb.Entry{{Term: new(uint64(2)), Index: new(uint64(4)), Type: raftpb.EntryNormal.Enum()}})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec, err = wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	if rec.HardState.Commit != 0 || rec.Last != 4 {
		t.Fatalf("recovered commit %d and last %d, want 0 and 4", rec.HardState.Commit, rec.Last)
	}
	n, err = New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(n.Members()); got != 3 {
		t.Fatalf("the restarted node knows %d members, want 3", got)
	}
	for range 3 * cfg.ElectionTicks {
		n.Tick()
		if _, err := n.Process(); err != nil {
			t.Fatal(err)
		}
	}
	if st := n.RaftStatus().RaftState; st != raft.StatePreCandidate && st != raft.StateCandidate {
		t.Fatalf("the restarted node is a %s after 3 election timeouts alone, want it to campaign", st)
	}
}
