package core

import (
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
	"testing"
)

type diag247Log struct {
	Log
	entries []*raftpb.Entry
}

func (l diag247Log) Entry(i uint64) (*raftpb.Entry, error) { return l.entries[i-1], nil }

// Equal recovered logs do not deadlock: the empty-configuration node can
// still grant a vote. The simulator reproduction gives it a newer log.
func TestDiag247EqualLogCanVote(t *testing.T) {
	fresh, err := New(DefaultConfig(1, []uint64{1, 2, 3}), &wal.Recovered{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	eff, err := fresh.Process()
	if err != nil {
		t.Fatal(err)
	}
	var entries []*raftpb.Entry
	for _, j := range eff.Persist {
		entries = append(entries, j.Entries...)
	}
	if len(entries) != 3 {
		t.Fatalf("bootstrap entries: %d", len(entries))
	}
	disk := diag247Log{entries: entries}
	c := &memCluster{t: t, jobs: make([][]PersistJob, 3), down: [3]bool{true, false, false}, nodes: make([]*Node, 3)}
	for _, id := range []uint64{2, 3} {
		commit := uint64(0)
		if id == 2 {
			commit = 3
		}
		n, err := New(DefaultConfig(id, []uint64{1, 2, 3}), &wal.Recovered{Last: 3, HardState: wal.HardState{Term: 3, Vote: 1, Commit: commit}}, disk)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id-1] = n
	}
	// Erased node 1 never runs. Node 2 knows all voters; node 3 has
	// durable bootstrap entries but has applied none of them.
	t.Logf("before: node2 applied=%d members=%v; node3 applied=%d members=%v", c.nodes[1].Applied(), c.nodes[1].Members(), c.nodes[2].Applied(), c.nodes[2].Members())
	for range 500 {
		c.nodes[1].Tick()
		c.nodes[2].Tick()
		for range 10 {
			c.step()
		}
		if c.nodes[1].Leader() != 0 && c.nodes[2].Leader() == c.nodes[1].Leader() && c.nodes[2].Applied() >= 3 {
			return
		}
	}
	t.Fatalf("no leader: node2=%+v node3=%+v", c.nodes[1].RaftStatus(), c.nodes[2].RaftStatus())
}
