package sim

import (
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"testing"
	"testing/synctest"
)

func TestDiag247ColdRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := newSim(Config{Seed: 21739})
		if err != nil {
			t.Fatal(err)
		}
		defer s.shutdown()
		s.healed = true
		cfg := core.DefaultConfig(1, []uint64{1, 2, 3})
		for i := range 3 {
			cfg.Members = append(cfg.Members, &etcdserverpb.Member{ID: uint64(i + 1), PeerURLs: []string{peerURL(i)}})
		}
		fresh, err := core.New(cfg, &wal.Recovered{}, nil)
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
		s.nodes[1].fenced = true
		for _, idx := range []int{0, 2} {
			w, _, err := wal.Open(s.nodes[idx].disk)
			if err != nil {
				t.Fatal(err)
			}
			commit := uint64(0)
			if idx == 0 {
				commit = 3
			}
			durable := entries
			if idx == 2 {
				// Node 3 persisted the leader's term-2 no-op but not the
				// commit index of the bootstrap entries.
				durable = append(append([]*raftpb.Entry(nil), entries...), &raftpb.Entry{Index: new(uint64(4)), Term: new(uint64(2)), Type: raftpb.EntryNormal.Enum()})
			}
			w.Append(&wal.HardState{Term: 2, Vote: 2, Commit: commit}, durable)
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.runFor(25 * Second); err != nil {
			t.Fatal(err)
		}
		t.Log(s.report())
		for _, l := range s.trace {
			t.Log(l)
		}
		if s.nodes[0].proc.core.Leader() == 0 || s.nodes[2].proc.core.Applied() < 3 {
			t.Fatal("cold restart did not elect")
		}
	})
}
