package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// noEntries is a log whose entries the core must not read: a read there
// would hold the loop through a slow disk (#682).
type noEntries struct {
	Log
	t testing.TB
}

func (l noEntries) Entry(index uint64) (*raftpb.Entry, error) {
	l.t.Errorf("the core read entry %d from the log", index)
	return l.Log.Entry(index)
}

// TestCheckpointRootWithoutTheLog restarts a node whose replay cuts a
// checkpoint at an entry that is not in memory, as the replay's entries
// never are: the checkpoint's root still gets its term, from the runs of
// terms the node keeps, without reading the log (#682).
func TestCheckpointRootWithoutTheLog(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) { cfg.CheckpointBytes = 1 << 40 })
	for id := uint64(1); id <= 60; id++ {
		if r := s.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id%7), Value: fmt.Appendf(nil, "value %d", id)}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if root := s.n.DurableCheckpoint(); root.Index != 0 {
		t.Fatalf("the first run made checkpoint %+v", root)
	}
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}

	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(1, []uint64{1})
	cfg.Boot = 2
	cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 256, 2, 64
	n, err := New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	if n.copy == nil {
		t.Fatal("the replay cut no checkpoint")
	}
	cut := n.copy.index
	if _, ok := n.storage.entryData(cut); ok {
		t.Fatalf("entry %d, where the replay cut, is in memory", cut)
	}
	e, err := w.Entry(cut)
	if err != nil {
		t.Fatal(err)
	}
	n.log = noEntries{Log: w, t: t}
	s = &single{t: t, disk: disk, log: w, n: n}
	for range 50 {
		n.Tick()
		s.run()
		if n.DurableCheckpoint().Index >= cut {
			break
		}
	}
	if root := n.DurableCheckpoint(); root.Index < cut {
		t.Fatalf("the checkpoint at %d never became durable; the newest is %+v", cut, root)
	}
	if root := n.DurableCheckpoint(); root.Index == cut && root.Term != e.GetTerm() {
		t.Fatalf("the checkpoint at %d has term %d; its entry's is %d", cut, root.Term, e.GetTerm())
	}
}

// TestTermRuns checks the runs of terms against a log that is cut back
// and written again, checkpointed and reset.
func TestTermRuns(t *testing.T) {
	s := newStorage()
	ent := func(index, term uint64) *raftpb.Entry {
		return &raftpb.Entry{Index: new(index), Term: new(term), Type: raftpb.EntryNormal.Enum()}
	}
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 9; i++ {
		ents = append(ents, ent(i, 1+i/4))
	}
	if err := s.add(ents); err != nil {
		t.Fatal(err)
	}
	// Entries 6 to 9 are replaced by 6 and 7 of term 5.
	if err := s.add([]*raftpb.Entry{ent(6, 5), ent(7, 5)}); err != nil {
		t.Fatal(err)
	}
	if err := s.trim(7); err != nil {
		t.Fatal(err)
	}
	want := map[uint64]uint64{1: 1, 3: 1, 4: 2, 5: 2, 6: 5, 7: 5}
	for index, term := range want {
		if got, ok := s.termOf(index); !ok || got != term {
			t.Errorf("entry %d: term %d, %v; want %d", index, got, ok, term)
		}
	}
	if _, ok := s.termOf(8); ok {
		t.Error("entry 8, past the last, has a term")
	}
	s.setCheckpoint(wal.Root{Index: 5, Term: 2})
	for index, term := range map[uint64]uint64{5: 2, 6: 5, 7: 5} {
		if got, ok := s.termOf(index); !ok || got != term {
			t.Errorf("after the checkpoint, entry %d: term %d, %v; want %d", index, got, ok, term)
		}
	}
	if len(s.terms) != 2 {
		t.Errorf("after the checkpoint at 5 the runs are %v, want those of terms 2 and 5", s.terms)
	}
}
