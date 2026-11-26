package wl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// A checkpoint at the newest's own index must not replace its file while
// the store, a held read and the writer's prev still read values in it.
func TestAHeldReadOutlivesASameIndexCheckpoint(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "held") // 2
	st.holdRead(2)
	defer st.releaseRead(2)
	putValue(t, st, l, "/a", "current") // 3
	st.compact(3)
	st.trimSteps(1 << 20)
	writeCkpt(t, n, 14)
	l.b = nil // every value lies in the checkpoint
	st.fit()
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}
	for range 3 {
		before, gen := n.ckptName, n.ckptGen
		if err := n.startCkpt(14, 1, 0); err != nil {
			t.Fatal(err)
		}
		r := n.ckpt
		// The name check guards against rename-over: reads through the
		// store's open handle alone would not catch that on Linux.
		if r.w.name == before || r.w.prev != st.ckpts[gen] {
			n.abortCkpt()
			t.Fatal("the same-index checkpoint reused the file it reads")
		}
		if err := n.ckptStep(false); err != nil {
			t.Fatal(err)
		}
		// Wait for the rename and CURRENT, but leave the old checkpoint
		// as the store's newest until both reads have used it again.
		if !r.lastOut {
			n.abortCkpt()
			t.Fatal("the small checkpoint did not hand out its last chunk")
		}
		for {
			c := <-r.w.out
			r.sent--
			if c.err != nil {
				t.Fatal(c.err)
			}
			if c.last {
				break
			}
		}
		for _, name := range []string{before, r.w.name} {
			if _, err := os.Stat(filepath.Join(n.ckptDir, name)); err != nil {
				t.Fatal(err)
			}
		}
		for rev, want := range map[int64]string{2: "held", 3: "current"} {
			if got, ok := valueAt(t, st, "/a", rev); !ok || got != want {
				t.Fatalf("before ckptDone: /a at %d reads %q, want %q", rev, got, want)
			}
		}
		if err := n.ckptDone(); err != nil {
			t.Fatal(err)
		}
		if st.ckpts[gen] != nil {
			t.Fatal("the replaced checkpoint stayed open")
		}
		for rev, want := range map[int64]string{2: "held", 3: "current"} {
			if got, ok := valueAt(t, st, "/a", rev); !ok || got != want {
				t.Fatalf("after ckptDone: /a at %d reads %q, want %q", rev, got, want)
			}
		}
	}
	// Loading follows CURRENT's whole name, and cleanup removes suffixed
	// files and unfinished writes without removing the newest.
	for _, name := range []string{"000000000000000e-0000000000000000.ckpt", "unfinished.ckpt.tmp"} {
		if err := os.WriteFile(filepath.Join(n.ckptDir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := ckptNode(t)
	m.ckptDir = n.ckptDir
	if _, err := m.loadNewest(); err != nil {
		t.Fatal(err)
	}
	if m.ckptName != n.ckptName || m.st.stateHash() != st.stateHash() {
		t.Fatal("CURRENT did not load the same-index checkpoint's state")
	}
	if got, ok := valueAt(t, m.st, "/a", 3); !ok || got != "current" {
		t.Fatalf("the loaded checkpoint reads %q", got)
	}
}

// A delayed remover must remove each replaced same-index file, not the
// newest; even a second replacement must not reuse a queued name.
func TestSameIndexCheckpointDoneRemovesOnlyTheOldFiles(t *testing.T) {
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", "value")
	writeCkpt(t, n, 14)
	idle := &remover{sys: n.sys, paths: make(chan string, removerQueue), done: make(chan struct{}), wake: func() {}}
	n.rm = idle
	old := []string{n.ckptName}
	for range 3 {
		writeCkptAt(t, n, 14)
		for _, name := range old {
			if name == n.ckptName {
				t.Fatal("a new checkpoint reused a name queued for removal")
			}
			if _, err := os.Stat(filepath.Join(n.ckptDir, name)); err != nil {
				t.Fatalf("the loop removed %s: %v", name, err)
			}
		}
		old = append(old, n.ckptName)
	}
	go idle.run()
	idle.close()
	for _, name := range old[:len(old)-1] {
		if _, err := os.Stat(filepath.Join(n.ckptDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the remover left %s: %v", name, err)
		}
	}
	names, err := os.ReadDir(n.ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("the remover left %d files, want CURRENT and the newest", len(names))
	}
	p, err := readPointer(n.sys, n.ckptDir)
	if err != nil || p.name != n.ckptName {
		t.Fatalf("CURRENT: %+v, %v", p, err)
	}
	if _, err := os.Stat(filepath.Join(n.ckptDir, p.name)); err != nil {
		t.Fatalf("the remover removed the newest: %v", err)
	}
}

// Replacing a checkpoint at the same index updates the snapshot's file,
// not the compacted entry's term, the first index or later entries. An
// install at that index still starts the storage just after it.
func TestSameIndexCheckpointKeepsStorageTermAndIndex(t *testing.T) {
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", "value")
	takeEveryValue(t)
	n.ckptFrom = int64(len(l.b))
	checkpoint := func() {
		t.Helper()
		if err := n.startCkpt(14, 7, 0); err != nil {
			t.Fatal(err)
		}
		for n.ckpt != nil {
			if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	checkpoint()
	s := n.storage
	s.applySnapshot(s.snap.GetMetadata(), n.ckptName)
	var ents []*pb.Entry
	var offs []int64
	// The compacted snapshot's term differs from both remaining entries.
	for i := uint64(15); i <= 16; i++ {
		e := testEntry(i, 8, "entry")
		ents = append(ents, e)
		offs = append(offs, addRecord(l, e))
	}
	if err := n.storage.append(ents, offs); err != nil {
		t.Fatal(err)
	}
	n.applied = 14
	before := n.ckptName
	checkpoint()
	if n.ckptName == before {
		t.Fatal("the new snapshot reused the old file")
	}
	for range 2 {
		s.compact(14)
		first, _ := s.FirstIndex()
		term, err := s.Term(14)
		if err != nil || term != 7 || first != 15 || s.lastIndex() != 16 || s.offOf(14) != -1 || s.offOf(15) != offs[0] {
			t.Fatalf("after same-index compaction: term %d (%v), first %d, last %d, offsets %d, %d", term, err, first, s.lastIndex(), s.offOf(14), s.offOf(15))
		}
		if _, err := s.Term(13); !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("term before the checkpoint: %v", err)
		}
		got, err := s.Entries(15, 17, 1<<20)
		if err != nil || len(got) != 2 || got[0].GetIndex() != 15 || got[1].GetIndex() != 16 || got[0].GetTerm() != 8 || got[1].GetTerm() != 8 {
			t.Fatalf("the entries after the checkpoint: %v, %v", got, err)
		}
	}
	snap, err := s.Snapshot()
	if err != nil || snap.GetMetadata().GetIndex() != 14 || snap.GetMetadata().GetTerm() != 7 || string(snap.GetData()) != n.ckptName {
		t.Fatalf("the snapshot: %v, %v", snap, err)
	}
	// Receive a self-contained copy, then install at the same index.
	b, err := os.ReadFile(filepath.Join(n.ckptDir, n.ckptName))
	if err != nil {
		t.Fatal(err)
	}
	recv := filepath.Join(n.ckptDir, "received.tmp")
	if err := os.WriteFile(recv, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if n.wal, err = openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.wal.close)
	if err := n.installSnapshot(&pb.Snapshot{Data: []byte(recv), Metadata: snap.GetMetadata()}); err != nil {
		t.Fatal(err)
	}
	first, _ := s.FirstIndex()
	term, err := s.Term(14)
	if err != nil || term != 7 || first != 15 || s.lastIndex() != 14 || s.offOf(14) != -1 {
		t.Fatalf("after same-index install: term %d (%v), first %d, last %d, offset %d", term, err, first, s.lastIndex(), s.offOf(14))
	}
	if got, ok := valueAt(t, n.st, "/a", n.st.rev); !ok || got != "value" {
		t.Fatalf("the installed value reads %q", got)
	}
}
