package wal_test

import (
	"errors"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestKillThenPowerLoss kills a process after it wrote, but did not
// sync, an entry, a new term and vote, and a root: the page cache keeps
// them, and the next process reads them. They must survive a power loss
// before that process writes anything, as it holds them durable (#361).
func TestKillThenPowerLoss(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	for _, i := range []uint64{1, 2} {
		f, err := w.CreateCheckpoint(i)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("ckpt")); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			t.Fatal(err)
		}
	}
	w.Append(&wal.HardState{Term: 1, Vote: 1}, []*raftpb.Entry{entry(1, 1, "a"), entry(1, 2, "b")})
	w.AppendRoot(wal.Root{Index: 1, Term: 1})
	if err := errors.Join(w.SyncDir(), w.Sync()); err != nil {
		t.Fatal(err)
	}
	w.Append(&wal.HardState{Term: 2, Vote: 2}, []*raftpb.Entry{entry(2, 3, "c")})
	w.AppendRoot(wal.Root{Index: 2, Term: 1})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// The kill: a new process opens the log, holds all of it durable, and
	// removes the checkpoint the older root names.
	w, rec := open(t, d)
	if rec.Last != 3 || rec.Root.Index != 2 || rec.HardState.Term != 2 {
		t.Fatalf("after the kill, recovered %+v", rec)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The power goes before the new process writes anything.
	_, rec, err := wal.Open(d.Durable())
	if err != nil {
		t.Fatalf("after the power loss: %v", err)
	}
	if rec.Last != 3 || rec.Root.Index != 2 || rec.HardState.Term != 2 || rec.HardState.Vote != 2 {
		t.Fatalf("after the power loss, recovered %+v; the restarted process held entries to 3, root 2, term 2 and vote 2", rec)
	}
}

// TestFailedSyncThenKill fails a sync of the log, which leaves the bytes
// it did not write readable in the page cache, and kills the process. The
// next process reads them, and later syncs do not write them, so a power
// loss after the segment is sealed would find a hole in it unless
// recovery wrote them again (#364).
func TestFailedSyncThenKill(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.Append(&wal.HardState{Term: 1, Commit: 1}, []*raftpb.Entry{entry(1, 1, "a")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Append(&wal.HardState{Term: 2, Commit: 1}, []*raftpb.Entry{entry(2, 2, "b")})
	d.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpSync {
			return simfs.ErrIO
		}
		return nil
	}
	if err := w.Sync(); !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("sync: %v, want EIO", err)
	}
	d.Fault = nil
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The kill keeps the page cache: the next process holds entry 2.
	w, rec := open(t, d)
	if rec.Last != 2 {
		t.Fatalf("after the kill, recovered entries to %d", rec.Last)
	}
	// It appends, and starts a new segment, which seals this one.
	w.SetSegmentBytes(1)
	w.Append(&wal.HardState{Term: 2, Commit: 2}, []*raftpb.Entry{entry(2, 3, "c")})
	if err := errors.Join(w.Sync(), w.Close()); err != nil {
		t.Fatal(err)
	}
	_, rec, err := wal.Open(d.Durable())
	if err != nil {
		t.Fatalf("after a power loss: %v", err)
	}
	if rec.Last != 3 || rec.HardState.Term != 2 {
		t.Fatalf("after a power loss, recovered entries to %d and %+v", rec.Last, rec.HardState)
	}
}
