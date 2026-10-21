package wal_test

import (
	"errors"
	"math/rand/v2"
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
// it did not write readable in the page cache, and kills the process.
// Later syncs do not write them, so if the next process read them, a power
// loss after the segment is sealed would find a hole in it. The failed
// sync must cut them off (CutUnsynced, #364).
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
	// The kill keeps the page cache, but not entry 2, which never
	// reached the disk.
	w, rec := open(t, d)
	if rec.Last != 1 {
		t.Fatalf("after the kill, recovered entries to %d, want 1", rec.Last)
	}
	// It appends, and starts a new segment, which seals this one.
	w.SetSegmentBytes(1)
	w.Append(&wal.HardState{Term: 2, Commit: 2}, []*raftpb.Entry{entry(2, 2, "B"), entry(2, 3, "c")})
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

// TestFailedRecoverySyncThenKill kills a process after it wrote, but did
// not sync, an entry, and fails the sync in which the next process's
// recovery would make it durable. That leaves the part the sync did not
// write readable in the page cache, and recovery cannot cut it off, as it
// does not know which part that is. The process after it, on the same page
// cache, must read the disk instead, or a power loss after it appended and
// synced would find a hole before its new entries (#372).
func TestFailedRecoverySyncThenKill(t *testing.T) {
	for seed := range uint64(16) {
		d := simfs.New()
		w, _ := open(t, d)
		w.Append(&wal.HardState{Term: 1, Commit: 1}, []*raftpb.Entry{entry(1, 1, "a")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w.Append(&wal.HardState{Term: 1, Commit: 1}, []*raftpb.Entry{entry(1, 2, "b")})
		if err := errors.Join(w.Flush(), w.Close()); err != nil {
			t.Fatal(err)
		}
		d.FailSync(rand.New(rand.NewPCG(seed, 0)))
		if _, _, err := wal.Open(d); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("seed %d: recovery: %v, want its failed sync", seed, err)
		}
		w, rec := open(t, d)
		var ents []*raftpb.Entry
		for i := rec.Last + 1; i <= 3; i++ {
			ents = append(ents, entry(2, i, "c"))
		}
		w.Append(&wal.HardState{Term: 2, Commit: 1}, ents)
		if err := errors.Join(w.Sync(), w.Close()); err != nil {
			t.Fatal(err)
		}
		_, rec, err := wal.Open(d.Durable())
		if err != nil {
			t.Fatalf("seed %d: after a power loss: %v", seed, err)
		}
		if rec.Last != 3 {
			t.Fatalf("seed %d: after a power loss, recovered entries to %d, want 3", seed, rec.Last)
		}
	}
}

// TestRestartedSegmentKeepsNoStaleRecords starts a segment whose first
// write a crash lost and whose second it kept, as recovery does, and then
// tears the first write of the new start. The stale records of the second
// write chain from the same records as the new ones, so recovery would
// take them for valid records after a damaged one, unless the restart cut
// them off (#368).
func TestRestartedSegmentKeepsNoStaleRecords(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	first := []*raftpb.Entry{entry(1, 1, "a")}
	w.Append(&wal.HardState{Term: 1, Vote: 1}, first)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	name := segs[len(segs)-1]
	n := len(d.Files()[name])
	w.Append(nil, []*raftpb.Entry{entry(1, 2, "b")})
	if err := errors.Join(w.Flush(), w.Close()); err != nil {
		t.Fatal(err)
	}
	// The crash kept the second write and lost the first.
	rewrite(t, d, name, func(b []byte) []byte { clear(b[:n]); return b })
	w, rec := open(t, d)
	if rec.Last != 0 {
		t.Fatalf("recovered entries to %d from a segment that never started", rec.Last)
	}
	// The new start writes the same records, and its write is torn.
	w.Append(&wal.HardState{Term: 1, Vote: 1}, first)
	if err := errors.Join(w.Flush(), w.Close()); err != nil {
		t.Fatal(err)
	}
	d.Rot(name, n-1)
	if _, rec, err := wal.Open(d); err != nil {
		t.Fatalf("after a torn write: %v", err)
	} else if rec.Last != 0 {
		t.Fatalf("after a torn write, recovered entries to %d", rec.Last)
	}
}

// TestCutKeepsDriverSyncs syncs records the way the node driver does,
// with Flush and a sync of Segment that it reports with Synced, and then
// fails a sync: SyncTail's on a clean stop, and the driver's own of the
// segment before a rotation's new one. The cut must keep what those syncs
// made durable, and drop the rest (#364).
func TestCutKeepsDriverSyncs(t *testing.T) {
	failSync := func(d *simfs.Disk) {
		d.Fault = func(op simfs.Op, _ string) error {
			if op == simfs.OpSync {
				d.Fault = nil
				return simfs.ErrIO
			}
			return nil
		}
	}
	t.Run("SyncTail", func(t *testing.T) {
		d := simfs.New()
		w, _ := open(t, d)
		w.Append(&wal.HardState{Term: 1}, []*raftpb.Entry{entry(1, 1, "a"), entry(1, 2, "b")})
		if err := errors.Join(w.Flush(), w.Segment().Sync()); err != nil {
			t.Fatal(err)
		}
		w.Synced(w.Segment(), w.Size())
		w.Append(nil, []*raftpb.Entry{entry(1, 3, "c")})
		failSync(d)
		if err := w.SyncTail(); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("SyncTail = %v, want EIO", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		w, rec := open(t, d)
		if rec.Last != 2 {
			t.Fatalf("after the failed SyncTail, recovered entries to %d, want 2", rec.Last)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("rotation", func(t *testing.T) {
		d := simfs.New()
		w, _ := open(t, d)
		w.Append(&wal.HardState{Term: 1}, []*raftpb.Entry{entry(1, 1, "a")})
		if err := errors.Join(w.Flush(), w.Segment().Sync()); err != nil {
			t.Fatal(err)
		}
		w.Synced(w.Segment(), w.Size())
		w.Append(nil, []*raftpb.Entry{entry(1, 2, "b")})
		if err := errors.Join(w.Flush(), w.Seal(), wal.PrepareSegment(d, w.Seq()+1)); err != nil {
			t.Fatal(err)
		}
		old := w.Rotate()
		failSync(d)
		if err := old.Sync(); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("sync = %v, want EIO", err)
		}
		if err := errors.Join(w.CutUnsynced(old), old.Close(), w.Close()); err != nil {
			t.Fatal(err)
		}
		w, rec := open(t, d)
		if rec.Last != 1 {
			t.Fatalf("after the failed sync of the old segment, recovered entries to %d, want 1", rec.Last)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
