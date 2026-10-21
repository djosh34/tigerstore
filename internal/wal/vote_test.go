package wal_test

import (
	"errors"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// syncVote appends a new term and vote, syncs the log and saves them in
// the copies outside it, as the node driver does.
func syncVote(t *testing.T, d *simfs.Disk, w *wal.WAL, term, vote uint64) {
	t.Helper()
	w.Append(&wal.HardState{Term: term, Vote: vote}, nil)
	if err := errors.Join(w.Sync(), wal.SaveVote(d, term, vote)); err != nil {
		t.Fatal(err)
	}
}

// TestDamagedLastRecordKeepsVote damages the log's last record, which
// holds a synced term and vote: recovery takes it for a torn write and
// cuts it off, and must take the term and vote from the copies (#357).
func TestDamagedLastRecordKeepsVote(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	syncVote(t, d, w, 1, 1)
	syncVote(t, d, w, 2, 3)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	rewrite(t, d, segs[len(segs)-1], func(b []byte) []byte { b[len(b)-1] ^= 1; return b })
	w, rec := open(t, d)
	if rec.HardState.Term != 2 || rec.HardState.Vote != 3 {
		t.Fatalf("recovered %+v, want term 2 and vote 3", rec.HardState)
	}
	// The next segment starts with the recovered term and vote.
	w.SetSegmentBytes(1)
	w.Append(nil, []*raftpb.Entry{entry(2, 1, "a")})
	if err := errors.Join(w.Sync(), w.Close()); err != nil {
		t.Fatal(err)
	}
	if _, rec = open(t, d); rec.HardState.Term != 2 || rec.HardState.Vote != 3 {
		t.Fatalf("after an append, recovered %+v", rec.HardState)
	}
}

// TestVoteCopies damages one copy of the term and vote, which recovery
// must repair from the other, and then both, which is corrupt.
func TestVoteCopies(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	syncVote(t, d, w, 4, 2)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery takes the newer copy, here one the log never held.
	if err := wal.SaveVote(d, 5, 0); err != nil {
		t.Fatal(err)
	}
	flip := func(name string) {
		rewrite(t, d, name, func(b []byte) []byte { b[9] ^= 1; return b })
	}
	for _, name := range []string{"vote-0", "vote-1"} {
		flip(name)
		w, rec := open(t, d)
		if rec.HardState.Term != 5 || rec.HardState.Vote != 0 {
			t.Fatalf("with %s damaged, recovered %+v", name, rec.HardState)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	flip("vote-0")
	flip("vote-1")
	wantCorrupt(t, d)
}

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
