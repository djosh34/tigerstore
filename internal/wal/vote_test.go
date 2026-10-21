package wal_test

import (
	"errors"
	"math/rand/v2"
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

// TestVoteCopyOnlyInPageCache kills the process after it wrote a new term
// and vote into vote-0 and before it synced it, so that the next process
// reads the new copy from the page cache, and crashes that recovery in the
// sync of vote-1, which a crash then tears with vote-0's unsynced write.
// Recovery must not take a copy for one on the disk before it synced it,
// or the crash can leave no copy that verifies (#373).
func TestVoteCopyOnlyInPageCache(t *testing.T) {
	errKilled := errors.New("killed")
	for seed := range uint64(32) {
		d := simfs.New()
		w, _ := open(t, d)
		syncVote(t, d, w, 1, 1)
		w.Append(&wal.HardState{Term: 2, Vote: 2}, nil)
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		d.Fault = func(op simfs.Op, name string) error {
			if op == simfs.OpSync && name == "vote-0" {
				return errKilled
			}
			return nil
		}
		if err := wal.SaveVote(d, 2, 2); !errors.Is(err, errKilled) {
			t.Fatalf("saving the vote: %v, want the kill", err)
		}
		d.Fault = func(op simfs.Op, name string) error {
			if op == simfs.OpSync && name == "vote-1" {
				return errKilled
			}
			return nil
		}
		if _, _, err := wal.Open(d); !errors.Is(err, errKilled) {
			t.Fatalf("seed %d: recovery: %v, want the crash in the sync of vote-1", seed, err)
		}
		d.Fault = nil
		d.CrashImage(rand.New(rand.NewPCG(seed, 0)), simfs.Prefix)
		w, rec, err := wal.Open(d)
		if err != nil {
			t.Fatalf("seed %d: after the crash: %v", seed, err)
		}
		if rec.HardState.Term != 2 || rec.HardState.Vote != 2 {
			t.Fatalf("seed %d: recovered %+v, want term 2 and vote 2", seed, rec.HardState)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
