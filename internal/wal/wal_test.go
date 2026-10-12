package wal_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

var errCrash = errors.New("crash")

func entry(term, index uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Term: new(term), Index: new(index), Data: []byte(data)}
}

func open(t *testing.T, d *simfs.Disk) (*wal.WAL, *wal.Recovered) {
	t.Helper()
	w, rec, err := wal.Open(d)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w, rec
}

func describe(ents []*raftpb.Entry) string {
	var b strings.Builder
	for _, e := range ents {
		fmt.Fprintf(&b, "%d/%d:%s ", e.GetIndex(), e.GetTerm(), e.GetData())
	}
	return b.String()
}

func TestAppendAndRecover(t *testing.T) {
	d := simfs.New()
	w, rec := open(t, d)
	if len(rec.Entries) != 0 || rec.HardState != (wal.HardState{}) {
		t.Fatalf("fresh log recovered %v", rec)
	}
	w.Append(&wal.HardState{Term: 1, Vote: 1}, []*raftpb.Entry{entry(1, 1, "a"), entry(1, 2, "b")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Append(&wal.HardState{Term: 2, Vote: 3, Commit: 2}, []*raftpb.Entry{entry(2, 2, "B"), entry(2, 3, "c")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, rec = open(t, d)
	if got := describe(rec.Entries); got != "1/1:a 2/2:B 3/2:c " {
		t.Fatalf("entries = %s", got)
	}
	if rec.HardState != (wal.HardState{Term: 2, Vote: 3, Commit: 2}) {
		t.Fatalf("hard state = %+v", rec.HardState)
	}
}

func TestUnsyncedAppendIsLostOnCrash(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.Append(nil, []*raftpb.Entry{entry(1, 1, "a")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Append(nil, []*raftpb.Entry{entry(1, 2, "b")})
	d.Crash(rand.New(rand.NewPCG(1, 2)))
	_, rec := open(t, d)
	if got := describe(rec.Entries); got != "1/1:a " {
		t.Fatalf("entries = %s", got)
	}
}

func TestCommitPastLastEntryIsCorrupt(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.Append(&wal.HardState{Term: 1, Commit: 3}, nil)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wal.Open(d); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("open = %v, want ErrCorrupt", err)
	}
}

func TestDamagedSealedSegmentIsCorrupt(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(64)
	for i := uint64(1); i <= 10; i++ {
		w.Append(nil, []*raftpb.Entry{entry(1, i, "0123456789")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 3 {
		t.Fatalf("expected several segments, got %v", names)
	}
	f, err := d.Open(names[0])
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(data) - 3)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wal.Open(d); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("open = %v, want ErrCorrupt", err)
	}
}

// TestCrashKeepsSyncedEntries crashes at random points with torn tails.
// Recovery must return the synced log plus a prefix of the unsynced
// records, and leave a log that accepts appends.
func TestCrashKeepsSyncedEntries(t *testing.T) {
	apply := func(log []string, e *raftpb.Entry) []string {
		return append(log[:e.GetIndex()-1:e.GetIndex()-1], string(e.GetData()))
	}
	for seed := range uint64(500) {
		rng := rand.New(rand.NewPCG(seed, 7))
		d := simfs.New()
		w, _ := open(t, d)
		w.SetSegmentBytes(int64(64 + rng.IntN(512)))
		var synced []string
		var pending []*raftpb.Entry
		var log []string
		term := uint64(1)
		for range 30 + rng.IntN(30) {
			if rng.IntN(8) == 0 {
				if rng.IntN(2) == 0 {
					// Crash inside Sync: the write reached the disk, the
					// fsync did not.
					d.Fault = func(op simfs.Op, _ string) error {
						if op == simfs.OpSync {
							return errCrash
						}
						return nil
					}
					if err := w.Sync(); !errors.Is(err, errCrash) && len(pending) > 0 {
						t.Fatalf("seed %d: sync during crash = %v", seed, err)
					}
					d.Fault = nil
				}
				d.Crash(rng)
				var rec *wal.Recovered
				w, rec = open(t, d)
				w.SetSegmentBytes(int64(64 + rng.IntN(512)))
				got := make([]string, len(rec.Entries))
				for i, e := range rec.Entries {
					if e.GetIndex() != uint64(i+1) {
						t.Fatalf("seed %d: entry %d at position %d", seed, e.GetIndex(), i)
					}
					got[i] = string(e.GetData())
				}
				want := synced
				ok := fmt.Sprint(got) == fmt.Sprint(want)
				for _, e := range pending {
					want = apply(want, e)
					ok = ok || fmt.Sprint(got) == fmt.Sprint(want)
				}
				if !ok {
					t.Fatalf("seed %d: recovered %v, synced %v, pending %s", seed, got, synced, describe(pending))
				}
				synced, log, pending = got, got, nil
				continue
			}
			// Append a batch, sometimes replacing a suffix.
			last := uint64(len(log))
			start := last + 1
			if last > 0 && rng.IntN(4) == 0 {
				start = last - uint64(rng.IntN(int(min(last, 3))))
				term++
			}
			var ents []*raftpb.Entry
			for i := start; i < start+uint64(1+rng.IntN(4)); i++ {
				e := entry(term, i, fmt.Sprintf("t%d-i%d", term, i))
				ents = append(ents, e)
				log = apply(log, e)
			}
			w.Append(&wal.HardState{Term: term}, ents)
			pending = append(pending, ents...)
			if rng.IntN(3) > 0 {
				if err := w.Sync(); err != nil {
					t.Fatal(err)
				}
				synced, pending = append([]string(nil), log...), nil
			}
		}
	}
}
