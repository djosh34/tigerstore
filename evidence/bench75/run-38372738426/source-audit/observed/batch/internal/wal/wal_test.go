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

// rewrite replaces a file's synced content, like damage on disk.
func rewrite(t *testing.T, d *simfs.Disk, name string, change func([]byte) []byte) {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	data = change(data)
	if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// segments writes n synced batches of one entry and a hard state, one
// segment each, and returns the segment names.
func segments(t *testing.T, d *simfs.Disk, n uint64) []string {
	t.Helper()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	for i := uint64(1); i <= n; i++ {
		w.Append(&wal.HardState{Term: i, Vote: 1, Commit: i}, []*raftpb.Entry{entry(i, i, "v")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func wantCorrupt(t *testing.T, d *simfs.Disk) {
	t.Helper()
	before, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if _, rec, err := wal.Open(d); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("open = %v with %d entries and %+v, want ErrCorrupt", err, len(rec.Entries), rec.HardState)
	}
	after, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("open of a corrupt log changed the files from %v to %v", before, after)
	}
}

func TestMissingSegmentIsCorrupt(t *testing.T) {
	// With 3 batches the gap is before a segment with records; with 2 it
	// is right before the empty newest segment that rotation left.
	for _, n := range []uint64{2, 3} {
		d := simfs.New()
		names := segments(t, d, n)
		if err := d.Remove(names[1]); err != nil {
			t.Fatal(err)
		}
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		wantCorrupt(t, d)
	}
}

func TestDamagedNewestHeaderIsCorrupt(t *testing.T) {
	d := simfs.New()
	segments(t, d, 4)
	// Put a record into the empty newest segment that rotation left.
	w, _ := open(t, d)
	w.Append(&wal.HardState{Term: 5, Vote: 1, Commit: 5}, []*raftpb.Entry{entry(5, 5, "v")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	newest := names[len(names)-1]
	rewrite(t, d, newest, func(b []byte) []byte {
		b[len("tigerwal")+8] ^= 1 // the chain crc in the header
		return b
	})
	wantCorrupt(t, d)
}

func TestDamagedRecordBeforeValidOneIsCorrupt(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	for i := uint64(1); i <= 5; i++ {
		w.Append(&wal.HardState{Term: i, Vote: 1, Commit: i}, []*raftpb.Entry{entry(i, i, "value")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	// The second record is the first hard state, right after the header
	// and the first entry record of 9+17+5 bytes. Damage its body, its
	// type and its stored crc in turn.
	for _, at := range []int{20 + 31 + 9 + 3, 20 + 31 + 8, 20 + 31 + 5} {
		rewrite(t, d, names[0], func(b []byte) []byte { b[at] ^= 0x40; return b })
		wantCorrupt(t, d)
		rewrite(t, d, names[0], func(b []byte) []byte { b[at] ^= 0x40; return b })
	}
}

var errCrash2 = errors.New("crash in recovery")

// failFrom fails every disk operation from the k-th writing one on.
// Reads are not counted, so k reaches recovery's truncate, sync, remove
// and new-segment steps.
func failFrom(d *simfs.Disk, k int) {
	n := 0
	d.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpRead || op == simfs.OpOpen {
			return nil
		}
		n++
		if n >= k {
			return errCrash2
		}
		return nil
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
				// Sometimes crash again in the middle of recovery.
				for rng.IntN(2) == 0 {
					failFrom(d, 1+rng.IntN(6))
					if _, _, err := wal.Open(d); err != nil && !errors.Is(err, errCrash2) {
						t.Fatalf("seed %d: open during crash: %v", seed, err)
					}
					d.Fault = nil
					d.Crash(rng)
				}
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

// TestCrashWhileReplacingDamagedNewestHeader crashes at each writing step
// of the repair of a header-only newest segment whose header is damaged:
// remove, directory sync, and the create, write and syncs of the new
// segment. The synced log must survive every time.
func TestCrashWhileReplacingDamagedNewestHeader(t *testing.T) {
	for k := 1; k <= 6; k++ {
		d := simfs.New()
		names := segments(t, d, 2)
		rewrite(t, d, names[len(names)-1], func(b []byte) []byte { b[0] ^= 1; return b })
		failFrom(d, k)
		if _, _, err := wal.Open(d); !errors.Is(err, errCrash2) {
			t.Fatalf("k=%d: open with a fault = %v", k, err)
		}
		d.Fault = nil
		d.Crash(rand.New(rand.NewPCG(uint64(k), 1)))
		w, rec := open(t, d)
		if len(rec.Entries) != 2 || rec.HardState.Commit != 2 {
			t.Fatalf("k=%d: recovered %d entries and %+v", k, len(rec.Entries), rec.HardState)
		}
		w.Append(nil, []*raftpb.Entry{entry(3, 3, "v")})
		if err := w.Sync(); err != nil {
			t.Fatalf("k=%d: %v", k, err)
		}
		if _, rec := open(t, d); len(rec.Entries) != 3 {
			t.Fatalf("k=%d: after an append, recovered %d entries", k, len(rec.Entries))
		}
	}
}
