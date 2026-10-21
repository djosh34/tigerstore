package wal_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
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

// read returns the entries after the root up to the last one, read back
// from disk.
func read(t *testing.T, w *wal.WAL, rec *wal.Recovered) []*raftpb.Entry {
	t.Helper()
	var out []*raftpb.Entry
	for i := rec.Root.Index + 1; i <= rec.Last; i++ {
		e, err := w.Entry(i)
		if err != nil {
			t.Fatalf("read entry %d: %v", i, err)
		}
		out = append(out, e)
	}
	return out
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
	if rec.Last != 0 || rec.HardState != (wal.HardState{}) {
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
	w, rec = open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:a 2/2:B 3/2:c " {
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
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:a " {
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
	segs, _ := files(t, d)
	return segs
}

func wantCorrupt(t *testing.T, d *simfs.Disk) {
	t.Helper()
	before, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if _, rec, err := wal.Open(d); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("open = %v with entries to %d and %+v, want ErrCorrupt", err, rec.Last, rec.HardState)
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
	segs, _ := files(t, d)
	newest := segs[len(segs)-1]
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
	// The first record is the first hard state, with the new term and
	// vote, right after the header: they go before the entries. Damage its
	// body, its type and its stored crc in turn.
	for _, at := range []int{24 + 9 + 3, 24 + 8, 24 + 5} {
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
				ents := read(t, w, rec)
				got := make([]string, len(ents))
				for i, e := range ents {
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

// TestDamagedNewestHeader opens logs whose newest segment holds only a
// header, damaged, as a first write that did not reach the disk whole
// leaves it: the log starts that segment again without a write, and an
// append goes into it. With a record after the damaged header, the log is
// corrupt.
func TestDamagedNewestHeader(t *testing.T) {
	d := simfs.New()
	names := segments(t, d, 2)
	last := names[len(names)-1]
	header := []byte("tigerwal\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04")
	rewrite(t, d, last, func([]byte) []byte { return header })
	d.Fault = func(op simfs.Op, name string) error {
		// Recovery syncs what it read and writes the copies of the term
		// and vote, but writes nothing into the log.
		if op != simfs.OpRead && op != simfs.OpOpen && op != simfs.OpCreate && op != simfs.OpSyncDir && op != simfs.OpSync && !wal.IsVoteFile(name) {
			return fmt.Errorf("%v of %s", op, name)
		}
		return nil
	}
	w, rec := open(t, d)
	d.Fault = nil
	if rec.Last != 2 || rec.HardState.Commit != 2 {
		t.Fatalf("recovered entries to %d and %+v", rec.Last, rec.HardState)
	}
	w.Append(nil, []*raftpb.Entry{entry(3, 3, "v")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec = open(t, d)
	if rec.Last != 3 {
		t.Fatalf("after an append, recovered entries to %d", rec.Last)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	rewrite(t, d, segs[len(segs)-1], func(b []byte) []byte { b[0] ^= 1; return b })
	wantCorrupt(t, d)
}

// fill appends entries from..to, one synced batch each with its commit,
// in segments of about one batch.
func fill(t *testing.T, w *wal.WAL, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: i}, []*raftpb.Entry{entry(1, i, fmt.Sprintf("v%d", i))})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRootAndRemoval names a checkpoint at 6, removes the segments below
// it, and recovers the entries after it, the hard state and the root.
func TestRootAndRemoval(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 6, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, 9, 10)
	if err := w.RemoveBelow(7); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Entry(3); !errors.Is(err, wal.ErrNotOnDisk) {
		t.Fatalf("entry 3 after its segment went: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Root != (wal.Root{Index: 6, Term: 1}) || rec.Last != 10 || rec.HardState != (wal.HardState{Term: 1, Vote: 2, Commit: 10}) {
		t.Fatalf("recovered %+v", rec)
	}
	if got := describe(read(t, w, rec)); got != "7/1:v7 8/1:v8 9/1:v9 10/1:v10 " {
		t.Fatalf("entries after the root: %s", got)
	}
	b, err := w.Value(9, 1, 1)
	if err != nil || string(b) != "9" {
		t.Fatalf("value of entry 9: %q, %v", b, err)
	}

	// Without the root, a log that starts above 1 is damage.
	d2 := simfs.New()
	w2, _ := open(t, d2)
	w2.SetSegmentBytes(1)
	fill(t, w2, 1, 6)
	if err := w2.RemoveBelow(4); err != nil {
		t.Fatal(err)
	}
	wantCorrupt(t, d2)
}

// TestHardStateSurvivesRemoval writes the only vote in the first segment,
// then many entries without a hard state, and removes the old segments:
// the vote is still there.
func TestHardStateSurvivesRemoval(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	w.Append(&wal.HardState{Term: 7, Vote: 3, Commit: 0}, nil)
	for i := uint64(1); i <= 6; i++ {
		w.Append(nil, []*raftpb.Entry{entry(7, i, "v")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.AppendRoot(wal.Root{Index: 5, Term: 7})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(6); err != nil {
		t.Fatal(err)
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if segs := slices.DeleteFunc(names, func(n string) bool { return !strings.HasSuffix(n, ".wal") }); len(segs) > 4 {
		t.Fatalf("segments left: %v", segs)
	}
	_, rec := open(t, d)
	if rec.HardState.Term != 7 || rec.HardState.Vote != 3 || rec.Root.Index != 5 || rec.Last != 6 {
		t.Fatalf("recovered %+v", rec)
	}
}

// TestResetRootAndValues installs a snapshot at 20 on a log that held 1 to
// 12 and had applied 10: the values of 11 to 20 come as value records,
// the entries after 10 of the old log are gone, and the log goes on at 21.
func TestResetRootAndValues(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 12)
	w.AppendValue(15, 3, []byte("fifteen"))
	w.AppendRoot(wal.Root{Index: 20, Term: 4, Reset: true, Keep: 10})
	w.Append(&wal.HardState{Term: 4, Commit: 21}, []*raftpb.Entry{entry(4, 21, "x21")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	check := func(w *wal.WAL) {
		t.Helper()
		if _, err := w.Entry(11); !errors.Is(err, wal.ErrNotOnDisk) {
			t.Fatalf("entry 11 of the replaced log: %v", err)
		}
		if e, err := w.Entry(10); err != nil || string(e.GetData()) != "v10" {
			t.Fatalf("entry 10: %v, %v", e, err)
		}
		if b, err := w.Value(15, 5, 3); err != nil || string(b) != "fte" {
			t.Fatalf("value in entry 15: %q, %v", b, err)
		}
		if _, err := w.Value(15, 0, 3); !errors.Is(err, wal.ErrNotOnDisk) {
			t.Fatalf("bytes of entry 15 that no record holds: %v", err)
		}
	}
	check(w)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Root.Index != 20 || rec.Last != 21 || rec.HardState.Commit != 21 {
		t.Fatalf("recovered %+v", rec)
	}
	check(w)
	if got := describe(read(t, w, rec)); got != "21/4:x21 " {
		t.Fatalf("entries after the root: %s", got)
	}
}

// TestEntryChecksRecord damages an entry record after Open: reading the
// entry fails instead of returning wrong bytes.
func TestEntryChecksRecord(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 3)
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	data := d.DurableBytes(names[0])
	at := strings.Index(string(data), "v2")
	d.FlipDurable(names[0], at+1)
	_, err = w.Entry(2)
	var re *wal.RecordError
	if !errors.Is(err, wal.ErrCorrupt) || !errors.As(err, &re) || re.Entry != 2 || re.Segment != 1 {
		t.Fatalf("damaged entry 2: %v, want a RecordError of entry 2 in segment 1", err)
	}
	if e, err := w.Entry(3); err != nil || string(e.GetData()) != "v3" {
		t.Fatalf("entry 3: %v, %v", e, err)
	}
}

// TestCheckpointFiles keeps only the checkpoint file that the root names.
func TestCheckpointFiles(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 5)
	for _, i := range []uint64{2, 4, 5} {
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
	if err := w.SyncDir(); err != nil {
		t.Fatal(err)
	}
	w.AppendRoot(wal.Root{Index: 4, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	open(t, d)
	var ckpts []string
	for _, n := range d.DurableNames() {
		if strings.HasPrefix(n, "ckpt-") {
			ckpts = append(ckpts, n)
		}
	}
	if fmt.Sprint(ckpts) != "["+wal.CheckpointName(4)+"]" {
		t.Fatalf("checkpoint files after open: %v", ckpts)
	}
}

// TestOtherFormatVersion refuses a segment of another format version.
func TestOtherFormatVersion(t *testing.T) {
	d := simfs.New()
	names := segments(t, d, 2)
	rewrite(t, d, names[0], func(b []byte) []byte { b[8] = wal.Version + 1; return b })
	if _, _, err := wal.Open(d); !errors.Is(err, wal.ErrFormat) {
		t.Fatalf("open of version %d: %v", wal.Version+1, err)
	}
}

// TestRemovalKeepsSegmentsConsecutive removes segments in front of one
// that holds only a hard state: segments go only from the front, so the
// rest stay consecutive and the log opens.
func TestRemovalKeepsSegmentsConsecutive(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 2)
	w.Append(&wal.HardState{Term: 2, Vote: 1, Commit: 2}, nil)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, 3, 6)
	w.AppendRoot(wal.Root{Index: 5, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(6); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "6/1:v6 " {
		t.Fatalf("entries after the root: %s", got)
	}
}

// TestResetRootLowersCommit crashes right after a snapshot's root, before
// the entries after it: the commit index of the replaced log is past the
// root, and recovery lowers it to the root instead of refusing the log.
func TestResetRootLowersCommit(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 12)
	w.AppendRoot(wal.Root{Index: 10, Term: 4, Reset: true, Keep: 8})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	_, rec := open(t, d)
	if rec.HardState.Commit != 10 || rec.Last != 10 || rec.Root.Index != 10 {
		t.Fatalf("recovered %+v", rec)
	}
}

// TestResetRootCopiedPlain installs a snapshot, appends entries after it
// until a new segment starts, and recovers: the new segment's copy of the
// root does not reset the log again.
func TestResetRootCopiedPlain(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 5)
	w.AppendRoot(wal.Root{Index: 8, Term: 2, Reset: true, Keep: 5})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.SetSegmentBytes(1)
	fill(t, w, 9, 12)
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "9/1:v9 10/1:v10 11/1:v11 12/1:v12 " || rec.Root != (wal.Root{Index: 8, Term: 2}) {
		t.Fatalf("recovered %+v with entries %s", rec, got)
	}
}

// TestTornAppendKeepsTermWithEntries crashes in the middle of an append
// that brings a new term and its first entries, keeping every prefix of
// the unsynced bytes (#173). An entry that survives must come with a hard
// state of its term or later.
func TestTornAppendKeepsTermWithEntries(t *testing.T) {
	for seed := range uint64(200) {
		d := simfs.New()
		w, _ := open(t, d)
		w.Append(&wal.HardState{Term: 1, Vote: 2}, []*raftpb.Entry{entry(1, 1, "a")})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w.Append(&wal.HardState{Term: 2, Vote: 0, Commit: 1}, []*raftpb.Entry{entry(2, 2, "b"), entry(2, 3, "c")})
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		d.Crash(rand.New(rand.NewPCG(seed, 173)))
		w, rec := open(t, d)
		ents := read(t, w, rec)
		if last := ents[len(ents)-1]; last.GetTerm() > rec.HardState.Term {
			t.Fatalf("seed %d: entry %d of term %d survived with the hard state %+v", seed, last.GetIndex(), last.GetTerm(), rec.HardState)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestShortValueReadNamesTheFile cuts the segment inside a value: the
// error names the file and the byte the read started at, so that a short
// read seen only once explains itself (#300).
func TestShortValueReadNamesTheFile(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	fill(t, w, 1, 1)
	seq, at, ok := w.ValuePlace(1, 0, 2)
	if !ok {
		t.Fatal("entry 1 is not in a segment")
	}
	rewrite(t, d, wal.SegmentName(seq), func(b []byte) []byte { return b[:at+1] })
	_, err := w.Value(1, 0, 2)
	want := fmt.Sprintf("wal: read 1 of 2 bytes at byte %d of %s, the value at 0+2 of entry 1: EOF", at, wal.SegmentName(seq))
	if err == nil || err.Error() != want {
		t.Fatalf("short read: %v, want %s", err, want)
	}
}
