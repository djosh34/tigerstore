package wal_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// files returns the segment and spare names on d.
func files(t *testing.T, d *simfs.Disk) (segs, spares []string) {
	t.Helper()
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		switch {
		case strings.HasSuffix(n, ".wal"):
			segs = append(segs, n)
		case strings.HasPrefix(n, "spare-"):
			spares = append(spares, n)
		}
	}
	return segs, spares
}

// TestRemoveSpares removes spare files beyond one, then turns spares
// off: a removal after it removes the segment's file (#181).
func TestRemoveSpares(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 5, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(5); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) == 0 {
		t.Fatal("no spares after the removal")
	}
	if err := wal.RemoveSpares(d, 1); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) != 1 {
		t.Fatalf("spares %v after RemoveSpares(1)", spares)
	}
	w.SetSpares(0)
	w.AppendRoot(wal.Root{Index: 7, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(7); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) != 0 {
		t.Fatalf("spares %v with spares off", spares)
	}
}

// TestSegmentsReuseSpares removes old segments, which become spares, and
// writes on: new segments take the spare files instead of new ones.
func TestSegmentsReuseSpares(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 7, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(7); err != nil {
		t.Fatal(err)
	}
	segs, spares := files(t, d)
	if len(spares) != wal.MaxSpares {
		t.Fatalf("%d spares after removal, want %d: %v", len(spares), wal.MaxSpares, segs)
	}
	before := len(segs) + len(spares)
	fill(t, w, 9, 9+wal.MaxSpares-1)
	segs, spares = files(t, d)
	if len(spares) != 0 || len(segs) != before {
		t.Fatalf("after %d new segments: %v and %v, want %d segments", wal.MaxSpares, segs, spares, before)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Root.Index != 7 || rec.Last != 9+wal.MaxSpares-1 {
		t.Fatalf("recovered %+v", rec)
	}
	if got := describe(read(t, w, rec)); !strings.HasPrefix(got, "8/1:v8 9/1:v9 10/1:v10 ") {
		t.Fatalf("entries after the root: %s", got)
	}
}

// TestStaleRecordsAfterTheEnd puts the records of an older segment, which
// chain among themselves, right after the last record of the newest one,
// as a reused file holds them: recovery cuts them off as a torn tail and
// does not take them for damage before valid records. A sealed segment
// never holds them: rotation cuts a reused file to its records.
func TestStaleRecordsAfterTheEnd(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 4)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	older := d.DurableBytes(segs[1])
	rewrite(t, d, segs[len(segs)-1], func(b []byte) []byte { return append(b, older[24:]...) })
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:v1 2/1:v2 3/1:v3 4/1:v4 " {
		t.Fatalf("entries: %s", got)
	}
}

// v1Segment returns segment 1 in format version 1: entries 1 and 2 and a
// hard state, chained from crc 0 without the sequence number.
func v1Segment() []byte {
	table := crc32.MakeTable(crc32.Castagnoli)
	b := []byte("tigerwal")
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = binary.LittleEndian.AppendUint32(b, 0)
	crc := uint32(0)
	rec := func(typ byte, payload []byte) {
		start := len(b)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
		b = append(b, 0, 0, 0, 0, typ)
		b = append(b, payload...)
		crc = crc32.Update(crc, table, b[start+8:])
		binary.LittleEndian.PutUint32(b[start+4:], crc)
	}
	for i := uint64(1); i <= 2; i++ {
		var p []byte
		p = binary.LittleEndian.AppendUint64(p, 1)
		p = binary.LittleEndian.AppendUint64(p, i)
		p = append(p, 0)
		rec(1, append(p, fmt.Sprintf("v%d", i)...))
	}
	var hs []byte
	hs = binary.LittleEndian.AppendUint64(hs, 1)
	hs = binary.LittleEndian.AppendUint64(hs, 2)
	hs = binary.LittleEndian.AppendUint64(hs, 2)
	rec(2, hs)
	return b
}

// TestReadsVersion1 opens a log in format version 1, appends to its
// newest segment in that format, and starts the next one in the new.
func TestReadsVersion1(t *testing.T) {
	d := simfs.New()
	f, err := d.Create("0000000000000001.wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(v1Segment()); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Sync(), f.Close(), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Last != 2 || rec.HardState != (wal.HardState{Term: 1, Vote: 2, Commit: 2}) {
		t.Fatalf("recovered %+v", rec)
	}
	fill(t, w, 3, 3)
	w.SetSegmentBytes(1)
	fill(t, w, 4, 5)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec = open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:v1 2/1:v2 3/1:v3 4/1:v4 5/1:v5 " {
		t.Fatalf("entries: %s", got)
	}
	if b := d.DurableBytes("0000000000000002.wal"); len(b) < 12 || binary.LittleEndian.Uint32(b[8:]) != wal.Version {
		t.Fatalf("the next segment is not in version %d", wal.Version)
	}
}

// TestCrashWithSpares writes, names roots, removes the segments below
// them, so that new segments reuse their files, and crashes, also in the
// middle of a sync or a new segment, and in recovery. Recovery must
// return the synced entries after the root plus a prefix of the unsynced
// ones, and never call the log corrupt.
func TestCrashWithSpares(t *testing.T) {
	spared := 0
	for seed := range uint64(500) {
		rng := rand.New(rand.NewPCG(seed, 11))
		d := simfs.New()
		w, _ := open(t, d)
		size := int64(64 + rng.IntN(256))
		w.SetSegmentBytes(size)
		// log[i] is entry i+1; synced is the part that is surely durable.
		var log, synced []string
		var pending []*raftpb.Entry
		var root uint64
		term := uint64(1)
		madeSpares := false
		sync := func() error {
			err := w.Sync()
			if err == nil {
				synced, pending = slices.Clone(log), nil
			}
			return err
		}
		crash := func() {
			d.Crash(rng)
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
			w.SetSegmentBytes(size)
			if rec.Root.Index < root {
				t.Fatalf("seed %d: root %d recovered, %d was synced", seed, rec.Root.Index, root)
			}
			got := describe(read(t, w, rec))
			want := func(l []string) string {
				var b strings.Builder
				for i := rec.Root.Index; i < uint64(len(l)); i++ {
					fmt.Fprintf(&b, "%d/%s ", i+1, l[i])
				}
				return b.String()
			}
			l := slices.Clone(synced)
			match := slices.Clone(l)
			ok := got == want(l)
			for _, e := range pending {
				l = append(l[:e.GetIndex()-1:e.GetIndex()-1], fmt.Sprintf("%d:%s", e.GetTerm(), e.GetData()))
				if !ok && got == want(l) {
					ok, match = true, slices.Clone(l)
				}
			}
			if !ok {
				t.Fatalf("seed %d: recovered %s, synced %s, pending %s", seed, got, want(synced), describe(pending))
			}
			log, synced, pending, root = match, slices.Clone(match), nil, rec.Root.Index
		}
		for range 40 + rng.IntN(40) {
			switch r := rng.IntN(10); {
			case r == 0:
				crash()
				continue
			case r == 1 && len(synced) > int(root)+1:
				// A root at a synced entry, then the removal.
				index := root + 1 + uint64(rng.IntN(len(synced)-int(root)))
				w.AppendRoot(wal.Root{Index: index, Term: 1})
				if err := sync(); err != nil {
					t.Fatal(err)
				}
				root = index
				if err := w.RemoveBelow(root); err != nil {
					t.Fatal(err)
				}
				if _, sp := files(t, d); len(sp) > 0 && !madeSpares {
					madeSpares = true
					spared++
				}
				continue
			case r == 2:
				// A fault at one of the next writing operations of a sync,
				// which may start a new segment; then the node stops.
				failFrom(d, 1+rng.IntN(5))
				_ = sync()
				d.Fault = nil
				crash()
				continue
			}
			// Append a batch, sometimes replacing a suffix after the root.
			last := uint64(len(log))
			start := last + 1
			if last > root+1 && rng.IntN(4) == 0 {
				start = last - uint64(rng.IntN(int(min(last-root-1, 3))))
				term++
			}
			var ents []*raftpb.Entry
			for i := start; i < start+uint64(1+rng.IntN(4)); i++ {
				e := entry(term, i, fmt.Sprintf("v%d", i))
				ents = append(ents, e)
				log = append(log[:i-1:i-1], fmt.Sprintf("%d:v%d", term, i))
			}
			w.Append(&wal.HardState{Term: term}, ents)
			pending = append(pending, ents...)
			if rng.IntN(3) > 0 {
				if err := sync(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if spared < 400 {
		t.Fatalf("only %d of 500 seeds made spares", spared)
	}
}

// TestSpareOfAnUnsyncedRemoval renames a segment to a spare without
// syncing the directory, as the cleaner does before its own sync, and
// lets the next segment take the spare and crash before its directory
// sync. The removal must be durable before the spare gets a new header:
// else the old name comes back with the new segment's header, and the log
// does not open.
func TestSpareOfAnUnsyncedRemoval(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 6)
	w.AppendRoot(wal.Root{Index: 5, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	if err := d.Rename(segs[0], "spare-0000000000000001"); err != nil {
		t.Fatal(err)
	}
	renamed := false
	d.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpRename {
			renamed = true
		}
		if op == simfs.OpSyncDir && renamed {
			return errCrash
		}
		return nil
	}
	w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: 7}, []*raftpb.Entry{entry(1, 7, "v7")})
	if err := w.Sync(); !errors.Is(err, errCrash) {
		t.Fatalf("sync into the spare = %v", err)
	}
	d.Fault = nil
	d.Crash(rand.New(rand.NewPCG(1, 2)))
	_, rec := open(t, d)
	if rec.Root.Index != 5 || rec.Last < 6 {
		t.Fatalf("recovered %+v", rec)
	}
}

// TestPrepareSegment readies segment files under names of their own: a
// new file, never the file of the segment before, which may wait for its
// name, a file readied before, and the oldest spare. A spare that a
// removal takes meanwhile is skipped, and a file readied for the same
// segment before is removed.
func TestPrepareSegment(t *testing.T) {
	d := simfs.New()
	names := func() string {
		t.Helper()
		ns, err := d.List()
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(ns, " ")
	}
	prepare := func(seq uint64, want string) {
		t.Helper()
		if err := wal.PrepareSegment(d, seq); err != nil {
			t.Fatal(err)
		}
		if got := names(); got != want {
			t.Fatalf("after readying %d: %s, want %s", seq, got, want)
		}
	}
	prepare(5, "next-0000000000000005")
	prepare(6, "next-0000000000000005 next-0000000000000006")
	for _, name := range []string{"spare-0000000000000001", "spare-0000000000000002"} {
		f, err := d.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	prepare(8, "next-0000000000000006 next-0000000000000008 spare-0000000000000001 spare-0000000000000002")
	prepare(9, "next-0000000000000008 next-0000000000000009 spare-0000000000000001 spare-0000000000000002")
	if err := errors.Join(d.Remove("next-0000000000000008"), d.Remove("next-0000000000000009")); err != nil {
		t.Fatal(err)
	}
	d.Fault = func(op simfs.Op, name string) error {
		if op == simfs.OpRename && name == "spare-0000000000000001" {
			d.Fault = nil
			return d.Remove(name)
		}
		return nil
	}
	prepare(10, "next-000000000000000a")
	// A file readied for 10 before, as a crash leaves it, is removed: it
	// may hold records of 10.
	rewrite(t, d, "next-000000000000000a", func([]byte) []byte { return []byte("records of 10") })
	prepare(10, "next-000000000000000a")
	if b := d.Files()["next-000000000000000a"]; len(b) > 0 {
		t.Fatalf("readied 10 again in the file that held %q", b)
	}
	prepare(11, "next-000000000000000a next-000000000000000b")
}

// TestFileOfALostSegmentName crashes twice before the name of segment 2
// is durable, as seed 8326003 of #274 did. The first crash leaves records
// of segment 2 in the file readied for it. That file must not come back
// for segment 2 through the file readied for segment 3 in between: the new
// segment's records would end before the old ones, and the log would not
// open.
func TestFileOfALostSegmentName(t *testing.T) {
	d := simfs.New()
	rng := rand.New(rand.NewPCG(1, 2))
	w, _ := open(t, d)
	fill(t, w, 1, 2)
	// start readies the next segment's file, as a node does when it starts.
	start := func() {
		t.Helper()
		if err := wal.PrepareSegment(d, w.Seq()+1); err != nil {
			t.Fatal(err)
		}
	}
	// rotate starts the next segment with entry i in the old one, as the
	// node's disk goroutine does, and readies the file of the segment after
	// it before the new segment takes its name. It leaves the name for a
	// directory sync.
	rotate := func(i uint64) {
		t.Helper()
		w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: i}, []*raftpb.Entry{entry(1, i, fmt.Sprintf("v%d", i))})
		if err := errors.Join(w.Flush(), w.Seal()); err != nil {
			t.Fatal(err)
		}
		old := w.Rotate()
		if err := errors.Join(wal.PrepareSegment(d, w.Seq()+1), old.Sync(), old.Close(), w.Place()); err != nil {
			t.Fatal(err)
		}
	}
	start()
	rotate(3)
	// Records of the same sizes as the new segment's, so that one of them
	// starts where the new segment ends.
	for i := uint64(4); i <= 8; i++ {
		w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: i}, []*raftpb.Entry{entry(1, i, fmt.Sprintf("v%d", i))})
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Segment().Sync(); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	w, _ = open(t, d)
	start()
	rotate(4)
	d.Crash(rng)
	w, _ = open(t, d)
	start()
	rotate(5)
	w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: 6}, []*raftpb.Entry{entry(1, 6, "v6")})
	if err := errors.Join(w.Flush(), w.Segment().Sync(), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:v1 2/1:v2 3/1:v3 4/1:v4 5/1:v5 6/1:v6 " {
		t.Fatalf("entries: %s", got)
	}
}

// TestDamagedHeaderBeforeRecords opens logs whose newest segment has a
// damaged header: with a record of its own after it, in format 1 too, or
// as the only segment left when it is not segment 1, the log is corrupt;
// with only another segment's old records after it, as a reused spare
// holds them, the log starts the segment again.
func TestDamagedHeaderBeforeRecords(t *testing.T) {
	t.Run("format 1", func(t *testing.T) {
		d := simfs.New()
		b := v1Segment()
		b[0] ^= 1
		write(t, d, "0000000000000001.wal", b)
		wantCorrupt(t, d)
	})
	t.Run("only segment", func(t *testing.T) {
		d := simfs.New()
		w, _ := open(t, d)
		w.SetSegmentBytes(1)
		fill(t, w, 1, 4)
		w.AppendRoot(wal.Root{Index: 4, Term: 1})
		w.Append(&wal.HardState{Term: 1, Commit: 5}, []*raftpb.Entry{entry(1, 5, "v5")})
		if err := errors.Join(w.Sync(), w.Close()); err != nil {
			t.Fatal(err)
		}
		// Keep only the segment with the root and entry 5, as removal
		// keeps the one before a segment that never started.
		segs, _ := files(t, d)
		keep := segs[len(segs)-2]
		for _, name := range segs {
			if name != keep {
				if err := errors.Join(d.Remove(name), d.SyncDir()); err != nil {
					t.Fatal(err)
				}
			}
		}
		rewrite(t, d, keep, func(b []byte) []byte { b[0] ^= 1; return b })
		wantCorrupt(t, d)
	})
	t.Run("old spare bytes", func(t *testing.T) {
		d := simfs.New()
		names := segments(t, d, 2)
		old := d.DurableBytes(names[0])
		rewrite(t, d, names[len(names)-1], func([]byte) []byte {
			b := append([]byte("tigerwaX"), make([]byte, 12)...)
			return append(b, old[20:]...)
		})
		_, rec := open(t, d)
		if rec.Last != 2 {
			t.Fatalf("recovered entries to %d, want 2", rec.Last)
		}
	})
}

// write creates file name on d with b in it, durable.
func write(t *testing.T, d *simfs.Disk, name string, b []byte) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Sync(), f.Close(), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
}

// TestDropKeepsBeforeUnplaced drops old segments while the newest waits
// for Place: the segment before the newest may not be durable yet, so the
// one before that, which holds the last durable hard state and root,
// stays too. Cleaning keeps it as well.
func TestDropKeepsBeforeUnplaced(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 4)
	w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: 4}, []*raftpb.Entry{entry(1, 5, "v5")})
	if err := errors.Join(w.Flush(), w.Seal(), wal.PrepareSegment(d, w.Seq()+1)); err != nil {
		t.Fatal(err)
	}
	old := w.Rotate()
	var clean []uint64
	for _, f := range w.Cleanable(100) {
		clean = append(clean, f.N)
	}
	if fmt.Sprint(clean) != "[1 2 3]" {
		t.Fatalf("cleanable segments %v with segment 6 unplaced", clean)
	}
	names, err := w.DropBelow(100)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(names, " "); got != "0000000000000001.wal 0000000000000002.wal 0000000000000003.wal" {
		t.Fatalf("dropped %s with segment 6 unplaced", got)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
}
