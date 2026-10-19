package wal_test

import (
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// cleanable returns the segments and packs that cleaning may replace
// below index.
func cleanable(w *wal.WAL, below uint64) (segs, packs []uint64) {
	for _, f := range w.Cleanable(below) {
		if f.Pack {
			packs = append(packs, f.N)
		} else {
			segs = append(segs, f.N)
		}
	}
	return segs, packs
}

// writePack copies the values at positions into a new pack.
func writePack(t *testing.T, w *wal.WAL, positions ...[3]uint32) *wal.Pack {
	t.Helper()
	pw, err := w.NewPack()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range positions {
		b, err := w.Value(uint64(p[0]), p[1], p[2])
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.Add(uint64(p[0]), p[1], b); err != nil {
			t.Fatal(err)
		}
	}
	p, err := pw.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func wantValue(t *testing.T, w *wal.WAL, index uint64, off, n uint32, want string) {
	t.Helper()
	if b, err := w.Value(index, off, n); err != nil || string(b) != want {
		t.Fatalf("value at %d+%d of entry %d = %q, %v; want %q", off, n, index, b, err, want)
	}
}

func packFiles(t *testing.T, d *simfs.Disk) []string {
	t.Helper()
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(names, func(n string) bool { return !strings.HasPrefix(n, "pack-") })
}

// TestCleaningSteps cleans the segments below a checkpoint at 6 that
// still names the values of entries 2 and 5, and checks after a crash at
// each step that recovery finds both values and the entries after the
// checkpoint, and keeps no pack that no root names.
func TestCleaningSteps(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 6, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, 9, 10)

	check := func(step string, d *simfs.Disk, packs int) {
		t.Helper()
		w, rec := open(t, d)
		wantValue(t, w, 2, 1, 1, "2")
		wantValue(t, w, 5, 0, 2, "v5")
		if got := describe(read(t, w, rec)); got != "7/1:v7 8/1:v8 9/1:v9 10/1:v10 " {
			t.Fatalf("%s: entries after the root: %s", step, got)
		}
		if got := packFiles(t, d); len(got) != packs {
			t.Fatalf("%s: packs on disk %v, want %d", step, got, packs)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	segs, packs := cleanable(w, 6)
	if len(segs) == 0 || len(packs) != 0 {
		t.Fatalf("cleanable below 6: segments %v, packs %v", segs, packs)
	}
	p := writePack(t, w, [3]uint32{2, 1, 1}, [3]uint32{5, 0, 2})
	check("pack written", d.Durable(), 0)
	w.InstallPacks(p, nil)
	check("root buffered", d.Durable(), 0)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	check("root synced", d.Durable(), 1)
	if err := w.RemoveCleaned(segs[len(segs)-1], nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Entry(2); !errors.Is(err, wal.ErrNotOnDisk) {
		t.Fatalf("entry 2 after cleaning: %v", err)
	}
	wantValue(t, w, 2, 1, 1, "2")
	if where, ok := w.Where(5, 0, 2); !ok || !where.Pack || where.N != p.ID() {
		t.Fatalf("value of entry 5 is in %+v, %v; want pack %d", where, ok, p.ID())
	}
	check("segments removed", d.Durable(), 1)
	if _, err := w.Value(3, 1, 1); !errors.Is(err, wal.ErrNotOnDisk) {
		t.Fatalf("value the checkpoint does not name: %v", err)
	}
}

// TestPackOpenMemory opens a 16 MiB pack and checks that recovery reads
// it in pieces: a node whose pack outgrew its memory was killed reading
// the whole file at restart (#292).
func TestPackOpenMemory(t *testing.T) {
	const values, size = 32, 512 << 10
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	var positions [][3]uint32
	for i := uint64(1); i <= values; i++ {
		w.Append(&wal.HardState{Term: 1, Commit: i}, []*raftpb.Entry{entry(1, i, strings.Repeat("x", size))})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		positions = append(positions, [3]uint32{uint32(i), 0, size})
	}
	w.AppendRoot(wal.Root{Index: values, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, values+1, values+1)
	segs, _ := cleanable(w, values)
	p := writePack(t, w, positions...)
	w.InstallPacks(p, nil)
	if err := errors.Join(w.Sync(), w.RemoveCleaned(segs[len(segs)-1], nil), w.Close()); err != nil {
		t.Fatal(err)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	w, _ = open(t, d)
	runtime.ReadMemStats(&after)
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > uint64(p.Size()/16) {
		t.Fatalf("opening a pack of %d bytes allocated %d bytes", p.Size(), alloc)
	}
	wantValue(t, w, values, 0, 4, "xxxx")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPackMerge replaces a pack with one that keeps only part of it: the
// old one goes only after the root that drops it.
func TestPackMerge(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 6, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, 9, 10)
	segs, _ := cleanable(w, 6)
	p1 := writePack(t, w, [3]uint32{2, 0, 2}, [3]uint32{3, 0, 2}, [3]uint32{4, 0, 2})
	w.InstallPacks(p1, nil)
	if err := errors.Join(w.Sync(), w.RemoveCleaned(segs[len(segs)-1], nil)); err != nil {
		t.Fatal(err)
	}
	_, packs := cleanable(w, 6)
	if !slices.Equal(packs, []uint64{p1.ID()}) {
		t.Fatalf("cleanable packs %v", packs)
	}
	p2 := writePack(t, w, [3]uint32{3, 0, 2})
	w.InstallPacks(p2, []uint64{p1.ID()})
	wantValue(t, w, 3, 0, 2, "v3")
	if _, err := w.Value(2, 0, 2); !errors.Is(err, wal.ErrNotOnDisk) {
		t.Fatalf("value only the dropped pack held: %v", err)
	}
	// Before the root is synced, recovery keeps the old pack.
	crashed := d.Durable()
	w2, _ := open(t, crashed)
	wantValue(t, w2, 2, 0, 2, "v2")
	if got := packFiles(t, crashed); len(got) != 1 {
		t.Fatalf("packs after a crash before the root: %v", got)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveCleaned(0, []uint64{p2.ID()}); err == nil {
		t.Fatal("removed a pack the root names")
	}
	if err := w.RemoveCleaned(0, []uint64{p1.ID()}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, _ = open(t, d)
	wantValue(t, w, 3, 0, 2, "v3")
	if got := packFiles(t, d); len(got) != 1 {
		t.Fatalf("packs after the merge: %v", got)
	}
	// A new pack gets a number no pack had, and one given up leaves no
	// file.
	if p3 := writePack(t, w, [3]uint32{3, 0, 2}); p3.ID() <= p2.ID() {
		t.Fatalf("a new pack after %d is %d", p2.ID(), p3.ID())
	}
	pw, err := w.NewPack()
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := packFiles(t, d); len(got) != 2 {
		t.Fatalf("pack files %v, want the merged one and the new one", got)
	}
}

// TestDamagedPackIsCorrupt flips each bit 0 of a pack that a synced root
// names, or removes it: the node must not start.
func TestDamagedPackIsCorrupt(t *testing.T) {
	setup := func() (*simfs.Disk, string, int) {
		d := simfs.New()
		w, _ := open(t, d)
		w.SetSegmentBytes(1)
		fill(t, w, 1, 4)
		w.AppendRoot(wal.Root{Index: 3, Term: 1})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		p := writePack(t, w, [3]uint32{1, 0, 2}, [3]uint32{2, 0, 2})
		w.InstallPacks(p, nil)
		if err := errors.Join(w.Sync(), w.Close()); err != nil {
			t.Fatal(err)
		}
		name := packFiles(t, d)[0]
		return d, name, len(d.DurableBytes(name))
	}
	// A flipped format version is a newer build's pack, as in a segment.
	_, _, size := setup()
	for off := range size {
		if off >= 8 && off < 12 {
			continue
		}
		d, name, _ := setup()
		d.FlipDurable(name, off)
		wantCorrupt(t, d)
	}
	d, name, _ := setup()
	if err := errors.Join(d.Remove(name), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
	wantCorrupt(t, d)
}

// TestPackOutlivesRemoval checks that the root copied into each new
// segment names the packs, so removing the segment with the root that
// installed them keeps them.
func TestPackOutlivesRemoval(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 4)
	w.AppendRoot(wal.Root{Index: 3, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	p := writePack(t, w, [3]uint32{1, 0, 2})
	w.InstallPacks(p, nil)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	fill(t, w, 5, 12)
	w.AppendRoot(wal.Root{Index: 10, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(11); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Root.Index != 10 {
		t.Fatalf("recovered %+v", rec)
	}
	wantValue(t, w, 1, 0, 2, "v1")
}

// TestMapShrinks checks that the map of where values are shrinks when
// cleaning removes the segments it replaced.
func TestMapShrinks(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 200)
	w.AppendRoot(wal.Root{Index: 199, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	before := w.MapBytes()
	segs, _ := cleanable(w, 199)
	p := writePack(t, w, [3]uint32{7, 0, 2})
	w.InstallPacks(p, nil)
	if err := errors.Join(w.Sync(), w.RemoveCleaned(segs[len(segs)-1], nil)); err != nil {
		t.Fatal(err)
	}
	if after := w.MapBytes(); after*4 > before {
		t.Fatalf("map bytes went from %d to %d", before, after)
	}
}
