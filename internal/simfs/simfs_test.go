package simfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestCrashKeepsOnlyDurableState(t *testing.T) {
	d := simfs.New()
	rng := rand.New(rand.NewPCG(1, 1))
	a, err := d.Create("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("synced")); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Create("b"); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "a" {
		t.Fatalf("names after crash = %v, want [a]", names)
	}
	f, err := d.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "synced" {
		t.Fatalf("content after crash = %q", data)
	}

	// A truncate below the synced size is undone by a crash.
	if err := f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	f, err = d.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := f.ReadAll(); string(data) != "synced" {
		t.Fatalf("content after unsynced truncate and crash = %q", data)
	}
}

// TestCopiesKeepFileIDs checks that a file created on a copy of a disk
// gets an id no copied file has, so a trace tells them apart.
func TestCopiesKeepFileIDs(t *testing.T) {
	d := simfs.New()
	if _, err := d.Create("a"); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*simfs.Disk{"Durable": d.Durable(), "Copy": d.Copy()} {
		var lines []string
		c.Trace = func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
		if _, err := c.Create("b"); err != nil {
			t.Fatal(err)
		}
		if len(lines) != 1 || lines[0] != "create b (file 2)" {
			t.Errorf("%s: creating b traced %q, want \"create b (file 2)\"", name, lines)
		}
	}
}

// ok fails the test on an error.
func ok(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func create(t *testing.T, d *simfs.Disk, name string) wal.File {
	t.Helper()
	f, err := d.Create(name)
	ok(t, err)
	return f
}

func open(t *testing.T, d *simfs.Disk, name string) wal.File {
	t.Helper()
	f, err := d.Open(name)
	ok(t, err)
	return f
}

func readAll(t *testing.T, f wal.File) []byte {
	t.Helper()
	b, err := f.ReadAll()
	ok(t, err)
	return b
}

func writeAt(t *testing.T, f wal.File, p []byte, off int64) {
	t.Helper()
	_, err := f.WriteAt(p, off)
	ok(t, err)
}

func write(t *testing.T, f wal.File, p []byte) {
	t.Helper()
	_, err := f.Write(p)
	ok(t, err)
}

// TestCrashImages crashes a file with a synced part and four unsynced
// writes, each of a byte value of its own, many times with each image.
// Every image keeps the synced part. Prefix keeps a prefix of the bytes
// written, in order, up to garbage at its end past the synced part; Writes keeps each write
// whole or not at all; Sectors and Pages keep pieces of a write at their
// boundaries. Every image but Prefix sometimes keeps a later write and
// loses an earlier one.
func TestCrashImages(t *testing.T) {
	writes := []struct {
		off, n int
		b      byte
	}{{1000, 1500, 'a'}, {2500, 5000, 'b'}, {7500, 300, 'c'}, {200, 600, 'd'}}
	for img := simfs.Prefix; img <= simfs.Pages; img++ {
		reordered, torn := false, false
		for seed := range uint64(200) {
			d := simfs.New()
			f := create(t, d, "f")
			ok(t, d.SyncDir())
			write(t, f, bytes.Repeat([]byte{'s'}, 1000))
			ok(t, f.Sync())
			for _, w := range writes {
				writeAt(t, f, bytes.Repeat([]byte{w.b}, w.n), int64(w.off))
			}
			d.CrashImage(rand.New(rand.NewPCG(seed, 9)), img)
			got := readAll(t, open(t, d, "f"))
			// want is what each byte holds if its write was kept.
			want := bytes.Repeat([]byte{'s'}, 1000)
			for _, w := range writes {
				if end := w.off + w.n; end > len(want) {
					want = append(want, make([]byte, end-len(want))...)
				}
				copy(want[w.off:], bytes.Repeat([]byte{w.b}, w.n))
			}
			for i := range min(1000, len(got)) {
				// An overwrite keeps the old bytes or the new, never garbage.
				if got[i] != 's' && got[i] != 'd' {
					t.Fatalf("%v seed %d: synced byte %d is %q", img, seed, i, got[i])
				}
			}
			if len(got) < 1000 {
				t.Fatalf("%v seed %d: %d bytes kept, 1000 were synced", img, seed, len(got))
			}
			kept := map[byte]int{}
			for i := 1000; i < len(got); i++ {
				if got[i] != 0 {
					kept[got[i]]++
				}
				if img != simfs.Prefix && got[i] != 0 && got[i] != want[i] {
					t.Fatalf("%v seed %d: byte %d is %q, its write wrote %q", img, seed, i, got[i], want[i])
				}
			}
			switch img {
			case simfs.Prefix:
				// The writes kept in order: a later one only once every
				// earlier one is whole. Garbage holds a few bytes of each
				// value.
				if kept['b'] > 100 && kept['a'] < 1500 {
					t.Fatalf("prefix seed %d kept b with %d bytes of a", seed, kept['a'])
				}
			case simfs.Writes:
				if n := kept['b']; n != 0 && n != 5000 {
					t.Fatalf("whole writes seed %d kept %d of b's 5000 bytes", seed, n)
				}
			case simfs.Sectors, simfs.Pages:
				// Any piece may be kept.
			case simfs.Unwritten:
				// Not among those drawn here (TestUnwrittenImage).
			}
			reordered = reordered || kept['a'] == 0 && kept['b'] > 0
			torn = torn || kept['b'] > 0 && kept['b'] < 5000
		}
		if img != simfs.Prefix && !reordered {
			t.Errorf("%v: no crash kept a later write and lost an earlier one", img)
		}
		if (img == simfs.Sectors || img == simfs.Pages) && !torn {
			t.Errorf("%v: no crash tore a write", img)
		}
	}
}

// TestDirectoryOpsSurviveOnTheirOwn checks that each creation, rename and
// removal since the last directory sync survives a crash or not on its
// own, and that every combination comes up.
func TestDirectoryOpsSurviveOnTheirOwn(t *testing.T) {
	seen := map[string]bool{}
	for seed := range uint64(100) {
		d := simfs.New()
		create(t, d, "a")
		create(t, d, "b")
		ok(t, d.SyncDir())
		create(t, d, "c")
		ok(t, d.Rename("a", "a2"))
		ok(t, d.Remove("b"))
		d.Crash(rand.New(rand.NewPCG(seed, 3)))
		seen[fmt.Sprint(list(t, d))] = true
	}
	for _, want := range []string{"[a b]", "[a b c]", "[a2 b]", "[a]", "[a2 c]", "[a c]"} {
		if !seen[want] {
			t.Errorf("no crash left %s; got %v", want, seen)
		}
	}
	for names := range seen {
		if strings.Contains(names, "a ") && strings.Contains(names, "a2") {
			t.Errorf("a crash kept both names of a renamed file: %s", names)
		}
	}
}

// TestFailedSyncKeepsPart checks that a sync that fails after part of the
// writes reached the disk keeps that part, sometimes all and sometimes
// none, and that a later sync never makes the rest durable.
func TestFailedSyncKeepsPart(t *testing.T) {
	sizes := map[int]bool{}
	for seed := range uint64(50) {
		d := simfs.New()
		f := create(t, d, "f")
		ok(t, d.SyncDir())
		for range 3 {
			write(t, f, bytes.Repeat([]byte{'x'}, 700))
		}
		d.FailSync(rand.New(rand.NewPCG(seed, 5)))
		if err := f.Sync(); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("seed %d: failed sync = %v", seed, err)
		}
		write(t, f, []byte("later"))
		ok(t, f.Sync())
		d.CrashImage(rand.New(rand.NewPCG(seed, 6)), simfs.Prefix)
		got := readAll(t, open(t, d, "f"))
		if len(got) != 2105 || string(got[2100:]) != "later" {
			t.Fatalf("seed %d: %d bytes after the crash, ending %q", seed, len(got), got[max(0, len(got)-5):])
		}
		sizes[bytes.Count(got, []byte{'x'})] = true
	}
	if !sizes[0] || !sizes[2100] || len(sizes) < 3 {
		t.Errorf("failed syncs kept %v bytes; want none, all and some", sizes)
	}
}

// TestDropCache fails a sync, which leaves its bytes readable but not on
// the disk, and drops the page cache: the clean page reads what the disk
// holds in their place, zeros past its end, and a page with a write since
// the failed sync stays whole, as on Linux (#372).
func TestDropCache(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		d := simfs.New()
		f := create(t, d, "f")
		ok(t, d.SyncDir())
		write(t, f, []byte("aaaa"))
		ok(t, f.Sync())
		write(t, f, []byte("bbbb"))
		d.Fault = func(op simfs.Op, _ string) error {
			if op == simfs.OpSync {
				return simfs.ErrIO
			}
			return nil
		}
		if err := f.Sync(); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("failed sync = %v", err)
		}
		d.Fault = nil
		want := "aaaa\x00\x00\x00\x00"
		if dirty {
			writeAt(t, f, []byte("cc"), 8)
			want = "aaaabbbbcc"
		}
		ok(t, f.DropCache())
		if got := readAll(t, f); string(got) != want {
			t.Errorf("dirty %v: after the drop, read %q, want %q", dirty, got, want)
		}
	}
}

// TestShutdownAndReadOnly checks that a shut-down file system fails every
// operation, a read-only one every change, and that Remount ends both.
func TestShutdownAndReadOnly(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "f")
	write(t, f, []byte("abc"))
	d.Shutdown()
	if _, err := f.ReadAll(); !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("read after shutdown = %v", err)
	}
	if _, err := d.List(); !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("list after shutdown = %v", err)
	}
	d.Remount()
	d.ReadOnly()
	if got := readAll(t, f); string(got) != "abc" {
		t.Fatalf("read on a read-only disk = %q", got)
	}
	for name, err := range map[string]error{
		"write": func() error { _, err := f.Write([]byte("d")); return err }(),
		"sync":  f.Sync(), "create": func() error { _, err := d.Create("g"); return err }(),
		"remove": d.Remove("f"), "rename": d.Rename("f", "g"), "sync-dir": d.SyncDir(), "truncate": f.Truncate(0),
	} {
		if !errors.Is(err, simfs.ErrReadOnly) {
			t.Errorf("%s on a read-only disk = %v", name, err)
		}
	}
	d.Remount()
	write(t, f, []byte("d"))
}

// TestCapacity checks that a write past the capacity stores what fits and
// fails with ErrNoSpace.
func TestCapacity(t *testing.T) {
	d := simfs.New()
	d.Capacity = 100
	a := create(t, d, "a")
	write(t, a, make([]byte, 60))
	b := create(t, d, "b")
	n, err := b.Write(make([]byte, 50))
	if n != 40 || !errors.Is(err, simfs.ErrNoSpace) {
		t.Fatalf("write past the capacity = %d, %v; want 40, ErrNoSpace", n, err)
	}
	if n, err := a.WriteAt([]byte("over"), 10); n != 4 || err != nil {
		t.Fatalf("overwrite on a full disk = %d, %v", n, err)
	}
	if n, err := b.WriteAt([]byte("x"), 80); n != 0 || !errors.Is(err, simfs.ErrNoSpace) || d.Bytes() != 100 {
		t.Fatalf("write past the end of a full disk = %d, %v, %d bytes", n, err, d.Bytes())
	}
}

// TestBadSector checks that a bad sector fails every read that covers it,
// across a crash, until a write covers it.
func TestBadSector(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "f")
	ok(t, d.SyncDir())
	write(t, f, bytes.Repeat([]byte{'x'}, 600))
	ok(t, f.Sync())
	if !d.BadSector(rand.New(rand.NewPCG(1, 1)), "f") {
		t.Fatal("no bad sector")
	}
	if _, err := f.ReadAll(); !errors.Is(err, simfs.ErrBadSector) || !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("read of a bad sector = %v", err)
	}
	d.Crash(rand.New(rand.NewPCG(1, 2)))
	f = open(t, d, "f")
	if _, err := f.ReadAll(); !errors.Is(err, simfs.ErrBadSector) {
		t.Fatalf("read of a bad sector after a crash = %v", err)
	}
	writeAt(t, f, bytes.Repeat([]byte{'y'}, 600), 0)
	readAll(t, f)
}

// TestMisread checks that a misread returns wrong synced bytes once.
func TestMisread(t *testing.T) {
	for seed := range uint64(20) {
		d := simfs.New()
		f := create(t, d, "f")
		want := make([]byte, 3000)
		for i := range want {
			want[i] = byte(i)
		}
		write(t, f, want)
		ok(t, f.Sync())
		if !d.Misread(rand.New(rand.NewPCG(seed, 1)), "f") {
			t.Fatal("no misread")
		}
		p := make([]byte, 2000)
		_, err := f.ReadAt(p, 500)
		ok(t, err)
		if bytes.Equal(p, want[500:2500]) {
			t.Fatalf("seed %d: the misread returned the right bytes", seed)
		}
		if got := readAll(t, f); !bytes.Equal(got, want) {
			t.Fatalf("seed %d: the read after the misread is wrong too", seed)
		}
	}
}

// TestLoseWrite checks that a lost write zeros one synced sector.
func TestLoseWrite(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "f")
	ok(t, d.SyncDir())
	write(t, f, bytes.Repeat([]byte{'x'}, 1500))
	ok(t, f.Sync())
	if !d.LoseWrite(rand.New(rand.NewPCG(1, 1)), "f") {
		t.Fatal("no lost write")
	}
	got := readAll(t, f)
	if n := bytes.Count(got, []byte{0}); len(got) != 1500 || n == 0 || n > simfs.SectorBytes {
		t.Fatalf("lost write left %d bytes, %d of them zero", len(got), n)
	}
}

func list(t *testing.T, d *simfs.Disk) []string {
	t.Helper()
	names, err := d.List()
	ok(t, err)
	return names
}

// TestTruncateUnsynced checks that writes a truncate cut off never reach
// the disk: a sync after it makes the cut size durable.
func TestTruncateUnsynced(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "f")
	ok(t, d.SyncDir())
	write(t, f, make([]byte, 256))
	write(t, f, make([]byte, 256))
	ok(t, f.Truncate(100))
	ok(t, f.Sync())
	if n := len(d.DurableBytes("f")); n != 100 {
		t.Fatalf("%d bytes durable after a truncate to 100 and a sync", n)
	}
}

// TestAllocate checks that space allocated past a file's size takes room
// on the disk, keeps the size, lets writes into it through on a full disk,
// fails whole where it does not fit, and goes with a truncate.
func TestAllocate(t *testing.T) {
	d := simfs.New()
	d.Capacity = 100
	a := create(t, d, "a")
	write(t, a, make([]byte, 10))
	al := a.(interface{ Allocate(int64) error })
	ok(t, al.Allocate(60))
	if d.Bytes() != 60 || d.Free() != 40 {
		t.Fatalf("a file of 10 bytes allocated to 60 takes %d, leaving %d", d.Bytes(), d.Free())
	}
	if b, err := a.ReadAll(); err != nil || len(b) != 10 {
		t.Fatalf("an allocation changed the size to %d, %v", len(b), err)
	}
	b := create(t, d, "b")
	if n, err := b.Write(make([]byte, 50)); n != 40 || !errors.Is(err, simfs.ErrNoSpace) {
		t.Fatalf("a write past the room left = %d, %v; want 40, ErrNoSpace", n, err)
	}
	if n, err := a.WriteAt(make([]byte, 50), 10); n != 50 || err != nil {
		t.Fatalf("a write into allocated space on a full disk = %d, %v", n, err)
	}
	if err := b.(interface{ Allocate(int64) error }).Allocate(41); !errors.Is(err, simfs.ErrNoSpace) || d.Bytes() != 100 {
		t.Fatalf("an allocation past the capacity = %v, %d bytes taken", err, d.Bytes())
	}
	ok(t, a.Truncate(20))
	if d.Bytes() != 60 {
		t.Fatalf("a truncate left %d bytes taken, want 60", d.Bytes())
	}
}

// TestUnwrittenImage checks that the Unwritten crash image keeps the
// synced bytes and the file's growth by the writes since, with zeros.
func TestUnwrittenImage(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "u")
	write(t, f, bytes.Repeat([]byte{1}, 10))
	ok(t, f.Sync())
	ok(t, d.SyncDir())
	if _, err := f.WriteAt(bytes.Repeat([]byte{2}, 15), 5); err != nil {
		t.Fatal(err)
	}
	d.CrashImage(rand.New(rand.NewPCG(1, 2)), simfs.Unwritten)
	g := open(t, d, "u")
	got, err := g.ReadAll()
	ok(t, err)
	want := append(bytes.Repeat([]byte{1}, 10), make([]byte, 10)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("after the crash the file holds %v, want %v", got, want)
	}
}

// TestATruncatePastTheEndGrowsTheFile checks that a truncate to a size
// past a file's end grows it with zeros, as truncate(2) does: a start of
// the log whose checkpoint left it past its last segment's end grows the
// segment to there and reads its last block back (#726). The growth is
// durable once synced, and a crash before keeps it or not, as it does a
// write.
// A rename of a file over a directory fails, as rename(2) fails with
// EISDIR, and changes nothing (#689's re-check of area A).
func TestARenameOverADirectoryFails(t *testing.T) {
	d := simfs.New()
	ok(t, d.Mkdir("dir"))
	create(t, d, "f")
	if err := d.Rename("f", "dir"); !errors.Is(err, simfs.ErrIsDir) {
		t.Fatalf("a rename of a file over a directory gave %v, want ErrIsDir", err)
	}
	if !d.IsDir("dir") {
		t.Fatal("the directory is gone")
	}
	if _, err := d.Open("dir"); err == nil {
		t.Fatal("a file named as the directory opens")
	}
	if _, err := d.Open("f"); err != nil {
		t.Fatalf("the file is gone: %v", err)
	}
}

func TestATruncatePastTheEndGrowsTheFile(t *testing.T) {
	d := simfs.New()
	f := create(t, d, "f")
	ok(t, d.SyncDir())
	write(t, f, []byte("abc"))
	ok(t, f.Sync())
	ok(t, f.Truncate(3+2*simfs.PageBytes))
	want := append([]byte("abc"), make([]byte, 2*simfs.PageBytes)...)
	if got := readAll(t, f); !bytes.Equal(got, want) {
		t.Fatalf("after a truncate past the end the file holds %d bytes, want %d: %q...", len(got), len(want), got[:min(8, len(got))])
	}
	p := make([]byte, 100)
	if n, err := f.ReadAt(p, simfs.PageBytes); n != len(p) || err != nil || !bytes.Equal(p, make([]byte, len(p))) {
		t.Fatalf("a read in the grown part = %d, %v, %x", n, err, p[:8])
	}
	if !d.Unsynced("f") {
		t.Fatal("the growth needs no sync")
	}
	lost, kept := false, false
	for i := range 64 {
		c := d.Copy()
		g := open(t, c, "f")
		ok(t, g.Truncate(3+3*simfs.PageBytes))
		c.Crash(rand.New(rand.NewPCG(uint64(i), 7)))
		switch got := readAll(t, open(t, c, "f")); {
		case bytes.Equal(got, []byte("abc")):
			lost = true
		case len(got) > 3 && len(got) <= 3+3*simfs.PageBytes && bytes.Equal(got[:3], []byte("abc")):
			// The growth kept, whole or in part, and as zeros: Linux
			// reads a hole as zeros, never as a torn write's garbage.
			if i := slices.IndexFunc(got[3:], func(b byte) bool { return b != 0 }); i >= 0 {
				t.Fatalf("a crash left byte %d of the growth %#x, not zero", 3+i, got[3+i])
			}
			kept = true
		default:
			t.Fatalf("a crash left %d bytes: %q...", len(got), got[:min(8, len(got))])
		}
	}
	if !lost || !kept {
		t.Fatalf("crashes before the sync lost the growth %v and kept it %v, want both", lost, kept)
	}
	ok(t, f.Sync())
	if got := d.DurableBytes("f"); !bytes.Equal(got, want) {
		t.Fatalf("after the sync %d bytes are durable, want %d", len(got), len(want))
	}
}
