package osfs_test

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestWALOnDisk(t *testing.T) {
	dir, err := osfs.New(t.TempDir() + "/new/data/wal/")
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.SetSegmentBytes(100)
	for i := uint64(1); i <= 20; i++ {
		w.Append(&wal.HardState{Term: 1, Commit: i}, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(i), Data: []byte("value")}})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, rec, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Last != 20 || rec.HardState.Commit != 20 {
		t.Fatalf("recovered entries to %d, commit %d", rec.Last, rec.HardState.Commit)
	}
	names, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 5 {
		t.Fatalf("expected rotation into several segments, got %v", names)
	}
}

// TestReadAllMemory reads an 8 MiB segment back with one buffer of its
// size, as a restart reads every segment whole (#292).
func TestReadAllMemory(t *testing.T) {
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 8<<20)
	f, err := dir.Create("segment")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if f, err = dir.Open("segment"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := f.ReadAll()
	runtime.ReadMemStats(&after)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > uint64(len(data))+64<<10 {
		t.Fatalf("reading %d bytes allocated %d", len(data), alloc)
	}
}

// TestDropCacheReportsHeldPage splices a synced page of a file into a pipe,
// as a process that copies a segment with splice or sendfile does. Linux
// keeps the page in the page cache while the pipe holds it, and fadvise
// still succeeds: DropCache must report it, or recovery could read a page
// that a failed writeback left (#372). Once the pipe lets the page go, the
// drop succeeds.
func TestDropCacheReportsHeldPage(t *testing.T) {
	path := t.TempDir()
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		t.Fatal(err)
	}
	if st.Type == unix.TMPFS_MAGIC || st.Type == unix.RAMFS_MAGIC {
		t.Skip("the temporary directory is in memory, where DropCache drops nothing")
	}
	dir, err := osfs.New(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := dir.Create("seg")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	page := os.Getpagesize()
	if _, err := f.WriteAt(bytes.Repeat([]byte{'x'}, page), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	var p [2]int
	if err := unix.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := errors.Join(unix.Close(p[0]), unix.Close(p[1])); err != nil {
			t.Error(err)
		}
	}()
	off := int64(0)
	if _, err := unix.Splice(int(f.(interface{ Fd() uintptr }).Fd()), &off, p[1], nil, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.DropCache(); !errors.Is(err, osfs.ErrCached) {
		t.Fatalf("drop of a page a pipe holds = %v, want ErrCached", err)
	}
	if _, err := unix.Read(p[0], make([]byte, page)); err != nil {
		t.Fatal(err)
	}
	if err := f.DropCache(); err != nil {
		t.Fatalf("drop once the pipe let the page go = %v", err)
	}
}
