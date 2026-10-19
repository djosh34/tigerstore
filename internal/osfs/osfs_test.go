package osfs_test

import (
	"bytes"
	"runtime"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

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
