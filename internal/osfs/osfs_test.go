package osfs_test

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestWALOnDisk(t *testing.T) {
	dir, err := osfs.New(t.TempDir() + "/wal")
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
	if len(rec.Entries) != 20 || rec.HardState.Commit != 20 {
		t.Fatalf("recovered %d entries, commit %d", len(rec.Entries), rec.HardState.Commit)
	}
	names, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 5 {
		t.Fatalf("expected rotation into several segments, got %v", names)
	}
}
