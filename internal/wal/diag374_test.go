package wal_test

import (
	"errors"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Question: can a failed sync and failed cut leave a hole behind later
// successful syncs when recovery keeps the page cache? Diagnosis only.
// Run: go test ./internal/wal -run '^TestDiag374FailedCut$' -v
func TestDiag374FailedCut(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.Append(&wal.HardState{Term: 1, Vote: 1, Commit: 1}, []*raftpb.Entry{entry(1, 1, "a")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	end := w.Size()
	failedSync, failedCut := false, false
	d.Fault = func(op simfs.Op, name string) error {
		if !strings.HasSuffix(name, ".wal") {
			return nil
		}
		if op == simfs.OpSync && !failedSync {
			failedSync = true
			return simfs.ErrIO
		}
		if op == simfs.OpTruncate && !failedCut {
			failedCut = true
			return simfs.ErrIO
		}
		return nil
	}
	w.Append(&wal.HardState{Term: 2, Vote: 1, Commit: 1}, nil)
	if err := w.Sync(); !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("failed sync returned %v", err)
	}
	if !failedSync || !failedCut {
		t.Fatal("did not exercise both errors")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// No power loss: recovery sees the bytes the failed writeback left
	// readable, and considers them synced. A later entry seals the segment.
	w, _ = open(t, d)
	w.SetSegmentBytes(1)
	w.Append(&wal.HardState{Term: 3, Vote: 1, Commit: 2}, []*raftpb.Entry{entry(3, 2, "b")})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec, err := wal.Open(d.Durable())
	if err != nil {
		t.Fatalf("cold recovery, failed record at byte %d: %v", end, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if rec.Last != 2 {
		t.Fatalf("recovered to %d, want 2", rec.Last)
	}
}
