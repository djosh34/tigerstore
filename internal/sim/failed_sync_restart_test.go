package sim

import (
	"errors"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Can recovery's fsync alone repair clean page-cache bytes after a failed
// writeback, a kill and a restart, before that segment is sealed?
func TestFailedSyncKillRestart(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, _ uint64) struct{} {
		d := simfs.New()
		d.Trace = func(line string) { t.Log(line) }
		w, _, err := wal.Open(d)
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&wal.HardState{Term: 1, Commit: 1}, []*raftpb.Entry{{Term: 1, Index: 1, Data: []byte("synced")}})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w.Append(&wal.HardState{Term: 2, Commit: 1}, nil)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		// Failed writeback cleans the page cache without persisting this
		// record. A kill retains those readable but no longer dirty bytes.
		d.Fault = func(op simfs.Op, _ string) error {
			if op == simfs.OpSync {
				return simfs.ErrIO
			}
			return nil
		}
		if err := w.Sync(); !errors.Is(err, simfs.ErrIO) {
			t.Fatalf("sync: %v, want EIO", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		d.Fault = nil
		w, rec, err := wal.Open(d)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("restart accepted term %d from page cache", rec.HardState.Term)
		// Append a later record and seal this segment. A later fsync does
		// not retry the old failed write, so without a rewrite it leaves
		// an interior zero-filled hole in the now-immutable segment.
		w.SetSegmentBytes(1)
		w.Append(&wal.HardState{Term: 3, Commit: 1}, nil)
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w.SetSegmentBytes(wal.SegmentBytes)
		w.Append(&wal.HardState{Term: 4, Commit: 1}, nil)
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		cold, rec, err := wal.Open(d.Durable())
		if err != nil {
			t.Fatalf("cold restart of undamaged disk: %v", err)
		}
		if err := cold.Close(); err != nil {
			t.Fatal(err)
		}
		if rec.HardState.Term != 4 {
			t.Fatalf("cold term %d, want 4", rec.HardState.Term)
		}
		t.Logf("cold restart recovered term %d", rec.HardState.Term)
		return struct{}{}
	}, nil)
}
