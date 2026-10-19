//go:build chaosenv

package env

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestTornWriteAfterEmptyFlush flushes a new log before anything is
// appended, as the node does before a snapshot install, then tears the
// write of the first records. LazyFS keeps a part of the torn write and
// drops the unsynced writes before it. The log must open: nothing in it
// was synced (#295).
func TestTornWriteAfterEmptyFlush(t *testing.T) {
	d := newDisk(t, lazyfsBin(t))
	dir := filepath.Join(d.Data, "wal")
	fs, err := osfs.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := wal.Open(fs)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.TornWrite(filepath.Join(dir, wal.SegmentName(1))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	var ents []*raftpb.Entry
	for i := range uint64(12) {
		ents = append(ents, &raftpb.Entry{Term: new(uint64(1)), Index: new(i + 1), Data: bytes.Repeat([]byte("x"), 512)})
	}
	w.Append(&wal.HardState{Term: 1, Commit: 1}, ents)
	t.Logf("the torn write returned %v", w.Flush())
	if !waitFor(5*time.Second, d.Crashed) {
		t.Fatal("LazyFS did not crash after the torn write")
	}
	// The files are on the crashed mount, so closing them fails.
	t.Logf("closing the log on the crashed mount: %v", w.Close())
	if err := d.Remount(); err != nil {
		t.Fatal(err)
	}
	if fs, err = osfs.New(dir); err != nil {
		t.Fatal(err)
	}
	w, _, err = wal.Open(fs)
	if err != nil {
		t.Fatalf("open after the torn write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
