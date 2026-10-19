//go:build chaosenv

package env

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// The snapshot install path flushes the WAL before fetching. If a fresh
// segment only has its header, that makes the header a separate unsynced
// write. A torn next write can then persist records without the header.
func TestDiag295HeaderOnlyFlush(t *testing.T) {
	d := newDisk(t, lazyfsBin(t))
	fs, err := osfs.New(filepath.Join(d.Data, "wal"))
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
	path := filepath.Join(d.Data, "wal", "0000000000000001.wal")
	if err := d.TornWrite(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	var entries []*raftpb.Entry
	for i := range 12 {
		entries = append(entries, &raftpb.Entry{Index: new(uint64(i + 1)), Term: new(uint64(1)), Data: bytes.Repeat([]byte("x"), 512)})
	}
	w.Append(&wal.HardState{Term: 1, Commit: 1}, entries)
	t.Logf("the torn write returned %v", w.Flush())
	if !waitFor(5*time.Second, d.Crashed) {
		t.Fatal("LazyFS did not crash")
	}
	// The old descriptors refer to the dead mount. Close without syncing.
	if err := w.Close(); err != nil {
		t.Logf("closing the dead mount: %v", err)
	}
	if err := d.Remount(); err != nil {
		t.Fatal(err)
	}
	t.Logf("LazyFS fault log:\n%s", d.lazyLog())
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after the tear: %d bytes, prefix %x", len(b), b[:min(40, len(b))])
	fs, err = osfs.New(filepath.Join(d.Data, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	recovered, _, err := wal.Open(fs)
	if err != nil {
		t.Fatalf("recover after the torn write: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}
