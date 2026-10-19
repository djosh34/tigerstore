package node

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// closeRaceFS pauses one completed header read, before WAL.Entry looks up
// the reader again for the body. Close can then delete that reader unless
// it takes diskLog.mu. Only the test's file call is delayed, not the WAL.
type closeRaceFS struct {
	wal.FS
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (fs *closeRaceFS) Open(name string) (wal.File, error) {
	f, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &closeRaceFile{File: f, fs: fs}, nil
}

type closeRaceFile struct {
	wal.File
	fs *closeRaceFS
}

func (f *closeRaceFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	if f.fs.armed.CompareAndSwap(true, false) {
		close(f.fs.entered)
		<-f.fs.release
	}
	return n, err
}

func TestSnapshotServeConcurrentClose(t *testing.T) {
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := &closeRaceFS{FS: dir, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	w, _, err := wal.Open(fs)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(nil, []*raftpb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("snapshot entry")}})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	l := &diskLog{w: w}
	// Populate readers, so Close has the exact map entry from #286 to delete.
	if _, err := l.Entry(1); err != nil {
		t.Fatal(err)
	}
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: Config{Metrics: m}, log: l, inbox: make(chan input, 8), done: make(chan struct{})}
	fs.armed.Store(true)
	served := make(chan error, 1)
	go func() { served <- n.Serve(2, 1, 1, io.Discard) }()
	select {
	case <-fs.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot did not enter WAL.Entry")
	}
	closed := make(chan error, 1)
	go func() { closed <- l.close() }()
	// Do not receive from closed before the second reader lookup: that
	// would add a test-only happens-before edge hiding the production race.
	// This short pause merely gives the buggy closer time to delete readers.
	time.Sleep(100 * time.Millisecond)
	release.Do(func() { close(fs.release) })
	if err := <-served; err != nil {
		t.Errorf("the in-flight entry read failed: %v", err)
	}
	if err := <-closed; err != nil {
		t.Errorf("close: %v", err)
	}
	// A mutex alone is insufficient: a later snapshot must not reopen
	// readers in an already closed WAL (which would also leak handles).
	if _, err := l.Entry(1); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Entry after close = %v, want os.ErrClosed", err)
	}
	if _, err := l.Value(1, 0, 1); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Value after close = %v, want os.ErrClosed", err)
	}
	if _, err := l.Checkpoint(1); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Checkpoint after close = %v, want os.ErrClosed", err)
	}
	if err := n.Serve(2, 1, 1, io.Discard); err == nil {
		t.Error("Serve after close succeeded")
	}
}
