package node

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// blockedWriteFS holds the second append before its write reaches the OS.
// The segment header is the first WriteAt, and the first append is next.
type blockedWriteFS struct {
	wal.FS
	blocked, release chan struct{}
}

func (fs blockedWriteFS) Create(name string) (wal.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &blockedWriteFile{File: f, blocked: fs.blocked, release: fs.release}, nil
}

type blockedWriteFile struct {
	wal.File
	writes           int
	blocked, release chan struct{}
}

func (f *blockedWriteFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if f.writes == 3 {
		close(f.blocked)
		<-f.release
	}
	return f.File.WriteAt(p, off)
}

// A later append must not hold back an earlier durable append's response.
// With the old pipeline, persist waits inside the second Flush and cannot
// report the first sync, so a leader cannot count its durable copy toward
// commit. The channels force that ordering without a slow real disk.
func TestPersistReportsSyncDuringNextWrite(t *testing.T) {
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	w, _, err := wal.Open(blockedWriteFS{FS: dir, blocked: blocked, release: release})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	syncing := make(chan struct{})
	var first sync.Once
	n := &Node{
		cfg: Config{Metrics: m, Tick: 100 * time.Millisecond, BeforeSync: func() {
			first.Do(func() { close(syncing) })
			select {
			case <-blocked:
			case <-ctx.Done():
			}
		}},
		log:       &diskLog{w: w},
		persistQ:  make(chan persistBatch),
		persisted: make(chan persistDone, 1),
	}
	stopped := make(chan struct{})
	go func() {
		n.persist(ctx, nil)
		close(stopped)
	}()
	var unblock sync.Once
	defer func() {
		unblock.Do(func() { close(release) })
		cancel()
		<-stopped
	}()
	appendEntry := func(index uint64) {
		t.Helper()
		job := core.PersistJob{Entries: []*raftpb.Entry{{
			Term: new(uint64(1)), Index: new(index), Data: bytes.Repeat([]byte("v"), 1536<<10),
		}}}
		select {
		case n.persistQ <- persistBatch{jobs: []core.PersistJob{job}, queued: time.Now()}:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	appendEntry(1)
	select {
	case <-syncing:
	case <-ctx.Done():
		t.Fatal("first append never reached sync")
	}
	appendEntry(2)
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("second append never reached its blocked write")
	}
	receive := func(index uint64) {
		t.Helper()
		select {
		case done := <-n.persisted:
			if done.err != nil {
				t.Fatal(done.err)
			}
			if len(done.jobs) != 1 || len(done.jobs[0].Entries) != 1 || done.jobs[0].Entries[0].GetIndex() != index {
				t.Fatalf("persist response = %+v, want entry %d", done, index)
			}
		case <-ctx.Done():
			t.Fatalf("durable entry %d waited behind the next write", index)
		}
	}
	receive(1)
	select {
	case done := <-n.persisted:
		t.Fatalf("reported the blocked write before it was durable: %+v", done)
	default:
	}
	unblock.Do(func() { close(release) })
	receive(2)
}
