package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// rotationFS holds one sync at a segment boundary. Its stage is set only
// after wal.Open, so the initial header and directory syncs are real.
type rotationFS struct {
	wal.FS
	stage            string
	created          int
	blocked, release chan struct{}
	err              error
	once             sync.Once
}

func (fs *rotationFS) hold() error {
	var err error
	fs.once.Do(func() {
		close(fs.blocked)
		<-fs.release
		err = fs.err
	})
	return err
}

func (fs *rotationFS) Create(name string) (wal.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	fs.created++
	return &rotationFile{File: f, fs: fs, number: fs.created}, nil
}

func (fs *rotationFS) SyncDir() error {
	if fs.created == 2 && fs.stage == "directory" {
		if err := fs.hold(); err != nil {
			return err
		}
	}
	return fs.FS.SyncDir()
}

type rotationFile struct {
	wal.File
	fs     *rotationFS
	number int
}

func (f *rotationFile) Sync() error {
	if f.number == 1 && f.fs.stage == "current" || f.number == 2 && f.fs.stage == "header" {
		if err := f.fs.hold(); err != nil {
			return err
		}
	}
	return f.File.Sync()
}

// An append in a full segment is durable before the next segment's header
// or directory sync. Neither may hold back its response. The next append
// must still wait for the new segment, and every sync error stops the node.
func TestPersistReportsBeforeRotation(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		fail        bool
	}{
		{name: "header", stage: "header"},
		{name: "directory", stage: "directory"},
		{name: "current-error", stage: "current", fail: true},
		{name: "header-error", stage: "header", fail: true},
		{name: "directory-error", stage: "directory", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := osfs.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs := &rotationFS{FS: dir, blocked: make(chan struct{}), release: make(chan struct{})}
			w, _, err := wal.Open(fs)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				// A failed rotation has already closed the old segment.
				if err := w.Close(); err != nil && (!tc.fail || !errors.Is(err, os.ErrClosed)) {
					t.Error(err)
				}
			}()
			w.SetSegmentBytes(1024)
			fs.stage = tc.stage
			injected := errors.New("rotation sync failed")
			if tc.fail {
				fs.err = injected
			}
			m, err := metrics.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			n := &Node{
				cfg:       Config{Metrics: m, Tick: 100 * time.Millisecond},
				log:       &diskLog{w: w},
				persistQ:  make(chan persistBatch, 1),
				persisted: make(chan persistDone, 1),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stopped := make(chan struct{})
			go func() {
				n.persist(ctx, nil)
				close(stopped)
			}()
			var release sync.Once
			defer func() {
				release.Do(func() { close(fs.release) })
				cancel()
				<-stopped
			}()
			job := func(index uint64, size int) persistBatch {
				return persistBatch{queued: time.Now(), jobs: []core.PersistJob{{Entries: []*raftpb.Entry{{
					Term: new(uint64(1)), Index: new(index), Data: bytes.Repeat([]byte("v"), size),
				}}}}}
			}
			n.persistQ <- job(1, 2048)
			select {
			case <-fs.blocked:
			case <-ctx.Done():
				t.Fatal("writer never reached the held sync")
			}
			if tc.stage != "current" {
				select {
				case done := <-n.persisted:
					if done.err != nil || len(done.jobs) != 1 || done.jobs[0].Entries[0].GetIndex() != 1 {
						t.Fatalf("first durable report: %+v", done)
					}
				case <-ctx.Done():
					t.Fatal("durable entry waited for the next segment")
				}
			}
			n.persistQ <- job(2, 1)
			select {
			case done := <-n.persisted:
				t.Fatalf("reported before the held sync finished: %+v", done)
			default:
			}
			release.Do(func() { close(fs.release) })
			select {
			case done := <-n.persisted:
				if tc.fail {
					if !errors.Is(done.err, injected) {
						t.Fatalf("sync error = %v, want %v", done.err, injected)
					}
				} else if done.err != nil || len(done.jobs) != 1 || done.jobs[0].Entries[0].GetIndex() != 2 {
					t.Fatalf("second durable report: %+v", done)
				}
			case <-ctx.Done():
				t.Fatal("no report after releasing the sync")
			}
			if tc.fail {
				select {
				case <-stopped:
				case <-ctx.Done():
					t.Fatal("writer did not stop after the sync error")
				}
				select {
				case done := <-n.persisted:
					t.Fatalf("reported after the sync error: %+v", done)
				default:
				}
			}
		})
	}
}
