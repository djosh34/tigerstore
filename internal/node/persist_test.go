package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// persister runs Node.persist alone on a log in a temporary directory,
// seen through wrap, which holds an operation of the test's choice.
type persister struct {
	t       *testing.T
	n       *Node
	ctx     context.Context
	stopped chan struct{}
	// failing is set by a test whose sync fails, which closes the segment.
	failing bool
}

// startPersist opens the log, lets setup change it and the node's
// config, and runs persist until the test ends. release, run first at the
// end, lets go of what wrap holds.
func startPersist(t *testing.T, wrap func(wal.FS) wal.FS, setup func(*wal.WAL, *Config), release func()) *persister {
	t.Helper()
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := wal.Open(wrap(dir))
	if err != nil {
		t.Fatal(err)
	}
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Metrics: m, Tick: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	setup(w, &cfg)
	p := &persister{t: t, n: &Node{
		cfg:       cfg,
		log:       &diskLog{w: w},
		persistQ:  make(chan persistBatch, 1),
		persisted: make(chan persistDone, 1),
	}, ctx: ctx, stopped: make(chan struct{})}
	go func() {
		p.n.persist(ctx, nil)
		close(p.stopped)
	}()
	t.Cleanup(func() {
		release()
		cancel()
		<-p.stopped
		// A failed rotation has already closed the old segment.
		if err := w.Close(); err != nil && (!p.failing || !errors.Is(err, os.ErrClosed)) {
			t.Error(err)
		}
	})
	return p
}

// append hands persist one entry of size bytes.
func (p *persister) append(index uint64, size int) {
	p.t.Helper()
	b := persistBatch{queued: time.Now(), jobs: []core.PersistJob{{Entries: []*raftpb.Entry{{
		Term: new(uint64(1)), Index: new(index), Data: bytes.Repeat([]byte("v"), size),
	}}}}}
	select {
	case p.n.persistQ <- b:
	case <-p.ctx.Done():
		p.t.Fatal(p.ctx.Err())
	}
}

// wait waits for ch, or fails the test with what did not happen.
func (p *persister) wait(ch <-chan struct{}, what string) {
	p.t.Helper()
	select {
	case <-ch:
	case <-p.ctx.Done():
		p.t.Fatal(what)
	}
}

// reported waits for the report of entry index, and checks its times.
func (p *persister) reported(index uint64) {
	p.t.Helper()
	select {
	case done := <-p.n.persisted:
		if done.err != nil {
			p.t.Fatal(done.err)
		}
		if len(done.jobs) != 1 || len(done.jobs[0].Entries) != 1 || done.jobs[0].Entries[0].GetIndex() != index {
			p.t.Fatalf("persist response = %+v, want entry %d", done, index)
		}
		ts := done.times
		if ts.queued.IsZero() || !slices.IsSortedFunc([]time.Time{ts.queued, ts.start, ts.flush, ts.flushed, ts.sync, ts.synced}, time.Time.Compare) {
			p.t.Fatalf("persist times out of order: %+v", ts)
		}
	case <-p.ctx.Done():
		p.t.Fatalf("durable entry %d was not reported", index)
	}
}

// notReported checks that persist reported nothing yet.
func (p *persister) notReported() {
	p.t.Helper()
	select {
	case done := <-p.n.persisted:
		p.t.Fatalf("reported too early: %+v", done)
	default:
	}
}

// heldFS holds one operation until the test releases it, and then fails
// it with err. hold picks it: a "write", "sync" or "syncdir", with the
// number of the file it is on, counted in order of creation from 1 (for
// a syncdir, the newest file), and for a write, the number of the write
// on that file.
type heldFS struct {
	wal.FS
	hold             func(op string, file, write int) bool
	created          int
	blocked, release chan struct{}
	err              error
	once             sync.Once
	releaseOnce      sync.Once
}

func newHeldFS(hold func(op string, file, write int) bool) *heldFS {
	return &heldFS{hold: hold, blocked: make(chan struct{}), release: make(chan struct{})}
}

func (fs *heldFS) wrap(d wal.FS) wal.FS {
	fs.FS = d
	return fs
}

// at blocks the operation that hold picks, once.
func (fs *heldFS) at(op string, file, write int) error {
	if fs.hold == nil || !fs.hold(op, file, write) {
		return nil
	}
	var err error
	fs.once.Do(func() {
		close(fs.blocked)
		<-fs.release
		err = fs.err
	})
	return err
}

func (fs *heldFS) let() { fs.releaseOnce.Do(func() { close(fs.release) }) }

func (fs *heldFS) Create(name string) (wal.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	fs.created++
	return &heldFile{File: f, fs: fs, number: fs.created}, nil
}

func (fs *heldFS) SyncDir() error {
	if err := fs.at("syncdir", fs.created, 0); err != nil {
		return err
	}
	return fs.FS.SyncDir()
}

type heldFile struct {
	wal.File
	fs             *heldFS
	number, writes int
}

func (f *heldFile) Sync() error {
	if err := f.fs.at("sync", f.number, 0); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *heldFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if err := f.fs.at("write", f.number, f.writes); err != nil {
		return 0, err
	}
	return f.File.WriteAt(p, off)
}

// A later append must not hold back an earlier durable append's response.
// With the old pipeline, persist waits inside the second Flush and cannot
// report the first sync, so a leader cannot count its durable copy toward
// commit. The test holds the second append's write, the third WriteAt
// after the segment header and the first append, and the first sync
// until that write is held.
func TestPersistReportsSyncDuringNextWrite(t *testing.T) {
	fs := newHeldFS(func(op string, _, write int) bool { return op == "write" && write == 3 })
	syncing := make(chan struct{})
	var first sync.Once
	p := startPersist(t, fs.wrap, func(_ *wal.WAL, cfg *Config) {
		cfg.BeforeSync = func() {
			first.Do(func() { close(syncing) })
			select {
			case <-fs.blocked:
			case <-fs.release:
			}
		}
	}, fs.let)
	p.append(1, 1536<<10)
	p.wait(syncing, "first append never reached sync")
	p.append(2, 1536<<10)
	p.wait(fs.blocked, "second append never reached its held write")
	p.reported(1)
	p.notReported()
	fs.let()
	p.reported(2)
}

// An append in a full segment is durable before the next segment's header
// or directory sync. Neither may hold back its response. The next append
// must still wait for the new segment, and every sync error stops the node.
func TestPersistReportsBeforeRotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		// hold picks the held sync: of the full segment (file 1), of the
		// new segment's header (file 2), or of the directory after it.
		op   string
		file int
		fail bool
	}{
		{name: "header", op: "sync", file: 2},
		{name: "directory", op: "syncdir", file: 2},
		{name: "current-error", op: "sync", file: 1, fail: true},
		{name: "header-error", op: "sync", file: 2, fail: true},
		{name: "directory-error", op: "syncdir", file: 2, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newHeldFS(nil)
			injected := errors.New("rotation sync failed")
			if tc.fail {
				fs.err = injected
			}
			p := startPersist(t, fs.wrap, func(w *wal.WAL, _ *Config) {
				w.SetSegmentBytes(1024)
				// Only now, so that the syncs of wal.Open are real.
				fs.hold = func(op string, file, _ int) bool { return op == tc.op && file == tc.file }
			}, fs.let)
			p.failing = tc.fail
			p.append(1, 2048)
			p.wait(fs.blocked, "writer never reached the held sync")
			if tc.file == 2 {
				p.reported(1)
			}
			p.append(2, 1)
			p.notReported()
			fs.let()
			if !tc.fail {
				p.reported(2)
				return
			}
			select {
			case done := <-p.n.persisted:
				if !errors.Is(done.err, injected) {
					t.Fatalf("sync error = %v, want %v", done.err, injected)
				}
			case <-p.ctx.Done():
				t.Fatal("no report after releasing the sync")
			}
			p.wait(p.stopped, "writer did not stop after the sync error")
			p.notReported()
		})
	}
}
