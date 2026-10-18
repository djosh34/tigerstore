package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
)

// TestReadHoldsRemoval holds one value read from disk while roots sync.
// Every round overwrites the same keys and compacts, so each root makes
// the older segments obsolete, but their removals wait for the read, and the log
// grows. Once the read ends, the removals run without another root:
// nothing else is written (#165, #167).
func TestReadHoldsRemoval(t *testing.T) {
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	cfg := core.DefaultConfig(1, []uint64{1})
	cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
	cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
	dir := t.TempDir()
	n, err := New(Config{
		Core: cfg, DataDir: dir, Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, Logf: t.Logf,
		BeforeRead: func() {
			if armed.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-ran; err != nil {
			t.Errorf("the node stopped: %v", err)
		}
	})
	segments := func() int {
		ents, err := os.ReadDir(filepath.Join(dir, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		k := 0
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".wal") {
				k++
			}
		}
		return k
	}
	for round := range 4 {
		if _, err := putAll(n, round, keys("f", 16)...); err != nil {
			t.Fatal(err)
		}
	}
	armed.Store(true)
	read := make(chan error, 1)
	go func() {
		_, err := do(n, &pb.RangeRequest{Key: []byte("f00")})
		read <- err
	}()
	<-entered
	before := segments()
	for round := 4; round < 24; round++ {
		rev, err := putAll(n, round, keys("f", 16)...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := do(n, &pb.CompactionRequest{Revision: rev}); err != nil {
			t.Fatal(err)
		}
	}
	// A checkpoint the rounds started may still be on its way; its root
	// must sync while the read is held too.
	waiting := func() uint64 {
		n.log.mu.Lock()
		defer n.log.mu.Unlock()
		if n.log.removal == nil {
			return 0
		}
		return n.log.removal.root.Index
	}
	for last, since := waiting(), time.Now(); time.Since(since) < time.Second; time.Sleep(50 * time.Millisecond) {
		if w := waiting(); w != last {
			last, since = w, time.Now()
		}
	}
	held := segments()
	if waiting() == 0 || held < before+4 {
		t.Fatalf("while the read was held: the waiting removal's root is %d, segments went from %d to %d", waiting(), before, held)
	}
	once.Do(func() { close(release) })
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); segments() >= held; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d segments 5 s after the read ended, as many as while it was held", segments())
		}
	}
	t.Logf("segments: %d before the hold, %d while held, %d after", before, held, segments())
}

// failingRemoves fails every removal of a file of the log while failing
// is set, and the renames that keep a segment as a spare.
type failingRemoves struct {
	osfs.Dir
	failing atomic.Bool
}

var errRemove = errors.New("removal failed")

func (f *failingRemoves) Remove(name string) error {
	if f.failing.Load() {
		return errRemove
	}
	return f.Dir.Remove(name)
}

func (f *failingRemoves) Rename(from, to string) error {
	if f.failing.Load() && strings.HasPrefix(to, "spare-") {
		return errRemove
	}
	return f.Dir.Rename(from, to)
}

// TestFailedRemovalRetries fails the removals of old files for a while:
// the node takes every write meanwhile and counts the failures, and once
// removals work again, the old segments and checkpoints go.
func TestFailedRemovalRetries(t *testing.T) {
	dir := t.TempDir()
	d, err := osfs.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs := &failingRemoves{Dir: d}
	cfg := core.DefaultConfig(1, []uint64{1})
	cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
	cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
	n, err := NewOnFS(Config{Core: cfg, Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, Logf: t.Logf}, fs)
	if err != nil {
		t.Fatal(err)
	}
	fs.failing.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-ran; err != nil {
			t.Errorf("the node stopped: %v", err)
		}
	})
	files := func() (segs, ckpts int) {
		names, err := d.List()
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			switch {
			case strings.HasSuffix(name, ".wal"):
				segs++
			case strings.HasPrefix(name, "ckpt-"):
				ckpts++
			}
		}
		return segs, ckpts
	}
	for round := range 20 {
		rev, err := putAll(n, round, keys("f", 16)...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := do(n, &pb.CompactionRequest{Revision: rev}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := n.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segs, ckpts := files()
	if m.RemoveFailures == 0 || segs < 8 || ckpts < 2 {
		t.Fatalf("with removals failing: %d failures counted, %d segments and %d checkpoints on disk", m.RemoveFailures, segs, ckpts)
	}
	fs.failing.Store(false)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if s, c := files(); s <= 3 && c == 1 {
			break
		}
		if time.Now().After(deadline) {
			s, c := files()
			t.Fatalf("%d segments and %d checkpoints 10 s after removals work again, %d and %d before", s, c, segs, ckpts)
		}
	}
}
