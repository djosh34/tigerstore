//go:build simio

package simos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

// lockedDisk is a simfs disk whose calls take one lock, so that a test
// can copy it while the node runs, and whose next write to a log segment,
// once armed, fails after all its bytes reached the page cache.
type lockedDisk struct {
	mu   sync.Mutex
	d    *simfs.Disk
	fail bool
}

func (l *lockedDisk) do(f func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return f()
}

func (l *lockedDisk) List() (names []string, err error) {
	err = l.do(func() error { names, err = l.d.List(); return err })
	return names, err
}

func (l *lockedDisk) Create(name string) (wal.File, error) { return l.open(name, l.d.Create) }
func (l *lockedDisk) Open(name string) (wal.File, error)   { return l.open(name, l.d.Open) }

func (l *lockedDisk) open(name string, f func(string) (wal.File, error)) (wal.File, error) {
	var h wal.File
	err := l.do(func() (err error) { h, err = f(name); return err })
	if err != nil {
		return nil, err
	}
	return &lockedFile{l: l, h: h, seg: strings.HasSuffix(name, ".seg")}, nil
}

func (l *lockedDisk) Remove(name string) error { return l.do(func() error { return l.d.Remove(name) }) }

func (l *lockedDisk) Rename(o, n string) error { return l.do(func() error { return l.d.Rename(o, n) }) }
func (l *lockedDisk) SyncDir() error           { return l.do(l.d.SyncDir) }

type lockedFile struct {
	l   *lockedDisk
	h   wal.File
	seg bool
}

func (f *lockedFile) ReadAll() (b []byte, err error) {
	err = f.l.do(func() error { b, err = f.h.ReadAll(); return err })
	return b, err
}

func (f *lockedFile) ReadAt(p []byte, off int64) (n int, err error) {
	err = f.l.do(func() error { n, err = f.h.ReadAt(p, off); return err })
	return n, err
}

func (f *lockedFile) Write(p []byte) (n int, err error) {
	err = f.l.do(func() error { n, err = f.h.Write(p); return err })
	return n, err
}

func (f *lockedFile) WriteAt(p []byte, off int64) (n int, err error) {
	err = f.l.do(func() error {
		if n, err = f.h.WriteAt(p, off); err == nil && f.seg && f.l.fail {
			f.l.fail = false
			err = simfs.ErrNoSpace
		}
		return err
	})
	return n, err
}

func (f *lockedFile) Truncate(size int64) error {
	return f.l.do(func() error { return f.h.Truncate(size) })
}
func (f *lockedFile) Sync() error      { return f.l.do(f.h.Sync) }
func (f *lockedFile) DropCache() error { return f.l.do(f.h.DropCache) }
func (f *lockedFile) Close() error     { return f.l.do(f.h.Close) }

// recovered keeps what a start recovered, and the disk as a power loss
// right then leaves it.
type recovered struct {
	l    *lockedDisk
	done chan struct{}
	last uint64
	hs   raftpb.HardState
	disk *simfs.Disk
}

func (r *recovered) Recovered(last uint64, hs *raftpb.HardState, _ func() []wl.LogValue) {
	r.l.mu.Lock()
	r.last, r.disk = last, r.l.d.Durable()
	r.l.mu.Unlock()
	if hs != nil {
		r.hs = raftpb.HardState{Term: new(hs.GetTerm()), Vote: new(hs.GetVote()), Commit: new(hs.GetCommit())}
	}
	close(r.done)
}

func (*recovered) Append(uint64, uint64, bool)             {}
func (*recovered) Applied(*raftpb.Entry, uint64)           {}
func (*recovered) Durable(uint64, bool, *raftpb.HardState) {}
func (*recovered) Turn(wl.TurnState)                       {}
func (*recovered) SnapshotChunk(uint64, bool, []byte)      {}
func (*recovered) Checkpoint(uint64)                       {}
func (*recovered) Expired(int64)                           {}

// A write of the log that fails leaves its bytes in the page cache, where
// a start after a kill reads them back. The start makes what it recovered
// durable before it counts it: a power loss right after it leaves a disk
// from which a start recovers the same log and hard state (#701).
func TestAStartSyncsWhatItRecovered(t *testing.T) {
	c := newSimCluster(t)
	l := &lockedDisk{d: c.disks[2]}
	c.fs[2] = l
	for i := range 3 {
		c.start(t, i)
	}
	cli := c.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	put := func(k int) error {
		_, err := cli.Put(ctx, fmt.Sprintf("/k/%03d", k), "v")
		return err
	}
	for put(0) != nil {
		if ctx.Err() != nil {
			t.Fatal("no leader took a put")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Node 3's next write of its log fails with every byte in the page
	// cache, and the node stops on it.
	_ = l.do(func() error { l.fail = true; return nil })
	var stopErr error
	for k := 1; stopErr == nil; k++ {
		if err := put(k); err != nil && ctx.Err() != nil {
			t.Fatalf("put %d: %v", k, err)
		}
		select {
		case stopErr = <-c.stopped[2]:
			c.cancels[2]()
			c.cancels[2] = nil
		default:
		}
	}
	if !errors.Is(stopErr, simfs.ErrNoSpace) {
		t.Fatalf("node 3 stopped on %v, want the failed write", stopErr)
	}
	// It starts again, as after a kill, and the power goes right when it
	// counts its log recovered.
	first := &recovered{l: l, done: make(chan struct{})}
	c.hooks[2] = first
	c.start(t, 2)
	select {
	case <-first.done:
	case <-ctx.Done():
		t.Fatal("node 3 did not start")
	}
	c.stop(t, 2)
	// A start from what the power loss left recovers the same.
	l2 := &lockedDisk{d: first.disk}
	c.fs[2] = l2
	second := &recovered{l: l2, done: make(chan struct{})}
	c.hooks[2] = second
	c.start(t, 2)
	select {
	case <-second.done:
	case <-ctx.Done():
		t.Fatal("node 3 did not start after the power loss")
	}
	if second.last != first.last || second.hs.GetCommit() != first.hs.GetCommit() || second.hs.GetTerm() != first.hs.GetTerm() || second.hs.GetVote() != first.hs.GetVote() {
		t.Fatalf("after the power loss node 3 recovered entries to %d, hard state %v; before it, entries to %d, hard state %v",
			second.last, &second.hs, first.last, &first.hs)
	}
	t.Logf("node 3 recovered entries to %d, hard state %v, before and after the power loss", first.last, &first.hs)
}
