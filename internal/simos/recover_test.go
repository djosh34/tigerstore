//go:build simio

package simos

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
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
	// failSyncAfter, once set, fails the sync of ckpt/ that follows the
	// next rename it picks, which stays in the page cache alone.
	failSyncAfter func(from, to string) bool
	failNext      bool
	// holdSyncAfter, once set, holds the sync of ckpt/ that follows the
	// next rename it picks until release is closed, and closes held when
	// it does; created is told of each file created.
	holdSyncAfter func(from, to string) bool
	holdNext      bool
	held, release chan struct{}
	created       func(name string)
	// renamed is told of each rename done.
	renamed func(from, to string)
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

func (l *lockedDisk) Create(name string) (wal.File, error) {
	h, err := l.open(name, l.d.Create)
	var created func(string)
	_ = l.do(func() error { created = l.created; return nil })
	if err == nil && created != nil {
		created(name)
	}
	return h, err
}
func (l *lockedDisk) Open(name string) (wal.File, error) { return l.open(name, l.d.Open) }

func (l *lockedDisk) open(name string, f func(string) (wal.File, error)) (wal.File, error) {
	var h wal.File
	err := l.do(func() (err error) { h, err = f(name); return err })
	if err != nil {
		return nil, err
	}
	return &lockedFile{l: l, h: h, seg: strings.HasSuffix(name, ".seg")}, nil
}

func (l *lockedDisk) Remove(name string) error { return l.do(func() error { return l.d.Remove(name) }) }

func (l *lockedDisk) Rename(o, n string) error {
	return l.do(func() error {
		err := l.d.Rename(o, n)
		if err == nil && l.failSyncAfter != nil && l.failSyncAfter(o, n) {
			l.failSyncAfter, l.failNext = nil, true
		}
		if err == nil && l.holdSyncAfter != nil && l.holdSyncAfter(o, n) {
			l.holdSyncAfter, l.holdNext = nil, true
		}
		if err == nil && l.renamed != nil {
			l.renamed(o, n)
		}
		return err
	})
}
func (l *lockedDisk) SyncDir() error { return l.do(l.d.SyncDir) }

func (l *lockedDisk) SyncDirOf(dir string) error {
	var hold bool
	_ = l.do(func() error {
		if hold = l.holdNext && dir == "ckpt"; hold {
			l.holdNext = false
		}
		return nil
	})
	if hold {
		close(l.held)
		<-l.release
	}
	return l.do(func() error {
		if l.failNext && dir == "ckpt" {
			l.failNext = false
			return simfs.ErrIO
		}
		return l.d.SyncDirOf(dir)
	})
}

func (l *lockedDisk) Mkdir(name string) error { return l.do(func() error { return l.d.Mkdir(name) }) }

func (l *lockedDisk) IsDir(name string) (is bool) {
	_ = l.do(func() error { is = l.d.IsDir(name); return nil })
	return is
}

func (l *lockedDisk) Dirs() (dirs []string) {
	_ = l.do(func() error { dirs = l.d.Dirs(); return nil })
	return dirs
}

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

// installBegins tells, by closing begun, of the first snapshot its node
// appends: its loop installs it right then (appendNow), and aborts the
// checkpoint it writes first.
type installBegins struct {
	recovered
	once  sync.Once
	begun chan struct{}
}

func (h *installBegins) Append(_, _ uint64, install bool) {
	if install {
		h.once.Do(func() { close(h.begun) })
	}
}

// recovered keeps what a start recovered, and the disk as a power loss
// right then leaves it.
type recovered struct {
	l    *lockedDisk
	done chan struct{}
	// clone keeps the disk as it is, unsynced operations too, for a test
	// to choose the crash (simfs.Disk.CrashKeeping), not as a crash
	// leaves it.
	clone bool
	last  uint64
	hs    raftpb.HardState
	disk  *simfs.Disk
}

func (r *recovered) Recovered(last uint64, hs *raftpb.HardState, _ func() []wl.LogValue) {
	r.l.mu.Lock()
	r.last, r.disk = last, r.l.d.Durable()
	if r.clone {
		r.disk = r.l.d.Clone()
	}
	r.l.mu.Unlock()
	if hs != nil {
		r.hs = raftpb.HardState{Term: new(hs.GetTerm()), Vote: new(hs.GetVote()), Commit: new(hs.GetCommit())}
	}
	close(r.done)
}

func (*recovered) Append(uint64, uint64, bool)                  {}
func (*recovered) Applied(*raftpb.Entry, uint64, func() uint64) {}
func (*recovered) Durable(uint64, bool, *raftpb.HardState)      {}
func (*recovered) Turn(wl.TurnState, func() []wl.LogValue)      {}
func (*recovered) SnapshotChunk(_ uint64, _ bool, p []byte) int { return len(p) }
func (*recovered) SnapshotCut(uint64)                           {}
func (*recovered) Deliver(*raftpb.Message) bool                 { return true }
func (*recovered) Checkpoint(uint64, uint64)                    {}
func (*recovered) CheckpointDone(uint64, string)                {}
func (*recovered) NoRoom()                                      {}
func (*recovered) Loaded(uint64, uint64)                        {}
func (*recovered) Installing(uint64)                            {}
func (*recovered) Installed(uint64)                             {}
func (*recovered) Expired(int64)                                {}

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

// A start that finds a CURRENT whose rename is not durable makes it
// durable before it removes the checkpoint the pointer before named
// (c2329e82, #689's area-1 review, finding 9). Here node 1's sync of
// ckpt/ after its pointer's rename fails, so the new CURRENT is in the
// page cache alone, and the node stops. Its start removes the checkpoint
// before, and the power goes then: the crash keeps that removal and
// loses every operation not yet durable on ckpt/ that it can, the
// rename too. CURRENT must still name a checkpoint the disk holds, and
// the node start from it. Without the start's sync, the pointer before
// comes back and names the checkpoint removed.
func TestAStartMakesThePointerDurableBeforeItCleansUp(t *testing.T) {
	c := newSimCluster(t)
	c.ckptLog = 64 << 10
	l := &lockedDisk{d: c.disks[0]}
	c.fs[0] = l
	for i := range 3 {
		c.start(t, i)
	}
	cli := c.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pointer := func(b []byte) string {
		p, err := wl.ReadPointer(b)
		if err != nil {
			return ""
		}
		return p.Checkpoint
	}
	current := func(d *simfs.Disk) (name string) {
		h, err := d.Open("ckpt/CURRENT")
		if err != nil {
			return ""
		}
		b, _ := h.ReadAll()
		_ = h.Close()
		return pointer(b)
	}
	live := func() (name string) {
		_ = l.do(func() error { name = current(l.d); return nil })
		return name
	}
	value := strings.Repeat("v", 4<<10)
	k := 0
	put := func() {
		if _, err := cli.Put(ctx, fmt.Sprintf("/k/%05d", k), value); err != nil && ctx.Err() != nil {
			t.Fatalf("put %d: %v", k, err)
		}
		k++
	}
	for live() == "" {
		put()
	}
	before := live()
	// The next pointer's rename is not followed by a sync of ckpt/, which
	// fails, and the node stops.
	_ = l.do(func() error {
		l.failSyncAfter = func(_, to string) bool { return to == "ckpt/CURRENT" }
		return nil
	})
	var stopErr error
	for stopErr == nil {
		put()
		select {
		case stopErr = <-c.stopped[0]:
			c.cancels[0]()
			c.cancels[0] = nil
		default:
		}
	}
	if !errors.Is(stopErr, simfs.ErrIO) {
		t.Fatalf("node 1 stopped on %v, want the failed sync of ckpt/", stopErr)
	}
	var durable string
	_ = l.do(func() error { durable = pointer(l.d.DurableBytes("ckpt/CURRENT")); return nil })
	after := live()
	if after == before || durable != before {
		t.Fatalf("CURRENT names %s, durably %s; want a new checkpoint in the page cache and %s durably", after, durable, before)
	}
	// It starts again, as after a kill, and the disk is kept as it is when
	// the start has cleaned up and recovered its log.
	r := &recovered{l: l, done: make(chan struct{}), clone: true}
	c.hooks[0] = r
	c.start(t, 0)
	select {
	case <-r.done:
	case err := <-c.stopped[0]:
		t.Fatalf("node 1 did not start: %v", err)
	case <-ctx.Done():
		t.Fatal("node 1 did not start")
	}
	c.stop(t, 0)
	if _, err := r.disk.Open("ckpt/" + before); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the start left the checkpoint before, %s: %v", before, err)
	}
	// The power goes: the removal stays, and the rename is lost unless a
	// sync made it durable.
	img := r.disk
	img.CrashKeeping(rand.New(rand.NewPCG(9, 9)), simfs.Writes, func(op simfs.Op, _, to string) bool {
		return op != simfs.OpRename || to != "ckpt/CURRENT"
	})
	if got := current(img); got != after {
		t.Fatalf("after the power loss CURRENT names %q, want %s: the start removed %s before the new pointer was durable", got, after, before)
	}
	l2 := &lockedDisk{d: img}
	c.fs[0] = l2
	r2 := &recovered{l: l2, done: make(chan struct{})}
	c.hooks[0] = r2
	c.start(t, 0)
	select {
	case <-r2.done:
	case err := <-c.stopped[0]:
		c.cancels[0] = nil
		t.Fatalf("node 1 did not start after the power loss: %v", err)
	case <-ctx.Done():
		t.Fatal("node 1 did not start after the power loss")
	}
}

// installCluster is a cluster on the seam whose node 3 cuts its log at
// small checkpoints, and a client of it that puts keys of 4 KiB, from
// /k/00000 on, and reads node 3's newest.
type installCluster struct {
	*simCluster
	l   *lockedDisk
	cli *clientv3.Client
	ctx context.Context
	k   int
	// took is when a put last went through, or the cluster started.
	took time.Time
}

func newInstallCluster(t *testing.T, ctx context.Context, hooks3 func(*lockedDisk) wl.Hooks) *installCluster {
	c := newSimCluster(t)
	c.ckptLog, c.catchup = 64<<10, 16
	l := &lockedDisk{d: c.disks[2]}
	c.fs[2] = l
	if hooks3 != nil {
		c.hooks[2] = hooks3(l)
	}
	for i := range 3 {
		c.start(t, i)
	}
	return &installCluster{simCluster: c, l: l, cli: c.client(t), ctx: ctx, took: time.Now()}
}

// put puts the next key. A put may fail while a node is cut off; none
// going through for 20 s fails the test with the last one's error, which
// says where the cluster stalled (#689's re-check of 88ae3e64: the setup
// stalled in a few -race runs, every put failing, the cause unseen).
func (c *installCluster) put(t *testing.T) {
	t.Helper()
	// A put that takes 20 s logs what the network holds, and every
	// goroutine's stack, and the network again 10 s later: which side of
	// the connection a put stalled on (#732).
	var mu sync.Mutex
	returned := false
	logStall := func(f func()) {
		mu.Lock()
		defer mu.Unlock()
		if !returned {
			f()
		}
	}
	k := c.k
	t1 := time.AfterFunc(20*time.Second, func() {
		logStall(func() {
			buf := make([]byte, 16<<20)
			buf = buf[:runtime.Stack(buf, true)]
			t.Logf("put %d stalled for 20 s; the network:\n%s\nthe goroutines:\n%s", k, c.nw.state(), buf)
		})
	})
	t2 := time.AfterFunc(30*time.Second, func() {
		logStall(func() { t.Logf("put %d stalled for 30 s; the network:\n%s", k, c.nw.state()) })
	})
	defer func() {
		t1.Stop()
		t2.Stop()
		mu.Lock()
		returned = true
		mu.Unlock()
	}()
	_, err := c.cli.Put(c.ctx, fmt.Sprintf("/k/%05d", c.k), strings.Repeat("v", 4<<10))
	switch {
	case err == nil:
		c.took = time.Now()
	case c.ctx.Err() != nil || time.Since(c.took) > 20*time.Second:
		t.Fatalf("put %d: no put went through for %v; the last failed with: %v", c.k, time.Since(c.took).Round(time.Millisecond), err)
	}
	c.k++
}

// current is the checkpoint node 3's CURRENT names on disk d, "" for none.
func current(d *simfs.Disk) string {
	h, err := d.Open("ckpt/CURRENT")
	if err != nil {
		return ""
	}
	b, _ := h.ReadAll()
	_ = h.Close()
	p, err := wl.ReadPointer(b)
	if err != nil {
		return ""
	}
	return p.Checkpoint
}

// lagBehind cuts node 3 off while the others write past what their log
// keeps for it, and puts it back: the leader sends it a snapshot.
func (c *installCluster) lagBehind(t *testing.T) {
	t.Helper()
	c.nw.isolate("n3")
	for range 200 {
		c.put(t)
	}
	c.nw.heal("n3")
}

// An install whose pointer cannot be written leaves the checkpoint
// CURRENT names, so that the node starts again from it, as after a kill
// and after a power loss (1062b9ce, #689's area-1 review, finding 2 of
// the review, 7 of the PR's list). Here node 3's sync of ckpt/ before the
// install renames CURRENT fails, and it stops. Removed before the pointer
// was written, the checkpoint before left CURRENT naming a file gone.
func TestAnInstallThatCannotPointOnLeavesTheCheckpointBefore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newInstallCluster(t, ctx, nil)
	before := ""
	for before == "" {
		c.put(t)
		_ = c.l.do(func() error { before = current(c.l.d); return nil })
	}
	// The install renames the checkpoint it received into place; the
	// sync of ckpt/ after, the pointer's first step, fails.
	_ = c.l.do(func() error {
		c.l.failSyncAfter = func(from, _ string) bool { return strings.HasPrefix(from, "ckpt/recv-") }
		return nil
	})
	c.lagBehind(t)
	var stopErr error
	select {
	case stopErr = <-c.stopped[2]:
		c.cancels[2]()
		c.cancels[2] = nil
	case <-ctx.Done():
		t.Fatal("node 3 did not install a snapshot")
	}
	if !errors.Is(stopErr, simfs.ErrIO) {
		t.Fatalf("node 3 stopped on %v, want the failed sync of the install's pointer", stopErr)
	}
	var d *simfs.Disk
	_ = c.l.do(func() error { d = c.l.d.Clone(); return nil })
	if got := current(d); got != before {
		t.Fatalf("after the failed install CURRENT names %q, want %s", got, before)
	}
	// It starts again as after a kill, and as after a power loss that
	// keeps every operation not yet durable, or none.
	for _, keep := range []bool{true, false} {
		img := d.Clone()
		img.CrashKeeping(rand.New(rand.NewPCG(5, 5)), simfs.Writes, func(simfs.Op, string, string) bool { return keep })
		if got := current(img); got == "" {
			t.Fatalf("after a power loss keeping its pending operations %v, CURRENT names nothing", keep)
		} else if _, err := img.Open("ckpt/" + got); err != nil {
			t.Fatalf("after a power loss keeping its pending operations %v, CURRENT names %s: %v", keep, got, err)
		}
	}
	r := &recovered{l: c.l, done: make(chan struct{})}
	c.hooks[2] = r
	c.start(t, 2)
	select {
	case <-r.done:
	case err := <-c.stopped[2]:
		c.cancels[2] = nil
		t.Fatalf("node 3 did not start after the failed install: %v", err)
	case <-ctx.Done():
		t.Fatal("node 3 did not start after the failed install")
	}
}

// An install waits for the checkpoint its node is writing, so that the
// two never write CURRENT at once (1062b9ce, #689's area-1 review, the
// PR's finding 6). Here node 3's checkpoint writer is held right before
// it writes its pointer, its file renamed into place, while the node
// lags and the leader's snapshot comes. Released once the install had
// time to run, the writer wrote CURRENT naming its own, older checkpoint
// after the install's pointer, and the install had removed that
// checkpoint, had the install not waited.
// CURRENT must name a checkpoint at the snapshot or past it, and the
// node start from it with every key.
func TestAnInstallWaitsForTheCheckpointBeingWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var hooks *installBegins
	c := newInstallCluster(t, ctx, func(l *lockedDisk) wl.Hooks {
		hooks = &installBegins{l: l, done: make(chan struct{}), begun: make(chan struct{})}
		return hooks
	})
	c.l.held, c.l.release = make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-c.l.release:
		default:
			close(c.l.release)
		}
	})
	recv, pointed := make(chan struct{}), make(chan struct{})
	var once, pointedOnce sync.Once
	// renames are the install's rename of what it received ("install"),
	// and every rename of CURRENT ("current"), in order.
	var renameMu sync.Mutex
	var renames []string
	installedAs := ""
	_ = c.l.do(func() error {
		c.l.renamed = func(from, to string) {
			renameMu.Lock()
			defer renameMu.Unlock()
			switch {
			case strings.HasPrefix(from, "ckpt/recv-"):
				renames, installedAs = append(renames, "install"), strings.TrimPrefix(to, "ckpt/")
			case to == "ckpt/CURRENT":
				renames = append(renames, "current")
				if slices.Contains(renames, "install") {
					pointedOnce.Do(func() { close(pointed) })
				}
			}
		}
		c.l.holdSyncAfter = func(from, to string) bool {
			return strings.HasPrefix(from, "ckpt/") && !strings.HasPrefix(from, "ckpt/recv-") && strings.HasSuffix(to, ".ckpt")
		}
		c.l.created = func(name string) {
			if strings.HasPrefix(name, "ckpt/recv-") {
				once.Do(func() { close(recv) })
			}
		}
		return nil
	})
	for held := false; !held; {
		c.put(t)
		select {
		case <-c.l.held:
			held = true
		default:
		}
	}
	held := ""
	_ = c.l.do(func() error { held = current(c.l.d); return nil })
	c.lagBehind(t)
	select {
	case <-recv:
	case <-ctx.Done():
		t.Fatal("node 3 received no snapshot")
	}
	// The install renames the checkpoint it received into place, and
	// points CURRENT at it, once it aborted the one being written. Without
	// the wait in abortCkpt it does both while the writer is held, and the
	// release comes after; with it, the install waits for the writer, and
	// the release comes 2 s after the loop began the install with no
	// pointer. Counted from the snapshot's arrival instead, a slow receive
	// and load could pass the 2 s without the wait (#689's re-check, D6).
	select {
	case <-hooks.begun:
	case <-ctx.Done():
		t.Fatal("node 3 never began the install")
	}
	select {
	case <-pointed:
		t.Log("the install pointed CURRENT on while the writer was held")
	case <-time.After(2 * time.Second):
		t.Log("the install did not go on while the writer was held")
	case <-ctx.Done():
		t.Fatal("node 3 did not install the snapshot")
	}
	renameMu.Lock()
	released := len(renames)
	renameMu.Unlock()
	close(c.l.release)
	// Once the install pointed CURRENT on and the held writer, released,
	// went on, CURRENT names the installed checkpoint, or a newer one.
	// Without the wait the writer pointed it back at its own, older one.
	for {
		renameMu.Lock()
		install := slices.Index(renames, "install")
		done := install >= 0 && slices.Contains(renames[install+1:], "current") && slices.Contains(renames[released:], "current")
		renameMu.Unlock()
		if done {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("the install or the held writer never pointed CURRENT")
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := ""
	_ = c.l.do(func() error { got = current(c.l.d); return nil })
	renameMu.Lock()
	want := installedAs
	renameMu.Unlock()
	if got < want {
		t.Fatalf("after the install and the held writer CURRENT names %q, before the installed %q", got, want)
	}
	// Node 3 serves the newest key once it caught up.
	last := fmt.Sprintf("/k/%05d", c.k-1)
	n3 := c.clientOf(t, "n3:2379")
	for {
		r, err := n3.Get(ctx, last, clientv3.WithSerializable())
		if err == nil && len(r.Kvs) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("node 3 did not catch up")
		}
		c.put(t)
		time.Sleep(10 * time.Millisecond)
	}
	c.stop(t, 2)
	var d *simfs.Disk
	_ = c.l.do(func() error { d = c.l.d.Clone(); return nil })
	got = current(d)
	if got == "" || got <= held {
		t.Fatalf("after the install CURRENT names %q, at or before %q, which it named while the writer was held", got, held)
	}
	r := &recovered{l: c.l, done: make(chan struct{})}
	c.hooks[2] = r
	c.start(t, 2)
	select {
	case <-r.done:
	case err := <-c.stopped[2]:
		c.cancels[2] = nil
		t.Fatalf("node 3 did not start after the install: %v", err)
	case <-ctx.Done():
		t.Fatal("node 3 did not start after the install")
	}
}

// A node's first start makes wal/ and ckpt/ and syncs the data dir that
// holds them, so that a power loss keeps them and what was synced in
// them (f0630b8c, #689's area-1 review). Here node 3 has taken writes
// and the power goes, losing every directory operation not durable: its
// directories and its log are still there, and it starts again with the
// entries it had.
func TestAFreshStartMakesItsDirectoriesDurable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newSimCluster(t)
	l := &lockedDisk{d: c.disks[2]}
	c.fs[2] = l
	for i := range 3 {
		c.start(t, i)
	}
	cli := c.client(t)
	last := ""
	for k, done := 0, 0; done < 10; k++ {
		key := fmt.Sprintf("/k/%03d", k)
		if _, err := cli.Put(ctx, key, "v"); err == nil {
			last, done = key, done+1
		} else if ctx.Err() != nil {
			t.Fatalf("put %d: %v", k, err)
		}
	}
	// Node 3 has the writes: it serves the last.
	n3 := c.clientOf(t, "n3:2379")
	for {
		r, err := n3.Get(ctx, last, clientv3.WithSerializable())
		if err == nil && len(r.Kvs) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("node 3 did not take the writes: %v %v", r, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var img *simfs.Disk
	_ = l.do(func() error { img = l.d.Clone(); return nil })
	c.stop(t, 2)
	img.CrashKeeping(rand.New(rand.NewPCG(3, 3)), simfs.Writes, func(simfs.Op, string, string) bool { return false })
	if !img.IsDir("wal") || !img.IsDir("ckpt") {
		t.Fatalf("a power loss left the directories %v, want wal and ckpt", img.Dirs())
	}
	segs := 0
	for _, name := range img.DurableNames() {
		if strings.HasPrefix(name, "wal/") && strings.HasSuffix(name, ".seg") {
			segs++
		}
	}
	if segs == 0 {
		t.Fatalf("a power loss left no log segment: %v", img.DurableNames())
	}
	l2 := &lockedDisk{d: img}
	c.fs[2] = l2
	r := &recovered{l: l2, done: make(chan struct{})}
	c.hooks[2] = r
	c.start(t, 2)
	select {
	case <-r.done:
	case err := <-c.stopped[2]:
		c.cancels[2] = nil
		t.Fatalf("node 3 did not start after the power loss: %v", err)
	case <-ctx.Done():
		t.Fatal("node 3 did not start after the power loss")
	}
	if r.last < 10 {
		t.Fatalf("after the power loss node 3 recovered entries to %d, want its writes", r.last)
	}
}
