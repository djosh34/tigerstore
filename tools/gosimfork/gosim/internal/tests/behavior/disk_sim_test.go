//go:build sim

package behavior_test

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/jellevandenhooff/gosim"
)

// TODO: more scenarios for either triggering or not triggering GC (pending ops, rename, delete, etc.)

func getInode(f *os.File) (int, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("not a syscall.Stat_t")
	}
	return int(stat.Ino), nil
}

type gcTester struct {
	*testing.T
	dir *os.File
}

func newGCTester(t *testing.T) *gcTester {
	d, err := os.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	return &gcTester{
		T:   t,
		dir: d,
	}
}

func (t *gcTester) mustCreate(name string) (*os.File, int) {
	t.Helper()
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	i, err := getInode(f)
	if err != nil {
		t.Fatal(err)
	}
	return f, i
}

func (t *gcTester) mustWrite(f *os.File, b []byte) {
	t.Helper()
	if _, err := f.Write(b); err != nil {
		t.Error(err)
	}
}

func (t *gcTester) mustClose(f *os.File) {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func (t *gcTester) mustSync(f *os.File) {
	t.Helper()
	if err := f.Sync(); err != nil {
		t.Error(err)
	}
}

func (t *gcTester) checkMem(inode int, exists bool, links int) {
	t.Helper()
	got := gosim.CurrentMachine().GetInodeInfo(inode)
	if got.MemExists != exists || got.MemLinks != links {
		t.Errorf("inode %d: expected mem exists %v, links %d; got exists %v, links %d", inode, exists, links, got.MemExists, got.MemLinks)
	}
}

func (t *gcTester) checkDisk(inode int, exists bool, links int) {
	t.Helper()
	got := gosim.CurrentMachine().GetInodeInfo(inode)
	if got.DiskExists != exists || got.DiskLinks != links {
		t.Errorf("inode %d: expected disk exists %v, links %d; got exists %v, links %d", inode, exists, links, got.DiskExists, got.DiskLinks)
	}
}

func (t *gcTester) checkPendingOps(inode int, ops int) {
	t.Helper()
	got := gosim.CurrentMachine().GetInodeInfo(inode)
	if got.Ops != ops {
		t.Errorf("inode %d: expected ops %d, got %d", inode, ops, got.Ops)
	}
}

func (t *gcTester) checkOpenHandles(inode int, handles int) {
	t.Helper()
	got := gosim.CurrentMachine().GetInodeInfo(inode)
	if got.Handles != handles {
		t.Errorf("inode %d: expected handles %d, got %d", inode, handles, got.Handles)
	}
}

func (t *gcTester) rename(from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Error(err)
	}
}

func TestDiskGC(t *testing.T) {
	harness := newGCTester(t)

	// create a new file, expect mem but not disk representation
	foo, fooInode := harness.mustCreate("foo")
	harness.checkMem(fooInode, true, 1)
	harness.checkDisk(fooInode, false, 0)
	harness.checkOpenHandles(fooInode, 1) // f1
	harness.checkPendingOps(fooInode, 2)  // alloc, dir write

	// write to the file, expect more pending ops
	harness.mustWrite(foo, []byte("hello"))
	harness.checkPendingOps(fooInode, 4)  // alloc, dir write, resize, write
	harness.checkDisk(fooInode, false, 0) // still not visible

	// test that sync creates file on disk but does not flush the dir write
	harness.mustSync(foo)
	harness.checkDisk(fooInode, true, 0) // not yet visible
	harness.checkPendingOps(fooInode, 1) // dir write

	// check that handle changes after close
	harness.mustClose(foo)
	harness.checkOpenHandles(fooInode, 0)

	// create another file "bar", write to it, and flush the write
	bar, barInode := harness.mustCreate("bar")
	harness.mustWrite(bar, []byte("bye"))
	harness.mustSync(bar)
	harness.mustClose(bar)
	harness.checkDisk(barInode, true, 0) // not yet visible
	harness.checkPendingOps(barInode, 1) // dir write

	// sync dir to flush dir writes
	harness.mustSync(harness.dir)

	// now dir entries are flushed to disk and link count is updated
	harness.checkDisk(fooInode, true, 1)
	harness.checkPendingOps(fooInode, 0)
	harness.checkDisk(barInode, true, 1)
	harness.checkPendingOps(barInode, 0)

	// rename "bar" to "foo", deleting "foo"
	harness.rename("bar", "foo")

	// after rename in memory, memory GCs but disk is unchanged
	harness.checkMem(fooInode, false, 0) // XXX: deleted from memory even though still on disk... problem? could it come back?
	harness.checkDisk(fooInode, true, 1)
	harness.checkPendingOps(fooInode, 0) // rename does not mention i1
	harness.checkMem(barInode, true, 1)
	harness.checkDisk(barInode, true, 1)
	harness.checkPendingOps(barInode, 1) // rename

	// flush rename to disk, expect "foo" to disappear from disk
	harness.mustSync(harness.dir)
	harness.checkDisk(fooInode, false, 0)
	harness.checkDisk(barInode, true, 1)
	harness.checkPendingOps(barInode, 0)
}

// TODO: test delete using overwrite
// TODO: test delete using unlink
// TODO: test no delete until close, then delete
// TODO: test no delete until flush, then delete

func TestDiskInitial(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Errorf("getwd: %s", err)
	}
	if dir != "/app" {
		t.Errorf("bad initial path: expected /app, got %s", dir)
	}

	hosts, err := os.ReadFile("/etc/hosts")
	if err != nil {
		t.Errorf("read /etc/hosts: %s", err)
	}
	if !strings.Contains(string(hosts), "localhost") {
		t.Errorf("bad /etc/hosts %q, does not contain localhost", string(hosts))
	}
}
