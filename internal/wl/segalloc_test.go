//go:build !(sim || simio)

package wl

import (
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// makeTestSegment makes segment k in dir as the log makes its segments,
// with the file system's room left reported as free, and returns the
// segment's size and the bytes allocated to it.
func makeTestSegment(t *testing.T, dir string, k int64, free int64) (size, allocated int64) {
	t.Helper()
	real := freeSpaceOf
	freeSpaceOf = func(string) (int64, error) { return free, nil }
	defer func() { freeSpaceOf = real }()
	dfd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(dfd) }()
	w := &wal{dir: dir}
	sg, err := w.makeSegment(dfd, segPath(dir, k))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(sg.wfd); _ = unix.Close(sg.rfd) }()
	var st unix.Stat_t
	if err := unix.Stat(segPath(dir, k), &st); err != nil {
		t.Fatal(err)
	}
	return st.Size, st.Blocks * 512
}

// A segment's space is allocated before the helper writes it, its size kept
// at 0, while the file system has room for eight segments: an append then
// allocates no blocks, which on ext4 an O_DSYNC write that grows a file
// into new blocks journals (wal.go's makeSegment). With less room nothing
// is allocated ahead, so nothing is held back from a checkpoint that needs
// the disk's last room (campaign 43870771307).
func TestASegmentIsAllocatedAheadOnlyWithRoom(t *testing.T) {
	dir := t.TempDir()
	if err := unix.Fallocate(mustOpen(t, filepath.Join(dir, "probe")), unix.FALLOC_FL_KEEP_SIZE, 0, 4096); err != nil {
		t.Skipf("the file system of %s cannot allocate: %v", dir, err)
	}
	before, err := freeSpaceOf(dir)
	if err != nil {
		t.Fatal(err)
	}
	size, allocated := makeTestSegment(t, dir, 0, allocFree()-1)
	if size != 0 || allocated != 0 {
		t.Fatalf("with room for just under eight segments a new segment has size %d and %d bytes allocated; want 0 and 0", size, allocated)
	}
	if after, err := freeSpaceOf(dir); err != nil || before-after >= segSize/2 {
		t.Fatalf("a segment made with no room to spare took %d bytes of the file system (%v)", before-after, err)
	}
	size, allocated = makeTestSegment(t, dir, 1, allocFree())
	if size != 0 || allocated < segSize {
		t.Fatalf("with room for eight segments a new segment has size %d and %d bytes allocated; want 0 and %d", size, allocated, segSize)
	}
}

func mustOpen(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd
}
