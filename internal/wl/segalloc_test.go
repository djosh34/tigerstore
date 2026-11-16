//go:build !sim && !simio

package wl

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// A segment's space is allocated before the helper writes it, and its size
// stays 0: an append then allocates no blocks, which on ext4 an O_DSYNC
// write that grows a file into new blocks journals (wal.go's makeSegment).
func TestASegmentIsAllocatedAhead(t *testing.T) {
	dir := t.TempDir()
	w := &wal{dir: dir}
	dfd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(dfd) }()
	path := filepath.Join(dir, "0000000000000000.seg")
	sg, err := w.makeSegment(dfd, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(sg.wfd); _ = unix.Close(sg.rfd) }()
	if err := unix.Fallocate(sg.wfd, unix.FALLOC_FL_KEEP_SIZE, 0, 4096); err != nil {
		t.Skipf("the file system of %s cannot allocate: %v", dir, err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 || st.Blocks*512 < segSize {
		t.Fatalf("a new segment has size %d and %d bytes allocated; want 0 and %d", fi.Size(), st.Blocks*512, segSize)
	}
}
