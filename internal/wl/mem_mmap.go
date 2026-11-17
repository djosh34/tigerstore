//go:build !sim

package wl

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// The memory outside Go's heap of the real binary and of the simulator's
// runs under the race detector (tag simio): mapped as the real binary
// maps it. The log's helper writes the stage's last block whole while the
// loop writes on past the durable end in it; what lies there on disk is
// never trusted (the block is written again, and a scan stops at the
// first record that does not check), so the race detector, which does
// not see mapped memory, is not told of it here either.

// mmapBytes reserves n bytes of anonymous memory outside Go's heap, which
// the collector never scans and the memory limit never counts. Pages are
// backed on first touch.
func mmapBytes(n int) ([]byte, error) {
	b, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_NORESERVE)
	if err != nil {
		return nil, fmt.Errorf("mmap %d bytes: %w", n, err)
	}
	// Huge pages where the kernel gives them: the index and the slab are
	// read at random, and each miss would also miss the TLB.
	_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
	return b, nil
}

// unixMunmap gives back what mmapBytes reserved.
func unixMunmap(b []byte) error { return unix.Munmap(b) }

// pageSize is the system's page: what dropPages gives back is whole pages.
var pageSize = unix.Getpagesize()

// dropPages gives b's pages back to the system: they read as zeros, and
// take memory again when written. b starts on a page.
func dropPages(b []byte) { _ = unix.Madvise(b, unix.MADV_DONTNEED) }
