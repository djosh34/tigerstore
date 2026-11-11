//go:build !sim

package osfs

import (
	"errors"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Prototype of #602: the log's segments are written with O_DIRECT, in
// whole aligned blocks, so a write copies nothing into the page cache and
// the group's fdatasync has no dirty pages to write back. A new segment's
// file is preallocated, so writes do not grow it.

const block = 4096

// segmentFile reports whether name is a file that the log appends to.
func segmentFile(name string) bool {
	return strings.HasSuffix(name, ".wal") || strings.HasPrefix(name, "next-")
}

// direct is a second descriptor of a segment, opened with O_DIRECT, and
// the bytes of the last block it wrote part of.
type direct struct {
	fd        int
	buf       []byte
	tail      [block]byte
	tailBlock int64
}

func openDirect(path string) (*direct, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &direct{fd: fd, tailBlock: -1}, nil
}

// scratch returns an aligned buffer of n bytes, n a multiple of block.
func (d *direct) scratch(n int) []byte {
	if cap(d.buf) < n {
		raw := make([]byte, n+block)
		off := int(-uintptr(unsafe.Pointer(&raw[0])) & (block - 1))
		d.buf = raw[off : off+n : off+n]
	}
	return d.buf[:n]
}

// writeAt writes p at off in whole blocks. The part of the first block
// before off comes from the last write, or from read.
func (d *direct) writeAt(p []byte, off int64, read func([]byte, int64) (int, error)) error {
	start := off &^ (block - 1)
	head := int(off - start)
	end := off + int64(len(p))
	alignedEnd := (end + block - 1) &^ (block - 1)
	buf := d.scratch(int(alignedEnd - start))
	if head > 0 {
		if d.tailBlock == start {
			copy(buf[:head], d.tail[:head])
		} else {
			// What is not on disk reads as zeros.
			clear(buf[:head])
			read(buf[:head], start)
		}
	}
	copy(buf[head:], p)
	clear(buf[head+len(p):])
	for w := 0; w < len(buf); {
		n, err := unix.Pwrite(d.fd, buf[w:], start+int64(w))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		w += n
	}
	if end&(block-1) != 0 {
		last := alignedEnd - block
		copy(d.tail[:], buf[last-start:])
		d.tailBlock = last
	} else {
		d.tailBlock = -1
	}
	return nil
}

func (d *direct) close() error { return unix.Close(d.fd) }

// preallocate gives a new segment's file the room of a whole segment.
func preallocate(fd int) error {
	err := unix.Fallocate(fd, 0, 0, wal.SegmentBytes)
	if errors.Is(err, unix.EOPNOTSUPP) {
		return nil
	}
	return err
}

const directIO = true
