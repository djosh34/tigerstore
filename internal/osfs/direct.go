package osfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/wal"
)

// The log's newest segment is written with O_DIRECT and O_DSYNC: each
// write goes from the process's memory to the disk and returns once it is
// durable, so the kernel neither copies the log into its page cache nor
// writes it back, and no fdatasync follows. A buffered write and its
// fdatasync took about 20% of the leader's CPU in prototype 6 of #602, and
// 9.6% on crd-heavy in run 43668341529.
//
// O_DIRECT writes whole blocks from aligned memory at aligned offsets. The
// file keeps the last block it wrote, which an append fills further, and
// writes it again with the next append: those bytes are the same as on
// disk, so a write that a power loss tears keeps them as they were, as a
// buffered sync does when it writes a page back whole. Past the end of
// the records, the last block holds zeros, which recovery reads as a torn
// tail, as it reads what a reused file holds there.

// blockBytes is the alignment of O_DIRECT's memory, offsets and lengths:
// a page, which every block size of the disks we run on divides.
const blockBytes = 4096

// logFile is a segment opened for appends with O_DIRECT and O_DSYNC. Reads,
// truncation and the page cache's drops go through the plain file. The
// log syncs a segment on another goroutine than the one that writes it
// (wal.WAL.Segment), and cuts it there after a failed sync.
type logFile struct {
	file
	fd int
	// mu guards the writes and truncations. buf is aligned memory; its
	// first last bytes are the file's from lastAt on, the start of the
	// last block written, or lastAt is -1.
	mu     sync.Mutex
	buf    []byte
	lastAt int64
	last   int
	// cut is set by a truncation, which only a Sync makes durable. syncMu
	// makes a Sync wait for one under way, which may make the cut durable
	// for both.
	cut    atomic.Bool
	syncMu sync.Mutex
}

// OpenLog opens segment name for appends, with O_DIRECT and O_DSYNC where
// the file system allows them, as ext4 and xfs do. On one that does not,
// such as tmpfs, and without syncs, it is Open.
func (d Dir) OpenLog(name string) (wal.File, error) {
	if d.noSync {
		return d.Open(name)
	}
	path := filepath.Join(d.path, name)
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_DIRECT|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.EINVAL) {
		return d.Open(name)
	}
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, errors.Join(err, unix.Close(fd))
	}
	return &logFile{file: file{f, false}, fd: fd, lastAt: -1}, nil
}

// aligned returns n bytes of memory aligned to blockBytes.
func aligned(n int) []byte {
	b := make([]byte, n+blockBytes)
	skip := (blockBytes - int(uintptr(unsafe.Pointer(&b[0]))%blockBytes)) % blockBytes
	return b[skip : skip+n : skip+n]
}

// WriteAt writes p at off and returns once it is durable. It writes the
// blocks from the one that holds off to the one that holds the end of p,
// with what the file holds before off in the first, and zeros after p in
// the last.
func (f *logFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	base := off &^ (blockBytes - 1)
	head := int(off - base)
	need := (head + len(p) + blockBytes - 1) &^ (blockBytes - 1)
	if need > len(f.buf) {
		b := aligned(max(need, 2*len(f.buf), 1<<20))
		copy(b, f.buf[:f.last])
		f.buf = b
	}
	if f.lastAt != base || f.last < head {
		// The bytes before off in its block come from the file, zeros past
		// its end.
		n, err := f.ReadAt(f.buf[:head], base)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		clear(f.buf[n:head])
	}
	copy(f.buf[head:], p)
	clear(f.buf[head+len(p) : need])
	for done := 0; done < need; {
		n, err := unix.Pwrite(f.fd, f.buf[done:need], base+int64(done))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err == nil && n < blockBytes {
			err = io.ErrShortWrite
		}
		if err != nil {
			f.lastAt = -1
			return 0, &os.PathError{Op: "write", Path: f.Name(), Err: err}
		}
		// A short write ends on a block; the rest goes again.
		done += n &^ (blockBytes - 1)
	}
	end := off + int64(len(p))
	f.lastAt = end &^ (blockBytes - 1)
	f.last = int(end - f.lastAt)
	copy(f.buf, f.buf[f.lastAt-base:f.lastAt-base+int64(f.last)])
	return len(p), nil
}

// Write is not used on a segment: the log writes at its offsets.
func (f *logFile) Write([]byte) (int, error) {
	return 0, errors.New("osfs: Write on a segment opened for appends")
}

// Truncate cuts the file; the next Sync makes the cut durable.
func (f *logFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	if f.lastAt+int64(f.last) > size {
		f.lastAt = -1
	}
	f.cut.Store(true)
	return nil
}

// Sync makes a truncation durable. The writes are durable already.
func (f *logFile) Sync() error {
	f.syncMu.Lock()
	defer f.syncMu.Unlock()
	if !f.cut.Swap(false) {
		return nil
	}
	for {
		if err := unix.Fdatasync(f.fd); !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func (f *logFile) Close() error {
	return errors.Join(unix.Close(f.fd), f.File.Close())
}
