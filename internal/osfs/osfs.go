// Package osfs is the real disk adapter: a directory on the local file
// system that implements wal.FS.
package osfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Dir is a directory on disk.
type Dir struct {
	path   string
	noSync bool
}

var _ wal.FS = Dir{}

// New creates the directory and any missing parents, and syncs the
// directory and every ancestor, so the path survives a crash. It syncs
// them on every start, which also finishes a setup that an earlier start
// left half done.
func New(path string) (Dir, error) {
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return Dir{}, err
	}
	for p := path; ; p = filepath.Dir(p) {
		if err := syncPath(p); err != nil {
			return Dir{}, err
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	return Dir{path: path}, nil
}

// WithoutSync is the directory with its syncs and those of its files
// turned into no-ops, for etcd's --unsafe-no-fsync: a crash may lose or
// tear any write.
func (d Dir) WithoutSync() Dir {
	d.noSync = true
	return d
}

// List returns the names in the directory.
func (d Dir) List() ([]string, error) {
	ents, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	return names, nil
}

// Usage is the size of the files in the directory.
func (d Dir) Usage() (int64, error) {
	ents, err := os.ReadDir(d.path)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, e := range ents {
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			// Removed since the listing.
			continue
		}
		if err != nil {
			return 0, err
		}
		n += info.Size()
	}
	return n, nil
}

// Create makes a new empty file. Files are not opened with O_APPEND, so
// that WriteAt writes where it says.
func (d Dir) Create(name string) (wal.File, error) {
	f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return file{f, d.noSync}, nil
}

// Open opens an existing file for reading and writing.
func (d Dir) Open(name string) (wal.File, error) {
	f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return file{f, d.noSync}, nil
}

// Rename renames a file. The rename only survives a crash after SyncDir.
func (d Dir) Rename(oldName, newName string) error {
	return os.Rename(filepath.Join(d.path, oldName), filepath.Join(d.path, newName))
}

// Remove deletes a file.
func (d Dir) Remove(name string) error { return os.Remove(filepath.Join(d.path, name)) }

// SyncDir makes creations and removals durable.
func (d Dir) SyncDir() error {
	if d.noSync {
		return nil
	}
	return syncPath(d.path)
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

type file struct {
	*os.File
	noSync bool
}

// Sync makes the content durable with fdatasync, which also persists a
// changed size but not times: on blocks that are allocated and written
// already, it commits nothing to the journal of ext4.
func (f file) Sync() error {
	if f.noSync {
		return nil
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	cerr := rc.Control(func(fd uintptr) {
		for {
			if err = unix.Fdatasync(int(fd)); !errors.Is(err, unix.EINTR) {
				return
			}
		}
	})
	return errors.Join(cerr, err)
}

// ErrCached is the error of a drop that left pages of a file in the page
// cache: Linux keeps a page that something holds, such as a pipe that
// another process spliced it into.
var ErrCached = errors.New("osfs: pages stay in the page cache")

// DropCache drops the file's clean pages from the page cache with
// fadvise(POSIX_FADV_DONTNEED), so that a later read returns what the
// disk holds: after a sync every page is clean, a page that a failed
// writeback marked clean among them (#372). Linux skips a page that is
// held, and fadvise still succeeds, so DropCache then maps the file and
// asks mincore whether any page stayed, which is ErrCached. A file system
// in memory has no disk to read back, and neither has a directory whose
// syncs are no-ops.
func (f file) DropCache() error {
	if f.noSync {
		return nil
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	cerr := rc.Control(func(fd uintptr) {
		var fs unix.Statfs_t
		if err = unix.Fstatfs(int(fd), &fs); err != nil || fs.Type == unix.TMPFS_MAGIC || fs.Type == unix.RAMFS_MAGIC {
			return
		}
		if err = unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED); err != nil || st.Size() == 0 {
			return
		}
		err = cached(int(fd), st.Size())
	})
	return errors.Join(cerr, err)
}

// cached fails with ErrCached if a page of the size bytes of file fd is in
// the page cache. The mapping reads nothing, so it brings no page in.
func cached(fd int, size int64) error {
	m, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	vec := make([]byte, (size+int64(os.Getpagesize())-1)/int64(os.Getpagesize()))
	if err := errors.Join(mincore(m, vec), unix.Munmap(m)); err != nil {
		return err
	}
	n := 0
	for _, b := range vec {
		n += int(b & 1)
	}
	if n > 0 {
		return fmt.Errorf("%w: %d of %d pages", ErrCached, n, len(vec))
	}
	return nil
}

func (f file) ReadAll() ([]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// One buffer of the file's size: io.ReadAll's growing buffers held
	// about two and a half times a large file at once (#292).
	b := make([]byte, st.Size())
	n, err := f.ReadAt(b, 0)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return b[:n], err
}

// mincore sets the low bit of vec[i] if page i of mapping m is in the page
// cache, as mincore(2) does; x/sys/unix has no wrapper for it.
func mincore(m, vec []byte) error {
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return errno
	}
	return nil
}
