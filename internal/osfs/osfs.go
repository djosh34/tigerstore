// Package osfs is the real disk adapter: a directory on the local file
// system that implements wal.FS.
package osfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

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

// DropCache drops the file's clean pages from the page cache and starts
// writing back its dirty ones. A cgroup charges a node for the page cache
// of its files, which would grow past the eighth of the budget that
// node.GoMemory leaves for the kernel (#226).
func (f file) DropCache() error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	cerr := rc.Control(func(fd uintptr) { err = unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED) })
	return errors.Join(cerr, err)
}

func (f file) ReadAll() ([]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.NewSectionReader(f.File, 0, st.Size()))
}
