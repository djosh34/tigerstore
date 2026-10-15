// Package osfs is the real disk adapter: a directory on the local file
// system that implements wal.FS.
package osfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Dir is a directory on disk.
type Dir struct{ path string }

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

// Create makes a new empty file.
func (d Dir) Create(name string) (wal.File, error) {
	f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return file{f}, nil
}

// Open opens an existing file for reading and appending.
func (d Dir) Open(name string) (wal.File, error) {
	f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	return file{f}, nil
}

// Remove deletes a file.
func (d Dir) Remove(name string) error { return os.Remove(filepath.Join(d.path, name)) }

// SyncDir makes creations and removals durable.
func (d Dir) SyncDir() error { return syncPath(d.path) }

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

type file struct{ *os.File }

func (f file) ReadAll() ([]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.NewSectionReader(f.File, 0, st.Size()))
}
