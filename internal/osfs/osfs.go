// Package osfs is the real disk adapter: a directory on the local file
// system that implements wal.FS.
package osfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Dir is a directory on disk.
type Dir struct{ path string }

var _ wal.FS = Dir{}

// New creates the directory if needed and makes its creation durable.
func New(path string) (Dir, error) {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return Dir{}, err
	}
	if err := syncPath(filepath.Dir(path)); err != nil {
		return Dir{}, err
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
