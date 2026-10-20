package gosim291_test

import (
	"sync"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Only lock the existing simfs; do not substitute gosim's filesystem.
type disk struct {
	mu sync.Mutex
	d  *simfs.Disk
}

func (d *disk) List() ([]string, error) { d.mu.Lock(); defer d.mu.Unlock(); return d.d.List() }
func (d *disk) Usage() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(d.d.Bytes()), nil
}
func (d *disk) Create(name string) (wal.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.d.Create(name)
	if err != nil {
		return nil, err
	}
	return &file{d: d, f: f}, nil
}
func (d *disk) Open(name string) (wal.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.d.Open(name)
	if err != nil {
		return nil, err
	}
	return &file{d: d, f: f}, nil
}
func (d *disk) Remove(name string) error { d.mu.Lock(); defer d.mu.Unlock(); return d.d.Remove(name) }
func (d *disk) Rename(a, b string) error { d.mu.Lock(); defer d.mu.Unlock(); return d.d.Rename(a, b) }
func (d *disk) SyncDir() error           { d.mu.Lock(); defer d.mu.Unlock(); return d.d.SyncDir() }

type file struct {
	d *disk
	f wal.File
}

func (f *file) ReadAll() ([]byte, error) { f.d.mu.Lock(); defer f.d.mu.Unlock(); return f.f.ReadAll() }
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.ReadAt(p, off)
}
func (f *file) Write(p []byte) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Write(p)
}
func (f *file) WriteAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.WriteAt(p, off)
}
func (f *file) Truncate(n int64) error { f.d.mu.Lock(); defer f.d.mu.Unlock(); return f.f.Truncate(n) }
func (f *file) Sync() error            { f.d.mu.Lock(); defer f.d.mu.Unlock(); return f.f.Sync() }
func (f *file) Close() error           { f.d.mu.Lock(); defer f.d.mu.Unlock(); return f.f.Close() }
