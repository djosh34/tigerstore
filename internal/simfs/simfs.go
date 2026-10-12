// Package simfs is a simulated disk for one node. It keeps what a real
// disk keeps across a crash: file contents up to the last Sync and the
// directory as of the last SyncDir. A crash drops everything else, except
// for a random torn part of unsynced appends.
//
// It implements wal.FS and is deterministic for a given random source.
package simfs

import (
	"errors"
	"io/fs"
	"maps"
	"math/rand/v2"
	"slices"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Op names a disk operation, for fault hooks.
type Op int

// The operations a fault hook sees.
const (
	OpCreate Op = iota
	OpOpen
	OpRead
	OpWrite
	OpTruncate
	OpSync
	OpRemove
	OpSyncDir
)

type file struct {
	data    []byte
	durable []byte
	// cut is set when a truncate removed synced bytes, so data no longer
	// starts with durable.
	cut bool
}

var _ wal.FS = (*Disk)(nil)

// Disk is one node's directory.
type Disk struct {
	names   map[string]*file
	durable map[string]*file

	// Fault, if set, runs before every operation. A non-nil result fails
	// the operation without changing anything.
	Fault func(op Op, name string) error
}

// New returns an empty disk.
func New() *Disk {
	return &Disk{names: map[string]*file{}, durable: map[string]*file{}}
}

// Crash drops what was not made durable. Each file keeps its synced
// content plus a random prefix of its unsynced appends, and with
// probability 1/4 the last kept bytes are garbage, like a torn sector.
func (d *Disk) Crash(rng *rand.Rand) {
	d.names = make(map[string]*file, len(d.durable))
	for _, name := range sortedNames(d.durable) {
		f := d.durable[name]
		kept := slices.Clone(f.durable)
		if !f.cut && len(f.data) > len(f.durable) {
			extra := f.data[len(f.durable):]
			n := rng.IntN(len(extra) + 1)
			kept = append(kept, extra[:n]...)
			if n > 0 && rng.IntN(4) == 0 {
				g := rng.IntN(n) + 1
				for i := len(kept) - g; i < len(kept); i++ {
					kept[i] = byte(rng.Uint32())
				}
			}
		}
		f.data = kept
		f.durable = slices.Clone(kept)
		f.cut = false
		d.names[name] = f
	}
}

// Erase removes everything, as if the disk was replaced.
func (d *Disk) Erase() {
	d.names = map[string]*file{}
	d.durable = map[string]*file{}
}

// Bytes returns the total size of the files.
func (d *Disk) Bytes() int {
	n := 0
	for _, f := range d.names {
		n += len(f.data)
	}
	return n
}

func sortedNames(m map[string]*file) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

func (d *Disk) fault(op Op, name string) error {
	if d.Fault == nil {
		return nil
	}
	return d.Fault(op, name)
}

// List returns the file names in order.
func (d *Disk) List() ([]string, error) {
	if err := d.fault(OpRead, ""); err != nil {
		return nil, err
	}
	return sortedNames(d.names), nil
}

// Create makes a new empty file.
func (d *Disk) Create(name string) (wal.File, error) {
	if err := d.fault(OpCreate, name); err != nil {
		return nil, err
	}
	if _, ok := d.names[name]; ok {
		return nil, fs.ErrExist
	}
	f := &file{}
	d.names[name] = f
	return &Handle{d: d, f: f, name: name}, nil
}

// Open opens an existing file.
func (d *Disk) Open(name string) (wal.File, error) {
	if err := d.fault(OpOpen, name); err != nil {
		return nil, err
	}
	f, ok := d.names[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &Handle{d: d, f: f, name: name}, nil
}

// Remove deletes a file from the directory.
func (d *Disk) Remove(name string) error {
	if err := d.fault(OpRemove, name); err != nil {
		return err
	}
	if _, ok := d.names[name]; !ok {
		return fs.ErrNotExist
	}
	delete(d.names, name)
	return nil
}

// SyncDir makes the current set of names durable.
func (d *Disk) SyncDir() error {
	if err := d.fault(OpSyncDir, ""); err != nil {
		return err
	}
	d.durable = maps.Clone(d.names)
	return nil
}

// Handle is an open file.
type Handle struct {
	d      *Disk
	f      *file
	name   string
	closed bool
}

var errClosed = errors.New("simfs: file is closed")

func (h *Handle) check(op Op) error {
	if h.closed {
		return errClosed
	}
	return h.d.fault(op, h.name)
}

// ReadAll returns a copy of the content.
func (h *Handle) ReadAll() ([]byte, error) {
	if err := h.check(OpRead); err != nil {
		return nil, err
	}
	return slices.Clone(h.f.data), nil
}

// Write appends a copy of p.
func (h *Handle) Write(p []byte) (int, error) {
	if err := h.check(OpWrite); err != nil {
		return 0, err
	}
	h.f.data = append(h.f.data, p...)
	return len(p), nil
}

// Truncate cuts the file.
func (h *Handle) Truncate(size int64) error {
	if err := h.check(OpTruncate); err != nil {
		return err
	}
	if size < int64(len(h.f.data)) {
		h.f.data = h.f.data[:size:size]
	}
	if size < int64(len(h.f.durable)) {
		h.f.cut = true
	}
	return nil
}

// Sync makes the content durable.
func (h *Handle) Sync() error {
	if err := h.check(OpSync); err != nil {
		return err
	}
	if h.f.cut {
		h.f.durable = slices.Clone(h.f.data)
		h.f.cut = false
	} else {
		h.f.durable = append(h.f.durable, h.f.data[len(h.f.durable):]...)
	}
	return nil
}

// Close closes the handle.
func (h *Handle) Close() error {
	if h.closed {
		return errClosed
	}
	h.closed = true
	return nil
}
