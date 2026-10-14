// Package simfs is a simulated disk for one node. It keeps what a real
// disk keeps across a crash: file contents up to the last Sync and the
// directory as of the last SyncDir. A crash drops everything else, except
// for a random torn part of unsynced appends.
//
// It implements wal.FS and is deterministic for a given random source.
package simfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"

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

	// short, when set, makes the next write a short one.
	short *rand.Rand
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

// Durable returns a new disk that holds only this disk's durable state, as
// a copy taken by pulling the power.
func (d *Disk) Durable() *Disk {
	c := New()
	for _, name := range sortedNames(d.durable) {
		f := d.durable[name]
		g := &file{data: slices.Clone(f.durable), durable: slices.Clone(f.durable)}
		c.names[name] = g
		c.durable[name] = g
	}
	return c
}

// Corrupt copies 16 to 64 synced bytes from a random file and offset over
// synced bytes of a WAL segment, like a write that landed at the wrong
// offset. It leaves the newest segment alone: the WAL names segments in
// order, and damage in the newest one can look the same as a torn write,
// which recovery cuts off. It reports whether it changed anything.
func (d *Disk) Corrupt(rng *rand.Rand) bool {
	names := sortedNames(d.durable)
	var segs []string
	for _, name := range names {
		if strings.HasSuffix(name, ".wal") {
			segs = append(segs, name)
		}
	}
	if len(segs) < 2 {
		return false
	}
	var targets []string
	for _, name := range segs[:len(segs)-1] {
		if len(d.durable[name].durable) >= 64 {
			targets = append(targets, name)
		}
	}
	if len(targets) == 0 {
		return false
	}
	f := d.durable[targets[rng.IntN(len(targets))]]
	n := 16 + rng.IntN(49)
	src := d.durable[names[rng.IntN(len(names))]].durable
	if len(src) < n || len(f.durable) < n {
		return false
	}
	from := rng.IntN(len(src) - n + 1)
	at := rng.IntN(len(f.durable) - n + 1)
	if bytes.Equal(f.durable[at:at+n], src[from:from+n]) {
		return false
	}
	copy(f.durable[at:at+n], slices.Clone(src[from:from+n]))
	f.data = slices.Clone(f.durable)
	f.cut = false
	return true
}

// CorruptFile flips one bit of a file's synced bytes, if it has any.
func (d *Disk) CorruptFile(rng *rand.Rand, name string) bool {
	f, ok := d.durable[name]
	if !ok || len(f.durable) == 0 {
		return false
	}
	f.durable[rng.IntN(len(f.durable))] ^= 1 << rng.IntN(8)
	f.data = slices.Clone(f.durable)
	f.cut = false
	return true
}

// ErrNoSpace is the error of a write that ran out of space.
var ErrNoSpace = errors.New("simfs: no space left on device")

// ErrIO is the error of a failed write or sync.
var ErrIO = errors.New("simfs: input/output error")

// ClearFaults removes the fault hook and a pending short write.
func (d *Disk) ClearFaults() {
	d.Fault = nil
	d.short = nil
}

// ShortWrite makes the next write store only a prefix of its bytes and fail
// with ErrNoSpace, as a full disk does.
func (d *Disk) ShortWrite(rng *rand.Rand) {
	d.short = rng
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

// ReadAt reads len(p) bytes at offset off.
func (h *Handle) ReadAt(p []byte, off int64) (int, error) {
	if err := h.check(OpRead); err != nil {
		return 0, err
	}
	if off < 0 || off > int64(len(h.f.data)) {
		return 0, io.EOF
	}
	n := copy(p, h.f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Write appends a copy of p.
func (h *Handle) Write(p []byte) (int, error) {
	if err := h.check(OpWrite); err != nil {
		return 0, err
	}
	if rng := h.d.short; rng != nil {
		h.d.short = nil
		n := rng.IntN(len(p) + 1)
		h.f.data = append(h.f.data, p[:n]...)
		return n, ErrNoSpace
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
		if errors.Is(err, ErrIO) && !h.f.cut {
			// As on Linux, a failed writeback marks the pages clean: the
			// unsynced bytes stay readable but never reach the disk, which
			// keeps zeros there, and a later sync does not retry them.
			h.f.durable = append(h.f.durable, make([]byte, len(h.f.data)-len(h.f.durable))...)
		}
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

// DurableNames returns the names of the files on the durable directory,
// sorted.
func (d *Disk) DurableNames() []string { return sortedNames(d.durable) }

// DurableBytes returns a copy of a file's synced bytes.
func (d *Disk) DurableBytes(name string) []byte {
	if f, ok := d.durable[name]; ok {
		return slices.Clone(f.durable)
	}
	return nil
}

// Rot flips the lowest bit of one byte of a file, synced or not, while
// the file is in use, like a bit that rots under a running node.
func (d *Disk) Rot(name string, off int) {
	f := d.names[name]
	if off < len(f.durable) {
		f.durable[off] ^= 1
	}
	if off < len(f.data) {
		f.data[off] ^= 1
	}
}

// FlipDurable flips the lowest bit of one synced byte, like a bit that
// rots on the medium.
func (d *Disk) FlipDurable(name string, off int) {
	f := d.durable[name]
	f.durable[off] ^= 1
	f.data = slices.Clone(f.durable)
	f.cut = false
}
