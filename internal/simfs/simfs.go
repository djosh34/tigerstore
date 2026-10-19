// Package simfs is a simulated disk for one node. It keeps what a real
// disk keeps across a crash: file contents up to the last Sync and the
// directory as of the last SyncDir. A crash drops everything else, except
// for a random torn part of the unsynced writes: a prefix of them, in the
// order they were made.
//
// It implements wal.FS and is deterministic for a given random source.
package simfs

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
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
	OpRename
)

type file struct {
	// id tells files apart in the trace across their renames.
	id      int
	data    []byte
	durable []byte
	// pending are the writes since the last sync, oldest first.
	pending []write
	// cut is set when a truncate removed synced bytes, so data no longer
	// starts with durable.
	cut bool
}

// write is one write: p at offset off.
type write struct {
	off int
	p   []byte
}

// apply writes p at off into b, growing it with zeros as needed.
func apply(b []byte, off int, p []byte) []byte {
	if end := off + len(p); end > len(b) {
		b = append(b, make([]byte, end-len(b))...)
	}
	copy(b[off:], p)
	return b
}

var _ wal.FS = (*Disk)(nil)

// Disk is one node's directory.
type Disk struct {
	names   map[string]*file
	durable map[string]*file

	// Fault, if set, runs before every operation. A non-nil result fails
	// the operation without changing anything.
	Fault func(op Op, name string) error

	// Trace, if set, gets a line for every call that changes the disk,
	// and for each file a crash keeps, for a failure's report (#274).
	Trace func(string)

	// short, when set, makes the next write a short one.
	short *rand.Rand
	// files counts the files created, for their ids.
	files int
}

// New returns an empty disk.
func New() *Disk {
	return &Disk{names: map[string]*file{}, durable: map[string]*file{}}
}

func (d *Disk) tracef(format string, args ...any) {
	if d.Trace != nil {
		d.Trace(fmt.Sprintf(format, args...))
	}
}

// Files returns a copy of every file's content, synced or not, by name.
func (d *Disk) Files() map[string][]byte {
	out := map[string][]byte{}
	for name, f := range d.names {
		out[name] = slices.Clone(f.data)
	}
	return out
}

// Crash drops what was not made durable. Each file keeps its synced
// content plus a random prefix of the bytes of its unsynced writes, in
// order, and with probability 1/4 the last kept bytes are garbage, like a
// torn sector.
func (d *Disk) Crash(rng *rand.Rand) {
	d.names = make(map[string]*file, len(d.durable))
	for _, name := range sortedNames(d.durable) {
		f := d.durable[name]
		kept := slices.Clone(f.durable)
		var stream []byte
		for _, w := range f.pending {
			stream = append(stream, w.p...)
		}
		if !f.cut && len(stream) > 0 {
			n := rng.IntN(len(stream) + 1)
			stream = stream[:n]
			if n > 0 && rng.IntN(4) == 0 {
				g := rng.IntN(n) + 1
				for i := n - g; i < n; i++ {
					stream[i] = byte(rng.Uint32())
				}
			}
			for _, w := range f.pending {
				if len(stream) == 0 {
					break
				}
				k := min(len(stream), len(w.p))
				kept = apply(kept, w.off, stream[:k])
				stream = stream[k:]
			}
		}
		if d.Trace != nil {
			unsynced := 0
			for _, w := range f.pending {
				unsynced += len(w.p)
			}
			d.tracef("crash: %s (file %d) keeps %d bytes: %d synced, %d unsynced bytes written since, cut %v", name, f.id, len(kept), len(f.durable), unsynced, f.cut)
		}
		f.data = kept
		f.durable = slices.Clone(kept)
		f.pending, f.cut = nil, false
		d.names[name] = f
	}
}

// Durable returns a new disk that holds only this disk's durable state, as
// a copy taken by pulling the power.
func (d *Disk) Durable() *Disk {
	c := New()
	for _, name := range sortedNames(d.durable) {
		f := d.durable[name]
		g := &file{id: f.id, data: slices.Clone(f.durable), durable: slices.Clone(f.durable)}
		c.names[name] = g
		c.durable[name] = g
	}
	c.files = d.files
	return c
}

// Copy returns a new disk whose files hold what this disk's hold now,
// synced or not, all durable.
func (d *Disk) Copy() *Disk {
	c := New()
	for _, name := range sortedNames(d.names) {
		g := &file{id: d.names[name].id, data: slices.Clone(d.names[name].data), durable: slices.Clone(d.names[name].data)}
		c.names[name] = g
		c.durable[name] = g
	}
	c.files = d.files
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
	f.pending, f.cut = nil, false
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
	f.pending, f.cut = nil, false
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
	err := d.Fault(op, name)
	if err != nil {
		d.tracef("%s %s fails: %v", op, name, err)
	}
	return err
}

func (op Op) String() string {
	names := [...]string{"create", "open", "read", "write", "truncate", "sync", "remove", "sync-dir", "rename"}
	if int(op) < len(names) {
		return names[op]
	}
	return fmt.Sprintf("op %d", int(op))
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
	d.files++
	f := &file{id: d.files}
	d.names[name] = f
	d.tracef("create %s (file %d)", name, f.id)
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

// Rename gives file oldName the name newName, replacing any file of that
// name. Like a creation, it only survives a crash after SyncDir.
func (d *Disk) Rename(oldName, newName string) error {
	if err := d.fault(OpRename, oldName); err != nil {
		return err
	}
	f, ok := d.names[oldName]
	if !ok {
		return fs.ErrNotExist
	}
	if g, ok := d.names[newName]; ok {
		d.tracef("rename %s (file %d) to %s, replacing file %d", oldName, f.id, newName, g.id)
	} else {
		d.tracef("rename %s (file %d) to %s", oldName, f.id, newName)
	}
	delete(d.names, oldName)
	d.names[newName] = f
	return nil
}

// Remove deletes a file from the directory.
func (d *Disk) Remove(name string) error {
	if err := d.fault(OpRemove, name); err != nil {
		return err
	}
	f, ok := d.names[name]
	if !ok {
		return fs.ErrNotExist
	}
	d.tracef("remove %s (file %d)", name, f.id)
	delete(d.names, name)
	return nil
}

// SyncDir makes the current set of names durable.
func (d *Disk) SyncDir() error {
	if err := d.fault(OpSyncDir, ""); err != nil {
		return err
	}
	d.durable = maps.Clone(d.names)
	d.tracef("sync the directory")
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
	return h.WriteAt(p, int64(len(h.f.data)))
}

// WriteAt writes a copy of p at offset off, growing the file with zeros
// up to off if it is shorter.
func (h *Handle) WriteAt(p []byte, off int64) (int, error) {
	if err := h.check(OpWrite); err != nil {
		return 0, err
	}
	n, err := len(p), error(nil)
	if rng := h.d.short; rng != nil {
		h.d.short = nil
		n, err = rng.IntN(len(p)+1), ErrNoSpace
	}
	if gap := int(off) - len(h.f.data); gap > 0 {
		h.write(len(h.f.data), make([]byte, gap))
	}
	h.write(int(off), p[:n])
	h.d.tracef("write %s (file %d) at %d: %d of %d bytes, crc %08x, %v", h.name, h.f.id, off, n, len(p), crc32.ChecksumIEEE(p[:n]), err)
	return n, err
}

func (h *Handle) write(off int, p []byte) {
	p = slices.Clone(p)
	h.f.data = apply(h.f.data, off, p)
	h.f.pending = append(h.f.pending, write{off, p})
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
	h.d.tracef("truncate %s (file %d) to %d, cut %v", h.name, h.f.id, size, h.f.cut)
	// A later sync keeps only what is left of the writes before it.
	for i, w := range h.f.pending {
		if end := int64(w.off + len(w.p)); end > size {
			h.f.pending[i].p = w.p[:max(0, size-int64(w.off))]
		}
	}
	return nil
}

// Sync makes the content durable.
func (h *Handle) Sync() error {
	if err := h.check(OpSync); err != nil {
		if errors.Is(err, ErrIO) && !h.f.cut {
			// As on Linux, a failed writeback marks the pages clean: the
			// unsynced bytes stay readable but never reach the disk, which
			// keeps what it had there, zeros past its end, and a later
			// sync does not retry them.
			h.f.durable = append(h.f.durable, make([]byte, max(0, len(h.f.data)-len(h.f.durable)))...)
			h.f.pending = nil
		}
		return err
	}
	if h.f.cut {
		h.f.durable = slices.Clone(h.f.data)
	} else {
		for _, w := range h.f.pending {
			h.f.durable = apply(h.f.durable, w.off, w.p)
		}
	}
	h.f.pending, h.f.cut = nil, false
	h.d.tracef("sync %s (file %d): %d bytes durable", h.name, h.f.id, len(h.f.durable))
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
	for _, w := range f.pending {
		if off >= w.off && off < w.off+len(w.p) {
			w.p[off-w.off] ^= 1
		}
	}
}

// FlipDurable flips the lowest bit of one synced byte, like a bit that
// rots on the medium.
func (d *Disk) FlipDurable(name string, off int) {
	f := d.durable[name]
	f.durable[off] ^= 1
	f.data = slices.Clone(f.durable)
	f.pending, f.cut = nil, false
}
