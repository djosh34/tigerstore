// Package simfs is a simulated disk for one node. It keeps what a real
// disk keeps across a power loss: file contents up to the last Sync and
// the directory as of the last SyncDir. A crash drops everything else,
// except for what of it reached the medium anyway: some of the writes
// since the last sync (Image), and each directory operation since the
// last directory sync on its own. A process that dies without a power
// loss leaves the disk as it is: the page cache keeps every write.
//
// It also fails the way disks fail: a failed write or sync, a file system
// that shuts down or goes read-only, a full disk, and damage after a
// successful sync, such as sectors that fail every read, reads that
// return wrong data, and lost or misdirected writes.
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
	id int
	// durable is what the medium holds.
	durable pages
	// size is how long the file is now, synced or not, and dirty are the
	// page cache's pages that differ from durable's or that a pending
	// write touched. Every other page reads as durable's, with zeros past
	// size, and at or past cutAt when cut. A dirty page holds zeros past
	// size.
	size  int
	dirty map[int]*block
	// diverged is set when the page cache may hold bytes that neither the
	// medium nor a pending write holds: a failed sync left them readable
	// (#372), or a bit rotted under the running node.
	diverged bool
	// pending are the writes since the last sync, oldest first.
	pending []write
	// cut is set when a truncate removed synced bytes, so the content no
	// longer starts with durable: its bytes at or past cutAt are gone.
	cut   bool
	cutAt int
	// bad are the sectors of the medium, by index, that fail every read
	// until a write covers them. misread, if set, makes the next read of
	// synced bytes return them wrong.
	bad     map[int]bool
	misread *rand.Rand
}

// SectorBytes and PageBytes are the units a crash tears writes at: the
// disk's sector and the page cache's page.
const (
	SectorBytes = 512
	PageBytes   = 4096
)

// write is one write: p at offset off.
type write struct {
	off int
	p   []byte
}

// limit is where the clean pages' bytes end: past it they read as zeros.
func (f *file) limit() int {
	if f.cut {
		return min(f.size, f.cutAt)
	}
	return f.size
}

// read fills dst with the content at off, zeros past size.
func (f *file) read(dst []byte, off int) {
	n := max(0, min(len(dst), f.size-off))
	clear(dst[n:])
	limit := f.limit()
	for k := 0; k < n; {
		o := off + k
		i, at := o/PageBytes, o%PageBytes
		m := min(n-k, PageBytes-at)
		if d := f.dirty[i]; d != nil {
			copy(dst[k:k+m], d[at:at+m])
		} else {
			c := max(0, min(m, limit-o))
			f.durable.read(dst[k:k+c], o)
			clear(dst[k+c : k+m])
		}
		k += m
	}
}

// content returns a copy of the content.
func (f *file) content() []byte {
	b := make([]byte, f.size)
	f.read(b, 0)
	return b
}

// clean returns page i as it reads without the page cache's own bytes.
func (f *file) clean(i int) *block {
	b := new(block)
	lo := i * PageBytes
	c := max(0, min(PageBytes, f.limit()-lo))
	f.durable.read(b[:c], lo)
	return b
}

// dirtyPage returns page i of the page cache for a change.
func (f *file) dirtyPage(i int) *block {
	if d := f.dirty[i]; d != nil {
		return d
	}
	if f.dirty == nil {
		f.dirty = map[int]*block{}
	}
	d := f.clean(i)
	f.dirty[i] = d
	return d
}

// reset makes the content what the medium holds, with nothing pending.
func (f *file) reset() {
	f.size = f.durable.size
	f.dirty, f.diverged, f.pending, f.cut, f.cutAt = nil, false, nil, false, 0
}

var _ wal.FS = (*Disk)(nil)

// Disk is one node's directory.
type Disk struct {
	names   map[string]*file
	durable map[string]*file
	// dirOps are the directory operations since the last SyncDir, oldest
	// first; a crash keeps each of them or not on its own.
	dirOps []dirOp

	// Fault, if set, runs before every operation. A non-nil result fails
	// the operation without changing anything.
	Fault func(op Op, name string) error

	// Trace, if set, gets a line for every call that changes the disk,
	// and for each file a crash keeps, for a failure's report (#274): its
	// format and arguments, as fmt.Sprintf takes them, so that a caller
	// that keeps them formats only the lines it reports.
	Trace func(format string, args ...any)

	// Capacity, if set, is how many bytes the files may hold in all. A
	// write past it stores what fits and fails with ErrNoSpace.
	Capacity int

	// short, when set, makes the next write a short one, and failSync the
	// next sync fail after part of the writes reached the medium.
	short, failSync *rand.Rand
	// down, if set, is what every operation fails with while the file
	// system is shut down (ErrIO), or each that changes it while it is
	// read-only (ErrReadOnly).
	down error
	// files counts the files created, for their ids.
	files int
	// st is where the synced pages of large files go, if anywhere
	// (KeepIn), shared with the disk's copies.
	st *storage
	// Counts counts what the disk's faults did.
	Counts Counts
}

// Counts counts what a disk's faults did: writes a crash tore, crashes
// that kept a write and lost an earlier one of the same file, directory
// operations a crash lost, syncs that failed after part of their writes
// reached the disk, reads that hit a bad sector, and reads that returned
// wrong data.
type Counts struct {
	TornWrites, Reordered, LostDirOps, FailedSyncs, BadReads, Misreads int
}

// dirOp is a directory operation: a creation, a rename or a removal of
// file f.
type dirOp struct {
	op       Op
	name, to string
	f        *file
}

// New returns an empty disk.
func New() *Disk {
	return &Disk{names: map[string]*file{}, durable: map[string]*file{}}
}

// KeepIn makes the disk and the copies it makes from now on keep the
// synced pages of each file of from bytes or more in s, all but its last,
// instead of in memory. It changes nothing the disk does: a read of such
// a page reads s. An error of s is the disk's StoreErr, and a page s
// failed to read reads as zeros.
func (d *Disk) KeepIn(s Store, from int) {
	d.st = &storage{store: s, from: from}
	for _, files := range []map[string]*file{d.names, d.durable} {
		for _, f := range files {
			f.durable.st = d.st
		}
	}
}

// StoreErr returns the first error of the disk's Store (KeepIn), which
// leaves what the disk reads wrong: a caller must fail with it.
func (d *Disk) StoreErr() error {
	if d.st == nil {
		return nil
	}
	return d.st.err
}

func (d *Disk) tracef(format string, args ...any) {
	if d.Trace != nil {
		d.Trace(format, args...)
	}
}

// Files returns a copy of every file's content, synced or not, by name.
func (d *Disk) Files() map[string][]byte {
	out := map[string][]byte{}
	for name, f := range d.names {
		out[name] = f.content()
	}
	return out
}

// EachFile calls f with the name and the bytes of each file now, in name
// order.
func (d *Disk) EachFile(f func(name string, data []byte)) {
	for _, name := range sortedNames(d.names) {
		f(name, d.names[name].content())
	}
}

// Image says what a crash keeps of the writes since a file's last sync.
type Image int

const (
	// Prefix keeps a prefix of the bytes written, in the order written;
	// with probability 1/4 the last kept bytes are garbage, like a torn
	// sector.
	Prefix Image = iota
	// Writes keeps any subset of the writes, each one whole.
	Writes
	// Sectors keeps any subset of the writes, each whole, or torn at
	// sector boundaries into pieces of which each reached the medium or
	// not.
	Sectors
	// Pages is Sectors at page boundaries.
	Pages
)

func (img Image) String() string {
	return [...]string{"prefix", "whole writes", "sectors", "pages"}[img]
}

// Crash drops what was not made durable, as a power loss does, keeping a
// random Image of the unsynced writes.
func (d *Disk) Crash(rng *rand.Rand) { d.CrashImage(rng, Image(rng.IntN(4))) }

// CrashImage is Crash with the given image. Each directory operation
// since the last SyncDir survives or not on its own; each file the
// directory then names keeps its synced content plus what img keeps of
// its unsynced writes. Those were in place before the synced ones were,
// so a later sync never makes them durable.
func (d *Disk) CrashImage(rng *rand.Rand, img Image) {
	dir := maps.Clone(d.durable)
	for _, o := range d.dirOps {
		kept := rng.IntN(2) == 0
		d.tracef("crash: %v %s (file %d) %s, kept %v", o.op, o.name, o.f.id, o.to, kept)
		if !kept {
			d.Counts.LostDirOps++
			continue
		}
		switch {
		case o.op == OpCreate:
			dir[o.name] = o.f
		case dir[o.name] != o.f:
			// A rename or removal of a file whose creation was lost.
		case o.op == OpRename:
			delete(dir, o.name)
			dir[o.to] = o.f
		default:
			delete(dir, o.name)
		}
	}
	d.names = make(map[string]*file, len(dir))
	for _, name := range sortedNames(dir) {
		f := dir[name]
		d.names[name] = f
		synced := f.durable.size
		kept := f.survive(rng, img, &d.Counts)
		if d.Trace != nil {
			unsynced := 0
			for _, w := range f.pending {
				unsynced += len(w.p)
			}
			d.tracef("crash: %s (file %d) keeps %d bytes: %d synced, %d unsynced bytes written since, cut %v, %v", name, f.id, kept.size, synced, unsynced, f.cut, img)
		}
		f.durable = kept
		f.durable.spill()
		f.reset()
	}
	d.durable = maps.Clone(d.names)
	d.dirOps = nil
}

// survive returns f's synced content with what img keeps of its unsynced
// writes applied, in the order they were made, and counts the writes it
// tore and whether it kept one after an earlier one it lost. A file cut
// below its synced size keeps only that. f's synced content goes into
// the result, which its caller puts in its place.
func (f *file) survive(rng *rand.Rand, img Image, c *Counts) pages {
	kept := f.durable
	f.durable = pages{}
	if f.cut || len(f.pending) == 0 {
		return kept
	}
	if img == Prefix {
		var stream []byte
		for _, w := range f.pending {
			stream = append(stream, w.p...)
		}
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
			kept.write(w.off, stream[:k])
			stream = stream[k:]
		}
		return kept
	}
	lost, reordered := false, false
	for _, w := range f.pending {
		k := rng.IntN(3)
		if img == Writes {
			k = rng.IntN(2)
		}
		switch k {
		case 0:
			kept.write(w.off, w.p)
			reordered = reordered || lost
		case 1:
			lost = lost || len(w.p) > 0
		default:
			unit := SectorBytes
			if img == Pages {
				unit = PageBytes
			}
			ps := pieces(w, unit)
			n := 0
			for _, p := range ps {
				if rng.IntN(2) == 0 {
					kept.write(p.off, p.p)
					n++
				}
			}
			if n > 0 && n < len(ps) {
				c.TornWrites++
			}
			reordered = reordered || lost && n > 0
			lost = lost || n < len(ps)
		}
	}
	if reordered {
		c.Reordered++
	}
	return kept
}

// pieces splits w at the multiples of unit.
func pieces(w write, unit int) []write {
	var out []write
	for off, end := w.off, w.off+len(w.p); off < end; {
		next := min((off/unit+1)*unit, end)
		out = append(out, write{off, w.p[off-w.off : next-w.off]})
		off = next
	}
	return out
}

// Durable returns a new disk that holds only this disk's durable state, as
// a copy taken by pulling the power.
func (d *Disk) Durable() *Disk {
	c := New()
	for _, name := range sortedNames(d.durable) {
		f := d.durable[name]
		g := &file{id: f.id, durable: f.durable.share()}
		g.reset()
		c.names[name] = g
		c.durable[name] = g
	}
	c.files = d.files
	c.st = d.st
	return c
}

// Copy returns a new disk with this disk's files, under the names they
// have now, all durable: each holds what the medium holds and what the
// page cache holds, the writes since the last sync, which a sync writes,
// and the bytes a failed sync left readable, which none does (#372).
func (d *Disk) Copy() *Disk {
	c := New()
	for _, name := range sortedNames(d.names) {
		f := d.names[name]
		g := &file{id: f.id, durable: f.durable.share(), size: f.size, diverged: f.diverged, cut: f.cut, cutAt: f.cutAt}
		for i, p := range f.dirty {
			if g.dirty == nil {
				g.dirty = map[int]*block{}
			}
			b := *p
			g.dirty[i] = &b
		}
		for _, w := range f.pending {
			g.pending = append(g.pending, write{w.off, slices.Clone(w.p)})
		}
		c.names[name] = g
		c.durable[name] = g
	}
	c.files = d.files
	c.st = d.st
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
		if d.durable[name].durable.size >= 64 {
			targets = append(targets, name)
		}
	}
	if len(targets) == 0 {
		return false
	}
	f := d.durable[targets[rng.IntN(len(targets))]]
	n := 16 + rng.IntN(49)
	src := &d.durable[names[rng.IntN(len(names))]].durable
	if src.size < n || f.durable.size < n {
		return false
	}
	from := rng.IntN(src.size - n + 1)
	at := rng.IntN(f.durable.size - n + 1)
	b, old := make([]byte, n), make([]byte, n)
	src.read(b, from)
	f.durable.read(old, at)
	if bytes.Equal(old, b) {
		return false
	}
	f.durable.write(at, b)
	f.reset()
	return true
}

// CorruptFile flips one bit of a file's synced bytes, if it has any.
func (d *Disk) CorruptFile(rng *rand.Rand, name string) bool {
	f, ok := d.durable[name]
	if !ok || f.durable.size == 0 {
		return false
	}
	i := rng.IntN(f.durable.size)
	f.durable.own(i / PageBytes)[i%PageBytes] ^= 1 << rng.IntN(8)
	f.reset()
	return true
}

// LoseWrite makes one sector of a file's synced bytes read as zeros, as
// a write the drive reported done but never made. It reports whether it
// changed anything.
func (d *Disk) LoseWrite(rng *rand.Rand, name string) bool {
	f, ok := d.durable[name]
	if !ok || f.durable.size == 0 {
		return false
	}
	at := rng.IntN((f.durable.size+SectorBytes-1)/SectorBytes) * SectorBytes
	b := make([]byte, min(SectorBytes, f.durable.size-at))
	f.durable.read(b, at)
	if !slices.ContainsFunc(b, func(c byte) bool { return c != 0 }) {
		return false
	}
	f.durable.write(at, make([]byte, len(b)))
	f.reset()
	d.tracef("lost the write of %s (file %d) at %d", name, f.id, at)
	return true
}

// BadSector makes one sector of a file's synced bytes fail every read
// until a write covers it, like a latent sector error. It reports whether
// the file has such a sector.
func (d *Disk) BadSector(rng *rand.Rand, name string) bool {
	f, ok := d.durable[name]
	if !ok || f.durable.size == 0 {
		return false
	}
	if f.bad == nil {
		f.bad = map[int]bool{}
	}
	s := rng.IntN((f.durable.size + SectorBytes - 1) / SectorBytes)
	f.bad[s] = true
	d.tracef("sector %d of %s (file %d) is bad", s, name, f.id)
	return true
}

// Misread makes the next read of a file that returns synced bytes return
// them wrong, as a drive or a bus that hands back wrong data does. It
// reports whether the file has synced bytes.
func (d *Disk) Misread(rng *rand.Rand, name string) bool {
	f, ok := d.names[name]
	if !ok || f.durable.size == 0 {
		return false
	}
	f.misread = rng
	d.tracef("the next read of %s (file %d) is wrong", name, f.id)
	return true
}

// ErrNoSpace is the error of a write that ran out of space.
var ErrNoSpace = errors.New("simfs: no space left on device")

// ErrIO is the error of a failed operation.
var ErrIO = errors.New("simfs: input/output error")

// ErrReadOnly is the error of an operation that would change a file
// system that went read-only.
var ErrReadOnly = errors.New("simfs: read-only file system")

// ErrBadSector is the error of a read of a sector that the medium cannot
// read. It is an ErrIO.
var ErrBadSector = fmt.Errorf("%w: unreadable sector", ErrIO)

// ClearFaults removes the fault hook, a pending short write and a pending
// failed sync.
func (d *Disk) ClearFaults() {
	d.Fault = nil
	d.short = nil
	d.failSync = nil
}

// ShortWrite makes the next write store only a prefix of its bytes and fail
// with ErrNoSpace, as a full disk does.
func (d *Disk) ShortWrite(rng *rand.Rand) {
	d.short = rng
}

// FailSync makes the next sync fail with ErrIO after a random part of the
// file's unsynced writes reached the medium, as a writeback that failed
// partway does.
func (d *Disk) FailSync(rng *rand.Rand) {
	d.failSync = rng
}

// Shutdown makes every operation fail with ErrIO, as a file system does
// that shut itself down after an error, until Remount.
func (d *Disk) Shutdown() {
	d.down = ErrIO
	d.tracef("the file system shut down")
}

// ReadOnly makes every operation that would change the disk fail with
// ErrReadOnly, as ext4 does when an error remounts it read-only, until
// Remount.
func (d *Disk) ReadOnly() {
	d.down = ErrReadOnly
	d.tracef("the file system went read-only")
}

// Down reports whether the file system shut down or went read-only.
func (d *Disk) Down() bool { return d.down != nil }

// Remount ends a shutdown or a read-only state. A remount follows an
// unmount, which lost what the page cache could not write back: crash
// the disk first.
func (d *Disk) Remount() {
	if d.down != nil {
		d.down = nil
		d.tracef("the file system is mounted again")
	}
}

// Erase removes everything, as if the disk was replaced.
func (d *Disk) Erase() {
	d.names = map[string]*file{}
	d.durable = map[string]*file{}
	d.dirOps = nil
}

// Bytes returns the total size of the files.
func (d *Disk) Bytes() int {
	n := 0
	for _, f := range d.names {
		n += f.size
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
	var err error
	switch {
	case errors.Is(d.down, ErrIO) || d.down != nil && op.changes():
		err = d.down
	case d.Fault != nil:
		err = d.Fault(op, name)
	}
	if err != nil {
		d.tracef("%s %s fails: %v", op, name, err)
	}
	return err
}

// changes reports whether op changes the disk.
func (op Op) changes() bool { return op != OpOpen && op != OpRead }

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
	f := &file{id: d.files, durable: pages{st: d.st}}
	d.names[name] = f
	d.dirOps = append(d.dirOps, dirOp{op: OpCreate, name: name, f: f})
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
	d.dirOps = append(d.dirOps, dirOp{op: OpRename, name: oldName, to: newName, f: f})
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
	d.dirOps = append(d.dirOps, dirOp{op: OpRemove, name: name, f: f})
	return nil
}

// SyncDir makes the current set of names durable.
func (d *Disk) SyncDir() error {
	if err := d.fault(OpSyncDir, ""); err != nil {
		return err
	}
	d.durable = maps.Clone(d.names)
	d.dirOps = nil
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
	if err := h.readable(0, h.f.size); err != nil {
		return nil, err
	}
	b := h.f.content()
	h.misread(b, 0)
	return b, nil
}

// ReadAt reads len(p) bytes at offset off.
func (h *Handle) ReadAt(p []byte, off int64) (int, error) {
	if err := h.check(OpRead); err != nil {
		return 0, err
	}
	if off < 0 || off > int64(h.f.size) {
		return 0, io.EOF
	}
	if err := h.readable(int(off), int(off)+len(p)); err != nil {
		return 0, err
	}
	n := min(len(p), h.f.size-int(off))
	h.f.read(p[:n], int(off))
	h.misread(p[:n], int(off))
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// readable fails a read of bytes from to end that covers a bad sector.
func (h *Handle) readable(from, end int) error {
	end = min(end, h.f.size)
	for s := range h.f.bad {
		if s*SectorBytes < end && (s+1)*SectorBytes > from {
			h.d.tracef("read %s (file %d) at %d: sector %d is bad", h.name, h.f.id, from, s)
			h.d.Counts.BadReads++
			return ErrBadSector
		}
	}
	return nil
}

// misread makes b, read from offset off, wrong if a misread is due and b
// holds synced bytes: one bit flips, or, half the time, a sector of them
// holds what another sector of the file holds, as when the drive read the
// wrong place.
func (h *Handle) misread(b []byte, off int) {
	f := h.f
	rng := f.misread
	end := min(off+len(b), f.durable.size)
	if rng == nil || off >= end {
		return
	}
	f.misread = nil
	h.d.Counts.Misreads++
	if s := (off + rng.IntN(end-off)) / SectorBytes; rng.IntN(2) == 0 && f.durable.size >= 2*SectorBytes {
		from := rng.IntN(f.durable.size/SectorBytes - 1)
		if from >= s {
			from++
		}
		src := make([]byte, min(SectorBytes, f.durable.size-from*SectorBytes))
		f.durable.read(src, from*SectorBytes)
		at := s * SectorBytes
		lo, hi := max(at, off), min(at+len(src), end)
		if lo < hi && !bytes.Equal(b[lo-off:hi-off], src[lo-at:hi-at]) {
			copy(b[lo-off:hi-off], src[lo-at:hi-at])
			h.d.tracef("misread %s (file %d) at %d: sector %d returns sector %d", h.name, f.id, off, s, from)
			return
		}
	}
	i := off + rng.IntN(end-off)
	b[i-off] ^= 1 << rng.IntN(8)
	h.d.tracef("misread %s (file %d) at %d: byte %d flips a bit", h.name, f.id, off, i)
}

// Write appends a copy of p.
func (h *Handle) Write(p []byte) (int, error) {
	return h.WriteAt(p, int64(h.f.size))
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
	if c := h.d.Capacity; c > 0 {
		// What fits: the file may grow by the free bytes.
		if limit := h.f.size + c - h.d.Bytes(); int(off)+n > limit {
			n, err = max(0, min(n, limit-int(off))), ErrNoSpace
		}
	}
	if gap := int(off) - h.f.size; gap > 0 && (n > 0 || err == nil) {
		h.write(h.f.size, make([]byte, gap))
	}
	if n > 0 {
		h.write(int(off), p[:n])
	}
	if h.d.Trace != nil {
		h.d.tracef("write %s (file %d) at %d: %d of %d bytes, crc %08x, %v", h.name, h.f.id, off, n, len(p), crc32.ChecksumIEEE(p[:n]), err)
	}
	return n, err
}

func (h *Handle) write(off int, p []byte) {
	p = slices.Clone(p)
	f := h.f
	for k := 0; k < len(p); {
		i, at := (off+k)/PageBytes, (off+k)%PageBytes
		m := min(len(p)-k, PageBytes-at)
		copy(f.dirtyPage(i)[at:at+m], p[k:k+m])
		k += m
	}
	f.size = max(f.size, off+len(p))
	f.pending = append(f.pending, write{off, p})
	// The drive writes a bad sector again in a spare place.
	for s := range h.f.bad {
		if s*SectorBytes < off+len(p) && (s+1)*SectorBytes > off {
			delete(h.f.bad, s)
		}
	}
}

// Truncate cuts the file.
func (h *Handle) Truncate(size int64) error {
	if err := h.check(OpTruncate); err != nil {
		return err
	}
	f := h.f
	if n := int(size); n < f.size {
		for i, d := range f.dirty {
			switch lo := i * PageBytes; {
			case lo >= n:
				delete(f.dirty, i)
			case n < lo+PageBytes:
				clear(d[n-lo:])
			}
		}
		f.size = n
	}
	for s := range h.f.bad {
		if int64(s*SectorBytes) >= size {
			delete(h.f.bad, s)
		}
	}
	if n := int(size); n < f.durable.size {
		if !f.cut || n < f.cutAt {
			f.cutAt = n
		}
		f.cut = true
	}
	h.d.tracef("truncate %s (file %d) to %d, cut %v", h.name, h.f.id, size, h.f.cut)
	// A later sync keeps only what is left of the writes before it.
	h.f.pending = slices.DeleteFunc(h.f.pending, func(w write) bool { return int64(w.off) >= size })
	for i, w := range h.f.pending {
		if end := int64(w.off + len(w.p)); end > size {
			h.f.pending[i].p = w.p[:size-int64(w.off)]
		}
	}
	return nil
}

// Sync makes the content durable.
func (h *Handle) Sync() error {
	err := h.check(OpSync)
	if rng := h.d.failSync; err == nil && rng != nil {
		h.d.failSync = nil
		err = ErrIO
		// Some of the writes reached the medium whole before the
		// writeback failed. A torn image comes only from a crash, which
		// the simulator draws only while a quorum would stay.
		if !h.f.cut {
			h.f.durable = h.f.survive(rng, Writes, &h.d.Counts)
		}
		h.d.Counts.FailedSyncs++
		h.d.tracef("sync %s (file %d) fails after %d bytes reached the disk", h.name, h.f.id, h.f.durable.size)
	}
	f := h.f
	if err != nil {
		if errors.Is(err, ErrIO) && !f.cut {
			// As on Linux, a failed writeback marks the pages clean: the
			// unsynced bytes stay readable but never reach the disk, which
			// keeps what it had there, zeros past its end, and a later
			// sync does not retry them.
			f.durable.grow(f.size)
			f.diverged = f.diverged || len(f.pending) > 0
			f.pending = nil
		}
		return err
	}
	switch {
	case f.cut || !f.diverged:
		// The medium gets what the page cache holds: its pages become
		// the medium's.
		end := f.size
		if !f.cut {
			// Only the bytes written reach the medium: a truncate that
			// cut no synced bytes may have left zeros past them.
			end = f.durable.size
			for _, w := range f.pending {
				end = max(end, w.off+len(w.p))
			}
		}
		f.durable.cut(f.limit())
		for i, d := range f.dirty {
			f.durable.set(i, d)
		}
		f.durable.size = f.size
		f.durable.cut(end)
		f.dirty, f.diverged = nil, false
	default:
		for _, w := range f.pending {
			f.durable.write(w.off, w.p)
		}
		// The page cache keeps the pages that hold what the medium does
		// not.
		for i, d := range f.dirty {
			if *d == *f.clean(i) {
				delete(f.dirty, i)
			}
		}
		f.diverged = len(f.dirty) > 0
	}
	f.pending, f.cut, f.cutAt = nil, false, 0
	f.durable.spill()
	h.d.tracef("sync %s (file %d): %d bytes durable", h.name, f.id, f.durable.size)
	return nil
}

// DropCache drops the file's clean pages, as fadvise(POSIX_FADV_DONTNEED)
// does: a page that no write since the last sync touched reads what the
// disk holds, zeros past its end, so bytes that a failed sync left there
// read as the disk keeps them (#372). A dirty page stays whole, as on
// Linux. Linux also keeps a page that something holds, which osfs reports
// (osfs.ErrCached); the model holds none.
func (h *Handle) DropCache() error {
	if h.closed {
		return errClosed
	}
	f := h.f
	// Only a dirty page, or one with synced bytes that a cut hides, reads
	// otherwise than the disk does.
	var touched []int
	for i := range f.dirty {
		touched = append(touched, i)
	}
	if f.cut {
		for i := f.cutAt / PageBytes; i < pageCount(min(f.size, f.durable.size)); i++ {
			if f.dirty[i] == nil {
				touched = append(touched, i)
			}
		}
	}
	for _, i := range touched {
		off := i * PageBytes
		end := min(off+PageBytes, f.size)
		if slices.ContainsFunc(f.pending, func(w write) bool { return w.off < end && w.off+len(w.p) > off }) {
			continue
		}
		// The page reads what the disk holds, even a part a truncate
		// cut that no sync made final.
		b := new(block)
		f.durable.read(b[:end-off], off)
		if *b == *f.clean(i) {
			delete(f.dirty, i)
		} else {
			if f.dirty == nil {
				f.dirty = map[int]*block{}
			}
			f.dirty[i] = b
		}
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

// Unsynced reports whether a sync of file name would write anything: a
// write or a truncate since its last sync.
func (d *Disk) Unsynced(name string) bool {
	f, ok := d.names[name]
	return ok && (len(f.pending) > 0 || f.cut)
}

// DurableNames returns the names of the files on the durable directory,
// sorted.
func (d *Disk) DurableNames() []string { return sortedNames(d.durable) }

// DurableBytes returns a copy of a file's synced bytes.
func (d *Disk) DurableBytes(name string) []byte {
	if f, ok := d.durable[name]; ok {
		return f.durable.bytes()
	}
	return nil
}

// Rot flips the lowest bit of one byte of a file, synced or not, while
// the file is in use, like a bit that rots under a running node.
func (d *Disk) Rot(name string, off int) {
	f := d.names[name]
	i, at := off/PageBytes, off%PageBytes
	// The page cache's byte first, from what it reads now.
	if off < f.size {
		f.dirtyPage(i)[at] ^= 1
	}
	if off < f.durable.size {
		f.durable.own(i)[at] ^= 1
	}
	f.diverged = true
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
	f.durable.own(off / PageBytes)[off%PageBytes] ^= 1
	f.reset()
}
