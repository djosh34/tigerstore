// Package wal stores what one node keeps on disk: the Raft log with its
// hard state, the roots that name checkpoints, and the checkpoint files.
// The log is the value store: every write's key and value live in the
// entry that carried it, and the index names a value by its place there.
//
// The log is a sequence of segment files. Each segment starts with a header
// and holds records. A record is
//
//	u32 payload length | u32 crc | u8 type | payload
//
// The crc covers the segment's sequence number, type and payload and
// starts from the crc of the record before it, so a stale or misplaced
// record does not verify, nor one left from an earlier segment in a reused
// file. The header carries the format version and the crc that the
// segment's first record starts from.
//
// A new segment reuses the file of a removed one when there is a spare:
// removal renames up to MaxSpares segments to spare names instead of
// deleting them, and the log writes at its own offset, so a sync into a
// reused file changes no block allocation and, on ext4, commits no
// journal.
//
// An entry record replaces every entry at or after its index, so a
// truncated Raft suffix and its replacement are synced together. A root
// record names the checkpoint that recovery starts from and the packs, the
// files that cleaning made. A value record holds a value of an entry this
// node does not have, which a snapshot brought. Every segment starts with
// a copy of the newest hard state and root, so removing older segments
// never loses them.
//
// The package does no I/O of its own. It reads and writes through FS, so
// the simulator runs it on a simulated disk. It takes no locks: a driver
// that reads from other goroutines than the one that appends must hold
// one lock around Append, AppendRoot, AppendValue, InstallPacks,
// RemoveBelow, RemoveCleaned and every read, but need not hold it around
// Flush and Sync.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/planted"
)

// FS is the directory that holds the log.
type FS interface {
	// List returns the names of the files in the directory.
	List() ([]string, error)
	// Create makes a new empty file. The name only survives a crash after
	// SyncDir.
	Create(name string) (File, error)
	// Open opens an existing file for reading and appending.
	Open(name string) (File, error)
	// Remove deletes a file. The removal only survives a crash after SyncDir.
	Remove(name string) error
	// Rename gives file oldName the name newName, replacing any file of
	// that name. The rename only survives a crash after SyncDir.
	Rename(oldName, newName string) error
	// SyncDir makes creations and removals durable.
	SyncDir() error
}

// File is one segment or checkpoint file.
type File interface {
	// ReadAll returns the whole content.
	ReadAll() ([]byte, error)
	// ReadAt reads len(p) bytes at offset off, as io.ReaderAt does.
	ReadAt(p []byte, off int64) (int, error)
	// Write writes p after what Write wrote before; on a new file that is
	// an append.
	Write(p []byte) (int, error)
	// WriteAt writes p at offset off, as io.WriterAt does.
	WriteAt(p []byte, off int64) (int, error)
	// Truncate cuts the file to size bytes.
	Truncate(size int64) error
	// Sync makes the content durable.
	Sync() error
	Close() error
}

// ErrCorrupt means the log on disk is damaged in a way that a crash cannot
// cause. The node must not take part in the cluster with it.
var ErrCorrupt = errors.New("wal: corrupt log")

// ErrNotOnDisk means that an entry or value is not in the log any more, or
// never was.
var ErrNotOnDisk = errors.New("wal: not on disk")

// Version is the format version of new segments. Version 2 added the
// segment's sequence number to every record's crc; Open still reads
// segments of version 1, and appends to a newest one in its format.
const Version = 2

const (
	magic      = "tigerwal"
	headerSize = len(magic) + 4 + 8 + 4
	recHdrSize = 9
	// entryHdrSize is an entry record's payload before its data: term,
	// index and entry type.
	entryHdrSize = 17
	// valueHdrSize is a value record's payload before the value: entry
	// index and offset.
	valueHdrSize = 12

	// rootSize is a root record's payload before the numbers of its packs.
	rootSize = 29

	recEntry     = 1
	recHardState = 2
	recRoot      = 3
	recValue     = 4
	recEnd       = 5

	// SegmentBytes is the size after which the log starts a new segment.
	SegmentBytes = 64 << 20

	// MaxSpares is how many removed segments the log keeps by default as
	// spare files for new segments.
	MaxSpares   = 4
	sparePrefix = "spare-"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func segmentName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

func spareName(seq uint64) string { return fmt.Sprintf("%s%016x", sparePrefix, seq) }

// CheckpointName is the file name of the checkpoint at a Raft index.
func CheckpointName(index uint64) string { return fmt.Sprintf("ckpt-%016x", index) }

// HardState is the Raft hard state.
type HardState struct {
	Term, Vote, Commit uint64
}

// Root names the checkpoint that recovery starts from: the Raft index it
// was cut at and that entry's term. A checkpoint counts only once a synced
// root names it.
//
// A root with Reset set came with a snapshot: the log continues after
// Index, or after Keep if that is higher, and of the entries this node
// held before, only those up to Keep are still its log: its applied index
// then, or the last entry it held that the snapshot's source has too.
type Root struct {
	Index, Term uint64
	Reset       bool
	Keep        uint64
}

// Recovered is what Open read from disk.
type Recovered struct {
	HardState HardState
	// Root is the newest root, zero if there is none.
	Root Root
	// Last is the index of the last entry, or the root's index if no entry
	// follows it. Every entry after the root's index up to Last is on
	// disk; WAL.Entry reads them.
	Last uint64
	// Bytes is the size of the segments.
	Bytes int64
}

// run is where a run of consecutive entries is in segment seq. offs[i] is
// the file offset of the record of entry first+i, and seeds[i] the crc
// that record chains from, so that a read can check it. Every segment has
// a run, maybe empty, and several when a snapshot moved the log on.
type run struct {
	seq   uint64
	first uint64
	offs  []uint32
	seeds []uint32
	// top is the highest index of an entry or value the run ever held.
	top uint64
}

func (s *run) end() uint64 { return s.first + uint64(len(s.offs)) }

// cut drops the entries at or after index.
func (s *run) cut(index uint64) {
	if s.end() <= index {
		return
	}
	k := uint64(0)
	if index > s.first {
		k = index - s.first
	}
	s.offs, s.seeds = s.offs[:k], s.seeds[:k]
}

// valueRec is a value record: the file offset of its value and the place
// of the value in its entry.
type valueRec struct {
	seq      uint64
	at       uint32
	off, len uint32
}

// WAL appends to the newest segment and reads from all of them.
type WAL struct {
	fs  FS
	seg File
	seq uint64
	// version is the format of the newest segment, size where its records
	// end and fileSize the size of its file, which a reused one may make
	// larger than size.
	version        uint32
	size, fileSize int64
	crc            uint32
	last           uint64
	buf            []byte
	dirty          bool
	maxSize        int64
	// spares is how many removed segments to keep as spare files.
	spares int
	// hs and root are the newest hard state and root, copied into every
	// new segment; copied marks that the current segment has them.
	hs     HardState
	root   Root
	copied bool

	runs    []*run
	vals    map[uint64][]valueRec
	readers map[uint64]File
	// sizes holds the size of each segment that no longer changes, and
	// sealed the size of the last one to fill until the next append notes
	// it there: Sync starts new segments without the lock.
	sizes  map[uint64]int64
	sealed int64

	// packs are the packs the newest root names, by number, and nextPack
	// the number of the next one. named are the numbers of the newest
	// root that Open read.
	packs    []*Pack
	nextPack uint64
	named    []uint64
}

// Open reads the log in fs, cuts a torn tail off the newest segment, and
// returns the log ready for appends. It holds one segment in memory at a
// time and keeps only where each entry is. A damaged record anywhere else,
// a gap in the entries, or a commit index past the last entry is
// ErrCorrupt. It removes the checkpoint files that the newest root does
// not name.
func Open(fs FS) (*WAL, *Recovered, error) {
	names, err := fs.List()
	if err != nil {
		return nil, nil, err
	}
	var segs, ckpts, packs []string
	for _, n := range names {
		switch {
		case strings.HasSuffix(n, ".wal"):
			segs = append(segs, n)
		case strings.HasPrefix(n, "ckpt-"):
			ckpts = append(ckpts, n)
		case strings.HasPrefix(n, packPrefix):
			packs = append(packs, n)
		}
	}
	slices.Sort(segs)
	// Segment names must be consecutive: a gap is a lost segment, even
	// when the newest segment is empty. Old segments go only from the
	// front.
	for i := 1; i < len(segs); i++ {
		var prev, cur uint64
		if _, err := fmt.Sscanf(segs[i-1], "%016x.wal", &prev); err != nil {
			return nil, nil, fmt.Errorf("%w: segment name %s: %w", ErrCorrupt, segs[i-1], err)
		}
		if _, err := fmt.Sscanf(segs[i], "%016x.wal", &cur); err != nil {
			return nil, nil, fmt.Errorf("%w: segment name %s: %w", ErrCorrupt, segs[i], err)
		}
		if cur != prev+1 {
			return nil, nil, fmt.Errorf("%w: segment %s follows %s", ErrCorrupt, segs[i], segs[i-1])
		}
	}

	w := &WAL{fs: fs, maxSize: SegmentBytes, spares: MaxSpares, vals: map[uint64][]valueRec{}, readers: map[uint64]File{}, sizes: map[uint64]int64{}}
	rec := &Recovered{}
	// first is the index of the first entry the scan found, 0 for none.
	var first uint64
	for i, name := range segs {
		last := i == len(segs)-1
		f, err := fs.Open(name)
		if err != nil {
			return nil, nil, err
		}
		data, err := f.ReadAll()
		if err != nil {
			return nil, nil, errors.Join(err, f.Close())
		}
		version, seq, crc, ok := parseHeader(data)
		ok = ok && name == segmentName(seq)
		// The first segment may start anywhere once old segments are
		// removed; every later one continues the sequence and the crc
		// chain.
		if ok && i > 0 {
			ok = seq == w.seq+1 && crc == w.crc
		}
		if !ok {
			// newSegment syncs the header before the directory entry and
			// before any record, so only a newest segment with nothing
			// after its header can have a header that never reached the
			// disk. Anything else is damage or a lost segment.
			if !last || len(data) > headerSize {
				return nil, nil, errors.Join(fmt.Errorf("%w: bad header or missing segment before %s", ErrCorrupt, name), f.Close())
			}
			if err := errors.Join(f.Close(), fs.Remove(name), fs.SyncDir()); err != nil {
				return nil, nil, err
			}
			break
		}
		if version < 1 || version > Version {
			return nil, nil, errors.Join(fmt.Errorf("wal: segment %s has format version %d; this build reads versions 1 to %d", name, version, Version), f.Close())
		}
		w.seq, w.crc, w.version = seq, crc, version
		w.current()
		good, lastStart, err := w.replay(data, rec, &first)
		if err != nil {
			return nil, nil, errors.Join(fmt.Errorf("%s at byte %d: %w", name, good, err), f.Close())
		}
		if good < int64(len(data)) {
			if !last {
				return nil, nil, errors.Join(fmt.Errorf("%w: damaged record in %s at byte %d", ErrCorrupt, name, good), f.Close())
			}
			// A torn tail: the write was never synced, so nothing in it was
			// acknowledged.
			if planted.On("B04b") && lastStart > 0 {
				// Planted: the repair also cuts the last synced record, and
				// recovery starts over on what is left.
				if err := errors.Join(f.Truncate(lastStart), f.Sync(), f.Close()); err != nil {
					return nil, nil, err
				}
				return Open(fs)
			}
			if err := errors.Join(f.Truncate(good), f.Sync()); err != nil {
				return nil, nil, errors.Join(err, f.Close())
			}
		}
		rec.Bytes += good
		if !last {
			if err := f.Close(); err != nil {
				return nil, nil, err
			}
			w.sizes[seq] = good
			continue
		}
		w.seg, w.size, w.fileSize, w.copied = f, good, good, true
	}
	if err := w.check(first); err != nil {
		return nil, nil, err
	}
	if err := w.openPacks(packs, rec); err != nil {
		return nil, nil, errors.Join(err, w.Close())
	}
	w.last = max(w.last, w.root.Index)
	rec.Root, rec.Last = w.root, w.last
	if rec.HardState.Commit > w.last {
		return nil, nil, fmt.Errorf("%w: commit index %d is past the last entry %d", ErrCorrupt, rec.HardState.Commit, w.last)
	}
	if err := w.removeCheckpoints(ckpts, CheckpointName(w.root.Index)); err != nil {
		return nil, nil, err
	}
	if w.seg == nil {
		if err := w.newSegment(); err != nil {
			return nil, nil, errors.Join(err, w.closePacks())
		}
	}
	return w, rec, nil
}

// check requires the entries after the root to be on disk without a gap,
// and a log without a root to start at index 1.
func (w *WAL) check(first uint64) error {
	if first == 0 {
		return nil
	}
	if w.root.Index == 0 && first != 1 {
		return fmt.Errorf("%w: the log starts at entry %d and no root names a checkpoint", ErrCorrupt, first)
	}
	next := w.root.Index + 1
	for _, s := range w.runs {
		if len(s.offs) == 0 || s.end() <= next {
			continue
		}
		if s.first > next {
			return fmt.Errorf("%w: entries %d to %d are missing", ErrCorrupt, next, s.first-1)
		}
		next = s.end()
	}
	if next <= w.last {
		return fmt.Errorf("%w: entries %d to %d are missing", ErrCorrupt, next, w.last)
	}
	return nil
}

// removeCheckpoints removes every checkpoint file in names but keep.
func (w *WAL) removeCheckpoints(names []string, keep string) error {
	removed := false
	for _, n := range names {
		if n == keep {
			continue
		}
		if err := w.fs.Remove(n); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return w.fs.SyncDir()
	}
	return nil
}

func parseHeader(data []byte) (version uint32, seq uint64, crc uint32, ok bool) {
	if len(data) < headerSize || string(data[:len(magic)]) != magic {
		return 0, 0, 0, false
	}
	version = binary.LittleEndian.Uint32(data[len(magic):])
	seq = binary.LittleEndian.Uint64(data[len(magic)+4:])
	crc = binary.LittleEndian.Uint32(data[len(magic)+12:])
	return version, seq, crc, true
}

// replay applies the records of one segment and returns where its valid
// records end and where the last of them starts. A record that does not
// verify ends the prefix. A record that verifies but does not fit the log
// is an error.
func (w *WAL) replay(data []byte, rec *Recovered, first *uint64) (good, lastStart int64, err error) {
	off := headerSize
	for off < len(data) {
		if len(data)-off < recHdrSize {
			return int64(off), lastStart, nil
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		sum := binary.LittleEndian.Uint32(data[off+4:])
		end := off + recHdrSize + n
		if end > len(data) {
			return int64(off), lastStart, nil
		}
		body := data[off+8 : end]
		// Planted: B04a skips the check; B04c skips it for a record that
		// still decodes.
		skip := planted.On("B04a") || (planted.On("B04c") && w.decodes(body))
		if got := crc32.Update(w.seed(w.crc), castagnoli, body); got != sum && !skip {
			// The damage may be in the body or in the stored crc, so try
			// the next record chained from either.
			if verifiesAfter(data, end, w.seed(sum)) || verifiesAfter(data, end, w.seed(got)) {
				return int64(off), lastStart, fmt.Errorf("%w: damaged record followed by a valid one", ErrCorrupt)
			}
			return int64(off), lastStart, nil
		}
		if err := w.apply(body[0], body[1:], uint32(off), rec, first); err != nil {
			return int64(off), lastStart, err
		}
		lastStart = int64(off)
		w.crc = sum
		off = end
	}
	return int64(off), lastStart, nil
}

// verifiesAfter reports whether a whole record starts at off and verifies
// when chained from seed, made from the stored crc of the record before it. A torn
// write leaves garbage up to the end of the file, so a valid record after a
// bad one means the bad one was damaged after it was written. This can also
// fence a node after a crash whose disk persisted a later sector of the
// last write but not an earlier one; refusing to start is the safe answer.
func verifiesAfter(data []byte, off int, seed uint32) bool {
	if len(data)-off < recHdrSize {
		return false
	}
	n := int(binary.LittleEndian.Uint32(data[off:]))
	end := off + recHdrSize + n
	if end > len(data) {
		return false
	}
	return crc32.Update(seed, castagnoli, data[off+8:end]) == binary.LittleEndian.Uint32(data[off+4:])
}

// seed is what the crc of the record after one with crc crc starts from
// in the current segment: from version 2 on, crc with the segment's
// sequence number added.
func (w *WAL) seed(crc uint32) uint32 {
	if w.version < 2 {
		return crc
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], w.seq)
	return crc32.Update(crc, castagnoli, b[:])
}

// apply takes one verified record of the current segment, which starts at
// file offset at.
func (w *WAL) apply(typ byte, p []byte, at uint32, rec *Recovered, first *uint64) error {
	switch typ {
	case recEntry:
		if len(p) < entryHdrSize {
			return fmt.Errorf("%w: short entry record", ErrCorrupt)
		}
		index := binary.LittleEndian.Uint64(p[8:])
		if *first == 0 && index > 0 {
			// The first entry found; older ones may have been removed,
			// which check allows only below the root.
			*first, w.last = index, max(w.last, index-1)
		}
		if index == 0 || index > w.last+1 {
			return fmt.Errorf("%w: entry %d follows entry %d", ErrCorrupt, index, w.last)
		}
		w.placeEntry(index, at, w.seed(w.crc))
	case recHardState:
		if len(p) != 24 {
			return fmt.Errorf("%w: hard state record of %d bytes", ErrCorrupt, len(p))
		}
		rec.HardState = HardState{
			Term:   binary.LittleEndian.Uint64(p),
			Vote:   binary.LittleEndian.Uint64(p[8:]),
			Commit: binary.LittleEndian.Uint64(p[16:]),
		}
		w.hs = rec.HardState
	case recRoot:
		if len(p) < rootSize || len(p) != rootSize+8*int(binary.LittleEndian.Uint32(p[25:])) {
			return fmt.Errorf("%w: root record of %d bytes", ErrCorrupt, len(p))
		}
		w.named = w.named[:0]
		for b := p[rootSize:]; len(b) > 0; b = b[8:] {
			w.named = append(w.named, binary.LittleEndian.Uint64(b))
		}
		r := Root{
			Index: binary.LittleEndian.Uint64(p),
			Term:  binary.LittleEndian.Uint64(p[8:]),
			Reset: p[16] != 0,
			Keep:  binary.LittleEndian.Uint64(p[17:]),
		}
		if r.Index < w.root.Index {
			return fmt.Errorf("%w: root at %d after root at %d", ErrCorrupt, r.Index, w.root.Index)
		}
		w.setRoot(r)
		if r.Reset && rec.HardState.Commit > r.Index {
			// The commit index was the replaced log's; the leader tells
			// the node its commit again.
			rec.HardState.Commit, w.hs.Commit = r.Index, r.Index
		}
	case recValue:
		if len(p) < valueHdrSize {
			return fmt.Errorf("%w: short value record", ErrCorrupt)
		}
		w.placeValue(binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint32(p[8:]), uint32(len(p)-valueHdrSize), at+recHdrSize+valueHdrSize)
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return nil
}

// current is the run of the current segment that appends go to.
func (w *WAL) current() *run {
	if len(w.runs) == 0 || w.runs[len(w.runs)-1].seq != w.seq {
		w.runs = append(w.runs, &run{seq: w.seq})
	}
	return w.runs[len(w.runs)-1]
}

// placeEntry notes that entry index has its record at file offset at of
// the current segment, whose crc starts from seed. It replaces every entry at
// or after index. Non-empty runs are in index order, so the walk back
// stops at the first that ends at or below index.
func (w *WAL) placeEntry(index uint64, at, seed uint32) {
	for _, s := range slices.Backward(w.runs) {
		if len(s.offs) == 0 {
			continue
		}
		if s.end() <= index {
			break
		}
		s.cut(index)
	}
	cur := w.current()
	if len(cur.offs) > 0 && cur.end() != index {
		// The log moved on past a snapshot.
		cur = &run{seq: w.seq}
		w.runs = append(w.runs, cur)
	}
	if len(cur.offs) == 0 {
		cur.first = index
	}
	cur.offs = append(cur.offs, at)
	cur.seeds = append(cur.seeds, seed)
	cur.top = max(cur.top, index)
	w.last = index
}

// placeValue notes a value record of the current segment whose value is
// at file offset at.
func (w *WAL) placeValue(index uint64, off, n, at uint32) {
	cur := w.current()
	w.vals[index] = append(w.vals[index], valueRec{seq: cur.seq, at: at, off: off, len: n})
	cur.top = max(cur.top, index)
}

// setRoot makes r the newest root. A reset drops the entries after Keep;
// the root kept, and copied into new segments, is then a plain one.
func (w *WAL) setRoot(r Root) {
	if r.Reset {
		for _, s := range w.runs {
			s.cut(r.Keep + 1)
		}
		w.last = max(r.Index, r.Keep)
	}
	w.root = Root{Index: r.Index, Term: r.Term}
}

// newSegment starts the next segment, in a spare file if there is one. It
// syncs the header before the directory entry, so a segment that survives
// a crash always has its header; a spare gets its new header before it is
// renamed. The copies of the hard state and the root go in with the next
// write.
func (w *WAL) newSegment() error {
	seq := w.seq + 1
	spare, err := w.spare()
	if err != nil {
		return err
	}
	var f File
	var size int64
	if spare != "" {
		if f, err = w.fs.Open(spare); err == nil {
			size, err = fileSize(f)
		}
	} else {
		f, err = w.fs.Create(segmentName(seq))
	}
	if err != nil {
		return err
	}
	hdr := make([]byte, 0, headerSize)
	hdr = append(hdr, magic...)
	hdr = binary.LittleEndian.AppendUint32(hdr, Version)
	hdr = binary.LittleEndian.AppendUint64(hdr, seq)
	hdr = binary.LittleEndian.AppendUint32(hdr, w.crc)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if spare != "" {
		if err := w.fs.Rename(spare, segmentName(seq)); err != nil {
			return errors.Join(err, f.Close())
		}
	}
	// Planted: B05 takes records into the new segment before its directory
	// entry is durable.
	if !planted.On("B05") {
		if err := w.fs.SyncDir(); err != nil {
			return errors.Join(err, f.Close())
		}
	}
	w.sealed = w.size
	w.seq, w.seg, w.size, w.copied = seq, f, int64(headerSize), false
	w.version, w.fileSize = Version, max(size, int64(headerSize))
	return nil
}

// fileSize returns the size of f.
func fileSize(f File) (int64, error) {
	b, err := f.ReadAll()
	return int64(len(b)), err
}

// spare returns the name of the oldest spare file, or "" if there is none.
// It lists the directory, so that the goroutine that syncs shares nothing
// with the one that removes segments.
func (w *WAL) spare() (string, error) {
	spares, err := w.listSpares()
	if err != nil || len(spares) == 0 {
		return "", err
	}
	return spares[0], nil
}

// listSpares returns the names of the spare files in order.
func (w *WAL) listSpares() ([]string, error) {
	names, err := w.fs.List()
	if err != nil {
		return nil, err
	}
	var spares []string
	for _, n := range names {
		if strings.HasPrefix(n, sparePrefix) {
			spares = append(spares, n)
		}
	}
	slices.Sort(spares)
	return spares, nil
}

func (w *WAL) record(typ byte, payload ...[]byte) {
	w.buf, w.crc = appendRecord(w.buf, w.seed(w.crc), typ, payload...)
}

// appendRecord appends a record whose crc starts from seed to buf and
// returns the record's crc.
func appendRecord(buf []byte, seed uint32, typ byte, payload ...[]byte) ([]byte, uint32) {
	n := 1
	for _, p := range payload {
		n += len(p)
	}
	start := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(n-1))
	buf = append(buf, 0, 0, 0, 0, typ)
	for _, p := range payload {
		buf = append(buf, p...)
	}
	crc := crc32.Update(seed, castagnoli, buf[start+8:])
	binary.LittleEndian.PutUint32(buf[start+4:], crc)
	return buf, crc
}

// copyState starts a new segment with copies of the newest hard state and
// root, before its first record.
func (w *WAL) copyState() {
	// Every segment has a run, so that RemoveBelow sees it.
	w.current()
	if w.copied {
		return
	}
	w.copied = true
	if w.sealed > 0 {
		w.sizes[w.seq-1], w.sealed = w.sealed, 0
	}
	if w.hs != (HardState{}) {
		w.hardState(w.hs)
	}
	if w.root != (Root{}) || len(w.packs) > 0 {
		w.rootRecord(w.root)
	}
}

func (w *WAL) hardState(hs HardState) {
	var h [24]byte
	binary.LittleEndian.PutUint64(h[:], hs.Term)
	binary.LittleEndian.PutUint64(h[8:], hs.Vote)
	binary.LittleEndian.PutUint64(h[16:], hs.Commit)
	w.record(recHardState, h[:])
	w.hs = hs
}

// rootRecord buffers root r with the numbers of the packs.
func (w *WAL) rootRecord(r Root) {
	h := make([]byte, rootSize, rootSize+8*len(w.packs))
	binary.LittleEndian.PutUint64(h, r.Index)
	binary.LittleEndian.PutUint64(h[8:], r.Term)
	if r.Reset {
		h[16] = 1
	}
	binary.LittleEndian.PutUint64(h[17:], r.Keep)
	binary.LittleEndian.PutUint32(h[25:], uint32(len(w.packs)))
	for _, p := range w.packs {
		h = binary.LittleEndian.AppendUint64(h, p.id)
	}
	w.record(recRoot, h)
}

// EntryRecordBytes is the size of the record that stores e.
func EntryRecordBytes(e *raftpb.Entry) int64 {
	return recHdrSize + entryHdrSize + int64(len(e.GetData()))
}

// Append buffers entries and a hard state. Nothing is durable before Sync.
// A nil hard state leaves the stored one unchanged.
func (w *WAL) Append(hs *HardState, ents []*raftpb.Entry) {
	w.copyState()
	for _, e := range ents {
		var h [entryHdrSize]byte
		binary.LittleEndian.PutUint64(h[:], e.GetTerm())
		binary.LittleEndian.PutUint64(h[8:], e.GetIndex())
		h[16] = byte(e.GetType())
		at, seed := uint32(w.size)+uint32(len(w.buf)), w.seed(w.crc)
		w.record(recEntry, h[:], e.GetData())
		w.placeEntry(e.GetIndex(), at, seed)
	}
	if hs != nil {
		w.hardState(*hs)
	}
}

// AppendRoot buffers a root. The checkpoint it names counts once it is
// synced. A root with Reset moves the log to after the root's index.
func (w *WAL) AppendRoot(r Root) {
	w.copyState()
	w.rootRecord(r)
	w.setRoot(r)
}

// AppendValue buffers a value that starts at offset off of the data of
// entry index, an entry this node does not hold.
func (w *WAL) AppendValue(index uint64, off uint32, value []byte) {
	w.copyState()
	var h [valueHdrSize]byte
	binary.LittleEndian.PutUint64(h[:], index)
	binary.LittleEndian.PutUint32(h[8:], off)
	at := uint32(w.size) + uint32(len(w.buf)) + recHdrSize + valueHdrSize
	w.record(recValue, h[:], value)
	w.placeValue(index, off, uint32(len(value)), at)
}

// Flush writes what Append buffered without syncing it, at the end of
// the records. The simulator uses it to put a crash between the write and
// the fsync.
func (w *WAL) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.seg.WriteAt(w.buf, w.size)
	w.size += int64(n)
	w.fileSize = max(w.fileSize, w.size)
	w.dirty = true
	if err != nil {
		if planted.On("B12") {
			w.buf = w.buf[:0]
			return nil
		}
		return err
	}
	w.buf = w.buf[:0]
	return nil
}

// Sync writes what Append buffered and makes everything written durable.
// After an error the log is in an unknown state and the node must stop: a
// failed fsync is never retried.
func (w *WAL) Sync() error {
	if err := w.Flush(); err != nil {
		return err
	}
	if !w.dirty {
		return nil
	}
	if planted.On("B16") && w.size >= w.maxSize {
		// Planted: move on to a new segment without the old one's last
		// sync.
		w.dirty = false
		return errors.Join(w.seg.Close(), w.newSegment())
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	w.dirty = false
	if w.size < w.maxSize {
		return nil
	}
	// A reused file may hold more than the records; a segment that no
	// longer changes ends with its last record.
	if w.fileSize > w.size {
		if err := errors.Join(w.seg.Truncate(w.size), w.seg.Sync()); err != nil {
			return err
		}
	}
	if err := w.seg.Close(); err != nil {
		return err
	}
	return w.newSegment()
}

// Segment returns the file of the newest segment, which Flush writes to.
// A sync of it makes what Flush wrote durable, also while the next Flush
// runs; only Sync starts a new segment, after Full.
func (w *WAL) Segment() File { return w.seg }

// Full reports whether the newest segment is full: the next Sync starts a
// new one.
func (w *WAL) Full() bool { return w.size >= w.maxSize }

// Buffered is the size of what Append buffered for the next write. Only
// the goroutine that calls Sync may call it.
func (w *WAL) Buffered() int64 { return int64(len(w.buf)) }

// SetSegmentBytes changes the size after which a new segment starts. Tests
// and the simulator use small segments to exercise rotation.
func (w *WAL) SetSegmentBytes(n int64) { w.maxSize = n }

// SetSpares changes how many removed segments the log keeps as spare
// files. Call it before the log is shared.
func (w *WAL) SetSpares(n int) { w.spares = n }

// Close closes the newest segment without syncing, and every file opened
// for reading.
func (w *WAL) Close() error {
	var err error
	if w.seg != nil {
		err = w.seg.Close()
	}
	for seq, f := range w.readers {
		err = errors.Join(err, f.Close())
		delete(w.readers, seq)
	}
	return errors.Join(err, w.closePacks())
}

// locate returns the run that holds entry index and the entry's position
// in it, or nil.
func (w *WAL) locate(index uint64) (*run, int) {
	for _, s := range slices.Backward(w.runs) {
		if index >= s.first && index < s.end() {
			return s, int(index - s.first)
		}
	}
	return nil, 0
}

// readAt reads len(p) bytes at offset off of segment seq.
func (w *WAL) readAt(seq uint64, p []byte, off int64) error {
	f, ok := w.readers[seq]
	if !ok {
		var err error
		if f, err = w.fs.Open(segmentName(seq)); err != nil {
			return err
		}
		w.readers[seq] = f
	}
	n, err := f.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	return fmt.Errorf("wal: read %d of %d bytes at %d of segment %d: %w", n, len(p), off, seq, err)
}

// Has reports whether entry index is on disk.
func (w *WAL) Has(index uint64) bool {
	s, _ := w.locate(index)
	return s != nil
}

// Entry reads entry index from disk and checks its record.
func (w *WAL) Entry(index uint64) (*raftpb.Entry, error) {
	s, i := w.locate(index)
	if s == nil {
		return nil, fmt.Errorf("%w: entry %d", ErrNotOnDisk, index)
	}
	at := int64(s.offs[i])
	var hdr [recHdrSize]byte
	if err := w.readAt(s.seq, hdr[:], at); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n < entryHdrSize || n > MaxEntryBytes {
		return nil, fmt.Errorf("%w: the record of entry %d in segment %d has %d bytes", ErrCorrupt, index, s.seq, n)
	}
	body := make([]byte, 1+n)
	if err := w.readAt(s.seq, body, at+8); err != nil {
		return nil, err
	}
	if crc32.Update(s.seeds[i], castagnoli, body) != binary.LittleEndian.Uint32(hdr[4:]) || body[0] != recEntry ||
		binary.LittleEndian.Uint64(body[9:]) != index {
		return nil, fmt.Errorf("%w: the record of entry %d in segment %d does not verify", ErrCorrupt, index, s.seq)
	}
	p := body[1:]
	term := binary.LittleEndian.Uint64(p)
	etype := raftpb.EntryType(p[16])
	return &raftpb.Entry{Term: new(term), Index: new(index), Type: &etype, Data: p[entryHdrSize:]}, nil
}

// MaxEntryBytes bounds the record of one entry that Entry reads.
const MaxEntryBytes = 64 << 20

// Value reads n bytes at offset off of the data of entry index, from the
// entry's record, a value record or a pack. The caller checks them against
// the value's checksum.
func (w *WAL) Value(index uint64, off, n uint32) ([]byte, error) {
	b := make([]byte, n)
	if seq, at, ok := w.ValuePlace(index, off, n); ok {
		return b, w.readAt(seq, b, at)
	}
	if p, at, ok := w.packPlace(index, off, n); ok {
		return b, p.readAt(b, at)
	}
	return nil, fmt.Errorf("%w: value at %d+%d of entry %d", ErrNotOnDisk, off, n, index)
}

// ValuePlace returns the segment, and the offset in its file, of the n
// bytes at offset off of the data of entry index, from the entry's record
// or from a value record, if a segment holds them.
func (w *WAL) ValuePlace(index uint64, off, n uint32) (seq uint64, at int64, ok bool) {
	if s, i := w.locate(index); s != nil {
		return s.seq, int64(s.offs[i]) + recHdrSize + entryHdrSize + int64(off), true
	}
	for _, v := range w.vals[index] {
		if v.off <= off && uint64(off)+uint64(n) <= uint64(v.off)+uint64(v.len) {
			return v.seq, int64(v.at) + int64(off-v.off), true
		}
	}
	return 0, 0, false
}

// SegmentName is the file name of segment seq.
func SegmentName(seq uint64) string { return segmentName(seq) }

// Seq is the sequence number of the segment that appends go to; older
// segments no longer change.
func (w *WAL) Seq() uint64 { return w.seq }

// Oldest is the sequence number of the oldest segment on disk.
func (w *WAL) Oldest() uint64 {
	if len(w.runs) == 0 {
		return w.seq
	}
	return w.runs[0].seq
}

// RemoveBelow removes the oldest segments whose every entry and value is
// below index mark, then syncs the directory. It keeps the current
// segment and the one before it, which hold the newest hard state and
// root until the current one has them too.
func (w *WAL) RemoveBelow(mark uint64) error {
	// The runs of the segments to remove, which must all be below mark.
	n := 0
	for n < len(w.runs) && w.runs[n].seq+1 < w.seq {
		end := n
		for end < len(w.runs) && w.runs[end].seq == w.runs[n].seq {
			end++
		}
		below := true
		for _, s := range w.runs[n:end] {
			below = below && s.top < mark
		}
		if !below && !planted.On("B19") {
			break
		}
		n = end
		if !below {
			// Planted: this segment goes too, though it holds entries
			// that recovery from the checkpoint still needs.
			break
		}
	}
	if n == 0 {
		return nil
	}
	for i, s := range w.runs[:n] {
		if i > 0 && w.runs[i-1].seq == s.seq {
			continue
		}
		if err := w.removeSegment(s.seq); err != nil {
			return err
		}
	}
	w.runs = slices.Delete(w.runs, 0, n)
	return w.fs.SyncDir()
}

// removeSegment removes the file of segment seq, or keeps it as a spare,
// and forgets the value records in it. Every read of it must be over: a
// new segment may overwrite a spare as soon as the removal is done.
func (w *WAL) removeSegment(seq uint64) error {
	if f, ok := w.readers[seq]; ok {
		delete(w.readers, seq)
		if err := f.Close(); err != nil {
			return err
		}
	}
	spares, err := w.listSpares()
	if err != nil {
		return err
	}
	if len(spares) < w.spares {
		err = w.fs.Rename(segmentName(seq), spareName(seq))
	} else {
		err = w.fs.Remove(segmentName(seq))
	}
	if err != nil {
		return err
	}
	delete(w.sizes, seq)
	for index, vs := range w.vals {
		vs = slices.DeleteFunc(vs, func(v valueRec) bool { return v.seq == seq })
		if len(vs) == 0 {
			delete(w.vals, index)
		} else {
			w.vals[index] = vs
		}
	}
	return nil
}

// CreateCheckpoint creates the file of the checkpoint at index. It counts
// only once it is synced, the directory is synced, and a synced root
// names it.
func (w *WAL) CreateCheckpoint(index uint64) (File, error) {
	return w.fs.Create(CheckpointName(index))
}

// Checkpoint opens the checkpoint file at index for reading from the
// start.
func (w *WAL) Checkpoint(index uint64) (io.ReadCloser, error) {
	f, err := w.fs.Open(CheckpointName(index))
	if err != nil {
		return nil, err
	}
	return &fileReader{f: f}, nil
}

// fileReader reads a File from the start.
type fileReader struct {
	f   File
	off int64
}

func (r *fileReader) Read(p []byte) (int, error) {
	n, err := r.f.ReadAt(p, r.off)
	r.off += int64(n)
	if n > 0 && errors.Is(err, io.EOF) {
		err = nil
	}
	return n, err
}

func (r *fileReader) Close() error { return r.f.Close() }

// SyncDir makes the creation of a checkpoint file durable.
func (w *WAL) SyncDir() error { return w.fs.SyncDir() }

// RemoveCheckpoints removes the checkpoint files before index keep. Newer
// ones may still be being written.
func (w *WAL) RemoveCheckpoints(keep uint64) error {
	names, err := w.fs.List()
	if err != nil {
		return err
	}
	var old []string
	for _, n := range names {
		if strings.HasPrefix(n, "ckpt-") && n < CheckpointName(keep) {
			old = append(old, n)
		}
	}
	return w.removeCheckpoints(old, "")
}

// decodes reports whether a record body looks like the next record: an
// entry that follows the last one, or a hard state. Only the planted bug
// B04c uses it, to accept such a record without its checksum.
func (w *WAL) decodes(body []byte) bool {
	switch {
	case len(body) == 1+24 && body[0] == recHardState:
		return true
	case len(body) >= 1+entryHdrSize && body[0] == recEntry:
		index := binary.LittleEndian.Uint64(body[9:])
		return index >= 1 && index <= w.last+1
	}
	return false
}
