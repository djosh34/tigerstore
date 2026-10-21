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
// journal. PrepareSegment readies the file of the next segment ahead of
// time, under a name of its own, so that a rotation (Seal, Rotate, Place)
// does no sync of its own.
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
// one lock around Append, AppendRoot, AppendValue, InstallPacks, Rotate,
// Place, DropBelow, DropCleaned, RemoveBelow, RemoveCleaned and every read,
// but need not hold it around Flush, Seal and RemoveFiles. Sync starts
// the next segment itself, so such a driver uses Flush, Seal, Rotate and
// Place instead.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	iofs "io/fs"
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

// ErrFormat is a segment or pack of a format version this build does not
// read: a newer build's, as damage would also fail its checksum.
var ErrFormat = errors.New("wal: unknown format version")

// RecordError is an entry's record that does not verify when it is read:
// it is ErrCorrupt, and says which entry and segment.
type RecordError struct {
	Entry, Segment uint64
	// Reason says what is wrong.
	Reason string
}

func (e *RecordError) Error() string {
	return fmt.Sprintf("%v: the record of entry %d in segment %d %s", ErrCorrupt, e.Entry, e.Segment, e.Reason)
}

// Unwrap makes a RecordError an ErrCorrupt.
func (e *RecordError) Unwrap() error { return ErrCorrupt }

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
	nextPrefix  = "next-"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func segmentName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

func spareName(seq uint64) string { return fmt.Sprintf("%s%016x", sparePrefix, seq) }

func nextName(seq uint64) string { return fmt.Sprintf("%s%016x", nextPrefix, seq) }

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
	// end, and unplaced whether it waits for Place.
	version  uint32
	size     int64
	unplaced bool
	crc      uint32
	last     uint64
	buf      []byte
	dirty    bool
	maxSize  int64
	// spares is how many removed segments RemoveBelow and RemoveCleaned
	// keep as spare files.
	spares int
	// hs and root are the newest hard state and root, copied into every
	// new segment; copied marks that the current segment has them.
	hs     HardState
	root   Root
	copied bool

	runs    []*run
	vals    map[uint64][]valueRec
	readers map[uint64]File
	// sizes holds the size of each segment that no longer changes.
	sizes map[uint64]int64

	// packs are the packs the newest root names, by number, and nextPack
	// the number of the next one. named are the numbers of the newest
	// root that Open read.
	packs []*Pack
	// dropped are packs a root no longer names, whose files stay open
	// until RemoveCleaned removes them, for reads that found them before.
	dropped  []*Pack
	nextPack uint64
	named    []uint64
}

// Open reads the log in fs, cuts a torn tail off the newest segment, and
// returns the log ready for appends. It holds one segment in memory at a
// time and keeps only where each entry is. A damaged record anywhere else,
// a gap in the entries, or a commit index past the last entry is
// ErrCorrupt. It syncs what it read, takes the term and vote from the
// copies outside the log if those are newer (vote.go), and removes the
// checkpoint files that the newest root does not name.
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
		prev, err := segmentSeq(segs[i-1])
		if err != nil {
			return nil, nil, err
		}
		cur, err := segmentSeq(segs[i])
		if err != nil {
			return nil, nil, err
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
			// A segment takes its name, and its header goes to disk with its
			// first records, once every record of the segment before it is
			// durable, so only the newest segment can lack a header: one
			// whose first write did not reach the disk whole, in a new file
			// or a spare. Such a file holds no first record of this segment
			// after a header's place, and the log starts it again. The
			// segment before it is then still on disk, which the first
			// record chains from, unless it is segment 1. Anything else is
			// damage or a lost segment.
			seq, err := segmentSeq(name)
			if err == nil && (!last || i == 0 && seq > 1 || w.started(data, seq)) {
				err = fmt.Errorf("%w: bad header or missing segment before %s", ErrCorrupt, name)
			}
			if err != nil {
				return nil, nil, errors.Join(err, f.Close())
			}
			w.seq = seq - 1
			w.start(f)
			break
		}
		if version < 1 || version > Version {
			return nil, nil, errors.Join(fmt.Errorf("%w: segment %s has format version %d; this build reads versions 1 to %d", ErrFormat, name, version, Version), f.Close())
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
		w.seg, w.size, w.copied = f, good, true
	}
	if err := w.check(first); err != nil {
		return nil, nil, err
	}
	// A killed process leaves its writes in the page cache, where the next
	// process reads them. Unsynced, a power loss could still take back an
	// entry, a term and vote, or a root that names newer packs and a newer
	// checkpoint, after the node held them durable and removed the files
	// of the older root (#361). So what recovery read is synced before the
	// node acts on it, and before Open removes any file.
	if w.seg != nil {
		if err := w.seg.Sync(); err != nil {
			return nil, nil, errors.Join(err, w.Close())
		}
	}
	copies, err := readVotes(fs, names)
	if err != nil {
		return nil, nil, errors.Join(err, w.Close())
	}
	if saved := newestVote(copies); newerVote(saved, rec.HardState) {
		rec.HardState.Term, rec.HardState.Vote = saved.Term, saved.Vote
		w.hs = rec.HardState
	}
	if err := w.openPacks(packs, rec); err != nil {
		return nil, nil, errors.Join(err, w.Close())
	}
	w.last = max(w.last, w.root.Index)
	rec.Root, rec.Last = w.root, w.last
	if rec.HardState.Commit > w.last {
		return nil, nil, fmt.Errorf("%w: commit index %d is past the last entry %d", ErrCorrupt, rec.HardState.Commit, w.last)
	}
	// Both copies of the term and vote get the newest, which repairs a
	// copy that a crash tore, and the directory sync makes the names of
	// the log's files durable too.
	if err := errors.Join(writeVotes(fs, names, copies, HardState{Term: rec.HardState.Term, Vote: rec.HardState.Vote}), fs.SyncDir()); err != nil {
		return nil, nil, errors.Join(err, w.Close())
	}
	if err := w.removeCheckpoints(ckpts, CheckpointName(w.root.Index)); err != nil {
		return nil, nil, err
	}
	if w.seg == nil {
		if err := PrepareSegment(fs, w.seq+1); err != nil {
			return nil, nil, errors.Join(err, w.closePacks())
		}
		w.start(nil)
		if err := errors.Join(w.Place(), fs.SyncDir()); err != nil {
			return nil, nil, errors.Join(err, w.Close())
		}
	}
	return w, rec, nil
}

// started reports whether data, the content of segment seq, holds the
// segment's header or its first record in any format, chained from the
// segment before it.
func (w *WAL) started(data []byte, seq uint64) bool {
	_, hseq, crc, ok := parseHeader(data)
	if ok && hseq == seq && crc == w.crc {
		return true
	}
	for v := uint32(1); v <= Version; v++ {
		if verifiesAfter(data, headerSize, chainSeed(v, seq, w.crc)) {
			return true
		}
	}
	return false
}

// segmentSeq is the sequence number in a segment's file name.
func segmentSeq(name string) (uint64, error) {
	var seq uint64
	if _, err := fmt.Sscanf(name, "%016x.wal", &seq); err != nil {
		return 0, fmt.Errorf("%w: segment name %s: %w", ErrCorrupt, name, err)
	}
	return seq, nil
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
		if err := w.fs.Remove(n); err != nil && !errors.Is(err, iofs.ErrNotExist) {
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
func (w *WAL) seed(crc uint32) uint32 { return chainSeed(w.version, w.seq, crc) }

// chainSeed is seed for a segment of format version and sequence number
// seq.
func chainSeed(version uint32, seq uint64, crc uint32) uint32 {
	if version < 2 {
		return crc
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seq)
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

// PrepareSegment readies the file of segment seq, for Rotate, under a
// name that is not a segment's: a file it readied before for another
// segment than seq-1, which may wait for Place, the oldest spare, or a new
// file. Its name is durable when it returns. A file readied before for
// seq itself is removed: a crash after Place, before the directory sync,
// can have left records of segment seq in it, which would verify after
// the new ones. As a spare it could come back to seq through the file of
// seq+1, when another such crash loses seq's name again (#274). A spare
// that a removal took meanwhile is skipped.
func PrepareSegment(fs FS, seq uint64) error {
	for {
		names, err := fs.List()
		if err != nil {
			return err
		}
		if slices.Contains(names, nextName(seq)) {
			if err := fs.Remove(nextName(seq)); err != nil {
				return err
			}
			continue
		}
		var old, spares []string
		for _, n := range names {
			switch {
			case strings.HasPrefix(n, nextPrefix) && n != nextName(seq-1):
				old = append(old, n)
			case strings.HasPrefix(n, sparePrefix):
				spares = append(spares, n)
			}
		}
		slices.Sort(spares)
		var from string
		switch {
		case len(old) > 0:
			from = old[0]
		case len(spares) > 0:
			from = spares[0]
		default:
			f, err := fs.Create(nextName(seq))
			if err != nil {
				return err
			}
			return errors.Join(f.Close(), fs.SyncDir())
		}
		// The removal that made a spare must be durable before the file
		// takes another name: else a crash could leave the removed
		// segment under both.
		err = fs.SyncDir()
		if err == nil {
			err = fs.Rename(from, nextName(seq))
		}
		if err == nil {
			err = fs.SyncDir()
		}
		if !errors.Is(err, iofs.ErrNotExist) {
			return err
		}
	}
}

// listSpares returns the names of the spare files in order.
func listSpares(fs FS) ([]string, error) {
	names, err := fs.List()
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

// start makes f the next segment, or the file that PrepareSegment readied
// for it if f is nil, which waits for Place. Its header goes in front of
// what is buffered next, which must be nothing yet; the copies of the hard
// state and the root follow with the next write.
func (w *WAL) start(f File) {
	w.unplaced = f == nil
	w.seq++
	w.seg, w.size, w.copied, w.version = f, 0, false, Version
	w.buf = append(w.buf[:0], magic...)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, Version)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, w.seq)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, w.crc)
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
//
// A crash keeps a prefix of what was not synced, so a new term or vote
// goes before the entries: an entry never survives without the term it
// came in, which would leave Raft at a lower term than its log and make
// it panic when it rejects a vote (#173). That record keeps the old
// commit, which must not pass the entries on disk; the whole hard state
// follows the entries.
func (w *WAL) Append(hs *HardState, ents []*raftpb.Entry) {
	w.copyState()
	if hs != nil && len(ents) > 0 && (hs.Term != w.hs.Term || hs.Vote != w.hs.Vote) {
		w.hardState(HardState{Term: hs.Term, Vote: hs.Vote, Commit: w.hs.Commit})
	}
	for _, e := range ents {
		var h [entryHdrSize]byte
		binary.LittleEndian.PutUint64(h[:], e.GetTerm())
		binary.LittleEndian.PutUint64(h[8:], e.GetIndex())
		h[16] = byte(e.GetType())
		at, seed := uint32(w.size)+uint32(len(w.buf)), w.seed(w.crc)
		w.record(recEntry, h[:], e.GetData())
		w.placeEntry(e.GetIndex(), at, seed)
	}
	if hs != nil && *hs != w.hs {
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
// the records. The node driver writes a group with it and syncs it on
// another goroutine.
func (w *WAL) Flush() error {
	// A new segment's header waits for its first records, so that one
	// write carries both. Written alone and not synced, a crash could keep
	// the records after it but not the header, and recovery would take the
	// segment for a damaged one (#295).
	// Planted: B32 writes the lone header (#295).
	if len(w.buf) == 0 || w.size == 0 && len(w.buf) == headerSize && !planted.On("B32") {
		return nil
	}
	n, err := w.seg.WriteAt(w.buf, w.size)
	w.size += int64(n)
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
// failed fsync is never retried. A full segment then ends, and the next
// one starts in a file that Sync prepares itself: so a caller that
// prepares segments on another goroutine, or reads while Sync runs, uses
// Flush, Seal and Rotate instead.
func (w *WAL) Sync() error {
	if err := w.Flush(); err != nil {
		return err
	}
	if w.dirty {
		if err := w.seg.Sync(); err != nil {
			return err
		}
		w.dirty = false
	}
	if !w.Full() {
		return nil
	}
	if err := errors.Join(w.Seal(), w.seg.Sync()); err != nil {
		return err
	}
	if err := PrepareSegment(w.fs, w.seq+1); err != nil {
		return err
	}
	return errors.Join(w.Rotate().Close(), w.Place(), w.fs.SyncDir())
}

// SyncTail writes what Append buffered and syncs the newest segment,
// unless it waits for its name (Place), as etcd's WAL.Close syncs its
// tail. The node driver calls it on a clean stop, never after a failed
// write or sync.
func (w *WAL) SyncTail() error {
	if w.seg == nil {
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return w.seg.Sync()
}

// Seal cuts the full newest segment to its records, which a reused file
// would hold more after, and Flush must have written them: a segment that
// no longer changes ends with its last record. The cut must be durable,
// by the segment's next sync, before anything goes into the next segment.
func (w *WAL) Seal() error { return w.seg.Truncate(w.size) }

// Rotate makes segment Seq()+1, whose file PrepareSegment readied, the
// segment that appends go to, after Seal, and returns the old one's file
// for the caller to close once it is synced. Nothing is written into the
// new segment before Place, which waits for every record of the old one
// to be durable: the new header vouches for the old segment's last record.
func (w *WAL) Rotate() File {
	old := w.seg
	w.sizes[w.seq] = w.size
	w.start(nil)
	return old
}

// Place gives the segment that Rotate started its name, once every record
// of the segment before it is durable, so that a crash finds no newer
// segment than a torn one, and opens it under that name. The name must
// then be made durable with SyncDir before the segment's records count as
// durable.
func (w *WAL) Place() error {
	if !w.unplaced {
		return nil
	}
	if err := w.fs.Rename(nextName(w.seq), segmentName(w.seq)); err != nil {
		return err
	}
	f, err := w.fs.Open(segmentName(w.seq))
	if err != nil {
		return err
	}
	w.seg, w.unplaced = f, false
	return nil
}

// Segment returns the file of the newest segment, which Flush writes to.
// A sync of it makes what Flush wrote durable, also while the next Flush
// runs. Sync does not know of such outside syncs and syncs again what
// they covered, which costs little.
func (w *WAL) Segment() File { return w.seg }

// Full reports whether the newest segment is full: the next segment
// should start.
func (w *WAL) Full() bool { return w.size >= w.maxSize }

// Buffered is the size of what Append buffered for the next write. Only
// the goroutine that calls Sync may call it.
func (w *WAL) Buffered() int64 { return int64(len(w.buf)) }

// SetSegmentBytes changes the size after which a new segment starts. Tests
// and the simulator use small segments to exercise rotation.
func (w *WAL) SetSegmentBytes(n int64) { w.maxSize = n }

// SetSpares changes how many removed segments RemoveBelow and
// RemoveCleaned keep as spare files.
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

// reader returns the file of segment seq, opened for reads once.
func (w *WAL) reader(seq uint64) (File, error) {
	f, ok := w.readers[seq]
	if !ok {
		var err error
		if f, err = w.fs.Open(segmentName(seq)); err != nil {
			return nil, err
		}
		w.readers[seq] = f
	}
	return f, nil
}

// readAt reads len(p) bytes at offset off of segment seq.
func (w *WAL) readAt(seq uint64, p []byte, off int64) error {
	f, err := w.reader(seq)
	if err != nil {
		return err
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
		return nil, &RecordError{Entry: index, Segment: s.seq, Reason: fmt.Sprintf("has %d bytes", n)}
	}
	body := make([]byte, 1+n)
	if err := w.readAt(s.seq, body, at+8); err != nil {
		return nil, err
	}
	if crc32.Update(s.seeds[i], castagnoli, body) != binary.LittleEndian.Uint32(hdr[4:]) || body[0] != recEntry ||
		binary.LittleEndian.Uint64(body[9:]) != index {
		return nil, &RecordError{Entry: index, Segment: s.seq, Reason: "does not verify"}
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
	r, at, name, err := w.ValueReader(index, off, n)
	if err != nil {
		return nil, err
	}
	return ReadValue(r, at, name, index, off, n)
}

// ReadValue reads the n bytes at offset off of the data of entry index
// from r, the file name, at at, where ValueReader placed them.
func ReadValue(r io.ReaderAt, at int64, name string, index uint64, off, n uint32) ([]byte, error) {
	b := make([]byte, n)
	if k, err := r.ReadAt(b, at); k != len(b) {
		return nil, fmt.Errorf("wal: read %d of %d bytes at byte %d of %s, the value at %d+%d of entry %d: %w", k, n, at, name, off, n, index, err)
	}
	return b, nil
}

// ValueReader returns the file that holds the n bytes at offset off of
// the data of entry index, their offset in it and the file's name: a
// segment or a pack. The file stays open until the log removes it, or
// closes, so a caller that reads it after letting go of the log must keep
// those from running meanwhile.
func (w *WAL) ValueReader(index uint64, off, n uint32) (io.ReaderAt, int64, string, error) {
	if seq, at, ok := w.ValuePlace(index, off, n); ok {
		f, err := w.reader(seq)
		return f, at, segmentName(seq), err
	}
	if p, at, ok := w.packPlace(index, off, n); ok {
		return p.f, at, packName(p.id), nil
	}
	return nil, 0, "", fmt.Errorf("%w: value at %d+%d of entry %d", ErrNotOnDisk, off, n, index)
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

// keep is the oldest segment that must stay on disk: the newest segment
// and the one before it hold the newest hard state and root until the
// newest has them too, and while the newest waits for Place, the one
// before it may not be durable yet, so the one before that stays too.
func (w *WAL) keep() uint64 {
	if w.unplaced {
		return w.seq - 2
	}
	return w.seq - 1
}

// DropBelow forgets the oldest segments whose every entry and value is
// below index mark, and returns their file names, oldest first, for
// RemoveFiles. It keeps the segments that keep says. Every read of the
// segments it drops must be over; an error is a failed close of one,
// which is forgotten all the same.
func (w *WAL) DropBelow(mark uint64) ([]string, error) {
	// The runs of the segments to drop, which must all be below mark.
	n := 0
	for n < len(w.runs) && w.runs[n].seq < w.keep() {
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
	return w.drop(n)
}

// RemoveBelow is DropBelow and RemoveFiles at once.
func (w *WAL) RemoveBelow(mark uint64) error {
	names, err := w.DropBelow(mark)
	_, rerr := RemoveFiles(w.fs, names, w.spares)
	return errors.Join(err, rerr)
}

// drop forgets the segments of the first n runs, with the value records in
// them, and closes their files. It returns their names.
func (w *WAL) drop(n int) ([]string, error) {
	var names []string
	var err error
	for i, s := range w.runs[:n] {
		if i > 0 && w.runs[i-1].seq == s.seq {
			continue
		}
		names = append(names, segmentName(s.seq))
		if f, ok := w.readers[s.seq]; ok {
			delete(w.readers, s.seq)
			err = errors.Join(err, f.Close())
		}
		delete(w.sizes, s.seq)
	}
	w.runs = slices.Delete(w.runs, 0, n)
	if len(names) == 0 {
		return nil, err
	}
	for index, vs := range w.vals {
		vs = slices.DeleteFunc(vs, func(v valueRec) bool { return v.seq < w.Oldest() })
		if len(vs) == 0 {
			delete(w.vals, index)
		} else {
			w.vals[index] = vs
		}
	}
	return names, err
}

// RemoveFiles removes the files names, in order, that DropBelow or
// DropCleaned returned, and syncs the directory: a segment becomes a spare
// file while fewer than spares are on disk. It returns how many it
// removed before an error. A file that is gone already counts as removed,
// so a retry of the rest goes on where an error stopped it, and segments
// still go only from the front.
func RemoveFiles(fs FS, names []string, spares int) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	have, err := listSpares(fs)
	if err != nil {
		return 0, err
	}
	kept := len(have)
	for i, name := range names {
		seq, serr := segmentSeq(name)
		switch {
		case serr == nil && kept < spares:
			if err = fs.Rename(name, spareName(seq)); err == nil {
				kept++
			}
		default:
			err = fs.Remove(name)
		}
		if err != nil && !errors.Is(err, iofs.ErrNotExist) {
			return i, err
		}
	}
	return len(names), fs.SyncDir()
}

// RemoveSpares removes the spare files beyond the oldest keep, and syncs
// the directory if it removed any.
func RemoveSpares(fs FS, keep int) error {
	spares, err := listSpares(fs)
	if err != nil || len(spares) <= keep {
		return err
	}
	for _, name := range spares[keep:] {
		if err := fs.Remove(name); err != nil && !errors.Is(err, iofs.ErrNotExist) {
			return err
		}
	}
	return fs.SyncDir()
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
