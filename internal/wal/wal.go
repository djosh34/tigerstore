// Package wal stores the Raft log and hard state of one node. The log is
// the value store: every write's key and value live only here and in memory.
//
// The log is a sequence of segment files. Each segment starts with a header
// and holds records. A record is
//
//	u32 payload length | u32 crc | u8 type | payload
//
// The crc covers type and payload and starts from the crc of the record
// before it, so a stale or misplaced record does not verify. The header
// carries the crc that the segment's first record starts from.
//
// An entry record replaces every entry at or after its index, so a
// truncated Raft suffix and its replacement are synced together.
//
// The package does no I/O of its own. It reads and writes through FS, so
// the simulator runs it on a simulated disk.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
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
	// SyncDir makes creations and removals durable.
	SyncDir() error
}

// File is one segment file.
type File interface {
	// ReadAll returns the whole content.
	ReadAll() ([]byte, error)
	// Write appends p.
	Write(p []byte) (int, error)
	// Truncate cuts the file to size bytes.
	Truncate(size int64) error
	// Sync makes the content durable.
	Sync() error
	Close() error
}

// ErrCorrupt means the log on disk is damaged in a way that a crash cannot
// cause. The node must not take part in the cluster with it.
var ErrCorrupt = errors.New("wal: corrupt log")

const (
	magic      = "tigerwal"
	headerSize = len(magic) + 8 + 4
	recHdrSize = 9

	recEntry     = 1
	recHardState = 2

	// SegmentBytes is the size after which the log starts a new segment.
	SegmentBytes = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func segmentName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

// WAL appends to the newest segment.
type WAL struct {
	fs      FS
	seg     File
	seq     uint64
	size    int64
	crc     uint32
	last    uint64
	buf     []byte
	dirty   bool
	maxSize int64
}

// HardState is the Raft hard state.
type HardState struct {
	Term, Vote, Commit uint64
}

// Recovered is what Open read from disk.
type Recovered struct {
	HardState HardState
	Entries   []*raftpb.Entry
}

// Open reads the log in fs, cuts a torn tail off the newest segment, and
// returns the log ready for appends. A damaged record anywhere else, a gap
// in the entries, or a commit index past the last entry is ErrCorrupt.
func Open(fs FS) (*WAL, *Recovered, error) {
	names, err := fs.List()
	if err != nil {
		return nil, nil, err
	}
	var segs []string
	for _, n := range names {
		if strings.HasSuffix(n, ".wal") {
			segs = append(segs, n)
		}
	}
	slices.Sort(segs)
	// Segment names must be consecutive: a gap is a lost segment, even
	// when the newest segment is empty.
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

	w := &WAL{fs: fs, maxSize: SegmentBytes}
	rec := &Recovered{}
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
		seq, crc, ok := parseHeader(data)
		ok = ok && name == segmentName(seq)
		// The first segment may start anywhere once old segments are
		// cleaned; every later one continues the sequence and the crc chain.
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
		w.seq, w.crc = seq, crc
		good, err := w.replay(data, rec)
		if err != nil {
			return nil, nil, errors.Join(fmt.Errorf("%s at byte %d: %w", name, good, err), f.Close())
		}
		if good < int64(len(data)) {
			if !last {
				return nil, nil, errors.Join(fmt.Errorf("%w: damaged record in %s at byte %d", ErrCorrupt, name, good), f.Close())
			}
			// A torn tail: the write was never synced, so nothing in it was
			// acknowledged.
			if err := errors.Join(f.Truncate(good), f.Sync()); err != nil {
				return nil, nil, errors.Join(err, f.Close())
			}
		}
		if !last {
			if err := f.Close(); err != nil {
				return nil, nil, err
			}
			continue
		}
		w.seg, w.size = f, good
	}
	if rec.HardState.Commit > w.last {
		return nil, nil, fmt.Errorf("%w: commit index %d is past the last entry %d", ErrCorrupt, rec.HardState.Commit, w.last)
	}
	if w.seg == nil {
		if err := w.newSegment(); err != nil {
			return nil, nil, err
		}
	}
	return w, rec, nil
}

func parseHeader(data []byte) (seq uint64, crc uint32, ok bool) {
	if len(data) < headerSize || string(data[:len(magic)]) != magic {
		return 0, 0, false
	}
	seq = binary.LittleEndian.Uint64(data[len(magic):])
	crc = binary.LittleEndian.Uint32(data[len(magic)+8:])
	return seq, crc, true
}

// replay applies the records of one segment to rec and returns the length
// of the valid prefix. A record that does not verify ends the prefix. A
// record that verifies but does not fit the log is an error.
func (w *WAL) replay(data []byte, rec *Recovered) (int64, error) {
	off := headerSize
	for off < len(data) {
		if len(data)-off < recHdrSize {
			return int64(off), nil
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		sum := binary.LittleEndian.Uint32(data[off+4:])
		end := off + recHdrSize + n
		if end > len(data) {
			return int64(off), nil
		}
		body := data[off+8 : end]
		if got := crc32.Update(w.crc, castagnoli, body); got != sum {
			// The damage may be in the body or in the stored crc, so try
			// the next record chained from either.
			if verifiesAfter(data, end, sum) || verifiesAfter(data, end, got) {
				return int64(off), fmt.Errorf("%w: damaged record followed by a valid one", ErrCorrupt)
			}
			return int64(off), nil
		}
		if err := w.apply(body[0], body[1:], rec); err != nil {
			return int64(off), err
		}
		w.crc = sum
		off = end
	}
	return int64(off), nil
}

// verifiesAfter reports whether a whole record starts at off and verifies
// when chained from crc, the stored crc of the record before it. A torn
// write leaves garbage up to the end of the file, so a valid record after a
// bad one means the bad one was damaged after it was written. This can also
// fence a node after a crash whose disk persisted a later sector of the
// last write but not an earlier one; refusing to start is the safe answer.
func verifiesAfter(data []byte, off int, crc uint32) bool {
	if len(data)-off < recHdrSize {
		return false
	}
	n := int(binary.LittleEndian.Uint32(data[off:]))
	end := off + recHdrSize + n
	if end > len(data) {
		return false
	}
	return crc32.Update(crc, castagnoli, data[off+8:end]) == binary.LittleEndian.Uint32(data[off+4:])
}

func (w *WAL) apply(typ byte, p []byte, rec *Recovered) error {
	switch typ {
	case recEntry:
		if len(p) < 17 {
			return fmt.Errorf("%w: short entry record", ErrCorrupt)
		}
		term := binary.LittleEndian.Uint64(p)
		index := binary.LittleEndian.Uint64(p[8:])
		etype := raftpb.EntryType(p[16])
		if index == 0 || index > w.last+1 {
			return fmt.Errorf("%w: entry %d follows entry %d", ErrCorrupt, index, w.last)
		}
		first := uint64(1)
		if len(rec.Entries) > 0 {
			first = rec.Entries[0].GetIndex()
		}
		if index < first {
			return fmt.Errorf("%w: entry %d is before the first entry %d", ErrCorrupt, index, first)
		}
		rec.Entries = rec.Entries[:index-first]
		rec.Entries = append(rec.Entries, &raftpb.Entry{Term: new(term), Index: new(index), Type: &etype, Data: p[17:]})
		w.last = index
	case recHardState:
		if len(p) != 24 {
			return fmt.Errorf("%w: hard state record of %d bytes", ErrCorrupt, len(p))
		}
		rec.HardState = HardState{
			Term:   binary.LittleEndian.Uint64(p),
			Vote:   binary.LittleEndian.Uint64(p[8:]),
			Commit: binary.LittleEndian.Uint64(p[16:]),
		}
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return nil
}

// newSegment starts the next segment. It syncs the header before the
// directory entry, so a segment that survives a crash always has its
// header.
func (w *WAL) newSegment() error {
	seq := w.seq + 1
	f, err := w.fs.Create(segmentName(seq))
	if err != nil {
		return err
	}
	hdr := make([]byte, 0, headerSize)
	hdr = append(hdr, magic...)
	hdr = binary.LittleEndian.AppendUint64(hdr, seq)
	hdr = binary.LittleEndian.AppendUint32(hdr, w.crc)
	if _, err := f.Write(hdr); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := w.fs.SyncDir(); err != nil {
		return errors.Join(err, f.Close())
	}
	w.seq, w.seg, w.size = seq, f, int64(headerSize)
	return nil
}

func (w *WAL) record(typ byte, payload ...[]byte) {
	n := 1
	for _, p := range payload {
		n += len(p)
	}
	start := len(w.buf)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(n-1))
	w.buf = append(w.buf, 0, 0, 0, 0, typ)
	for _, p := range payload {
		w.buf = append(w.buf, p...)
	}
	w.crc = crc32.Update(w.crc, castagnoli, w.buf[start+8:])
	binary.LittleEndian.PutUint32(w.buf[start+4:], w.crc)
}

// EntryRecordBytes is the size of the record that stores e.
func EntryRecordBytes(e *raftpb.Entry) int64 { return recHdrSize + 17 + int64(len(e.GetData())) }

// Append buffers entries and a hard state. Nothing is durable before Sync.
// A nil hard state leaves the stored one unchanged.
func (w *WAL) Append(hs *HardState, ents []*raftpb.Entry) {
	for _, e := range ents {
		var h [17]byte
		binary.LittleEndian.PutUint64(h[:], e.GetTerm())
		binary.LittleEndian.PutUint64(h[8:], e.GetIndex())
		h[16] = byte(e.GetType())
		w.record(recEntry, h[:], e.GetData())
		w.last = e.GetIndex()
	}
	if hs != nil {
		var h [24]byte
		binary.LittleEndian.PutUint64(h[:], hs.Term)
		binary.LittleEndian.PutUint64(h[8:], hs.Vote)
		binary.LittleEndian.PutUint64(h[16:], hs.Commit)
		w.record(recHardState, h[:])
	}
}

// Flush writes what Append buffered without syncing it. The simulator
// uses it to put a crash between the write and the fsync.
func (w *WAL) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.seg.Write(w.buf)
	w.size += int64(n)
	w.dirty = true
	if err != nil {
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
	if err := w.seg.Sync(); err != nil {
		return err
	}
	w.dirty = false
	if w.size < w.maxSize {
		return nil
	}
	if err := w.seg.Close(); err != nil {
		return err
	}
	return w.newSegment()
}

// SetSegmentBytes changes the size after which a new segment starts. Tests
// and the simulator use small segments to exercise rotation.
func (w *WAL) SetSegmentBytes(n int64) { w.maxSize = n }

// Close closes the newest segment without syncing.
func (w *WAL) Close() error { return w.seg.Close() }
