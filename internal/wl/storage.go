package wl

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// logStorage is Raft's storage. It keeps each entry's term, type and
// place in the log in memory, and the entry itself while a ring holds its
// data. Once the ring lets that data go, the entry is built again from
// the log whenever Raft asks for it. Only the loop uses it.
type logStorage struct {
	hs *pb.HardState
	cs *pb.ConfState

	// The entry before the first one held, as a snapshot names it.
	snapIndex, snapTerm uint64
	// snap is the newest checkpoint, which a follower that lags gets; the
	// storage never drops an entry after it.
	snap *pb.Snapshot
	ents []storedEnt // from snapIndex+1 on
	// held is the data bytes of the entries the storage holds itself;
	// trim keeps it within heldMax (sizes.held).
	held, heldMax int64
	// trimmed: every entry before this index holds no data.
	trimmed uint64

	log logReader

	// confIndex is the newest membership change applied. A checkpoint
	// before it lacks a member that change added, whose Raft drops the
	// snapshot: the storage asks for a new checkpoint (wantCkpt) and has
	// none to give until then.
	confIndex uint64
	wantCkpt  bool

	// failed is why an entry could not be read back from the log. Raft
	// stops its process on any error of its storage, so Entries gives it
	// the entry without its data instead, and the loop stops on failed
	// before it uses anything Raft made of it (node.ready).
	failed error

	stats storageStats
}

// storageWindow is how many applied entries the storage keeps at least,
// for followers that lag; one further behind gets a checkpoint.
var storageWindow uint64 = 100000

type storedEnt struct {
	ent  *pb.Entry // nil once its data was let go
	term uint64
	typ  pb.EntryType
	off  int64 // where its data lies in the log
	n    uint32
}

type storageStats struct {
	letGo, reads uint64
}

var _ raft.Storage = (*logStorage)(nil)

func newLogStorage(log logReader) *logStorage {
	// A leader's Raft copies each proposal to the heap, so its entries are
	// held there and not in a ring.
	return &logStorage{hs: &pb.HardState{}, cs: &pb.ConfState{}, log: log, heldMax: sizeFor(0).held}
}

func (s *logStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	return s.hs, s.cs, nil
}

func (s *logStorage) lastIndex() uint64 { return s.snapIndex + uint64(len(s.ents)) }

func (s *logStorage) FirstIndex() (uint64, error) { return s.snapIndex + 1, nil }

func (s *logStorage) LastIndex() (uint64, error) { return s.lastIndex(), nil }

func (s *logStorage) Term(i uint64) (uint64, error) {
	switch {
	case i < s.snapIndex:
		return 0, raft.ErrCompacted
	case i == s.snapIndex:
		return s.snapTerm, nil
	case i > s.lastIndex():
		return 0, raft.ErrUnavailable
	}
	return s.ents[i-s.snapIndex-1].term, nil
}

// Entries returns the entries [lo, hi), at least one and at most maxSize
// bytes of them.
func (s *logStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	if lo <= s.snapIndex {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndex()+1 {
		return nil, fmt.Errorf("entries up to %d asked of a log that ends at %d", hi, s.lastIndex())
	}
	var out []*pb.Entry
	var size uint64
	for i := lo; i < hi; i++ {
		e := &s.ents[i-s.snapIndex-1]
		size += uint64(e.n) + 16
		if len(out) > 0 && size > maxSize {
			break
		}
		ent, err := s.entry(i, e)
		if err != nil {
			s.failed = cmp.Or(s.failed, err)
			ent = &pb.Entry{Index: new(i), Term: new(e.term), Type: e.typ.Enum()}
		}
		out = append(out, ent)
	}
	return out, nil
}

// ErrBadEntry is an entry read back from the log whose record does not
// check: the disk damaged it at rest. The node stops instead of sending
// it.
var ErrBadEntry = errors.New("wl: an entry read back from the log does not check")

// entry returns entry i, built again from the log if its data was let
// go: its whole record is read, and must check and name entry i.
func (s *logStorage) entry(i uint64, e *storedEnt) (*pb.Entry, error) {
	if e.ent != nil {
		return e.ent, nil
	}
	ent := &pb.Entry{Index: new(i), Term: new(e.term), Type: e.typ.Enum()}
	if e.n > 0 {
		rec := make([]byte, walEntHdr+int(e.n))
		if err := s.log.read(rec, e.off-walEntHdr); err != nil {
			return nil, fmt.Errorf("read entry %d back from the log: %w", i, err)
		}
		if !checks(rec, e.off-walEntHdr) || binary.LittleEndian.Uint64(rec[9:]) != i || binary.LittleEndian.Uint64(rec[17:]) != e.term {
			return nil, fmt.Errorf("entry %d: %w", i, &ReadBackError{Err: ErrBadEntry, Off: e.off - walEntHdr, Len: len(rec)})
		}
		ent.Data = rec[walEntHdr:]
		s.stats.reads++
	}
	return ent, nil
}

// Snapshot returns the newest checkpoint; its data is the checkpoint's
// file name.
func (s *logStorage) Snapshot() (*pb.Snapshot, error) {
	if s.snap == nil || s.snap.GetMetadata().GetIndex() < s.confIndex {
		s.wantCkpt = true
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	return s.snap, nil
}

// applySnapshot makes the storage start after a checkpoint the node now
// holds, named name: the entries it held are dropped.
func (s *logStorage) applySnapshot(meta *pb.SnapshotMetadata, name string) {
	s.snap = &pb.Snapshot{Data: []byte(name), Metadata: meta}
	s.snapIndex, s.snapTerm = meta.GetIndex(), meta.GetTerm()
	s.cs = meta.GetConfState()
	s.ents = s.ents[:0]
	s.held = 0
	s.trimmed = 0
}

// firstRecord is where the log record of the first entry the storage
// holds starts, or end if it holds none.
func (s *logStorage) firstRecord(end int64) int64 {
	if len(s.ents) == 0 {
		return end
	}
	return s.ents[0].off - walEntHdr
}

// append adds entries the log has just written, offs[i] the offset of
// ents[i]'s data. Entries from the first index on that the storage held
// already are replaced, as Raft truncates a log that conflicts.
func (s *logStorage) append(ents []*pb.Entry, offs []int64) error {
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].GetIndex()
	if first <= s.snapIndex {
		k := s.snapIndex + 1 - first
		if k >= uint64(len(ents)) {
			return nil
		}
		ents, offs = ents[k:], offs[k:]
		first = s.snapIndex + 1
	}
	if first > s.lastIndex()+1 {
		return fmt.Errorf("an append at %d to a log that ends at %d", first, s.lastIndex())
	}
	k := first - s.snapIndex - 1
	for _, e := range s.ents[k:] {
		if e.ent != nil {
			s.held -= int64(e.n)
		}
	}
	s.ents = s.ents[:k]
	s.trimmed = min(s.trimmed, first)
	for i, e := range ents {
		s.ents = append(s.ents, storedEnt{ent: e, term: e.GetTerm(), typ: e.GetType(), off: offs[i], n: uint32(len(e.GetData()))})
		s.held += int64(len(e.GetData()))
	}
	return nil
}

func (s *logStorage) setHardState(hs *pb.HardState) { s.hs = hs }

// restore takes a record the log holds, as the node starts: the newest
// hard state, and every entry with where its data lies. A later entry
// replaces the ones from its index on, as the append that wrote it did.
func (s *logStorage) restore(r *walRecord) error {
	switch r.kind {
	case recHardState:
		hs := &pb.HardState{}
		if err := proto.Unmarshal(r.data, hs); err != nil {
			return fmt.Errorf("the log's hard state at %d: %w", r.off, err)
		}
		s.hs = hs
	case recEntry:
		if r.index <= s.snapIndex {
			return nil
		}
		if r.index > s.lastIndex()+1 {
			return fmt.Errorf("the log has entry %d after %d", r.index, s.lastIndex())
		}
		s.ents = s.ents[:r.index-s.snapIndex-1]
		s.ents = append(s.ents, storedEnt{term: r.term, typ: r.typ, off: r.off, n: uint32(len(r.data))})
	default:
		return fmt.Errorf("a log record of kind %d at %d", r.kind, r.off)
	}
	return nil
}

// offOf returns where entry i's data lies in the log, or -1 if the
// storage does not hold it.
func (s *logStorage) offOf(i uint64) int64 {
	if i <= s.snapIndex || i > s.lastIndex() {
		return -1
	}
	return s.ents[i-s.snapIndex-1].off
}

// holds reports whether the storage holds ent itself as its entry.
func (s *logStorage) holds(ent *pb.Entry) bool {
	i := ent.GetIndex()
	return i > s.snapIndex && i <= s.lastIndex() && s.ents[i-s.snapIndex-1].ent == ent
}

// letGo drops ent, which the storage holds: its data is read back from
// the log from now on.
func (s *logStorage) letGo(ent *pb.Entry) {
	if s.holds(ent) {
		e := &s.ents[ent.GetIndex()-s.snapIndex-1]
		e.ent = nil
		s.held -= int64(e.n)
		s.stats.letGo++
	}
}

// trim lets go of the oldest entries while the storage holds more than
// heldMax of their data, as long as they are applied and their
// appends' responses are stepped: up to applied, and with their data
// before stable in the log.
func (s *logStorage) trim(applied uint64, stable int64) {
	for i := max(s.trimmed, s.snapIndex+1); i <= s.lastIndex() && s.held > s.heldMax; i++ {
		e := &s.ents[i-s.snapIndex-1]
		if i > applied || e.off+int64(e.n) > stable {
			return
		}
		if e.ent != nil {
			s.letGo(e.ent)
		}
		s.trimmed = i + 1
	}
}

// compact drops the entries up to and including i, never one after the
// newest checkpoint.
func (s *logStorage) compact(i uint64) {
	if s.snap == nil {
		return
	}
	s.drop(min(i, s.snap.GetMetadata().GetIndex()))
}

// drop lets go of the entries to index i.
func (s *logStorage) drop(i uint64) {
	if i <= s.snapIndex || i > s.lastIndex() {
		return
	}
	k := i - s.snapIndex
	for _, e := range s.ents[:k] {
		if e.ent != nil {
			s.held -= int64(e.n)
		}
	}
	s.snapTerm = s.ents[k-1].term
	s.snapIndex = i
	s.ents = append(s.ents[:0], s.ents[k:]...)
}
