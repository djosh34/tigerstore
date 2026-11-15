package wl

import (
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
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
	ents                []storedEnt // from snapIndex+1 on
	// held is the data bytes of the entries the storage holds itself;
	// trim keeps it within storageHeld.
	held int64
	// trimmed: every entry before this index holds no data.
	trimmed uint64

	log logReader

	stats storageStats
}

// storageWindow is how many applied entries the storage keeps at least,
// for followers that lag.
const storageWindow = 100000

// storageHeld is the most entry data the storage holds in memory. A
// leader's Raft copies each proposal to the heap, so its entries are held
// there and not in a ring.
const storageHeld = 64 << 20

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
	return &logStorage{hs: &pb.HardState{}, cs: &pb.ConfState{}, log: log}
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
			return nil, err
		}
		out = append(out, ent)
	}
	return out, nil
}

// entry returns entry i, built again from the log if its data was let
// go.
func (s *logStorage) entry(i uint64, e *storedEnt) (*pb.Entry, error) {
	if e.ent != nil {
		return e.ent, nil
	}
	ent := &pb.Entry{Index: new(i), Term: new(e.term), Type: e.typ.Enum()}
	if e.n > 0 {
		ent.Data = make([]byte, e.n)
		if err := s.log.read(ent.Data, e.off); err != nil {
			return nil, fmt.Errorf("read entry %d back from the log: %w", i, err)
		}
		s.stats.reads++
	}
	return ent, nil
}

// Snapshot is not taken yet: a follower that needs one is reported as
// failed by sendPeer.
func (s *logStorage) Snapshot() (*pb.Snapshot, error) {
	return &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(s.snapIndex), Term: new(s.snapTerm), ConfState: s.cs}}, nil
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
// storageHeld of their data, as long as they are applied and their
// appends' responses are stepped: up to applied, and with their data
// before stable in the log.
func (s *logStorage) trim(applied uint64, stable int64) {
	for i := max(s.trimmed, s.snapIndex+1); i <= s.lastIndex() && s.held > storageHeld; i++ {
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

// compact drops the entries up to and including i.
func (s *logStorage) compact(i uint64) {
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
