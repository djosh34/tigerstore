package core

import (
	"slices"
	"sort"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

// storage is what Raft reads of the log: the durable entries that are
// still in memory, a window of applied ones and those not applied yet.
// Older entries are on disk only. Membership is fixed: every peer is a
// voter from the start.
type storage struct {
	*raft.MemoryStorage
	conf *raftpb.ConfState
	// cum[i] is the data bytes of the entries in memory up to and
	// including entry first+i, counted from base.
	first uint64
	cum   []int64
	base  int64
	// ckpt is the newest durable checkpoint, which a snapshot names when
	// it is past the window.
	ckpt wal.Root
}

func newStorage(peers []uint64) *storage {
	return &storage{
		MemoryStorage: raft.NewMemoryStorage(),
		conf:          &raftpb.ConfState{Voters: append([]uint64(nil), peers...)},
		first:         1,
	}
}

// InitialState returns the recovered hard state and the fixed membership.
func (s *storage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := s.MemoryStorage.InitialState()
	return hs, s.conf, err
}

// Snapshot is what a follower behind the window gets: the state at the
// window's start, or at the newest durable checkpoint if that is later.
// It carries no data; the follower's driver fetches what it lacks.
func (s *storage) Snapshot() (*raftpb.Snapshot, error) {
	index := s.first - 1
	term, err := s.Term(index)
	if err != nil {
		return nil, err
	}
	if s.ckpt.Index > index {
		index, term = s.ckpt.Index, s.ckpt.Term
	}
	if index == 0 {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	return &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: s.conf}}, nil
}

// add appends durable entries to memory. An entry replaces those at and
// after its index.
func (s *storage) add(ents []*raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if err := s.Append(ents); err != nil {
		return err
	}
	from := ents[0].GetIndex()
	if from < s.first {
		ents = ents[s.first-from:]
		from = s.first
	}
	s.cum = s.cum[:from-s.first]
	total := s.base
	if len(s.cum) > 0 {
		total = s.cum[len(s.cum)-1]
	}
	for _, e := range ents {
		total += int64(len(e.GetData()))
		s.cum = append(s.cum, total)
	}
	return nil
}

// bytes returns the data bytes of the entries in memory up to index.
func (s *storage) bytes(index uint64) int64 {
	if index < s.first || len(s.cum) == 0 {
		return 0
	}
	i := min(index-s.first, uint64(len(s.cum)-1))
	return s.cum[i] - s.base
}

// total returns the data bytes of every entry in memory.
func (s *storage) total() int64 { return s.bytes(s.first + uint64(len(s.cum))) }

// trim drops the entries up to index from memory.
func (s *storage) trim(index uint64) error {
	if index < s.first {
		return nil
	}
	if err := s.Compact(index); err != nil {
		return err
	}
	k := index - s.first + 1
	s.base = s.cum[k-1]
	s.cum = slices.Clone(s.cum[k:])
	s.first = index + 1
	return nil
}

// keep returns the index up to which entries must leave memory so that
// at most limit bytes of those up to applied stay, or 0 if they fit.
func (s *storage) keep(applied uint64, limit int64) uint64 {
	upto := s.bytes(applied)
	if upto <= limit {
		return 0
	}
	// The first entry such that the bytes after it, up to applied, fit.
	n := int(applied - s.first + 1)
	i := sort.Search(n, func(i int) bool { return upto-(s.cum[i]-s.base) <= limit })
	return s.first + uint64(i)
}

// reset empties memory at a snapshot's index.
func (s *storage) reset(snap *raftpb.Snapshot) error {
	if err := s.ApplySnapshot(snap); err != nil {
		return err
	}
	s.first, s.cum, s.base = snap.GetMetadata().GetIndex()+1, nil, 0
	return nil
}

func (s *storage) setHardState(h wal.HardState) error {
	return s.SetHardState(raftHardState(h))
}

func raftHardState(h wal.HardState) *raftpb.HardState {
	return &raftpb.HardState{Term: new(h.Term), Vote: new(h.Vote), Commit: new(h.Commit)}
}
