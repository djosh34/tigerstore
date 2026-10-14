package core

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

// storage is what Raft reads of the durable log. It holds the entries that
// the WAL made durable, in memory, so it is also where values are read
// from. Membership is fixed: every peer is a voter from the start.
type storage struct {
	*raft.MemoryStorage
	conf *raftpb.ConfState
	// snap is the A+ prototype's snapshot source.
	snap func() (*raftpb.Snapshot, error)
}

// Snapshot returns the A+ snapshot, with the fixed membership.
func (s *storage) Snapshot() (*raftpb.Snapshot, error) {
	if s.snap == nil {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	return s.snap()
}

func newStorage(peers []uint64) *storage {
	return &storage{
		MemoryStorage: raft.NewMemoryStorage(),
		conf:          &raftpb.ConfState{Voters: append([]uint64(nil), peers...)},
	}
}

// InitialState returns the recovered hard state and the fixed membership.
func (s *storage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := s.MemoryStorage.InitialState()
	return hs, s.conf, err
}

func (s *storage) load(rec *wal.Recovered) error {
	if err := s.Append(rec.Entries); err != nil {
		return err
	}
	return s.SetHardState(raftHardState(rec.HardState))
}

// loadFrom starts the log at a checkpoint at index, term and appends the
// recovered entries after it (A+ prototype).
func (s *storage) loadFrom(rec *wal.Recovered, index, term uint64, hs wal.HardState) error {
	if err := s.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: s.conf}}); err != nil {
		return err
	}
	var ents []*raftpb.Entry
	for _, e := range rec.Entries {
		if e.GetIndex() > index {
			ents = append(ents, e)
		}
	}
	if err := s.Append(ents); err != nil {
		return err
	}
	if rec.HasHardState {
		hs = rec.HardState
	}
	if hs.Commit < index {
		hs.Commit = index
	}
	return s.SetHardState(raftHardState(hs))
}

func (s *storage) append(job PersistJob) error {
	if job.Snapshot != nil {
		ents := job.Entries[:0:0]
		for _, e := range job.Entries {
			if e.GetIndex() > job.Snapshot.GetMetadata().GetIndex() {
				ents = append(ents, e)
			}
		}
		job.Entries = ents
	}
	if err := s.Append(job.Entries); err != nil {
		return err
	}
	if job.HardState == nil {
		return nil
	}
	return s.SetHardState(raftHardState(*job.HardState))
}

func raftHardState(h wal.HardState) *raftpb.HardState {
	return &raftpb.HardState{Term: new(h.Term), Vote: new(h.Vote), Commit: new(h.Commit)}
}
