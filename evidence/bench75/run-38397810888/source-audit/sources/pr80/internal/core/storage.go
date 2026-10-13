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

func (s *storage) append(job PersistJob) error {
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
