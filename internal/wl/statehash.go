package wl

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"maps"
	"slices"
)

// The simulator compares nodes' stores at the indexes their checkpoints
// were taken at, and each checkpoint with the store it was taken of
// (internal/sim's Checkpoint and CheckpointDone hooks). wl's nodes take
// their checkpoints at indexes of their own, so files rarely meet across
// nodes, as main's do: the hash below stands for a checkpoint's state.

// stateHash is a hash of the store as a checkpoint of it at its revision
// holds it: the revisions, the leases, each key a read at the compacted
// revision or later can see with its states and their values, and the
// changes after the compacted revision, all by key name. States older
// than what such a read sees are left out: a checkpoint being written
// holds them back from trimming on one node and not on another (seed 7
// of TestWLRunsTheFaults). A node with the
// same state has the same hash whatever its key ids, and so does a store
// loaded from the checkpoint. A value stands in it by its checksum, which
// the apply took of it: the hash reads nothing back, so that taking it
// changes nothing the simulator's disk sees.
func (st *store) stateHash() uint64 {
	h := fnv.New64a()
	var b []byte
	put := func(vs ...uint64) {
		b = b[:0]
		for _, v := range vs {
			b = binary.AppendUvarint(b, v)
		}
		_, _ = h.Write(b)
	}
	put(uint64(st.rev), uint64(st.compactRev), boolUvarint(st.compacted))
	ids := slices.Sorted(maps.Keys(st.leases))
	put(uint64(len(ids)))
	for _, id := range ids {
		put(uint64(id), uint64(st.leases[id].ttl))
	}
	// Every key id, as the checkpoint's writer takes them, in the order
	// of their names.
	var named []uint32
	var states []ckptState
	for id := range st.nkeys {
		if len(st.ckptStates(id, st.rev, st.compactRev, states[:0])) > 0 {
			named = append(named, id)
		}
	}
	slices.SortFunc(named, func(a, b uint32) int { return bytes.Compare(st.key(a), st.key(b)) })
	for _, id := range named {
		states = st.ckptStates(id, st.rev, st.compactRev, states[:0])
		key := st.key(id)
		put(uint64(len(key)))
		_, _ = h.Write(key)
		put(uint64(len(states)))
		for _, s := range states {
			put(boolUvarint(s.deleted), uint64(s.create), uint64(s.mod), uint64(s.ver), uint64(s.lease), uint64(s.vlen), uint64(s.vsum))
		}
	}
	for _, ch := range st.changes[searchChanges(st.changes, st.compactRev):] {
		if ch.rev > st.rev {
			break
		}
		key := st.key(ch.key)
		put(uint64(ch.rev), boolUvarint(ch.del), uint64(len(key)))
		_, _ = h.Write(key)
	}
	return h.Sum64()
}

// logFunc reads the log's bytes at an offset.
type logFunc func(p []byte, off int64) error

func (f logFunc) read(p []byte, off int64) error { return f(p, off) }

// bytesFile is a checkpoint file's bytes.
type bytesFile struct {
	*bytes.Reader
	name string
}

func (f *bytesFile) Close() error { return nil }
func (f *bytesFile) Name() string { return f.name }

// CheckpointStateHash loads the checkpoint file b, named name, as a start
// would, with readLog reading the values it names in the node's log,
// checks every value it names against its checksum, and returns the hash
// of the store it holds (stateHash).
func CheckpointStateHash(b []byte, name string, readLog func(p []byte, off int64) error) (uint64, error) {
	st, err := newStore()
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.close() }()
	st.log = logFunc(readLog)
	if _, err := loadCkpt(&bytesFile{Reader: bytes.NewReader(b), name: name}, 0, st); err != nil {
		return 0, err
	}
	var states []ckptState
	for id := range st.nkeys {
		states = st.ckptStates(id, st.rev, st.compactRev, states[:0])
		for _, s := range states {
			if _, err := st.readValue(s.voff, s.vlen, s.vsum); err != nil {
				return 0, err
			}
		}
	}
	return st.stateHash(), nil
}
