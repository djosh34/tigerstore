package core

// Prototype E (#133). Values are named by their Raft position; the core
// keeps only a window of Raft entries in RAM, cuts metadata-only
// checkpoints in chunks per committer turn, restarts from a checkpoint
// plus the raw log after it, and installs a Raft snapshot by applying the
// raw entries a helper fetched from the leader's disk.

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"strconv"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

func envInt(name string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return def
}

var (
	// WindowBytes is how much entry data stays in RAM at least;
	// WindowMax is the most, even if a follower is behind.
	WindowBytes = envInt("TS_E_WINDOW", 64<<20)
	WindowMax   = envInt("TS_E_WINDOW_MAX", 128<<20)
	// A checkpoint is cut once the log grew by CkptMin and by CkptK
	// times the last checkpoint's size.
	CkptMin = envInt("TS_E_CKPT_MIN", 256<<20)
	CkptK   = envInt("TS_E_CKPT_K", 16)
	// CkptBytes is about how much of the index the committer copies per
	// turn.
	CkptBytes = int(envInt("TS_E_CKPT_BYTES", 128<<10))
)

// DiskValues reads values that left the RAM window.
type DiskValues interface {
	Read(r mvcc.Ref) []byte
}

type values struct {
	st   *storage
	disk DiskValues
}

func (v values) Hot(r mvcc.Ref) ([]byte, bool) {
	ents, err := v.st.Entries(r.Idx, r.Idx+1, math.MaxUint64)
	if err != nil || len(ents) == 0 {
		return nil, false
	}
	d := ents[0].GetData()
	return d[r.Off : r.Off+r.N], true
}

func (v values) Read(r mvcc.Ref) []byte {
	if b, ok := v.Hot(r); ok {
		return b
	}
	return v.disk.Read(r)
}

// SetDiskValues installs the reader of cold values.
func (n *Node) SetDiskValues(d DiskValues) { mvcc.Values = values{st: n.storage, disk: d} }

type winEnt struct {
	idx  uint64
	size int64
}

// trackWindow notes entries that entered the in-memory log.
func (n *Node) trackWindow(ents []*raftpb.Entry) {
	for _, e := range ents {
		for len(n.win) > 0 && n.win[len(n.win)-1].idx >= e.GetIndex() {
			n.winBytes -= n.win[len(n.win)-1].size
			n.win = n.win[:len(n.win)-1]
		}
		s := int64(len(e.GetData()))
		n.win = append(n.win, winEnt{e.GetIndex(), s})
		n.winBytes += s
	}
}

// trimWindow drops applied entries from RAM beyond the window. Below
// WindowMax it keeps what a connected follower still needs.
func (n *Node) trimWindow() {
	if n.winBytes <= WindowBytes || n.installing {
		return
	}
	limit := n.applied
	if n.isLeader && n.winBytes <= WindowMax {
		n.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
			if id != n.cfg.ID && pr.Match < limit {
				limit = pr.Match
			}
		})
	}
	var t uint64
	for len(n.win) > 0 && n.winBytes > WindowBytes && n.win[0].idx <= limit {
		t = n.win[0].idx
		n.winBytes -= n.win[0].size
		n.win = n.win[1:]
	}
	if t == 0 {
		return
	}
	if _, err := n.storage.CreateSnapshot(t, n.storage.conf, nil); err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
		n.fail(err)
		return
	}
	if err := n.storage.Compact(t); err != nil && !errors.Is(err, raft.ErrCompacted) {
		n.fail(err)
	}
	n.WindowTrims++
}

// CkptHeader is the start of a checkpoint.
type CkptHeader struct {
	Index, Term     uint64
	Rev, CompactRev int64
	Leases          []byte
}

// Encode is the header's bytes.
func (h *CkptHeader) Encode() []byte {
	b := binary.AppendUvarint(nil, h.Index)
	b = binary.AppendUvarint(b, h.Term)
	b = binary.AppendVarint(b, h.Rev)
	b = binary.AppendVarint(b, h.CompactRev)
	b = binary.AppendUvarint(b, uint64(len(h.Leases)))
	return append(b, h.Leases...)
}

// DecodeCkptHeader reads a header.
func DecodeCkptHeader(b []byte) (*CkptHeader, error) {
	h := &CkptHeader{}
	var n int
	bad := errors.New("core: bad checkpoint header")
	u := func() uint64 {
		v, k := binary.Uvarint(b)
		if k <= 0 {
			n = -1
			return 0
		}
		b = b[k:]
		return v
	}
	s := func() int64 {
		v, k := binary.Varint(b)
		if k <= 0 {
			n = -1
			return 0
		}
		b = b[k:]
		return v
	}
	h.Index, h.Term, h.Rev, h.CompactRev = u(), u(), s(), s()
	l := u()
	if n < 0 || uint64(len(b)) < l {
		return nil, bad
	}
	h.Leases = b[:l]
	return h, nil
}

// CkptChunk is a piece of a checkpoint for the writer: the header first,
// then key chunks, then Done.
type CkptChunk struct {
	Header *CkptHeader
	Data   []byte
	Done   bool
}

// CheckpointStep starts a checkpoint when the log has grown enough and
// copies the next chunk of one in progress. lastSize is the size of the
// last durable checkpoint.
func (n *Node) CheckpointStep(lastSize int64) []CkptChunk {
	if n.err != nil || n.installing {
		return nil
	}
	var out []CkptChunk
	if !n.ckptActive {
		if n.sinceCkpt < max(CkptMin, CkptK*lastSize) {
			return nil
		}
		term, err := n.storage.Term(n.applied)
		if err != nil {
			term = n.appliedTerm
		}
		rev, crev := n.kv.BeginCheckpoint()
		n.ckptActive, n.sinceCkpt = true, 0
		out = append(out, CkptChunk{Header: &CkptHeader{Index: n.applied, Term: term, Rev: rev, CompactRev: crev, Leases: n.state.encodeLeases()}})
	}
	data, done := n.kv.CheckpointChunk(make([]byte, 0, CkptBytes+4096), CkptBytes)
	out = append(out, CkptChunk{Data: data, Done: done})
	if done {
		n.ckptActive = false
	}
	return out
}

func (st *State) encodeLeases() []byte {
	b := binary.AppendVarint(nil, st.stamps)
	for _, l := range st.sortedLeases() {
		b = binary.AppendVarint(b, l.id)
		b = binary.AppendVarint(b, l.ttl)
		b = binary.AppendVarint(b, l.stamp)
	}
	return b
}

func (st *State) decodeLeases(b []byte) {
	v := func() int64 {
		x, k := binary.Varint(b)
		b = b[k:]
		return x
	}
	st.stamps = v()
	for len(b) > 0 {
		l := &lease{id: v(), keys: map[string]struct{}{}}
		l.ttl, l.stamp = v(), v()
		st.leases[l.id] = l
	}
}

// NewE makes a node that recovers from a checkpoint and a streamed log:
// LoadCheckpoint, then Replay for each committed entry, then Start.
func NewE(cfg Config) *Node {
	st := newStorage(cfg.Peers)
	state := NewState(MinLeaseTTL(cfg))
	return &Node{
		cfg:      cfg,
		storage:  st,
		state:    state,
		kv:       state.Store(),
		waiting:  map[uint64]*pending{},
		streams:  map[uint64]*watchStream{},
		recovery: true,
	}
}

// LoadCheckpointHeader starts loading a checkpoint.
func (n *Node) LoadCheckpointHeader(h *CkptHeader) {
	n.applied, n.appliedTerm = h.Index, h.Term
	n.kv.SetRevs(h.Rev, h.CompactRev)
	n.state.decodeLeases(h.Leases)
}

// LoadCheckpointChunk loads keys.
func (n *Node) LoadCheckpointChunk(b []byte) error { return n.kv.LoadCheckpointChunk(b) }

// FinishCheckpointLoad rebuilds the lease key sets.
func (n *Node) FinishCheckpointLoad() {
	n.kv.EachLease(func(key []byte, id int64) {
		if l, ok := n.state.leases[id]; ok {
			l.keys[string(key)] = struct{}{}
		}
	})
}

// Replay applies one committed entry during recovery.
func (n *Node) Replay(e *raftpb.Entry) error {
	if e.GetIndex() <= n.applied {
		return nil
	}
	return n.apply([]*raftpb.Entry{e})
}

// Start hands Raft the log above what recovery applied.
func (n *Node) Start(rec *wal.Recovered) error {
	n.recovery = false
	if n.applied > 0 {
		if err := n.storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(n.applied), Term: new(n.appliedTerm), ConfState: n.storage.conf}}); err != nil {
			return err
		}
	}
	if err := n.storage.Append(rec.Entries); err != nil {
		return err
	}
	n.trackWindow(rec.Entries)
	hs := rec.HardState
	hs.Commit = max(hs.Commit, n.applied)
	if err := n.storage.SetHardState(raftHardState(hs)); err != nil {
		return err
	}
	rn, err := raft.NewRawNode(raftConfig(n.cfg, n.storage))
	if err != nil {
		return err
	}
	n.rn = rn
	n.term, n.voteTerm, n.vote = hs.Term, hs.Term, hs.Vote
	return nil
}

// Applied entries from a snapshot fetch, on the committer.
func (n *Node) ApplyFetched(ents []*raftpb.Entry) {
	if n.err != nil {
		return
	}
	for _, e := range ents {
		if e.GetIndex() != n.applied+1 {
			continue
		}
		if err := n.apply([]*raftpb.Entry{e}); err != nil {
			n.fail(err)
			return
		}
	}
}

// Installing reports whether a snapshot install is running.
func (n *Node) Installing() bool { return n.installing }

// NewStateFromHeader starts loading a checkpoint into a new state, off
// the committer; LoadChunk and FinishLoad complete it.
func NewStateFromHeader(cfg Config, h *CkptHeader) *State {
	st := NewState(MinLeaseTTL(cfg))
	st.kv.SetRevs(h.Rev, h.CompactRev)
	st.decodeLeases(h.Leases)
	return st
}

// LoadChunk loads keys of a checkpoint.
func (st *State) LoadChunk(b []byte) error { return st.kv.LoadCheckpointChunk(b) }

// FinishLoad rebuilds the lease key sets.
func (st *State) FinishLoad() {
	st.kv.EachLease(func(key []byte, id int64) {
		if l, ok := st.leases[id]; ok {
			l.keys[string(key)] = struct{}{}
		}
	})
}

// InstallState swaps in a state built from a checkpoint at idx.
func (n *Node) InstallState(st *State, idx, term uint64) {
	n.state, n.kv = st, st.Store()
	n.applied, n.appliedTerm = idx, term
}

// snapshotProgress tells Raft, on the leader, that a follower finished
// installing a snapshot (its match reached it) or gave up (no progress
// for snapTicks), as etcd's leader does with ReportSnapshot. Without it a
// follower whose install ended below the leader's moved window stays
// paused.
func (n *Node) snapshotProgress() {
	if !n.isLeader {
		n.snapWait = nil
		return
	}
	if n.snapWait == nil {
		n.snapWait = map[uint64]int64{}
	}
	type rep struct {
		id     uint64
		status raft.SnapshotStatus
	}
	var reps []rep
	n.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		if pr.State != tracker.StateSnapshot {
			delete(n.snapWait, id)
			return
		}
		switch {
		case pr.Match >= pr.PendingSnapshot:
			reps = append(reps, rep{id, raft.SnapshotFinish})
			delete(n.snapWait, id)
		case n.snapWait[id] == 0:
			n.snapWait[id] = n.now
		case n.now-n.snapWait[id] > snapTicks:
			reps = append(reps, rep{id, raft.SnapshotFailure})
			delete(n.snapWait, id)
		}
	})
	for _, r := range reps {
		n.rn.ReportSnapshot(r.id, r.status)
	}
}

// snapTicks is how long a snapshot may take before the leader tries
// again: 20 s at 100 ms ticks. A follower that was down when its
// snapshot went out gets a new one this long after it is back.
const snapTicks = 200
