package core

// A+ prototype (#133): the committer's side. It names each applied value
// by its place in the mapped log, keeps a bounded window of Raft entries,
// captures the keys changed since the last checkpoint, picks files to clean and swaps the
// references the cleaner moved. All I/O is in the driver (internal/node).

import (
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/aplus"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// CkptJob asks the driver to write a captured checkpoint.
type CkptJob struct {
	Capture   *mvcc.Capture
	Index     uint64
	Term      uint64
	Vote      uint64
	HSTerm    uint64
	ReplaySeq uint64
	// Refs are the files with live values at the capture.
	Refs map[uint64]bool
}

// CkptResult is a written checkpoint.
type CkptResult struct {
	Job *CkptJob
	// LeafBytes is the size of all leaf pages of the checkpoint.
	LeafBytes int64
}

// CleanJob asks the driver to copy live values out of a file.
type CleanJob struct {
	Fid uint64
}

// CleanResult is part of a finished cleaning: the moved values, and Last
// on the final part.
type CleanResult struct {
	Fid, NewFid uint64
	Relocs      []mvcc.Reloc
	News        [][]byte
	Last        bool
}

// Ckpt is a durable checkpoint as the committer knows it.
type Ckpt struct {
	Index, Term uint64
	ReplaySeq   uint64
	Refs        map[uint64]bool
}

type mark struct {
	index uint64
	bytes int64
}

type aplusNode struct {
	locs         map[uint64]wal.Loc
	lastSeg      uint64
	appliedBytes int64
	marks        []mark
	compactedTo  uint64
	ckptBusy     bool
	pin          uint64
	pinUntil     int64
	forceCkpt    bool
	ckptAt       int64
	durable      *Ckpt
	leafBytes    int64
	cleanBusy    bool
	snapBusy     bool
	snap         *raftpb.Snapshot
	wantSnap     bool
	sizes        map[uint64]int64
	// known are the value files whose references the index holds.
	known map[uint64]bool
}

// AplusEffects are the prototype's effects, read by the driver after
// Process.
type AplusEffects struct {
	Ckpt      *CkptJob
	Clean     *CleanJob
	Delete    []uint64
	BuildSnap *Ckpt
}

// TakeAplus returns and clears the prototype's effects.
func (n *Node) TakeAplus() AplusEffects {
	e := n.apOut
	n.apOut = AplusEffects{}
	return e
}

// AddLocs records where persisted entries are on disk.
func (n *Node) AddLocs(locs []wal.Loc) {
	for _, l := range locs {
		n.ap.locs[l.Index] = l
	}
}

// applyAt sets up the store to name the values of e by their place in the
// log, and returns a function that ends it.
func (n *Node) applyAt(e *raftpb.Entry) {
	l, ok := n.ap.locs[e.GetIndex()]
	if !ok {
		panic("aplus: no location for applied entry")
	}
	delete(n.ap.locs, e.GetIndex())
	m := aplus.Get(l.Seg)
	n.kv.SetEntry(e.GetData(), m[l.Off:l.Off+int64(len(e.GetData()))], l.Seg)
	n.ap.lastSeg = l.Seg
	n.ap.appliedBytes += int64(len(e.GetData()))
	if len(n.ap.marks) == 0 || n.ap.appliedBytes-n.ap.marks[len(n.ap.marks)-1].bytes >= 1<<20 {
		n.ap.marks = append(n.ap.marks, mark{e.GetIndex(), n.ap.appliedBytes})
	}
}

// afterApply trims the Raft window and cuts a checkpoint when due.
func (n *Node) afterApply() {
	n.kv.ClearEntry()
	if n.storage == nil {
		// Replaying before the node starts.
		return
	}
	// Window: keep Window bytes of applied entries. A restart replays
	// from the log on disk, so the window does not depend on the
	// checkpoint. While a snapshot is out, keep the entries after it, so
	// the follower can continue from it.
	{
		target := uint64(0)
		cut := 0
		for i, m := range n.ap.marks {
			if n.ap.appliedBytes-m.bytes < aplus.Window || (n.now < n.ap.pinUntil && m.index > n.ap.pin) {
				break
			}
			target, cut = m.index, i
		}
		if target > n.ap.compactedTo+0 && target > n.ap.compactedTo {
			first, _ := n.storage.FirstIndex()
			if target >= first {
				if err := n.storage.Compact(target); err == nil {
					n.ap.compactedTo = target
				}
			}
			n.ap.marks = n.ap.marks[cut:]
		}
	}
	if n.ap.snap != nil && n.now >= n.ap.pinUntil {
		// Drop the built snapshot once its pin ends (crude: it is one
		// blob in RAM).
		n.ap.snap = nil
	}
	every := max(aplus.CkptEvery, int64(aplus.CkptRatio*float64(n.ap.leafBytes)))
	if !n.ap.ckptBusy && (n.ap.forceCkpt || n.logBytes-n.ap.ckptAt >= every) && n.applied > 0 {
		n.ap.forceCkpt = false
		n.cutCheckpoint()
	}
}

func (n *Node) cutCheckpoint() {
	term, err := n.storage.Term(n.applied)
	if err != nil {
		return
	}
	refs := map[uint64]bool{}
	for f, b := range n.kv.LiveBytes() {
		if b > 0 {
			refs[f] = true
		}
	}
	c := n.kv.Capture()
	n.ap.ckptBusy = true
	n.ap.ckptAt = n.logBytes
	n.apOut.Ckpt = &CkptJob{Capture: c, Index: n.applied, Term: term, Vote: n.vote, HSTerm: n.voteTerm, ReplaySeq: n.ap.lastSeg, Refs: refs}
}

// CkptDone reports a durable checkpoint.
func (n *Node) CkptDone(r CkptResult) {
	n.ap.ckptBusy = false
	n.ap.leafBytes = r.LeafBytes
	n.ap.durable = &Ckpt{Index: r.Job.Index, Term: r.Job.Term, ReplaySeq: r.Job.ReplaySeq, Refs: r.Job.Refs}
	aplus.Checkpoints.Add(1)
	if n.ap.forceCkpt {
		n.ap.forceCkpt = false
		n.cutCheckpoint()
	}
	n.maintain()
}

// SetDurable sets the checkpoint a restart loaded.
func (n *Node) SetDurable(c *Ckpt) {
	n.ap.durable = c
	if c == nil {
		n.ap.durable = nil
	}
	for f := range n.kv.LiveBytes() {
		if f >= aplus.ValueFileBase {
			n.knowValueFile(f)
		}
	}
}

// FileSizes reports the sizes of the log and value files.
func (n *Node) FileSizes(sizes map[uint64]int64) {
	n.ap.sizes = sizes
	n.maintain()
}

// maintain deletes files nothing needs and starts cleaning the emptiest
// sealed file.
func (n *Node) maintain() {
	d := n.ap.durable
	if d == nil || n.ap.sizes == nil {
		return
	}
	live := n.kv.LiveBytes()
	sealed := func(f uint64) bool {
		if f >= aplus.MetaFileBase {
			return false
		}
		if f >= aplus.ValueFileBase {
			return n.ap.known[f]
		}
		return f < d.ReplaySeq
	}
	var best uint64
	bestU := 2.0
	var total, liveTotal int64
	for f, size := range n.ap.sizes {
		if live[f] <= 0 && f < aplus.MetaFileBase {
			// Dead; it goes at the next checkpoint without copying.
			continue
		}
		total += size
		liveTotal += max(live[f], 0)
	}
	// Clean only while the files take more than SpaceAmp times the live
	// values: the longer a file waits, the less of it is still live.
	needSpace := float64(total) > aplus.SpaceAmp*float64(liveTotal)
	for f, size := range n.ap.sizes {
		if !sealed(f) || size == 0 {
			continue
		}
		l := live[f]
		if l <= 0 && !d.Refs[f] {
			n.apOut.Delete = append(n.apOut.Delete, f)
			delete(n.ap.sizes, f)
			delete(live, f)
			continue
		}
		if u := float64(l) / float64(size); l > 0 && u <= aplus.CleanU && u < bestU {
			best, bestU = f, u
		}
	}
	if best != 0 && needSpace && !n.ap.cleanBusy {
		n.ap.cleanBusy = true
		n.apOut.Clean = &CleanJob{Fid: best}
	}
}

// CleanDone swaps in the values the cleaner copied.
func (n *Node) CleanDone(r *CleanResult) {
	if r.NewFid != 0 {
		n.knowValueFile(r.NewFid)
	}
	start := time.Now()
	n.kv.Swap(r.Relocs, r.News)
	aplus.MaxPause(&aplus.PauseCleanNs, &aplus.PauseCleanMax, time.Since(start))
	if r.Last {
		n.ap.cleanBusy = false
		aplus.Cleanings.Add(1)
	}
}

func (n *Node) knowValueFile(f uint64) {
	if n.ap.known == nil {
		n.ap.known = map[uint64]bool{}
	}
	n.ap.known[f] = true
}

// SnapBuilt hands over a snapshot the driver built from checkpoint c.
func (n *Node) SnapBuilt(s *raftpb.Snapshot) {
	n.ap.snapBusy = false
	n.ap.wantSnap = false
	n.ap.snap = s
	// Keep the entries after it for 30 s (300 ticks), so a follower
	// can catch up from it.
	n.ap.pin, n.ap.pinUntil = s.GetMetadata().GetIndex(), n.now+300
}

// snapshotFor is the storage's Snapshot: the built snapshot if any, else
// ask the driver to build one.
func (n *Node) snapshotFor() (*raftpb.Snapshot, error) {
	if s := n.ap.snap; s != nil {
		first, _ := n.storage.MemoryStorage.FirstIndex()
		if s.GetMetadata().GetIndex()+1 >= first {
			return s, nil
		}
	}
	n.ap.wantSnap = true
	return nil, raft.ErrSnapshotTemporarilyUnavailable
}

func (n *Node) maybeBuildSnap() {
	if !n.ap.wantSnap || n.ap.snapBusy {
		return
	}
	first, _ := n.storage.MemoryStorage.FirstIndex()
	if n.ap.durable == nil || n.ap.durable.Index+1 < first {
		// The newest checkpoint is older than the window: cut one now
		// and build the snapshot from it.
		if !n.ap.ckptBusy {
			n.cutCheckpoint()
		} else {
			n.ap.forceCkpt = true
		}
		return
	}
	n.ap.wantSnap = false
	n.ap.snapBusy = true
	n.ap.pin, n.ap.pinUntil = n.ap.durable.Index, n.now+300
	n.apOut.BuildSnap = n.ap.durable
}

// InstallSnapshot swaps in the store a snapshot built, at index i.
func (n *Node) installSnapshot(snap *raftpb.Snapshot, kv *mvcc.Store) {
	meta := snap.GetMetadata()
	if err := n.storage.MemoryStorage.ApplySnapshot(&raftpb.Snapshot{Metadata: meta}); err != nil {
		n.fail(err)
		return
	}
	for f := range kv.LiveBytes() {
		if f >= aplus.ValueFileBase {
			n.knowValueFile(f)
		}
	}
	n.state = newStateFrom(kv, n.state.minTTL)
	n.kv = kv
	n.applied = meta.GetIndex()
	n.ap.marks = nil
	n.ap.compactedTo = meta.GetIndex()
	for i := range n.ap.locs {
		if i <= meta.GetIndex() {
			delete(n.ap.locs, i)
		}
	}
}

// Replayer applies committed log entries to a store rebuilt from a
// checkpoint before the node starts, so a restart holds one segment of
// entries at a time instead of the whole log since the checkpoint.
type Replayer struct {
	n           *Node
	Index, Term uint64
}

// NewReplayer applies to kv, which is the state after entry index.
func NewReplayer(cfg Config, kv *mvcc.Store, index uint64) *Replayer {
	n := &Node{cfg: cfg, kv: kv, state: newStateFrom(kv, MinLeaseTTL(cfg)), waiting: map[uint64]*pending{}, streams: map[uint64]*watchStream{}}
	n.ap.locs = map[uint64]wal.Loc{}
	n.applied = index
	return &Replayer{n: n, Index: index}
}

// Segment applies the committed entries at the front of rec.Entries and
// removes them.
func (r *Replayer) Segment(rec *wal.Recovered) {
	i := 0
	for i < len(rec.Entries) && rec.Entries[i].GetIndex() <= rec.HardState.Commit {
		i++
	}
	var ents []*raftpb.Entry
	for _, e := range rec.Entries[:i] {
		if e.GetIndex() <= r.Index {
			continue
		}
		ents = append(ents, e)
		r.n.ap.locs[e.GetIndex()] = rec.Locs[e.GetIndex()]
	}
	if len(ents) > 0 {
		if err := r.n.apply(ents); err != nil {
			panic(err)
		}
		last := ents[len(ents)-1]
		r.Index, r.Term = last.GetIndex(), last.GetTerm()
	}
	for _, e := range rec.Entries[:i] {
		delete(rec.Locs, e.GetIndex())
	}
	rec.Entries = rec.Entries[i:]
}
