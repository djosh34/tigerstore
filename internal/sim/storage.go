package sim

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The simulator runs design E's steps with tiny limits, so that every
// seed cuts checkpoints, removes segments, reads values from disk and
// sends snapshots: a window of a few entries, a checkpoint every few
// kilobytes of log, and 8 KiB segments.
func tinyLimits(cfg *core.Config) {
	cfg.WindowBytes = 16
	cfg.WindowMaxBytes = 32
	cfg.UnappliedBytes = 16 << 10
	cfg.CheckpointBytes = 256
	cfg.CheckpointRatio = 2
	cfg.CheckpointChunk = 32
	cfg.SnapshotTicks = 10
	cfg.Record = true
}

// logRec is one committed entry as the first node to reach it applied it,
// and the hash of the state after it. The simulator keeps every one, apart
// from what the nodes retain, for its checks.
type logRec struct {
	term uint64
	typ  raftpb.EntryType
	data []byte
	// known is false when only the hash is known: the state came from a
	// checkpoint.
	known bool
	hash  uint64
	node  uint64
}

// record checks an entry a node applied, or the state it loaded from a
// checkpoint, against what the other nodes have at that index, and keeps
// it.
func (s *Sim) record(n *node, a core.Applied) {
	log := s.log
	if a.Index <= s.baseUpTo {
		// A cold restart: up to the index applied at the checkpoint, the
		// run's own record is the truth.
		log = s.base
	}
	r, ok := log[a.Index]
	if !ok {
		log[a.Index] = logRec{term: a.Term, typ: a.Type, data: a.Data, known: !a.Checkpoint, hash: a.Hash, node: n.id}
		s.maxLogged = max(s.maxLogged, a.Index)
		return
	}
	switch {
	case r.term != a.Term || (r.known && !a.Checkpoint && !bytes.Equal(r.data, a.Data)):
		s.fail("node %d and node %d applied different entries at index %d", n.id, r.node, a.Index)
		return
	case !r.known && !a.Checkpoint:
		r.typ, r.data, r.known = a.Type, a.Data, true
		log[a.Index] = r
	}
	if r.hash != a.Hash {
		s.fail("node %d and node %d applied the same log to different states at index %d", n.id, r.node, a.Index)
	}
}

// committed returns the committed entries up to index last, from the
// simulator's own record.
func (s *Sim) committed(last uint64) ([]*raftpb.Entry, error) {
	out := make([]*raftpb.Entry, 0, last)
	for i := uint64(1); i <= last; i++ {
		r, ok := s.log[i]
		if !ok || !r.known {
			return nil, fmt.Errorf("the record of committed entries has no entry %d", i)
		}
		out = append(out, &raftpb.Entry{Term: new(r.term), Index: new(i), Type: r.typ.Enum(), Data: r.data})
	}
	return out, nil
}

// writeCheckpoint writes a node's checkpoint parts to its disk. The last
// part is synced, with the directory, after a disk latency.
func (s *Sim) writeCheckpoint(n *node, chunks []core.CheckpointChunk) {
	for _, c := range chunks {
		if n.ckptFile == nil || n.ckptIndex != c.Index {
			f, err := n.log.CreateCheckpoint(c.Index)
			if err != nil {
				s.diskFailed(n, err)
				return
			}
			n.ckptFile, n.ckptIndex = f, c.Index
		}
		if _, err := n.ckptFile.Write(c.Data); err != nil {
			s.diskFailed(n, err)
			return
		}
		if s.stepCrash(n) {
			return
		}
		if c.Done {
			// One writer, as in the node: checkpoints sync in order.
			n.ckptSyncAt = max(s.now+50*Microsecond+s.rng.Int64N(950*Microsecond), n.ckptSyncAt+1)
			s.push(&event{at: n.ckptSyncAt, kind: evCheckpointSync, node: n.idx, gen: n.gen, stream: c.Index, ckpt: n.ckptFile})
			n.ckptFile = nil
		}
	}
}

// checkpointSynced syncs a written checkpoint and its directory, and
// tells the core.
func (s *Sim) checkpointSynced(e *event) {
	n := s.nodes[e.node]
	if !n.up || n.gen != e.gen {
		return
	}
	f, index := e.ckpt, e.stream
	if err := f.Sync(); err != nil {
		s.diskFailed(n, err)
		return
	}
	if s.stepCrash(n) {
		return
	}
	if err := errors.Join(f.Close(), n.log.SyncDir()); err != nil {
		s.diskFailed(n, err)
		return
	}
	s.ckptWritten(n, index)
	if s.stepCrash(n) {
		return
	}
	if n.installing {
		s.stats.CheckpointsDuringInstall++
	}
	n.core.CheckpointWritten(index, nil)
	s.process(n)
}

// ckptWritten checks that every node writes the same bytes for the
// checkpoint at an index.
func (s *Sim) ckptWritten(n *node, index uint64) {
	b := n.disk.DurableBytes(wal.CheckpointName(index))
	sum := hashBytes(b)
	if c, ok := s.ckpts[index]; ok && c.sum != sum {
		s.fail("node %d and node %d wrote different checkpoints at index %d", n.id, c.node, index)
		return
	}
	s.ckpts[index] = ckptRec{sum: sum, node: n.id}
	s.stats.Checkpoints++
}

type ckptRec struct {
	sum  uint64
	node uint64
}

func hashBytes(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

// removed carries out a synced root's removals. The first root a run
// syncs and its first removal of a segment each take an erase-one-disk
// copy at once, so that the cold restarts check E's steps (#163).
func (s *Sim) removed(n *node, j core.PersistJob) bool {
	n.root, n.roots = j.Root.Index, append(n.roots, *j.Root)
	if s.cfg.Faults.Checkpoint && s.stats.ErasedAtRoot == 0 {
		s.stats.ErasedAtRoot++
		s.takeCheckpoint()
	}
	if j.RemoveBelow == 0 {
		return true
	}
	oldest := n.log.Oldest()
	if s.stepCrash(n) {
		return false
	}
	if err := n.log.RemoveBelow(j.RemoveBelow); err != nil {
		s.diskFailed(n, err)
		return false
	}
	if s.stepCrash(n) {
		return false
	}
	if err := n.log.RemoveCheckpoints(j.Root.Index); err != nil {
		s.diskFailed(n, err)
		return false
	}
	n.roots = []wal.Root{*j.Root}
	if s.cfg.Faults.Checkpoint && s.stats.ErasedAtRemoval == 0 && n.log.Oldest() > oldest {
		s.stats.ErasedAtRemoval++
		s.takeCheckpoint()
	}
	return true
}

// firstReplaced is the first entry an install may take from n's log: the
// first after its applied index that is not the committed one. Those
// before it the node may have acknowledged, and the install keeps them.
func (s *Sim) firstReplaced(n *node, in *core.Install) uint64 {
	for i := in.Applied + 1; i <= in.Index; i++ {
		r, ok := s.log[i]
		if i <= s.baseUpTo {
			r, ok = s.base[i]
		}
		if !ok || !n.log.Has(i) {
			return i
		}
		e, err := n.log.Entry(i)
		if err != nil {
			// The install fails at an entry the node holds but cannot
			// read, and replaces nothing (core.ReceiveSnapshot).
			return in.Index + 1
		}
		if e.GetTerm() != r.term {
			return i
		}
	}
	return in.Index + 1
}

var errFetch = errors.New("sim: the snapshot's source cannot be reached")

// install fetches a snapshot for n from the node that sent it, as a
// separate connection, and writes it to n's log. The source of the first
// stream of a run crashes partway through it, and the first stream that
// arrives whole, and one in four after it, arrives damaged.
func (s *Sim) install(n *node, in *core.Install) error {
	i := slices.IndexFunc(s.nodes, func(n *node) bool { return n.id == in.From })
	if i < 0 {
		return errFetch
	}
	src := s.nodes[i]
	if !src.up || src.pausedUntil > s.now || !s.links[src.idx][n.idx].up || !s.links[n.idx][src.idx].up {
		return errFetch
	}
	src.core.SnapshotStarted(n.id)
	var buf bytes.Buffer
	err := core.ServeSnapshot(src.log, src.roots, in.Applied+1, in.Index, &buf)
	s.push(&event{at: s.now + s.clientLatency(), kind: evSnapshotSent, node: src.idx, gen: src.gen, from: n.idx, err: err})
	if err != nil {
		s.stats.ServeFailures++
		return err
	}
	data := buf.Bytes()
	s.stats.Snapshots++
	newest := src.root
	if s.cfg.Faults.FetchCrash && !s.healed && s.stats.FetchCrashes == 0 {
		s.stats.FetchCrashes++
		data = data[:s.rng.IntN(len(data))]
		s.stopNode(src)
	} else if s.cfg.Faults.FlipSnapshot && !s.healed && len(data) > 0 && (s.stats.SnapshotFlips == 0 || s.rng.IntN(4) == 0) {
		data[s.rng.IntN(len(data))] ^= byte(1 + s.rng.IntN(255))
		s.stats.SnapshotFlips++
	}
	s.mix(0x5a, uint64(n.idx), in.Index, uint64(len(data)))
	sink := &core.LogSink{WAL: n.log, Step: s.snapshotStep(n)}
	err = errors.Join(core.ReceiveSnapshot(bytes.NewReader(data), sink, n.log, in), sink.Close())
	if err == nil && in.Checkpoint.Index > 0 {
		s.stats.CheckpointSnapshots++
		if in.Checkpoint.Index < newest {
			s.stats.OlderCheckpoints++
		}
	}
	return err
}

// errStepCrash ends an install that a step crash stopped.
var errStepCrash = errors.New("sim: crashed during the install")

// snapshotStep is the Step of a node's snapshot sink: it writes what the
// log buffered after each item, so that a crash keeps part of it, and may
// crash there.
func (s *Sim) snapshotStep(n *node) func(string, int) error {
	return func(string, int) error {
		if err := n.log.Flush(); err != nil {
			return fmt.Errorf("%w: %w", core.ErrDisk, err)
		}
		if s.stepCrash(n) {
			return errStepCrash
		}
		return nil
	}
}

// stepCrash crashes n, once the run armed it, at one of the steps of a
// checkpoint, a removal or an install, and reports whether it did.
func (s *Sim) stepCrash(n *node) bool {
	if !s.stepCrashDue || s.healed || s.rng.IntN(4) != 0 {
		return false
	}
	s.stepCrashDue = false
	s.stats.StepCrashes++
	s.mix(0x5c, uint64(n.idx))
	s.stopNode(n)
	return true
}

// installFailed handles a snapshot that could not be fetched or did not
// check out, as the real node does: it syncs what the stream wrote and
// at once starts a new core from its disk, and Raft sends another
// snapshot.
func (s *Sim) installFailed(n *node, err error) {
	switch {
	case errors.Is(err, errStepCrash):
		return
	case n.rotted && rotStop(n, err):
		// It read the entry the run damaged, which it held, and stopped
		// instead of replacing it.
		s.stopNode(n)
		return
	case errors.Is(err, core.ErrDisk):
		s.diskFailed(n, err)
		return
	case errors.Is(err, simfs.ErrIO) || errors.Is(err, simfs.ErrNoSpace):
		s.fail("node %d: a failed write of its own disk is not core.ErrDisk: %w", n.id, err)
		return
	}
	s.stats.InstallFailures++
	s.mix(0x5f, uint64(n.idx), uint64(len(err.Error())))
	if err = core.FetchFailed(err, n.log.Sync); core.FailureOf(err) != core.Reload {
		s.diskFailed(n, err)
		return
	}
	s.down(n)
	if serr := s.start(n); serr != nil {
		s.fail("%w", errors.Join(err, serr))
	}
}

// newestSegment is the name of the newest WAL segment on a disk.
func newestSegment(d *simfs.Disk) string {
	var newest string
	for _, name := range d.DurableNames() {
		if strings.HasSuffix(name, ".wal") {
			newest = name
		}
	}
	return newest
}

// rot flips a bit of the newest value of a key on a node's disk while it
// runs, a value that is only on disk, once per run. The node must never
// return it: it stops when it reads it, and refuses its log on the next
// start.
func (s *Sim) rot() bool {
	if s.healed {
		return false
	}
	for _, m := range s.nodes {
		if m.corrupted {
			return true
		}
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	for range numKeys {
		refs := n.core.Store().Refs(key(s.rng.IntN(numKeys)))
		if len(refs) == 0 {
			continue
		}
		// Clients read the newest value most, unless the key was deleted
		// since; the restart check catches the damage either way.
		r := refs[len(refs)-1]
		if r.Index >= n.core.FirstIndex() {
			continue
		}
		seq, at, ok := n.log.ValuePlace(r.Index, r.Off, r.Len)
		if !ok {
			continue
		}
		name := wal.SegmentName(seq)
		if name == newestSegment(n.disk) {
			// Damage to the newest segment's last durable record looks
			// like a torn write; a later entry must be durable after it.
			next, end, ok := n.log.ValuePlace(r.Index+2, 0, 0)
			if !ok || next != seq || end >= int64(len(n.disk.DurableBytes(name))) {
				continue
			}
		}
		n.disk.Rot(name, int(at)+s.rng.IntN(int(r.Len)))
		n.corrupted, n.rotted, n.rotFile, n.rotEntry, n.rotSeg = true, true, name, r.Index, seq
		s.stats.Rots++
		s.mix(0x60, uint64(n.idx), r.Index)
		return true
	}
	return false
}
