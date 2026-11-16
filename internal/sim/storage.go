package sim

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	tnode "github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

// The simulator runs design E's steps with tiny limits, so that every
// seed cuts checkpoints, removes segments, reads values from disk and
// sends snapshots: a window of a few entries, a checkpoint every few
// kilobytes of log, and 8 KiB segments. The window counts what an entry
// holds besides its data too: two small entries, and the leader keeps four
// for a follower behind. A window that holds no entry sends every follower
// that is one entry behind a snapshot, and one that installs snapshots on a
// slow disk never catches up.
func tinyLimits(cfg *core.Config) {
	cfg.WindowBytes = 2 * (16 + core.EntryOverhead)
	cfg.WindowMaxBytes = 4 * (16 + core.EntryOverhead)
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
	// data is the entry's data in its entryLog.
	data span
	// known is false when only the hash is known: the state came from a
	// checkpoint.
	known bool
	hash  uint64
	node  uint64
}

// entryLog is the record of committed entries, by index, in memory that
// holds no pointers, as a run applies one entry after the other on every
// node and keeps them all.
type entryLog struct {
	recs []logRec
	// set marks the indexes that have a record.
	set  []bool
	data arena
}

// get returns the record of index i.
func (l *entryLog) get(i uint64) (logRec, bool) {
	if i >= uint64(len(l.recs)) || !l.set[i] {
		return logRec{}, false
	}
	return l.recs[i], true
}

// put keeps r at index i.
func (l *entryLog) put(i uint64, r logRec) {
	if i >= uint64(len(l.recs)) {
		n := max(i+1, 2*uint64(len(l.recs)))
		l.recs = append(l.recs, make([]logRec, n-uint64(len(l.recs)))...)
		l.set = append(l.set, make([]bool, n-uint64(len(l.set)))...)
	}
	l.recs[i], l.set[i] = r, true
}

// bytes returns the data of r, which the log keeps.
func (l *entryLog) bytes(r logRec) []byte { return l.data.bytes(r.data) }

// record checks an entry a node applied, or the state it loaded from a
// checkpoint, against what the other nodes have at that index, and keeps
// it. The lock is held.
func (s *Sim) record(n *node, a core.Applied) {
	log := s.log
	if a.Index <= s.baseUpTo {
		// A cold restart: up to the index applied at the checkpoint, the
		// run's own record is the truth.
		log = s.base
	}
	r, ok := log.get(a.Index)
	if !ok {
		log.put(a.Index, logRec{term: a.Term, typ: a.Type, data: log.data.add(a.Data), known: !a.Checkpoint, hash: a.Hash, node: n.id})
		s.maxLogged = max(s.maxLogged, a.Index)
		return
	}
	switch {
	case r.term != a.Term || (r.known && !a.Checkpoint && !bytes.Equal(log.bytes(r), a.Data)):
		s.fail("node %d and node %d applied different entries at index %d", n.id, r.node, a.Index)
		return
	case !r.known && !a.Checkpoint:
		r.typ, r.data, r.known = a.Type, log.data.add(a.Data), true
		log.put(a.Index, r)
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
		r, ok := s.log.get(i)
		if !ok || !r.known {
			return nil, fmt.Errorf("the record of committed entries has no entry %d", i)
		}
		out = append(out, &raftpb.Entry{Term: new(r.term), Index: new(i), Type: r.typ.Enum(), Data: s.log.bytes(r)})
	}
	return out, nil
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

// grayDrops reports whether node n drops message m from a peer: an
// append or a snapshot to or from the gray node. It counts what it
// drops and what it lets through. The lock is held.
func (s *Sim) grayDrops(n *node, m *raftpb.Message) bool {
	g := s.gray
	drop := g != nil && (g == n || g.id == m.GetFrom()) && (m.GetType() == raftpb.MessageType_MsgApp || m.GetType() == raftpb.MessageType_MsgSnap)
	if !drop {
		s.stats.Messages++
		return false
	}
	s.stats.Dropped++
	if !s.grayDropped {
		s.grayDropped = true
		s.stats.GrayNodes++
	}
	if s.healed {
		// The guaranteed gray period, which waited for this.
		s.gray, s.grayOnce = nil, false
	}
	return true
}

// deliverer is the Deliver of p's transport: it hands the node the peer
// frames that arrive, except the appends to and from a gray node, which
// it drops.
func (s *Sim) deliverer(p *proc, tn *tnode.Node) func(*raftpb.Message) {
	return func(m *raftpb.Message) {
		s.mu.Lock()
		drop := s.grayDrops(p.n, m)
		switch t := m.GetType(); {
		case !s.votes || drop:
		case t == raftpb.MessageType_MsgVote, t == raftpb.MessageType_MsgPreVote, t == raftpb.MessageType_MsgVoteResp, t == raftpb.MessageType_MsgPreVoteResp:
			s.tracef("node %d: %s from %d at term %d, last entry %d at term %d, reject %v", p.n.id, t, m.GetFrom(), m.GetTerm(), m.GetIndex(), m.GetLogTerm(), m.GetReject())
		}
		s.mu.Unlock()
		if !drop {
			tn.Deliver(m)
		}
	}
}

// server is the Serve of p's transport: the node serves the snapshot
// stream a follower fetches. Half the streams during the faults start late
// (SlowFetch). The first stream of a run, and one in four after it, is
// damaged on its way (FlipSnapshot), and the source of the first one
// crashes partway through it instead (FetchCrash); such a stream is
// served whole first, then sent as the fault leaves it.
func (s *Sim) server(p *proc, tn *tnode.Node) func(id, first, last uint64, w io.Writer) error {
	return func(id, first, last uint64, w io.Writer) error {
		n := p.n
		s.mu.Lock()
		var wait int64
		if s.cfg.Faults.SlowFetch && !s.healed && s.rng.IntN(2) == 0 {
			s.stats.SlowFetches++
			wait = 200*Millisecond + s.rng.Int64N(1800*Millisecond)
		}
		crash := s.cfg.Faults.FetchCrash && !s.healed && s.stats.FetchCrashes == 0
		flip := !crash && s.cfg.Faults.FlipSnapshot && !s.healed && (s.stats.SnapshotFlips == 0 || s.rng.IntN(4) == 0)
		s.mu.Unlock()
		t := time.NewTimer(time.Duration(wait))
		select {
		case <-t.C:
		case <-p.ctx.Done():
			t.Stop()
			return errDead
		}
		var buf bytes.Buffer
		out := w
		if crash || flip {
			out = &buf
		}
		err := tn.Serve(id, first, last, out)
		s.mu.Lock()
		if err != nil {
			s.stats.ServeFailures++
			if n.corrupted && core.FailureOf(err) == core.Fence {
				n.servedDamage = true
			}
			s.mu.Unlock()
			return err
		}
		s.stats.Snapshots++
		s.servedRoot[id] = n.root
		data := buf.Bytes()
		switch {
		case !crash && !flip:
			s.mu.Unlock()
			return nil
		case len(data) == 0:
		case crash:
			// A prefix of at least a byte.
			k := 1
			if len(data) > 1 {
				k += s.rng.IntN(len(data) - 1)
			}
			data = data[:k]
		default:
			data[s.rng.IntN(len(data))] ^= byte(1 + s.rng.IntN(255))
			s.stats.SnapshotFlips++
		}
		s.tracef("node %d: serving %d bytes of a snapshot to %d, crash %v, flip %v", n.id, len(data), id, crash, flip)
		s.mu.Unlock()
		_, err = w.Write(data)
		if crash {
			s.crashSource(p, id, tn.Done())
			return errDead
		}
		return err
	}
}

// crashSource crashes p's node, the source of a snapshot stream to node
// id that the run cut after a prefix (FetchCrash), once the follower read
// the prefix, or its connection broke. Only a prefix the follower read
// counts as a fault that took effect: every connection to it that held
// unread bytes right after the write, the stream's among them, was read
// to the end without breaking, and the link was not half-open, which
// swallows the prefix while older frames on it still arrive. done ends
// the wait when the process ends.
func (s *Sim) crashSource(p *proc, id uint64, done <-chan struct{}) {
	n := p.n
	s.mu.Lock()
	swallowed := false
	for _, m := range s.nodes {
		if m.id == id {
			swallowed = s.links[n.idx][m.idx].halfOpenUntil > s.now()
		}
	}
	s.mu.Unlock()
	pipes := s.net.queued(s, n.idx, id)
	read := len(pipes) > 0 && !swallowed
	for len(pipes) > 0 {
		s.mu.Lock()
		pipes = slices.DeleteFunc(pipes, func(q *pipe) bool {
			read = read && q.err == nil
			return q.err != nil || len(q.chunks) == 0
		})
		s.mu.Unlock()
		if len(pipes) > 0 && !s.sleepUnless(10*Millisecond, done) {
			return
		}
	}
	s.mu.Lock()
	if read {
		s.stats.FetchCrashes++
	}
	if n.proc == p {
		n.why = "the run crashed it while it served a snapshot"
		s.stopNode(n)
	}
	s.mu.Unlock()
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
// start. It runs on the node's committer, with its core c. The lock is
// held.
func (s *Sim) rot(n *node, c *core.Node) {
	if slices.ContainsFunc(s.nodes, (*node).mayBeDamaged) {
		// A crash damaged a node since the rot was due, or may have; a
		// quorum must stay whole.
		return
	}
	w, _, err := wal.Open(n.disk.Durable())
	if err != nil {
		return
	}
	defer func() {
		if err := w.Close(); err != nil {
			s.fail("%w", err)
		}
	}()
	live, err := n.disk.List()
	if err != nil {
		return
	}
	for range numKeys {
		refs := c.Store().Refs(key(s.rng.IntN(numKeys)))
		if len(refs) == 0 {
			continue
		}
		// Clients read the newest value most, unless the key was deleted
		// since; the restart check catches the damage either way.
		r := refs[len(refs)-1]
		if r.Index >= c.FirstIndex() {
			continue
		}
		seq, at, ok := w.ValuePlace(r.Index, r.Off, r.Len)
		if !ok {
			continue
		}
		name := wal.SegmentName(seq)
		if !slices.Contains(live, name) {
			continue
		}
		if name == newestSegment(n.disk) && !witnessed(n, w, name, seq, r.Index) {
			continue
		}
		n.disk.Rot(name, int(at)+s.rng.IntN(int(r.Len)))
		n.corrupted, n.rotted, n.rotFile, n.rotEntry, n.rotSeg = true, true, name, r.Index, seq
		s.stats.Rots++
		s.tracef("node %d: rotted a value of entry %d in %s", n.id, r.Index, name)
		return
	}
}

// rotWL is rot on a wl node (Config.wl), which its loop's turn runs with
// values, the values its store names in the log's durable part. It flips
// a bit of one where the node's start would find it: before the place
// its checkpoint left the log, where the start checks each value its
// store names; or after it, where damageWL damages the log, in a segment
// with data two segments on (wlSealed), whose records the start checks.
// The node must never serve it: a read of it stops the node, as does a
// read of its entry's record (rotStop). The lock is held.
func (s *Sim) rotWL(n *node, values func() []wl.LogValue) {
	if slices.ContainsFunc(s.nodes, (*node).mayBeDamaged) {
		// As rot: a quorum must stay whole.
		return
	}
	ptr, err := wlPointer(n.disk)
	if err != nil {
		return
	}
	sealed := wlSealed(n.disk, ptr.FirstSegment)
	vs := values()
	for range 4 * numKeys {
		if len(vs) == 0 {
			return
		}
		v := vs[s.rng.IntN(len(vs))]
		off := v.Off + int64(s.rng.IntN(v.Len))
		k, at := wl.SegmentPlace(off)
		name := wlSegment(k)
		if off >= ptr.LogFrom && !slices.Contains(sealed, name) || at >= int64(len(n.disk.DurableBytes(name))) {
			continue
		}
		n.disk.Rot(name, int(at))
		n.corrupted, n.rotted, n.rotFile, n.rotOff = true, true, name, off
		s.stats.Rots++
		s.tracef("node %d: rotted a value at %#x in %s", n.id, off, name)
		if s.cfg.restartRotted {
			gen := n.gen
			s.after(10*Millisecond, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if n.gen == gen && n.proc != nil {
					s.restartAfterExit(n)
				}
			})
		}
		return
	}
}

// namesByte reports whether one of values holds the byte at log offset
// off.
func namesByte(values []wl.LogValue, off int64) bool {
	return slices.ContainsFunc(values, func(v wl.LogValue) bool { return v.Off <= off && off < v.Off+int64(v.Len) })
}

// witnessed reports whether damage to entry index, in n's newest segment
// name of sequence number seq, would show. Recovery takes damage to the
// newest segment's last durable record for a torn write, so entry index+2
// must follow it in the segment, durable. It must also be reported
// durable: a failed sync cuts the segment back to what the node driver
// reported, and with it an entry that an earlier sync wrote along by
// chance (#375).
func witnessed(n *node, w *wal.WAL, name string, seq, index uint64) bool {
	next, end, ok := w.ValuePlace(index+2, 0, 0)
	return ok && next == seq && end < int64(len(n.disk.DurableBytes(name))) && index+2 <= n.floor()
}
