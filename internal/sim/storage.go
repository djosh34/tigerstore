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
// it. The lock is held.
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

// deliverer is the Deliver of p's transport: it hands the node the peer
// frames that arrive, except the appends to and from a gray node, which
// it drops.
func (s *Sim) deliverer(p *proc, tn *tnode.Node) func(*raftpb.Message) {
	return func(m *raftpb.Message) {
		s.mu.Lock()
		g := s.gray
		drop := g != nil && (g == p.n || g.id == m.GetFrom()) && (m.GetType() == raftpb.MessageType_MsgApp || m.GetType() == raftpb.MessageType_MsgSnap)
		if drop {
			s.stats.Dropped++
			if !s.grayDropped {
				s.grayDropped = true
				s.stats.GrayNodes++
			}
			if s.healed {
				// The guaranteed gray period, which waited for this.
				s.gray, s.grayOnce = nil, false
			}
		} else {
			s.stats.Messages++
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
			s.mu.Unlock()
			return err
		}
		s.stats.Snapshots++
		data := buf.Bytes()
		switch {
		case !crash && !flip:
			s.mu.Unlock()
			return nil
		case len(data) == 0:
		case crash:
			s.stats.FetchCrashes++
			data = data[:s.rng.IntN(len(data))]
		default:
			data[s.rng.IntN(len(data))] ^= byte(1 + s.rng.IntN(255))
			s.stats.SnapshotFlips++
		}
		s.tracef("node %d: serving %d bytes of a snapshot to %d, crash %v, flip %v", n.id, len(data), id, crash, flip)
		s.mu.Unlock()
		_, err = w.Write(data)
		if crash {
			s.mu.Lock()
			if n.proc == p {
				s.stopNode(n)
			}
			s.mu.Unlock()
			return errDead
		}
		return err
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
// start. It runs on the node's committer, with its core c. The lock is
// held.
func (s *Sim) rot(n *node, c *core.Node) {
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
		if name == newestSegment(n.disk) {
			// Damage to the newest segment's last durable record looks
			// like a torn write; a later entry must be durable after it.
			next, end, ok := w.ValuePlace(r.Index+2, 0, 0)
			if !ok || next != seq || end >= int64(len(n.disk.DurableBytes(name))) {
				continue
			}
		}
		n.disk.Rot(name, int(at)+s.rng.IntN(int(r.Len)))
		n.corrupted, n.rotted, n.rotFile, n.rotEntry, n.rotSeg = true, true, name, r.Index, seq
		s.stats.Rots++
		s.tracef("node %d: rotted a value of entry %d in %s", n.id, r.Index, name)
		return
	}
}
