package sim

import (
	"bytes"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wl"
)

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
	sealed := wlSealed(n.disk, ptr)
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
