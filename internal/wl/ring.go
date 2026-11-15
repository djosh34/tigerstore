package wl

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A ring holds bytes outside Go's heap that something names for a while:
// proposals and received entries until the log has them and Raft's
// storage lets them go, and Txn failure branches until their stream has
// its answer. It never overwrites what is still named. Each piece is a
// span, in order of its absolute position (the ring's laps laid end to
// end). A write takes room only below the oldest span still held, a lap
// later; when the ring has no such room, the caller puts the bytes on the
// heap instead. Spans are let go only at a sweep between turns, when Raft
// holds no message that points into them.
type ring struct {
	mem   []byte
	pos   int   // where the next piece goes
	lap   int64 // the absolute position of mem[0] in this lap
	spans []span
	head  int // the oldest span still held

	stats ringStats
}

type ringStats struct {
	spans, heap uint64 // pieces taken, and pieces put on the heap for want of room
	sweeps      uint64
}

// held describes the oldest span still held, for the loop's log.
func (r *ring) held() string {
	if r.head == len(r.spans) {
		return "none"
	}
	s := &r.spans[r.head]
	return fmt.Sprintf("kind %d, %d MiB behind", s.kind, (r.lap+int64(r.pos)-s.start)>>20)
}

type spanKind uint8

const (
	spanFree    spanKind = iota
	spanUnnamed          // a batch or received entry no append holds yet
	spanAppend           // in an append waiting for room in the log's stage
	spanStored           // an entry Raft's storage holds
	spanFail             // a Txn's failure branch, kept by its stream
)

type span struct {
	start, end int64 // absolute
	kind       spanKind
	at         time.Time // when it was taken
	ent        *pb.Entry // spanAppend, spanStored
	st         *stream   // spanFail
	// A batch a forward table names by its origin and sequence, which the
	// table lets go of with the span.
	fwd    bool
	origin uint64
	seq    uint32
}

// unnamedFor is how long a span no append holds is kept: a proposal that
// was lost, or a batch whose append never came.
const unnamedFor = time.Second

func newRing(size int) (*ring, error) {
	mem, err := mmapBytes(size)
	if err != nil {
		return nil, err
	}
	return &ring{mem: mem}, nil
}

// limit is the absolute position the ring may write up to: the oldest
// held span's start, a lap later.
func (r *ring) limit() int64 {
	if r.head < len(r.spans) {
		return r.spans[r.head].start + int64(len(r.mem))
	}
	return math.MaxInt64
}

// reserve returns room for at least n bytes at the ring's write position,
// or at its start when the end has too little, or nil when the ring has
// no room that large. Its capacity runs to the end of the free room.
// Nothing is taken until commit; reserving again before that returns the
// same start if the room is there.
func (r *ring) reserve(n int) []byte {
	start, lap := r.pos, r.lap
	if start+n > len(r.mem) {
		start, lap = 0, r.lap+int64(len(r.mem))
	}
	end := int64(len(r.mem))
	if l := r.limit(); l < lap+end {
		end = l - lap
	}
	if int64(start+n) > end {
		return nil
	}
	return r.mem[start:start:end]
}

// offset returns where p lies in the ring, or -1 if it is not in it.
func (r *ring) offset(p []byte) int {
	if cap(p) == 0 {
		return -1
	}
	off := int(uintptr(unsafe.Pointer(unsafe.SliceData(p[:1]))) - uintptr(unsafe.Pointer(unsafe.SliceData(r.mem))))
	if off < 0 || off >= len(r.mem) {
		return -1
	}
	return off
}

// commit takes p, which starts where reserve's room did, as a span.
func (r *ring) commit(p []byte, s span) *span {
	off := r.offset(p)
	if off < r.pos {
		r.lap += int64(len(r.mem))
	}
	r.pos = off + len(p)
	if r.head > 1024 && r.head*2 > len(r.spans) {
		r.spans = append(r.spans[:0], r.spans[r.head:]...)
		r.head = 0
	}
	s.start, s.end = r.lap+int64(off), r.lap+int64(r.pos)
	r.spans = append(r.spans, s)
	r.stats.spans++
	return &r.spans[len(r.spans)-1]
}

// find returns the held span that starts at p's first byte.
func (r *ring) find(p []byte) *span {
	off := r.offset(p)
	if off < 0 {
		return nil
	}
	a := r.lap + int64(off)
	if off >= r.pos {
		a -= int64(len(r.mem))
	}
	lo, hi := r.head, len(r.spans)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if r.spans[m].start < a {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(r.spans) && r.spans[lo].start == a {
		return &r.spans[lo]
	}
	return nil
}

// sweep lets go of the oldest spans while free reports them free, but
// only those more than half a lap behind the write position: the rest
// stay where Raft's apply and its appends to followers read them, rather
// than from the log.
func (r *ring) sweep(free func(*span) bool) {
	r.stats.sweeps++
	behind := r.lap + int64(r.pos) - int64(len(r.mem))/2
	for r.head < len(r.spans) && r.spans[r.head].start < behind && free(&r.spans[r.head]) {
		r.spans[r.head] = span{}
		r.head++
	}
}

// ---- the node's rings

// entrySpan returns the span of an entry's data in the proposal or the
// receive ring, if it lies in one.
func (n *node) entrySpan(data []byte) *span {
	if len(data) == 0 {
		return nil
	}
	if sp := n.ring.find(data); sp != nil {
		return sp
	}
	return n.rx.find(data)
}

// nameEntries marks the spans of ents' data as held by kind.
func (n *node) nameEntries(ents []*pb.Entry, kind spanKind) {
	for _, e := range ents {
		if sp := n.entrySpan(e.GetData()); sp != nil {
			sp.kind, sp.ent = kind, e
		}
	}
}

// sweepRings lets go of every span nothing holds any more. It runs between
// turns, once Raft has handed out every message it had.
func (n *node) sweepRings(now time.Time) {
	free := func(s *span) bool {
		if !n.spanFree(s, now) {
			return false
		}
		if s.fwd {
			n.dropForward(s)
		}
		return true
	}
	n.ring.sweep(free)
	n.rx.sweep(free)
	n.side.sweep(free)
	n.storage.trim(n.applied, n.wal.stable)
}

func (n *node) spanFree(s *span, now time.Time) bool {
	switch s.kind {
	case spanFree:
		return true
	case spanUnnamed:
		return now.Sub(s.at) >= unnamedFor
	case spanAppend:
		return false
	case spanStored:
		st := n.storage
		if !st.holds(s.ent) {
			return true
		}
		i := s.ent.GetIndex()
		if st.offOf(i)+int64(len(s.ent.GetData())) > n.wal.stable {
			// Raft's unstable log holds it until the append's responses
			// are stepped.
			return false
		}
		st.letGo(s.ent)
		return true
	case spanFail:
		return s.st.fail == nil || s.st.failAt != s.start
	}
	return true
}

// dropForward clears the forward table's slot that names the batch in s,
// if it still does: an append that names the batch then finds it missing
// and takes the carry path.
func (n *node) dropForward(s *span) {
	f := &n.fwdOf(s.origin)[s.seq&(fwdSlots-1)]
	if f.seq == s.seq && f.data != nil {
		f.data = nil
	}
}
