package wl

import (
	"time"
	"unsafe"
)

// A guard keeps a ring from overwriting bytes something still names.
// Each piece handed out is a span, in order of its absolute position (the
// ring's laps laid end to end). A span is named until what names it lets
// go:
//   - Raft's storage, once the span holds an entry's data (index set):
//     until the storage is compacted past that entry;
//   - a stream's failure branch (st set): until the stream's branch is
//     another or none;
//   - nothing known yet (a proposal or a batch not yet in the log): for
//     unnamedFor, after which the proposal is taken as lost.
//
// Before a write into the ring, take makes room: spans the write would
// overlap are let go if nothing names them, or once the storage can be
// compacted past them (every entry up to the node's applied index). A
// span still named after that is counted as an overrun.
type guard struct {
	base  unsafe.Pointer
	size  int64
	lap   int64 // absolute position of the ring's offset 0 in this lap
	spans []span
	head  int
}

type span struct {
	start, end int64 // absolute
	index      uint64
	st         *stream
	at         time.Time
}

const unnamedFor = time.Second

var guardStats struct {
	spans, compactions, overruns, waits uint64
}

func newGuard(ring []byte) *guard {
	return &guard{base: unsafe.Pointer(&ring[0]), size: int64(len(ring))}
}

// wrapped tells the guard the ring went back to offset 0.
func (g *guard) wrapped() { g.lap += g.size }

// abs returns the absolute position of ring offset pos in this lap.
func (g *guard) abs(pos int) int64 { return g.lap + int64(pos) }

// add records ring bytes [pos, pos+n) of this lap as a span.
func (g *guard) add(pos, n int, now time.Time) *span {
	if n == 0 {
		return nil
	}
	if g.head > 1024 && g.head*2 > len(g.spans) {
		g.spans = append(g.spans[:0], g.spans[g.head:]...)
		g.head = 0
	}
	g.spans = append(g.spans, span{start: g.abs(pos), end: g.abs(pos + n), at: now})
	guardStats.spans++
	return &g.spans[len(g.spans)-1]
}

// find returns the span that starts at p's first byte, if p lies in the
// ring.
func (g *guard) find(p []byte) *span {
	if len(p) == 0 {
		return nil
	}
	off := int64(uintptr(unsafe.Pointer(unsafe.SliceData(p))) - uintptr(g.base))
	if off < 0 || off >= g.size {
		return nil
	}
	a := g.lap + off
	if a >= g.lap+g.size || (len(g.spans) > g.head && a > g.spans[len(g.spans)-1].start) {
		a -= g.size
	}
	lo, hi := g.head, len(g.spans)
	for lo < hi {
		m := (lo + hi) / 2
		if g.spans[m].start < a {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(g.spans) && g.spans[lo].start == a {
		return &g.spans[lo]
	}
	return nil
}

// take makes room to write ring bytes [pos, pos+n) of this lap.
func (g *guard) take(n0 *node, pos, n int, now time.Time) {
	limit := g.abs(pos+n) - g.size
	for g.head < len(g.spans) && g.spans[g.head].start < limit {
		s := &g.spans[g.head]
		if s.named(n0, now) {
			guardStats.waits++
			if s.index != 0 && s.index <= n0.applied {
				// Compact Raft's storage up to the applied index: a
				// follower that needs entries before it gets a snapshot.
				guardStats.compactions++
				_ = n0.storage.Compact(n0.applied)
			}
			if s.named(n0, now) {
				guardStats.overruns++
			}
		}
		*s = span{}
		g.head++
	}
}

func (s *span) named(n *node, now time.Time) bool {
	switch {
	case s.index != 0:
		first, _ := n.storage.FirstIndex()
		return s.index >= first
	case s.st != nil:
		return s.st.fail != nil && s.st.failAt == s.start
	default:
		return now.Sub(s.at) < unnamedFor
	}
}
