package wl

import (
	"encoding/binary"
	"time"
)

// Large-put diagnostics are opt-in (Config.TraceLargePuts). Only metadata
// is kept, never keys or values. A request is identified by node/run/id;
// batch/op ties it to Raft even after its client cancels or times out.
// Times are observations on this server, not the remote leader's clock.
const putTimingBytes = 1 << 20

type putTimings struct {
	next uint64
	// Side table: no diagnostic pointer or admission cost in stream slots.
	active   map[*stream]*putTiming
	pending  map[uint64][]*putTiming
	applying *putTiming
}

type putTiming struct {
	id, batch, index uint64
	op, bytes        int
	decodedBytes     int
	decodedComplete  bool
	kind             uint8
	began            time.Time
	committed        bool
	applied, replied bool
}

func (n *node) putTimingOf(s *stream) *putTiming {
	if n.putTimings == nil {
		return nil
	}
	return n.putTimings.active[s]
}

// Observe a known length before waitsForRoom can park the DATA frame.
// The gRPC header can be split between the old part and this frame.
func (n *node) tracePutHeader(s *stream, p []byte) {
	if len(s.buf)+len(p) < 5 {
		return
	}
	var h [5]byte
	k := copy(h[:], s.buf)
	copy(h[k:], p)
	n.tracePutStart(s, int(binary.BigEndian.Uint32(h[1:])))
}

func (n *node) tracePutStart(s *stream, size int) {
	n.beginPutTiming(s, size, 0, false)
}

func (n *node) beginPutTiming(s *stream, wire, decoded int, complete bool) {
	if n.putTimings == nil || n.putTimingOf(s) != nil || max(wire, decoded) < putTimingBytes || s.kind != kPut && s.kind != kTxn {
		return
	}
	n.putTimings.next++
	if n.putTimings.active == nil {
		n.putTimings.active = make(map[*stream]*putTiming)
	}
	t := &putTiming{id: n.putTimings.next, kind: s.kind, bytes: wire, decodedBytes: decoded, decodedComplete: complete, began: time.Now()}
	n.putTimings.active[s] = t
	n.tracePutEvent(t, "receive", -1)
}

func (n *node) tracePutDecoded(s *stream, wire int, decoded []byte, complete bool) {
	if n.putTimings == nil {
		return
	}
	n.beginPutTiming(s, wire, len(decoded), complete)
	if t := n.putTimingOf(s); t != nil {
		t.decodedBytes, t.decodedComplete = len(decoded), complete
	}
}

func (n *node) tracePutEvent(t *putTiming, stage string, code int) {
	if t == nil {
		return
	}
	now := time.Now()
	kind := "Put"
	if t.kind == kTxn {
		kind = "Txn"
	}
	n.logf("large-put request=%d/%d/%d kind=%s bytes=%d decoded_bytes=%d decoded_complete=%t batch=%d op=%d index=%d stage=%s at_ns=%d elapsed_ns=%d code=%d",
		n.id, n.run, t.id, kind, t.bytes, t.decodedBytes, t.decodedComplete, t.batch, t.op, t.index, stage, now.UnixNano(), now.Sub(t.began).Nanoseconds(), code)
}

func (n *node) tracePutAdd(h handle, batch uint64, op int) {
	if n.putTimings == nil {
		return
	}
	_, s := n.clientOf(h)
	t := n.putTimingOf(s)
	if t == nil {
		return
	}
	t.batch, t.op = batch, op
	traces := n.putTimings.pending[batch]
	for len(traces) <= op {
		traces = append(traces, nil)
	}
	traces[op] = t
	n.putTimings.pending[batch] = traces
	n.tracePutEvent(t, "batched", -1)
}

func (n *node) putBatchTimings(data []byte) []*putTiming {
	if n.putTimings != nil {
		if origin, seq, ok := batchOrigin(data); ok && origin == n.id {
			return n.putTimings.pending[seq]
		}
	}
	return nil
}

func (n *node) tracePutBatch(data []byte, stage string, index uint64) {
	for _, t := range n.putBatchTimings(data) {
		if t == nil {
			continue
		}
		if stage == "commit" {
			if t.committed {
				continue
			}
			t.committed, t.index = true, index
		}
		n.tracePutEvent(t, stage, -1)
	}
}

// A lost proposal's metadata survives its timeout, but only until its
// forward-table slot is reused. This bounds diagnostics when no writes
// commit. Applied batches are removed at once, also after cancellation.
func (n *node) forgetPutTimings(seq uint64) {
	if n.putTimings != nil {
		for _, t := range n.putTimings.pending[seq] {
			if t != nil && !t.applied {
				n.tracePutEvent(t, "forgotten", -1)
			}
		}
		delete(n.putTimings.pending, seq)
	}
}

func (n *node) tracePutApply(traces []*putTiming, op int) {
	if n.putTimings == nil {
		return
	}
	n.putTimings.applying = nil
	if op < len(traces) {
		n.putTimings.applying = traces[op]
		n.tracePutEvent(traces[op], "apply_begin", -1)
	}
}

// Put and Txn call this after their state-machine work, before building
// or holding the reply. applyBatch also calls it on early errors, where
// finishing an apply need not mean that the request changed the store.
func (n *node) tracePutApplied() {
	if n.putTimings != nil {
		if t := n.putTimings.applying; t != nil && !t.applied {
			t.applied = true
			n.tracePutEvent(t, "apply", -1)
		}
	}
}

func (n *node) tracePutClosed(s *stream) {
	if t := n.putTimingOf(s); t != nil {
		if !t.replied {
			n.tracePutEvent(t, "stream_closed", -1)
		}
		delete(n.putTimings.active, s)
	}
}

func (n *node) tracePutReply(s *stream, code int) {
	if t := n.putTimingOf(s); t != nil && !t.replied {
		t.replied = true
		n.tracePutEvent(t, "reply_queued", code)
	}
}
