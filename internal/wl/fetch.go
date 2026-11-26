package wl

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"time"
)

// Values read off the loop (#704). The history's values, and the records
// the budget let go, lie only on disk, in the log or a checkpoint. A read
// served on the loop, a range or a watch that catches up, reads the store
// with noWait: a value the page cache does not hold is not read there,
// where the loop would wait for the disk, and the read fails with
// errAway. A range then waits in awayReads, and a watch where it is (it
// catches up again each turn), while a goroutine reads the values and
// hands them to the loop (fetchAway). The loop keeps them fetchedFor, and
// serves the ranges that waited again; values it read in the meantime
// may lead to another fetch. The apply still reads what it needs on the
// loop: the records its writes replace, and a Txn's ranges.

// fetchedValue is a value read off the loop, or the error its read met.
type fetchedValue struct {
	val []byte
	err error
	at  time.Time
}

// fetchedFor is how long the loop keeps a value fetched, for every read
// that waits on it.
const fetchedFor = time.Second

// fetchedMax is the most the loop keeps of values fetched, in bytes: a
// thirty-second of the budget, none past it. Kept a second each with no
// bound, page-cache reads at about 120 MB/s held 359 MB live in Go's heap
// on past-ram's 560 MiB budget, past what the store's tables left Go, and
// the member's cgroup killed it. A sixteenth, counted with the other
// buffers, left a budget of 256 MiB with six followers less than a
// quarter for the store (TestTheBuffersFitTheBudget, #689's area-3
// review).
func fetchedMax(budget int64) int64 {
	if budget == 0 {
		return math.MaxInt64
	}
	return budget / 32
}

// fetchedAt is a value fetched, by its place, and when it came.
type fetchedAt struct {
	off int64
	at  time.Time
}

// keepFetched keeps v, the value fetched at off.
func (st *store) keepFetched(off int64, v fetchedValue) {
	if st.fetched == nil {
		st.fetched = map[int64]fetchedValue{}
	}
	st.forgetFetched(off)
	st.fetched[off] = v
	st.fetchedBytes += int64(len(v.val))
	st.fetchedOrder = append(st.fetchedOrder, fetchedAt{off: off, at: v.at})
}

// forgetFetched lets go of the value fetched at off, if kept.
func (st *store) forgetFetched(off int64) {
	if v, ok := st.fetched[off]; ok {
		delete(st.fetched, off)
		st.fetchedBytes -= int64(len(v.val))
	}
}

// trimFetched lets go of the values fetched before since, and of the
// oldest past max bytes.
func (st *store) trimFetched(since time.Time, max int64) {
	k := 0
	for ; k < len(st.fetchedOrder); k++ {
		e := st.fetchedOrder[k]
		v, ok := st.fetched[e.off]
		if !ok || !v.at.Equal(e.at) {
			continue // let go of already, or kept again since
		}
		if !e.at.Before(since) && st.fetchedBytes <= max {
			break
		}
		st.forgetFetched(e.off)
	}
	st.fetchedOrder = st.fetchedOrder[k:]
	if len(st.fetchedOrder) == 0 {
		st.fetchedOrder = nil
	}
}

// awayRead is a range that waits for values read off the loop: its
// stream and its request.
type awayRead struct {
	h   handle
	msg []byte
}

// awaitValues has the range of h, msg, wait for the values the store
// found away, and fetches them.
func (n *node) awaitValues(h handle, msg []byte) {
	if n.awayServing {
		if n.awayPlanning {
			// The head read owns a whole batch. Sharing individual
			// offsets with other batches split overlapping ranges across
			// trims, so that none ever had every value at once (#887).
			n.awayActive = n.st.tag
			n.queueFetch(slices.Clone(n.st.away), true)
		}
		n.st.away = n.st.away[:0]
		n.awayMissed = true
		return
	}
	if c, _ := n.clientOf(h); c != nil {
		c.growOut(len(msg))
		c.heldCounted += int64(len(msg))
	}
	n.awayReads = append(n.awayReads, awayRead{h: h, msg: bytes.Clone(msg)})
	n.st.away = n.st.away[:0]
	n.serveAway(false)
}

// serveAway serves the cold ranges in arrival order. Only the head plans
// a fetch, and it plans with an empty fetched cache: its batch must not
// depend on an earlier result surviving a trim. A completed batch is
// consumed before any trim, even when it is larger than fetchedMax.
// Later ranges keep only their requests, not a list of offsets each.
func (n *node) serveAway(ready bool) {
	if n.awayServing || n.awayActive != nil && n.awayActive == n.st.tag && !ready {
		return
	}
	n.awayActive = nil
	for len(n.awayReads) > 0 {
		r := n.awayReads[0]
		c, s := n.clientOf(r.h)
		if c != nil {
			var req rangeReq
			if err := parseRange(r.msg, &req); err == nil {
				if !ready {
					clear(n.st.fetched)
					n.st.fetchedBytes, n.st.fetchedOrder = 0, nil
				}
				n.awayServing, n.awayPlanning, n.awayMissed = true, !ready, false
				n.serveRange(c, s, &req, r.msg)
				n.awayServing = false
				if n.awayActive != nil {
					return
				}
				if n.awayMissed {
					// A write or checkpoint moved values during the read.
					// Replan the same head, not just its missing subset.
					ready = false
					continue
				}
			}
			c.growOut(-len(r.msg))
			c.heldCounted -= int64(len(r.msg))
		}
		n.awayReads[0] = awayRead{}
		n.awayReads = n.awayReads[1:]
		ready = false
	}
	n.awayReads = nil
}

// dropAway drops a client's waiting ranges, all for sid 0, else that
// stream's. The active head stays as an empty marker until its result
// arrives; it must not wake the next head with the wrong batch.
func (n *node) dropAway(c *client, sid uint32) {
	k := 0
	for i, r := range n.awayReads {
		idx, gen, stream := r.h.parts()
		if idx == c.idx && gen == c.gen && (sid == 0 || stream == sid) {
			c.growOut(-len(r.msg))
			c.heldCounted -= int64(len(r.msg))
			if i == 0 && n.awayActive != nil {
				n.awayReads[k] = awayRead{h: noHandle}
				k++
			}
			continue
		}
		n.awayReads[k] = r
		k++
	}
	clear(n.awayReads[k:])
	n.awayReads = n.awayReads[:k]
}

// valueFetch is a batch of values to read off the loop, from the store
// and checkpoint files that named them when the read found them away.
type valueFetch struct {
	tag       *byte // identity only, never a pointer back to the store
	ckpts     [2]ckptFile
	vs        []logValue
	bytes     int64
	rangeRead bool
}

// fetchAway queues the values the store found away to read off the loop,
// but for those a fetch under way or queued reads already.
func (n *node) fetchAway() {
	st := n.st
	if n.fetchTag != st.tag {
		n.fetchTag = st.tag
		n.fetching = map[int64]bool{}
	}
	var vs []logValue
	for _, v := range st.away {
		if !n.fetching[v.off] {
			n.fetching[v.off] = true
			vs = append(vs, v)
		}
	}
	st.away = st.away[:0]
	if len(vs) == 0 {
		return
	}
	n.queueFetch(vs, false)
}

// queueFetch keeps one plan; only its places wait for read room.
func (n *node) queueFetch(vs []logValue, rangeRead bool) {
	size := int64(0)
	for _, v := range vs {
		size += int64(v.len)
	}
	n.fetchQueue = append(n.fetchQueue, valueFetch{tag: n.st.tag, ckpts: n.st.ckpts, vs: vs, bytes: size, rangeRead: rangeRead})
	n.pumpFetch()
}

// pumpFetch bounds the values being read, including their handoff to the
// loop, by fetchedMax too. A goroutine per range left eighty batches of
// 1.6 MB waiting for the loop, and the small-budget node collected for
// most of its time (#833). Only places wait in the queue, not values. One
// batch may pass the share, alone: takeFetched must serve its waiting
// reads before trimming it.
func (n *node) pumpFetch() {
	for len(n.fetchQueue) > 0 {
		f := n.fetchQueue[0]
		if f.tag != n.st.tag {
			n.fetchQueue[0] = valueFetch{}
			n.fetchQueue = n.fetchQueue[1:]
			continue // a snapshot replaced the store before this read began
		}
		if n.fetchBytes > 0 && f.bytes > fetchedMax(n.st.budget)-n.fetchBytes {
			return
		}
		n.fetchQueue[0] = valueFetch{}
		n.fetchQueue = n.fetchQueue[1:]
		if len(n.fetchQueue) == 0 {
			n.fetchQueue = nil
		}
		n.fetchBytes += f.bytes
		n.stats.fetches++
		n.stats.fetched += len(f.vs)
		n.stats.fetchMost = max(n.stats.fetchMost, f.bytes)
		tag, ckpts, vs := f.tag, f.ckpts, f.vs
		n.shell.offLoop(func() func() error {
			got := make([]fetchedValue, len(vs))
			for i, v := range vs {
				got[i].val = make([]byte, v.len)
				if inCkpt(v.off) {
					got[i].err = readCkptValue(ckpts[ckptGenOf(v.off)], got[i].val, v.off)
				} else {
					got[i].err = n.wal.readDurable(got[i].val, v.off)
				}
			}
			return func() error {
				err := n.takeFetched(tag, ckpts, vs, got, f.rangeRead)
				n.fetchBytes -= f.bytes
				if err == nil {
					n.pumpFetch()
				}
				return err
			}
		})
	}
}

// readCkptValue reads the value at off of checkpoint f, off the loop. The
// loop may close f meanwhile: then the read fails, and the store no longer
// names the value there.
func readCkptValue(f ckptFile, p []byte, off int64) error {
	if f == nil {
		return fmt.Errorf("a value at %#x names a checkpoint the store does not have", off)
	}
	_, err := f.ReadAt(p, off&ckptOffMask)
	return err
}

// takeFetched keeps the values read for the store identified by tag,
// from its log and checkpoints ckpts, and serves the range that waited. A store
// installed since names other places, and a checkpoint file written since
// in a generation holds other values at the same places.
func (n *node) takeFetched(tag *byte, ckpts [2]ckptFile, vs []logValue, got []fetchedValue, rangeRead bool) error {
	if tag != n.st.tag {
		return nil
	}
	st := n.st
	if !rangeRead {
		for _, v := range vs {
			delete(n.fetching, v.off)
		}
	}
	for i, v := range vs {
		if inCkpt(v.off) && st.ckpts[ckptGenOf(v.off)] != ckpts[ckptGenOf(v.off)] {
			// Read from a checkpoint the store has let go of since.
			continue
		}
		got[i].at = n.now
		st.keepFetched(v.off, got[i])
	}
	if rangeRead {
		n.serveAway(true)
	}
	// The watches that wait catch up too, before the trim: trimmed first,
	// a fetch larger than fetchedMax lost values a watch needed, which it
	// fetched again, turn after turn, for good (#689's area-3 review).
	if len(n.w.unsynced) > 0 {
		for _, i := range n.w.unsynced {
			n.w.ws[i].fetchWait = false
		}
		if err := n.syncWatchers(); err != nil {
			return err
		}
	}
	// So do the large range replies that waited for values.
	for _, b := range n.bigs {
		b.fetching = false
	}
	n.pumpBig()
	// And the large watch responses (bigwatch.go).
	for _, b := range n.bigWatches {
		b.fetching = false
	}
	n.pumpBigWatches()
	// And the held replies of writes (reserve.go).
	if len(n.heldReplies) > 0 {
		for _, hr := range n.heldReplies {
			hr.fetching = false
		}
		n.serveHeld()
	}
	// The ranges and watches that waited took what they needed: past
	// fetchedMax, the oldest values go, those of this fetch last.
	n.st.trimFetched(n.now.Add(-fetchedFor), fetchedMax(n.st.budget))
	return nil
}

// setCkpt makes f the checkpoint file of generation gen, and lets go of
// the values fetched from the one before, whose places f reuses.
func (st *store) setCkpt(gen int, f ckptFile) {
	for off := range st.fetched {
		if inCkpt(off) && ckptGenOf(off) == gen {
			st.forgetFetched(off)
		}
	}
	st.ckpts[gen] = f
}

// dropFetched lets go of the values fetched more than fetchedFor ago.
func (n *node) dropFetched() {
	n.st.trimFetched(n.now.Add(-fetchedFor), fetchedMax(n.st.budget))
}
