package wl

import (
	"bytes"
	"fmt"
	"math"
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
	h    handle
	msg  []byte
	wait []int64 // the values it waits for, nil when it waits for room
}

// awaitValues has the range of h, msg, wait for the values the store
// found away, and fetches them.
func (n *node) awaitValues(h handle, msg []byte) {
	r := awayRead{h: h, msg: bytes.Clone(msg), wait: make([]int64, len(n.st.away))}
	for i, v := range n.st.away {
		r.wait[i] = v.off
	}
	n.awayReads = append(n.awayReads, r)
	n.fetchAway()
}

// valueFetch is a batch of values to read off the loop, from the store
// and checkpoint files that named them when the read found them away.
type valueFetch struct {
	st    *store
	ckpts [2]ckptFile
	vs    []logValue
	bytes int64
}

// fetchAway queues the values the store found away to read off the loop,
// but for those a fetch under way or queued reads already.
func (n *node) fetchAway() {
	st := n.st
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
	n.stats.fetches++
	n.stats.fetched += len(vs)
	size := int64(0)
	for _, v := range vs {
		size += int64(v.len)
	}
	n.stats.fetchMost = max(n.stats.fetchMost, size)
	n.fetchQueue = append(n.fetchQueue, valueFetch{st: st, ckpts: st.ckpts, vs: vs, bytes: size})
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
		if n.fetchBytes > 0 && f.bytes > fetchedMax(n.st.budget)-n.fetchBytes {
			return
		}
		n.fetchQueue[0] = valueFetch{}
		n.fetchQueue = n.fetchQueue[1:]
		if len(n.fetchQueue) == 0 {
			n.fetchQueue = nil
		}
		n.fetchBytes += f.bytes
		st, ckpts, vs := f.st, f.ckpts, f.vs
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
				err := n.takeFetched(st, ckpts, vs, got)
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

// takeFetched keeps the values a fetch of store st read, from its log and
// its checkpoints ckpts, and serves the ranges that waited again. A store
// installed since names other places, and a checkpoint file written since
// in a generation holds other values at the same places.
func (n *node) takeFetched(st *store, ckpts [2]ckptFile, vs []logValue, got []fetchedValue) error {
	for _, v := range vs {
		delete(n.fetching, v.off)
	}
	if st == n.st {
		for i, v := range vs {
			if inCkpt(v.off) && st.ckpts[ckptGenOf(v.off)] != ckpts[ckptGenOf(v.off)] {
				// Read from a checkpoint the store has let go of since.
				continue
			}
			got[i].at = n.now
			st.keepFetched(v.off, got[i])
		}
	}
	reads := n.awayReads
	n.awayReads = nil
	for _, r := range reads {
		c, s := n.clientOf(r.h)
		if c == nil {
			continue
		}
		waiting := false
		for _, off := range r.wait {
			if n.fetching[off] {
				waiting = true
				break
			}
		}
		if waiting {
			// Another batch has not read this range's values yet. Do not
			// scan and build its records on every unrelated completion.
			n.awayReads = append(n.awayReads, r)
			continue
		}
		var req rangeReq
		if err := parseRange(r.msg, &req); err != nil {
			continue
		}
		n.serveRange(c, s, &req, r.msg)
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
