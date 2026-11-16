package wl

import (
	"bytes"
	"fmt"
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

// awayRead is a range that waits for values read off the loop: its
// stream and its request.
type awayRead struct {
	h   handle
	msg []byte
}

// awaitValues has the range of h, msg, wait for the values the store
// found away, and fetches them.
func (n *node) awaitValues(h handle, msg []byte) {
	n.awayReads = append(n.awayReads, awayRead{h: h, msg: bytes.Clone(msg)})
	n.fetchAway()
}

// fetchAway reads the values the store found away off the loop, but for
// those a fetch under way reads already.
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
	ckpts := st.ckpts
	n.shell.offLoop(func() func() {
		got := make([]fetchedValue, len(vs))
		for i, v := range vs {
			got[i].val = make([]byte, v.len)
			if inCkpt(v.off) {
				got[i].err = readCkptValue(ckpts[ckptGenOf(v.off)], got[i].val, v.off)
			} else {
				got[i].err = n.wal.readDurable(got[i].val, v.off)
			}
		}
		return func() { n.takeFetched(st, ckpts, vs, got) }
	})
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
func (n *node) takeFetched(st *store, ckpts [2]ckptFile, vs []logValue, got []fetchedValue) {
	for _, v := range vs {
		delete(n.fetching, v.off)
	}
	if st == n.st {
		if st.fetched == nil {
			st.fetched = map[int64]fetchedValue{}
		}
		for i, v := range vs {
			if inCkpt(v.off) && st.ckpts[ckptGenOf(v.off)] != ckpts[ckptGenOf(v.off)] {
				// Read from a checkpoint the store has let go of since.
				continue
			}
			got[i].at = n.now
			st.fetched[v.off] = got[i]
		}
	}
	reads := n.awayReads
	n.awayReads = nil
	for _, r := range reads {
		c, s := n.clientOf(r.h)
		if c == nil {
			continue
		}
		var req rangeReq
		if err := parseRange(r.msg, &req); err != nil {
			continue
		}
		n.serveRange(c, s, &req, r.msg)
	}
}

// setCkpt makes f the checkpoint file of generation gen, and lets go of
// the values fetched from the one before, whose places f reuses.
func (st *store) setCkpt(gen int, f ckptFile) {
	for off := range st.fetched {
		if inCkpt(off) && ckptGenOf(off) == gen {
			delete(st.fetched, off)
		}
	}
	st.ckpts[gen] = f
}

// dropFetched lets go of the values fetched more than fetchedFor ago.
func (n *node) dropFetched() {
	for off, f := range n.st.fetched {
		if n.now.Sub(f.at) > fetchedFor {
			delete(n.st.fetched, off)
		}
	}
}
