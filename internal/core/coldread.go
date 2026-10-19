package core

import (
	"errors"
	"fmt"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// A value whose entry left memory is read from the log on disk, and the
// committer never reads the disk: it hands its driver a ValueRead, which
// a helper goroutine runs.
//
// A response or a watch event that carries such values is built at once,
// with every other field and the values' positions, and waits until the
// read is done. A watch stream's responses stay in order: one that waits
// holds back those after it, progress notifications included.
//
// A write or a read that needs a value at once, to compare or sort by
// it, stops before it changes anything (mvcc.ColdError) and runs again
// once the values are read. Ordered apply waits with such a write; the
// committer goes on with ticks, messages and reads.

// ValueRead is values that only the log has, for the replies, watch
// responses, write or read that wait for them. The driver runs it on a
// helper goroutine, never the committer, and hands it back with
// ValuesRead.
type ValueRead struct {
	log   Log
	fills []mvcc.Fill
	done  bool
	// replies wait for the read, and so do the held responses of streams.
	replies []Reply
	streams []*watchStream
	// read is a read that runs again once the values are in. apply is set
	// when ordered apply waits for them.
	read  *pending
	apply bool
}

// Run reads every value from the log and checks it against its checksum.
func (r *ValueRead) Run() error { return ReadValues(r.log, r.fills) }

// ReadValues reads the value of every fill from log and checks it against
// its checksum.
func ReadValues(log Log, fills []mvcc.Fill) error {
	for _, f := range fills {
		b, err := log.Value(f.Ref.Index, f.Ref.Off, f.Ref.Len)
		if err != nil {
			return fmt.Errorf("core: read the value of %q: %w", f.Key, err)
		}
		if err := f.Set(b); err != nil {
			return err
		}
	}
	return nil
}

// pausedWrite is a write that waits for values from the log, with ordered
// apply behind it: its entry, how many writes of the entry are applied,
// the read that brings the values, and those read for it so far.
type pausedWrite struct {
	index    uint64
	write    int
	read     *ValueRead
	fetched  map[mvcc.Ref][]byte
	compares *mvcc.Compares
}

// errPaused stops the walk of an entry at a write that waits for values.
var errPaused = errors.New("core: a write waits for values from the log")

// readValues starts a read of fills.
func (n *Node) readValues(fills []mvcc.Fill) *ValueRead {
	r := &ValueRead{log: n.log, fills: fills}
	n.cache.misses += int64(len(fills))
	n.cold[r] = struct{}{}
	n.out.Reads = append(n.out.Reads, r)
	return r
}

// fetch starts a read of the values a write or a read needs at once.
func (n *Node) fetch(need []mvcc.Fill) *ValueRead {
	return n.readValues(withDst(need))
}

// withDst points every fill of need at a slot of its own, for the values
// a write or a read needs at once.
func withDst(need []mvcc.Fill) []mvcc.Fill {
	vals := make([][]byte, len(need))
	for i := range need {
		need[i].Dst = &vals[i]
	}
	return need
}

// keep adds the values r read to fetched.
func keep(fetched map[mvcc.Ref][]byte, r *ValueRead) {
	for _, f := range r.fills {
		fetched[f.Ref] = *f.Dst
	}
}

// readNow reads the values a write needs while the node starts, when no
// driver runs reads yet and nothing else waits. They stay in
// values.fetched for the write's next run.
func (n *Node) readNow(need []mvcc.Fill) error {
	r := &ValueRead{fills: withDst(need)}
	if err := ReadValues(n.log, r.fills); err != nil {
		return err
	}
	if n.values.fetched == nil {
		n.values.fetched = map[mvcc.Ref][]byte{}
	}
	keep(n.values.fetched, r)
	return nil
}

// answer replies to p, at once or, if the reply carries values that only
// the log has, once a helper read them.
func (n *Node) answer(p *pending, r Reply, fills []mvcc.Fill) {
	if !n.settle(p) {
		return
	}
	if p.refuse != nil {
		r, fills = Reply{ID: r.ID, Err: p.refuse}, nil
	}
	if len(fills) == 0 {
		n.out.Replies = append(n.out.Replies, r)
		return
	}
	v := n.readValues(fills)
	v.replies = append(v.replies, r)
}

// pause stops ordered apply at write number write of entry index until
// the values cold needs are read.
func (n *Node) pause(index uint64, write int, cold *mvcc.ColdError) {
	fetched := map[mvcc.Ref][]byte{}
	if r := n.resume; r != nil && r.index == index && r.write == write && !batch(cold.Compares) {
		// The write needs more values than the last read brought; it
		// keeps those too.
		fetched = r.fetched
	}
	n.paused = &pausedWrite{index: index, write: write, fetched: fetched, read: n.fetch(cold.Need), compares: cold.Compares}
	n.paused.read.apply = true
}

// batch reports whether the values a transaction waits for are a batch of
// a compare's, so that those it read before are no longer needed.
func batch(c *mvcc.Compares) bool { return c != nil && c.Batch }

// runApply applies Raft's apply messages in order, until a write waits
// for values.
func (n *Node) runApply() {
	for len(n.applyQ) > 0 && n.paused == nil && n.err == nil {
		m := n.applyQ[0]
		if err := n.apply(m.Entries); err != nil {
			n.fail(err)
			return
		}
		if n.paused != nil {
			return
		}
		n.applyQ[0] = nil
		n.applyQ = n.applyQ[1:]
		n.deliver(m.GetResponses())
	}
	n.releaseHeld()
}

// ValuesRead hands back a ValueRead the driver ran, with the error of its
// Run. The replies and watch responses that waited for it go out, and a
// write or read that needed its values runs again.
func (n *Node) ValuesRead(r *ValueRead, err error) {
	if n.err != nil {
		return
	}
	r.done = true
	delete(n.cold, r)
	if err != nil {
		// As with a value in memory: one that cannot be read, or does not
		// match its checksum, never reaches a client. The node stops.
		n.fail(err)
		return
	}
	for _, f := range r.fills {
		if n.kv.Current(f.Key, f.Ref) {
			n.cache.add(f.Ref, *f.Dst)
		}
	}
	n.out.Replies = append(n.out.Replies, r.replies...)
	for _, s := range r.streams {
		n.releaseWatches(s)
	}
	if p := r.read; p != nil && !p.done {
		if p.fetched == nil || batch(p.compares) {
			p.fetched = map[mvcc.Ref][]byte{}
		}
		keep(p.fetched, r)
		n.values.fetched = p.fetched
		n.kv.Resume(p.compares)
		n.serveRead(p)
		n.kv.Resume(nil)
		n.values.fetched = nil
		if p.done {
			p.fetched, p.compares = nil, nil
		}
	}
	if w := n.paused; w != nil && w.read == r {
		keep(w.fetched, r)
		n.paused, n.resume = nil, w
		n.values.fetched = w.fetched
		n.kv.Resume(w.compares)
		n.runApply()
		n.kv.Resume(nil)
		n.values.fetched, n.resume = nil, nil
	}
}

// readMark lowers mark, the Raft index below which the log may remove
// segments, to keep every value a read under way needs.
func (n *Node) readMark(mark uint64) uint64 {
	for r := range n.cold {
		for _, f := range r.fills {
			mark = min(mark, f.Ref.Index)
		}
	}
	return mark
}
