package check

import (
	"cmp"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

// The witness tries many orders, each a request stepped through the model
// on top of the order so far. A copy of the whole store for every try cost
// most of a long seed's check (#441): the store holds every key of the
// history. So a try borrows the store and puts back only what it wrote
// (try), and an answer is checked on the keys its request reads or writes
// alone (scopedState). Both give the model's exact outcome; the witness with
// slow set copies the whole store instead, for the tests that compare the
// two.

// try returns a state that shares s's values, for stepping req through,
// and undo, which puts back every value req may have written: the keys
// of a put, the range of a delete, both branches of a transaction, or
// every key of a revoked lease. The scalars of the state are its own, and
// its leases change only into a new set (leaseSet). undo must run before
// anything else changes s.
func (r *revisionOrder) try(s *state, req history.Request) (t *state, undo func()) {
	if r.w.slow {
		return s.clone(), func() {}
	}
	var written []int
	// Every key a request puts is in the store's keys (keysOf).
	put := func(p *history.PutRequest) { written = append(written, s.index(p.Key)) }
	del := func(d *history.DeleteRequest) {
		lo, hi := s.span(d.Key, d.End)
		for k := lo; k < hi; k++ {
			written = append(written, k)
		}
	}
	switch {
	case req.Put != nil:
		put(req.Put)
	case req.Delete != nil:
		del(req.Delete)
	case req.Txn != nil:
		for _, ops := range [][]history.TxnOp{req.Txn.Success, req.Txn.Failure} {
			for _, op := range ops {
				switch {
				case op.Put != nil:
					put(op.Put)
				case op.Delete != nil:
					del(op.Delete)
				}
			}
		}
	case req.LeaseRevoke != nil:
		for k, kv := range s.kvs {
			if kv.Version != 0 && kv.Lease == req.LeaseRevoke.ID {
				written = append(written, k)
			}
		}
	}
	slices.Sort(written)
	written = slices.Compact(written)
	before := make([]history.KeyValue, len(written))
	for j, k := range written {
		before[j] = s.kvs[k]
	}
	c := *s
	return &c, func() {
		for j, k := range written {
			s.kvs[k] = before[j]
		}
	}
}

// tryOptional reports whether take accepts optional o stepped through
// on top of s, which it leaves as it was.
func (r *revisionOrder) tryOptional(s *state, o optional, take func(*state, optional) bool) bool {
	t, undo := r.try(s, r.w.reqs[o.op])
	defer undo()
	return take(t, o)
}

// scopedState returns state j for answering op i, which changed nothing:
// only the keys its request can read or write, every key of each range it
// names, so that a missing key, a count or an extra value is still checked,
// and both branches and every compare of a transaction. A revoke can touch
// any key and gets them all.
func (w *witness) scopedState(i, j int) *state {
	if w.slow {
		return w.stateAt(j)
	}
	keys := w.scope[i]
	if keys == nil {
		keys = requestKeys(w.keys, w.reqs[i])
		if keys == nil {
			keys = []string{}
		}
		w.scope[i] = keys
	}
	sm := w.states[j]
	s := &state{keys: keys, kvs: make([]history.KeyValue, len(keys)), leases: sm.leases, rev: sm.rev, compacted: sm.compacted, past: w.past}
	for k, key := range keys {
		s.kvs[k], _ = w.past.at(key, sm.rev)
	}
	return s
}

// requestKeys returns the keys, of keys, that req can read or write, in
// order.
func requestKeys(keys []string, req history.Request) []string {
	if req.LeaseRevoke != nil {
		return keys
	}
	all := &state{keys: keys}
	var key, rangeEnd string
	switch {
	case req.Range != nil:
		key, rangeEnd = req.Range.Key, req.Range.End
	case req.Put != nil:
		key = req.Put.Key
	case req.Delete != nil:
		key, rangeEnd = req.Delete.Key, req.Delete.End
	}
	if req.Range != nil || req.Put != nil || req.Delete != nil {
		lo, hi := all.span(key, rangeEnd)
		return keys[lo:hi]
	}
	var spans [][2]int
	add := func(key, end string) {
		if lo, hi := all.span(key, end); lo < hi {
			spans = append(spans, [2]int{lo, hi})
		}
	}
	branch := func(ops []history.TxnOp) {
		for _, o := range ops {
			switch {
			case o.Range != nil:
				add(o.Range.Key, o.Range.End)
			case o.Put != nil:
				add(o.Put.Key, "")
			case o.Delete != nil:
				add(o.Delete.Key, o.Delete.End)
			}
		}
	}
	switch {
	case req.Range != nil:
		add(req.Range.Key, req.Range.End)
	case req.Put != nil:
		add(req.Put.Key, "")
	case req.Delete != nil:
		add(req.Delete.Key, req.Delete.End)
	case req.Txn != nil:
		for _, c := range req.Txn.Compare {
			add(c.Key, "")
		}
		branch(req.Txn.Success)
		branch(req.Txn.Failure)
	}
	slices.SortFunc(spans, func(a, b [2]int) int { return cmp.Compare(a[0], b[0]) })
	var out []string
	end := 0
	for _, p := range spans {
		lo := max(p[0], end)
		if lo < p[1] {
			out = append(out, keys[lo:p[1]]...)
		}
		end = max(end, p[1])
	}
	return out
}
