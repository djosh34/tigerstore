package check

import (
	"slices"

	"github.com/djosh34/tigerstore/history"
)

type trialCell struct {
	k      int
	before history.KeyValue
}

// trial borrows the live key array, recording only potentially written
// cells. The caller must restore it before applying an event. Recording
// both transaction branches is conservative and also handles a put then
// delete of the same key. Scalars and leases are private to the trial.
func (r *revisionOrder) trial(s *state, req history.Request) (*state, func()) {
	var indexes []int
	put := func(p *history.PutRequest) {
		if k := s.index(p.Key); k >= 0 {
			indexes = append(indexes, k)
		}
	}
	del := func(d *history.DeleteRequest) {
		lo, hi := s.span(d.Key, d.End)
		for k := lo; k < hi; k++ {
			indexes = append(indexes, k)
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
				if op.Put != nil {
					put(op.Put)
				}
				if op.Delete != nil {
					del(op.Delete)
				}
			}
		}
	case req.LeaseRevoke != nil:
		for k, kv := range s.kvs {
			if kv.Version != 0 && kv.Lease == req.LeaseRevoke.ID {
				indexes = append(indexes, k)
			}
		}
	}
	slices.Sort(indexes)
	indexes = slices.Compact(indexes)
	cells := make([]trialCell, len(indexes))
	for j, k := range indexes {
		cells[j] = trialCell{k, s.kvs[k]}
	}
	c := *s
	c.leases = slices.Clone(s.leases)
	r.w.metrics.UndoTrials++
	r.w.metrics.UndoKeyCells += int64(len(cells))
	r.w.metrics.UndoDenseKeysAvoided += int64(len(s.keys))
	return &c, func() {
		for _, cell := range cells {
			s.kvs[cell.k] = cell.before
		}
	}
}

func (r *revisionOrder) undoTrials() bool { return r.w.variant == "undo" || r.w.variant == "ultimate" }

func (r *revisionOrder) testOptional(s *state, o optional, take func(*state, optional) bool) bool {
	if !r.undoTrials() {
		return take(r.optionalTrial(s, o), o)
	}
	r.w.metrics.OptionalTrials++
	if r.w.reqs[o.op].Compact != nil {
		r.w.metrics.OptionalCompactTrials++
	}
	t, restore := r.trial(s, r.w.reqs[o.op])
	defer restore()
	return take(t, o)
}
