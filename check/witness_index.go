package check

import (
	"cmp"
	"iter"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

// indexStep charges up to 256 index work items as one step of the shared
// budget. Index passes do much less per item than a search step (which may
// replay an operation). Even inside callbacks and sorts, the stop hook is at
// most 256*1024 items away. Once stopped, every enclosing pass stops too; index
// discards its partial results and returns ErrWitnessTimeout.
func (r *revisionOrder) indexStep() bool {
	if r.indexStopped {
		return true
	}
	if r.indexLeft > 0 {
		r.indexLeft--
		return false
	}
	r.indexStopped = r.w.tick()
	r.indexLeft = 255
	return r.indexStopped
}

func indexSort[T any](r *revisionOrder, s []T, compare func(T, T) int) {
	sortWithStep(s, compare, r.indexStep)
}

// sortWithStep is a stable merge sort that can stop within a pass. A library
// sort's comparison callback cannot propagate a stop. Already sorted inputs,
// common in histories, need only one pass and no scratch space.
func sortWithStep[T any](s []T, compare func(T, T) int, stop func() bool) {
	if stop == nil {
		// The time sorter also runs outside the revision index.
		slices.SortStableFunc(s, compare)
		return
	}
	sorted := true
	for k := 1; k < len(s); k++ {
		if stop() {
			return
		}
		if compare(s[k-1], s[k]) > 0 {
			sorted = false
			break
		}
	}
	if sorted {
		return
	}
	a, b := s, make([]T, len(s))
	for width := 1; width < len(s); width *= 2 {
		for lo := 0; lo < len(s); lo += 2 * width {
			mid, hi := min(lo+width, len(s)), min(lo+2*width, len(s))
			i, j := lo, mid
			for k := lo; k < hi; k++ {
				if stop() {
					return
				}
				if j == hi || i < mid && compare(a[i], a[j]) <= 0 {
					b[k] = a[i]
					i++
				} else {
					b[k] = a[j]
					j++
				}
			}
		}
		a, b = b, a
	}
	for k := range s {
		if stop() {
			return
		}
		s[k] = a[k]
	}
}

// indexPuts yields puts with a budget check even for transaction ops that do not
// put a key. A large transaction must be stoppable too.
func (r *revisionOrder) indexPuts(req history.Request) iter.Seq[*history.PutRequest] {
	return func(yield func(*history.PutRequest) bool) {
		if req.Put != nil {
			if !r.indexStep() {
				yield(req.Put)
			}
			return
		}
		if req.Txn != nil {
			for o := range req.Txn.Ops() {
				if r.indexStep() {
					return
				}
				if o.Put != nil && !yield(o.Put) {
					return
				}
			}
		}
	}
}

// These are Mutates, respChanges and bumps with checks inside transaction
// branches too: one large read-only transaction must not bypass the budget.
func (r *revisionOrder) indexMutates(req history.Request) bool {
	if req.Txn == nil {
		return req.Mutates()
	}
	for o := range req.Txn.Ops() {
		if r.indexStep() {
			return false
		}
		if o.Range == nil {
			return true
		}
	}
	return false
}

func (r *revisionOrder) indexChanges(op *history.Op) bool {
	if op.Req.Txn == nil {
		return respChanges(op)
	}
	for k, o := range branch(op) {
		if r.indexStep() {
			return false
		}
		if o.Put != nil || o.Delete != nil && op.Resp.Txn.Results[k].Delete.Deleted > 0 {
			return true
		}
	}
	return false
}

func (r *revisionOrder) indexMutation(op *history.Op) bool {
	if !r.indexMutates(op.Req) {
		return false
	}
	if op.Resp != nil {
		return r.indexChanges(op)
	}
	return op.Err.Ambiguous
}

func (r *revisionOrder) indexBumps(op *history.Op) bool {
	if op.Resp == nil {
		return false
	}
	if op.Req.Put != nil {
		return true
	}
	return (op.Req.Delete != nil || op.Req.Txn != nil) && r.indexChanges(op)
}

// optionalPrefix holds ops by requested revision, and the highest header and
// call up to each. Every optional compaction shares prefixes of these slices:
// materializing its reads and predecessors separately would still be quadratic.
type optionalPrefix struct {
	ops          []int
	revs         []int64
	header, call []int64
}

func (r *revisionOrder) optionalPrefix(ops []int) optionalPrefix {
	h := r.w.h
	p := optionalPrefix{ops: ops}
	indexSort(r, p.ops, func(a, b int) int {
		return cmp.Or(cmp.Compare(requestedRevision(h.Ops[a].Req), requestedRevision(h.Ops[b].Req)), cmp.Compare(a, b))
	})
	for k, i := range p.ops {
		if r.indexStep() {
			return p
		}
		header, call := h.Ops[i].Resp.Revision, h.Ops[i].Call
		if k > 0 {
			header, call = max(header, p.header[k-1]), max(call, p.call[k-1])
		}
		p.revs = append(p.revs, requestedRevision(h.Ops[i].Req))
		p.header, p.call = append(p.header, header), append(p.call, call)
	}
	return p
}
