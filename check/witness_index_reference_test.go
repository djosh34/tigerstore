package check

import (
	"cmp"
	"math"
	"slices"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

// scheduleOptionalReference is scheduleOptional before #849. Keep its scans in
// this test-only oracle, so random histories compare the constraints themselves,
// not just the verdict of the subsequent search.
func (r *revisionOrder) scheduleOptionalReference() {
	h := r.w.h
	present, gone := map[int64]int64{}, map[int64]int64{}
	granted, revoked := map[int64]bool{}, map[int64]bool{}
	type rejection struct {
		rev, call, ret int64
		read           bool
	}
	compacts := func(c int64, rj rejection) bool { return c > rj.rev || c == rj.rev && !rj.read }
	var compacted []rejection
	var compactions, reads []int
	note := func(lease, rev int64) {
		if lease != 0 {
			present[lease] = max(present[lease], rev)
		}
	}
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil {
			rev := op.Resp.Revision
			r.observed(op, func(kv history.KeyValue, to int64, _ bool) { note(kv.Lease, to) })
			switch {
			case op.Req.LeaseGrant != nil:
				granted[op.Resp.LeaseGrant.ID] = true
			case op.Req.LeaseRevoke != nil:
				note(op.Req.LeaseRevoke.ID, rev)
				revoked[op.Req.LeaseRevoke.ID] = true
			case op.Req.Range != nil && op.Req.Range.Revision > 0:
				reads = append(reads, i)
			case op.Req.Compact != nil:
				compactions = append(compactions, i)
			}
			for _, o := range branch(op) {
				if o.Put != nil {
					note(o.Put.Lease, rev)
				}
			}
			continue
		}
		if op.Err.Ambiguous {
			continue
		}
		switch {
		case sameError(op.Err, errLeaseNotFound):
			leases := leasesOf(op.Req)
			if op.Req.LeaseRevoke != nil {
				leases = append(leases, op.Req.LeaseRevoke.ID)
			}
			for _, lease := range leases {
				if at, ok := gone[lease]; !ok || op.Return < at {
					gone[lease] = op.Return
				}
			}
		case sameError(op.Err, errLeaseExists):
			note(op.Req.LeaseGrant.ID, 1)
		case sameError(op.Err, errCompacted):
			compacted = append(compacted, rejection{requestedRevision(op.Req), op.Call, op.Return, op.Req.Range != nil})
		}
	}
	for _, w := range h.Watches {
		for _, resp := range w.Responses {
			for _, e := range resp.Events {
				note(e.KV.Lease, e.Revision())
			}
		}
	}
	for i := range h.Ops {
		if op := &h.Ops[i]; op.Resp != nil && op.Req.Mutates() && mutation(op) {
			r.mutations = append(r.mutations, i)
		}
	}
	slices.SortFunc(r.mutations, func(a, b int) int { return cmp.Compare(r.w.returns[a], r.w.returns[b]) })
	revoking := map[int64]bool{}
	for _, i := range r.ambiguous {
		op := &h.Ops[i]
		at, call := r.lb[i], op.Call
		o := optional{op: i}
		switch {
		case op.Req.LeaseRevoke != nil:
			id := op.Req.LeaseRevoke.ID
			goneAt, ok := gone[id]
			if !ok || revoked[id] || revoking[id] {
				continue
			}
			revoking[id] = true
			at = max(at, present[id])
			o.late, o.gone, o.by = true, goneAt, r.lowestAfter(goneAt)
		case op.Req.LeaseGrant != nil:
			id := op.Req.LeaseGrant.ID
			if _, ok := present[id]; !ok || granted[id] {
				continue
			}
		case op.Req.Compact != nil:
			c := op.Req.Compact.Revision
			goneAt := int64(math.MaxInt64)
			for _, rj := range compacted {
				explained := h.Start != nil && rj.rev > 0 && compacts(h.Start.Rev, rj) || slices.ContainsFunc(compactions, func(k int) bool {
					return compacts(h.Ops[k].Req.Compact.Revision, rj) && r.w.returns[k] < rj.call
				})
				if compacts(c, rj) && !explained {
					goneAt = min(goneAt, rj.ret)
				}
			}
			if goneAt == math.MaxInt64 {
				continue
			}
			for _, k := range reads {
				if read := &h.Ops[k]; read.Req.Range.Revision < c {
					at = max(at, read.Resp.Revision)
					call = max(call, read.Call)
					o.reads = append(o.reads, k)
				}
			}
			for _, k := range compactions {
				if h.Ops[k].Req.Compact.Revision <= c {
					at = max(at, h.Ops[k].Resp.Revision)
					o.follow = append(o.follow, k)
				}
			}
			o.late, o.gone, o.by = true, goneAt, r.lowestAfter(goneAt)
		default:
			continue
		}
		o.from = at
		o.after = sort.Search(len(r.mutations), func(k int) bool { return h.Ops[r.mutations[k]].Return >= call })
		r.optional = append(r.optional, o)
	}
}
