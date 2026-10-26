package check

// THROWAWAY #448 prototype. Not a replacement for Witnessed. It checks
// the KV projection against one log replay, resolves writes by unique
// values, and checks revision/real-time constraints including applied
// ambiguous writes. It deliberately does NOT establish the temporal
// order of non-revision-changing lease/compaction events, semantic errors,
// watches, or physical durability. See research/448/REPORT.md.

import (
	"cmp"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

func floorKey(q history.Request) string {
	switch {
	case q.Put != nil:
		return "value:" + q.Put.Value
	case q.Txn != nil:
		for _, branch := range [][]history.TxnOp{q.Txn.Success, q.Txn.Failure} {
			for _, o := range branch {
				if o.Put != nil {
					return "value:" + o.Put.Value
				}
			}
		}
	}
	k, _ := fingerprint(q)
	return "request:" + k
}

// Floor448 performs one replay, with successful non-changing operations
// sorted by header revision. Historical reads use the replay's per-key
// version lists. It never clones the full K-key state for an observation.
func Floor448(h *history.History) error {
	if err := h.Validate(); err != nil {
		return err
	}
	if len(h.Log) == 0 {
		return errNoLog
	}
	reqs := make([]history.Request, len(h.Ops))
	var reads []int
	for i := range h.Ops {
		reqs[i] = modelRequest(&h.Ops[i])
		op := &h.Ops[i]
		if op.Resp != nil && (op.Req.Range != nil || !respChanges(op)) {
			reads = append(reads, i)
		}
	}
	slices.SortFunc(reads, func(i, j int) int { return cmp.Compare(h.Ops[i].Resp.Revision, h.Ops[j].Resp.Revision) })
	s := newState(keysOf(slices.Concat(reqs, h.Log)))
	s.past = versions{}
	outcomes := make([]outcome, len(h.Log))
	bump := make([]bool, len(h.Log))
	entries := map[string][]int{}
	claimed := make([]bool, len(h.Log))
	read := 0
	flush := func() error {
		for read < len(reads) && h.Ops[reads[read]].Resp.Revision <= s.rev {
			i := reads[read]
			op := &h.Ops[i]
			if op.Resp.Revision != s.rev {
				return fmt.Errorf("op %d: missing revision", i)
			}
			// Successful historical reads may precede compaction within this
			// revision. This prototype checks their data, not compaction timing.
			compacted := s.compacted
			s.compacted = -1
			o := s.step(reqs[i], false)
			s.compacted = compacted
			if o.err != nil || !matches(o, op.Resp) {
				return fmt.Errorf("op %d: KV response mismatch at %d", i, s.rev)
			}
			read++
		}
		return nil
	}
	for e, q := range h.Log {
		if err := flush(); err != nil {
			return err
		}
		prev := s.rev
		o := s.step(q, true)
		outcomes[e] = o
		bump[e] = s.rev > prev
		s.past.add(o.events)
		if o.err != nil || !changes(q, o) {
			claimed[e] = true
			continue
		}
		k := floorKey(q)
		entries[k] = append(entries[k], e)
		if g := q.LeaseGrant; g != nil {
			k = floorKey(anyGrant(g.TTL))
			entries[k] = append(entries[k], e)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if read != len(reads) {
		return fmt.Errorf("reads above final revision")
	}
	if h.Target > s.rev {
		return fmt.Errorf("target above log")
	}
	placed := make([]int, len(h.Ops))
	for i := range placed {
		placed[i] = -1
	}
	claim := func(i int, amb bool) error {
		q := reqs[i]
		if amb && q.LeaseGrant != nil && q.LeaseGrant.ID == 0 {
			q = anyGrant(q.LeaseGrant.TTL)
		}
		for _, e := range entries[floorKey(q)] {
			if claimed[e] {
				continue
			}
			same := reflect.DeepEqual(reqs[i], h.Log[e])
			if amb && q.LeaseGrant != nil && q.LeaseGrant.ID == -1 {
				same = true
			}
			if !same || !amb && !matches(outcomes[e], h.Ops[i].Resp) {
				continue
			}
			claimed[e] = true
			placed[i] = e
			return nil
		}
		if !amb {
			return fmt.Errorf("acked mutation %d missing from log", i)
		}
		return nil
	}
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil && op.Req.Mutates() && respChanges(op) {
			if err := claim(i, false); err != nil {
				return err
			}
		}
	}
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Err != nil && op.Err.Ambiguous && op.Req.Mutates() {
			if err := claim(i, true); err != nil {
				return err
			}
		}
	}
	for e, q := range h.Log {
		if !claimed[e] && q.LeaseRevoke == nil {
			return fmt.Errorf("unclaimed log entry %d", e)
		}
	}
	type point struct {
		call, ret, rev int64
		write          bool
	}
	points := make([]point, 0, len(h.Ops))
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Req.Range != nil && op.Req.Range.Serializable {
			continue
		}
		if op.Resp != nil {
			b := bumps(op)
			if placed[i] >= 0 {
				b = bump[placed[i]]
			}
			points = append(points, point{op.Call, op.Return, op.Resp.Revision, b})
		} else if e := placed[i]; e >= 0 {
			ret := int64(math.MaxInt64)
			if op.Effect != nil {
				ret = op.Effect.By
				if op.Effect.Revision != outcomes[e].resp.Revision {
					return fmt.Errorf("effect revision mismatch")
				}
			}
			points = append(points, point{op.Call, ret, outcomes[e].resp.Revision, bump[e]})
		}
	}
	returns := slices.Clone(points)
	slices.SortFunc(points, func(a, b point) int { return cmp.Compare(a.call, b.call) })
	slices.SortFunc(returns, func(a, b point) int { return cmp.Compare(a.ret, b.ret) })
	j := 0
	var highest int64
	for _, p := range points {
		for j < len(returns) && returns[j].ret < p.call {
			highest = max(highest, returns[j].rev)
			j++
		}
		if p.rev < highest || p.write && p.rev == highest {
			return fmt.Errorf("real-time revision violation")
		}
	}
	return nil
}
