package check

import (
	"cmp"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

// scopedState replays an answer using only keys its request can inspect or
// modify. It keeps every key in every requested range, not just the keys
// the real answer returned: missing keys, counts and wrong extra values
// are still checked. Both transaction branches and every compare count.
// Lease revocation, which can touch arbitrary keys, uses the whole store.
func (w *witness) scopedState(i, j int) *state {
	if w.scopeKeys == nil {
		w.scopeKeys = map[int][]string{}
	}
	keys, ok := w.scopeKeys[i]
	if !ok {
		keys = requestKeys(w.live, w.reqs[i])
		w.scopeKeys[i] = keys
	}
	sm := w.states[j]
	s := &state{keys: keys, kvs: make([]history.KeyValue, len(keys)), leases: slices.Clone(sm.leases), rev: sm.rev, compacted: sm.compacted, past: w.past}
	w.metrics.ScopedAnswers++
	w.metrics.ScopedKeyCells += int64(len(keys))
	w.metrics.ScopedKeysAvoided += int64(len(w.keys) - len(keys))
	for k, key := range keys {
		s.kvs[k], _ = w.past.at(key, sm.rev)
	}
	return s
}

func requestKeys(s *state, req history.Request) []string {
	if req.LeaseRevoke != nil {
		return s.keys
	}
	var spans [][2]int
	add := func(key, end string) {
		lo, hi := s.span(key, end)
		if lo < hi {
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
	merged := spans[:0]
	for _, p := range spans {
		if len(merged) > 0 && p[0] <= merged[len(merged)-1][1] {
			merged[len(merged)-1][1] = max(merged[len(merged)-1][1], p[1])
		} else {
			merged = append(merged, p)
		}
	}
	if len(merged) == 1 {
		return s.keys[merged[0][0]:merged[0][1]]
	}
	var keys []string
	for _, p := range merged {
		keys = append(keys, s.keys[p[0]:p[1]]...)
	}
	return keys
}
