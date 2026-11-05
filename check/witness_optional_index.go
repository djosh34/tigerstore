package check

import "math"

// Prototype index over the original optional order. Each subtree stores
// the earliest deadlines of its unused late and early mutations. A query
// visits only subtrees with a due mutation; used leaves disappear lazily.
// Backtracking discards the index, because it can make a used op unused.
type optionalDeadlines struct {
	nodes []optionalDeadline
	n     int
}

type optionalDeadline struct {
	late, early    bool
	gone, by, from int64
}

func newOptionalDeadlines(os []optional) *optionalDeadlines {
	d := &optionalDeadlines{nodes: make([]optionalDeadline, max(1, 4*len(os))), n: len(os)}
	var build func(int, int, int)
	build = func(p, lo, hi int) {
		if hi-lo == 1 {
			o := os[lo]
			d.nodes[p] = optionalDeadline{late: o.late, early: !o.late, gone: o.gone, by: o.by, from: o.from}
			return
		}
		mid := (lo + hi) / 2
		build(p*2, lo, mid)
		build(p*2+1, mid, hi)
		d.pull(p)
	}
	if d.n > 0 {
		build(1, 0, d.n)
	}
	return d
}

func (d *optionalDeadlines) pull(p int) {
	a, b := d.nodes[p*2], d.nodes[p*2+1]
	c := optionalDeadline{late: a.late || b.late, early: a.early || b.early, gone: math.MaxInt64, by: math.MaxInt64, from: math.MaxInt64}
	if a.late {
		c.gone, c.by = a.gone, a.by
	}
	if b.late {
		c.gone, c.by = min(c.gone, b.gone), min(c.by, b.by)
	}
	if a.early {
		c.from = a.from
	}
	if b.early {
		c.from = min(c.from, b.from)
	}
	d.nodes[p] = c
}

func (r *revisionOrder) indexedOptional(call, rev int64, late bool) []optional {
	if r.deadlines == nil {
		r.deadlines = newOptionalDeadlines(r.optional)
	}
	return r.deadlines.query(r, call, rev, late)
}

func (d *optionalDeadlines) query(r *revisionOrder, call, rev int64, late bool) []optional {
	var out []optional
	var visit func(int, int, int)
	visit = func(p, lo, hi int) {
		r.diagIndexVisits++
		nd := d.nodes[p]
		if late {
			if !nd.late || (nd.gone >= call && nd.by > rev) {
				return
			}
		} else if !nd.early || nd.from > rev {
			return
		}
		if hi-lo == 1 {
			o := r.optional[lo]
			if r.used[o.op] || (o.late && r.w.reqs[o.op].Compact != nil && r.w.reqs[o.op].Compact.Revision <= r.w.live.compacted) {
				d.nodes[p] = optionalDeadline{}
			} else {
				out = append(out, o)
			}
			return
		}
		mid := (lo + hi) / 2
		visit(p*2, lo, mid)
		visit(p*2+1, mid, hi)
		d.pull(p)
	}
	if d.n > 0 {
		visit(1, 0, d.n)
	}
	return out
}
