package check

import "math"

// The optional mutations of a history include the expiry of every lease
// it grants, so a busy history has thousands, and due and insertOptional
// scanned all of them, used or not, at every event and revision: on a
// busy no-fault history of 825,606 ops, 3.9 billion looks for 3,833 picks
// (#499). dueIndex finds them without the scan. It is a tree over
// r.optional, in its order, whose nodes keep, of their subtree's optional
// mutations not used yet, the earliest gone and by of the late ones and
// the earliest from of the early ones. A query visits only subtrees that
// hold a due one, and drops a leaf it finds used, or a compaction the
// order already passed, for good: along one order neither comes back.
// Going back to an earlier order can bring both back, so restore drops
// the whole index, and the next query builds it again.
type dueIndex struct {
	nodes []dueNode
	n     int
}

type dueNode struct {
	late, early    bool
	gone, by, from int64
}

func newDueIndex(os []optional) *dueIndex {
	d := &dueIndex{nodes: make([]dueNode, max(1, 4*len(os))), n: len(os)}
	var build func(p, lo, hi int)
	build = func(p, lo, hi int) {
		if hi-lo == 1 {
			o := os[lo]
			d.nodes[p] = dueNode{late: o.late, early: !o.late, gone: o.gone, by: o.by, from: o.from}
			return
		}
		mid := (lo + hi) / 2
		build(2*p, lo, mid)
		build(2*p+1, mid, hi)
		d.pull(p)
	}
	if d.n > 0 {
		build(1, 0, d.n)
	}
	return d
}

// pull sets node p from its two children.
func (d *dueIndex) pull(p int) {
	a, b := d.nodes[2*p], d.nodes[2*p+1]
	c := dueNode{late: a.late || b.late, early: a.early || b.early, gone: math.MaxInt64, by: math.MaxInt64, from: math.MaxInt64}
	for _, e := range []dueNode{a, b} {
		if e.late {
			c.gone, c.by = min(c.gone, e.gone), min(c.by, e.by)
		}
		if e.early {
			c.from = min(c.from, e.from)
		}
	}
	d.nodes[p] = c
}

// lateDue returns, in r.optional's order, the late optional mutations not
// used yet that are due by call or rev: gone before call, or by at most
// rev; and none to a revision the order already compacted.
func (r *revisionOrder) lateDue(call, rev int64) []optional {
	return r.dueTree().query(r, func(n dueNode) bool { return n.late && (n.gone < call || n.by <= rev) })
}

// earlyDue returns, in r.optional's order, the early optional mutations
// not used yet that may go in at rev.
func (r *revisionOrder) earlyDue(rev int64) []optional {
	return r.dueTree().query(r, func(n dueNode) bool { return n.early && n.from <= rev })
}

// dueTree returns the order's index of optional mutations, built anew
// after a restore.
func (r *revisionOrder) dueTree() *dueIndex {
	if r.dues == nil {
		r.dues = newDueIndex(r.optional)
	}
	return r.dues
}

// query returns the leaves whose node holds, visiting only the subtrees
// that hold one.
func (d *dueIndex) query(r *revisionOrder, holds func(dueNode) bool) []optional {
	var out []optional
	var visit func(p, lo, hi int)
	visit = func(p, lo, hi int) {
		if !holds(d.nodes[p]) {
			return
		}
		if hi-lo == 1 {
			o := r.optional[lo]
			if c := r.w.reqs[o.op].Compact; r.used[o.op] || o.late && c != nil && c.Revision <= r.w.live.compacted {
				d.nodes[p] = dueNode{}
			} else {
				out = append(out, o)
			}
			return
		}
		mid := (lo + hi) / 2
		visit(2*p, lo, mid)
		visit(2*p+1, mid, hi)
		d.pull(p)
	}
	if d.n > 0 {
		visit(1, 0, d.n)
	}
	return out
}
