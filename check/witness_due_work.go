package check

import (
	"cmp"
	"iter"
	"math/bits"
)

// dueWork keeps late candidates in call order, and the sets due by time and
// revision separately. A query moves only the thresholds that changed; it
// neither visits the dueIndex again nor sorts and copies optional mutations.
// Zero is queried separately (expireEarly and fits alternate those queries),
// so asking by revision does not undo the last query by time, or vice versa.
// The sets are bitmaps in candidate order. Used candidates leave available
// immediately, and restore rebuilds it, keeping the sorted indexes and buffers.
//
// A query snapshots bitmap words, not candidates: putIn can ask due again
// while walking an outer query. Each nesting level borrows its own buffer.
// Without that snapshot a nested query could change the outer query's set.
type dueWork struct {
	candidates        []optional
	gone, by          []int
	rank              []int
	available         []uint64
	timed, revision   []uint64
	zeroTime, zeroRev []uint64
	timeAt, revAt     int
	valid             bool
	remaining         int
	hasZeroTime       bool
	hasZeroRev        bool
	buffers           [][]uint64
}

func (r *revisionOrder) newDueWork() *dueWork {
	d := &dueWork{rank: make([]int, len(r.used))}
	for _, o := range r.optional {
		if r.step() {
			return d
		}
		if o.late {
			d.candidates = append(d.candidates, o)
		}
	}
	sortWithStep(d.candidates, func(a, b optional) int {
		return cmp.Compare(r.w.calls[a.op], r.w.calls[b.op])
	}, r.step)
	for j, o := range d.candidates {
		if r.step() {
			return d
		}
		d.rank[o.op] = j + 1
		d.gone = append(d.gone, j)
		d.by = append(d.by, j)
	}
	sortWithStep(d.gone, func(a, b int) int { return cmp.Compare(d.candidates[a].gone, d.candidates[b].gone) }, r.step)
	sortWithStep(d.by, func(a, b int) int { return cmp.Compare(d.candidates[a].by, d.candidates[b].by) }, r.step)
	n := (len(d.candidates) + 63) / 64
	d.available, d.timed, d.revision = make([]uint64, n), make([]uint64, n), make([]uint64, n)
	d.zeroTime, d.zeroRev = make([]uint64, n), make([]uint64, n)
	for j, o := range d.candidates {
		if r.step() {
			return d
		}
		if o.gone < 0 {
			d.hasZeroTime = true
			d.zeroTime[j/64] |= uint64(1) << (j % 64)
		}
		if o.by <= 0 {
			d.hasZeroRev = true
			d.zeroRev[j/64] |= uint64(1) << (j % 64)
		}
	}
	return d
}

// use marks an optional or a gap's writer used, removing it from every
// subsequent due query without another visit to it. A query already walking
// its snapshot still checks used, as putIn did with the old slice.
func (r *revisionOrder) use(i int) {
	r.used[i] = true
	if d := r.dueWork; d != nil && d.rank[i] > 0 {
		d.remove(d.rank[i] - 1)
	}
}

func (d *dueWork) remove(j int) {
	bit := uint64(1) << (j % 64)
	if d.available[j/64]&bit != 0 {
		d.available[j/64] &^= bit
		d.remaining--
	}
}

func (d *dueWork) reset(r *revisionOrder) {
	clear(d.available)
	d.remaining = 0
	for j, o := range d.candidates {
		if r.step() {
			return
		}
		if !r.used[o.op] {
			d.remaining++
			d.available[j/64] |= uint64(1) << (j % 64)
		}
	}
	d.valid = true
}

// move changes one due set by only the candidates its threshold crossed,
// forwards or backwards: trial calls need not increase along an order.
func (d *dueWork) move(r *revisionOrder, order []int, at *int, set []uint64, holds func(optional) bool) {
	for *at > 0 && !holds(d.candidates[order[*at-1]]) {
		if r.step() {
			return
		}
		*at--
		j := order[*at]
		set[j/64] &^= uint64(1) << (j % 64)
	}
	for *at < len(order) && holds(d.candidates[order[*at]]) {
		if r.step() {
			return
		}
		j := order[*at]
		set[j/64] |= uint64(1) << (j % 64)
		*at++
	}
}

func (d *dueWork) query(r *revisionOrder, call, rev int64, yield func(optional) bool) {
	if r.step() {
		return
	}
	if !d.valid {
		d.reset(r)
	}
	if d.remaining == 0 || r.w.stopped {
		return
	}
	timed, revision := d.zeroTime, d.zeroRev
	if call != 0 {
		d.move(r, d.gone, &d.timeAt, d.timed, func(o optional) bool { return o.gone < call })
		timed = d.timed
	}
	if rev != 0 {
		d.move(r, d.by, &d.revAt, d.revision, func(o optional) bool { return o.by <= rev })
		revision = d.revision
	}
	if (call == 0 && !d.hasZeroTime || call != 0 && d.timeAt == 0) &&
		(rev == 0 && !d.hasZeroRev || rev != 0 && d.revAt == 0) {
		return
	}
	// Match the old slice's snapshot even if a nested putIn advances
	// compaction before the outer walk reaches another candidate.
	compacted := r.w.live.compacted
	var snapshot []uint64
	if n := len(d.buffers); n > 0 {
		snapshot, d.buffers = d.buffers[n-1], d.buffers[:n-1]
	} else {
		snapshot = make([]uint64, len(d.available))
	}
	defer func() { d.buffers = append(d.buffers, snapshot) }()
	for j := range snapshot {
		if r.step() {
			return
		}
		snapshot[j] = (timed[j] | revision[j]) & d.available[j]
	}
	for j, word := range snapshot {
		if r.step() {
			return
		}
		for word != 0 {
			if r.step() {
				return
			}
			k := j*64 + bits.TrailingZeros64(word)
			word &= word - 1
			o := d.candidates[k]
			c := r.w.reqs[o.op].Compact
			if r.used[o.op] || c != nil && c.Revision <= compacted {
				d.remove(k)
				continue
			}
			if !yield(o) {
				return
			}
		}
	}
}

// due lists the late optional mutations not yet placed that must go in
// before the event of an op called at time call, or by the end of revision
// rev; all of them if call is past every op. They come in call order: one
// called before a rejection returned may have to go in before one called
// after it, which ends the rejection's states. The slow witness keeps the
// old scan; its stable sort also defines the order of tied calls.
func (r *revisionOrder) due(call, rev int64) iter.Seq[optional] {
	if !r.w.slow {
		return func(yield func(optional) bool) {
			if r.dueWork == nil {
				r.dueWork = r.newDueWork()
			}
			if !r.w.stopped {
				r.dueWork.query(r, call, rev, yield)
			}
		}
	}
	return func(yield func(optional) bool) {
		var out []optional
		for _, o := range r.optional {
			if r.step() {
				return
			}
			if o.late && !r.used[o.op] && (o.gone < call || o.by <= rev) {
				out = append(out, o)
			}
		}
		sortWithStep(out, func(a, b optional) int { return cmp.Compare(r.w.calls[a.op], r.w.calls[b.op]) }, r.step)
		for _, o := range out {
			if r.step() || !yield(o) {
				return
			}
		}
	}
}

// step bounds work inside a search phase as indexStep does in the index.
// A search work item is at most one candidate, key, bitmap word, or sort
// comparison. Every 64 items spend a step of the shared budget; cancellation
// is latched by tick, so partial results can never turn a stop into a verdict.
func (r *revisionOrder) step() bool {
	if r.w.stopped {
		return true
	}
	if r.workLeft > 0 {
		r.workLeft--
		return false
	}
	r.workLeft = 63
	return r.w.tick()
}
