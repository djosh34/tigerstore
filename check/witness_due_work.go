package check

import (
	"cmp"
	"iter"
	"math"
	"math/bits"
)

// dueWork keeps late candidates in call order, and the sets due by time and
// revision and eligible by lower-bound revision separately. A query moves
// only the thresholds that changed; it neither visits the dueIndex again nor
// sorts and copies optional mutations.
// Zero is queried separately (expireEarly and fits alternate those queries),
// so asking by revision does not undo the last query by time, or vice versa.
// The sets are bitmaps in candidate order. Used candidates leave available
// immediately, and restore rebuilds it, keeping the sorted indexes and buffers.
//
// A query snapshots bitmap words, not candidates: putIn can ask due again
// while walking an outer query. Each nesting level borrows its own buffer.
// Without that snapshot a nested query could change the outer query's set.
type dueWork struct {
	candidates                []optional
	gone, by, from            []int
	rank                      []int
	available                 []uint64
	timed, revision, eligible []uint64
	zeroTime, zeroRev         []uint64
	timeAt, revAt, fromAt     int
	valid                     bool
	remaining, eligibleLeft   int
	timedLeft, revisionLeft   int
	zeroTimeLeft, zeroRevLeft int
	buffers                   [][]uint64
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
		d.from = append(d.from, j)
	}
	sortWithStep(d.gone, func(a, b int) int { return cmp.Compare(d.candidates[a].gone, d.candidates[b].gone) }, r.step)
	sortWithStep(d.by, func(a, b int) int { return cmp.Compare(d.candidates[a].by, d.candidates[b].by) }, r.step)
	sortWithStep(d.from, func(a, b int) int { return cmp.Compare(d.candidates[a].from, d.candidates[b].from) }, r.step)
	n := (len(d.candidates) + 63) / 64
	d.available, d.timed, d.revision = make([]uint64, n), make([]uint64, n), make([]uint64, n)
	d.eligible = make([]uint64, n)
	d.zeroTime, d.zeroRev = make([]uint64, n), make([]uint64, n)
	for j, o := range d.candidates {
		if r.step() {
			return d
		}
		if o.gone < 0 {
			d.zeroTime[j/64] |= uint64(1) << (j % 64)
		}
		if o.by <= 0 {
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
		if d.eligible[j/64]&bit != 0 {
			d.eligibleLeft--
		}
		for _, set := range []struct {
			words []uint64
			left  *int
		}{{d.timed, &d.timedLeft}, {d.revision, &d.revisionLeft}, {d.zeroTime, &d.zeroTimeLeft}, {d.zeroRev, &d.zeroRevLeft}} {
			if set.words[j/64]&bit != 0 {
				*set.left--
			}
		}
	}
}

func (d *dueWork) reset(r *revisionOrder) {
	clear(d.available)
	d.remaining, d.eligibleLeft = 0, 0
	d.timedLeft, d.revisionLeft, d.zeroTimeLeft, d.zeroRevLeft = 0, 0, 0, 0
	for j, o := range d.candidates {
		if r.step() {
			return
		}
		if !r.used[o.op] {
			d.remaining++
			bit := uint64(1) << (j % 64)
			d.available[j/64] |= bit
			if d.eligible[j/64]&bit != 0 {
				d.eligibleLeft++
			}
			for _, set := range []struct {
				words []uint64
				left  *int
			}{{d.timed, &d.timedLeft}, {d.revision, &d.revisionLeft}, {d.zeroTime, &d.zeroTimeLeft}, {d.zeroRev, &d.zeroRevLeft}} {
				if set.words[j/64]&bit != 0 {
					*set.left++
				}
			}
		}
	}
	d.valid = true
}

// move changes one due set by only the candidates its threshold crossed,
// forwards or backwards: trial calls need not increase along an order.
func (d *dueWork) move(r *revisionOrder, order []int, at *int, set []uint64, left *int, holds func(optional) bool) {
	for *at > 0 && !holds(d.candidates[order[*at-1]]) {
		if r.step() {
			return
		}
		*at--
		j := order[*at]
		bit := uint64(1) << (j % 64)
		set[j/64] &^= bit
		if d.available[j/64]&bit != 0 {
			*left--
		}
	}
	for *at < len(order) && holds(d.candidates[order[*at]]) {
		if r.step() {
			return
		}
		j := order[*at]
		bit := uint64(1) << (j % 64)
		set[j/64] |= bit
		if d.available[j/64]&bit != 0 {
			*left++
		}
		*at++
	}
}

func (d *dueWork) query(r *revisionOrder, call, rev, from int64, yield func(optional) bool) {
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
	timedLeft, revisionLeft := d.zeroTimeLeft, d.zeroRevLeft
	if call != 0 {
		d.move(r, d.gone, &d.timeAt, d.timed, &d.timedLeft, func(o optional) bool { return o.gone < call })
		timed, timedLeft = d.timed, d.timedLeft
	}
	if rev != 0 {
		d.move(r, d.by, &d.revAt, d.revision, &d.revisionLeft, func(o optional) bool { return o.by <= rev })
		revision, revisionLeft = d.revision, d.revisionLeft
	}
	// Candidates due by rejection time can still be many revisions ahead
	// of this order. Intersect their retained lower-bound set before walking
	// candidates or making trial copies. Nested queries may move it either
	// way, just as they move the time and revision thresholds.
	d.move(r, d.from, &d.fromAt, d.eligible, &d.eligibleLeft, func(o optional) bool { return o.from <= from })
	// Available eligible counts keep unchanged all-future queries O(1).
	// Empty due sets also exit here; an empty intersection alone can still
	// scan bitmap words when both inputs contain available candidates.
	if d.eligibleLeft == 0 || timedLeft == 0 && revisionLeft == 0 {
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
		snapshot[j] = (timed[j] | revision[j]) & d.available[j] & d.eligible[j]
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
	return r.dueAt(call, rev, math.MaxInt64)
}

// dueAt keeps due's snapshot and stable call order, restricted to candidates
// whose lower-bound revision is at most from. Callers that cannot advance
// the revision while walking may use their live revision; expireEnd must use
// due instead, because one expiry can write a revision enabling a later one.
func (r *revisionOrder) dueAt(call, rev, from int64) iter.Seq[optional] {
	if !r.w.slow {
		return func(yield func(optional) bool) {
			if r.testDueQuery != nil {
				r.testDueQuery(call, rev, from, yield)
				return
			}
			if r.dueWork == nil {
				r.dueWork = r.newDueWork()
			}
			if !r.w.stopped {
				r.dueWork.query(r, call, rev, from, yield)
			}
		}
	}
	return func(yield func(optional) bool) {
		var out []optional
		for _, o := range r.optional {
			if r.step() {
				return
			}
			if o.late && o.from <= from && !r.used[o.op] && (o.gone < call || o.by <= rev) {
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

// Inner work still offers a stop poll every 64 items, but budget steps
// weigh it by cost: ordinary replay/search items at 1/1,024, and capacity
// items at 1/16,384. The latter are short integer-vector/bitmap operations
// and bulk sort estimates, not operation replays. #872's saved-history
// measurements give the weights and the simulator budget's conversion.
const (
	searchWeight     = 16
	capacityWeight   = 256
	workUnitsPerStep = 64 * capacityWeight
	ordinaryUnits    = capacityWeight / searchWeight
)

// charge accounts for bulk ordinary work without a callback per item.
func (r *revisionOrder) charge(work int64) bool {
	return r.chargeItems(work, ordinaryUnits)
}

func (r *revisionOrder) capacityCharge(work int64) bool {
	return r.chargeItems(work, 1)
}

// chargeItems stops at each polling or budget boundary, just as item-wise
// work does. The two classes share both carries, so switching between them
// cannot delay a poll or leave fractional work uncharged.
func (r *revisionOrder) chargeItems(work int64, units uint16) bool {
	for work > 0 {
		if r.w.stopped {
			return true
		}
		n := min(work, int64(r.workLeft), int64(r.budgetLeft/units))
		if n > 0 {
			r.workLeft -= uint8(n)
			r.budgetLeft -= uint16(n) * units
			work -= n
		} else {
			if r.workItem(units) {
				return true
			}
			work--
		}
	}
	return r.w.stopped
}

// step charges an ordinary inner item: a candidate, key, bitmap word or
// sort comparison. The slow path handles either counter's boundary.
func (r *revisionOrder) step() bool {
	if r.workLeft > 0 && r.budgetLeft >= ordinaryUnits && !r.w.stopped {
		r.workLeft--
		r.budgetLeft -= ordinaryUnits
		return false
	}
	return r.workItem(ordinaryUnits)
}

func (r *revisionOrder) capacityStep() bool {
	if r.workLeft > 0 && r.budgetLeft > 0 && !r.w.stopped {
		r.workLeft--
		r.budgetLeft--
		return false
	}
	return r.workItem(1)
}

func (r *revisionOrder) workItem(units uint16) bool {
	if r.w.stopped {
		return true
	}
	if r.budgetLeft < units {
		if r.w.budgetStep() {
			return true
		}
		r.budgetLeft += workUnitsPerStep
	}
	r.budgetLeft -= units
	if r.workLeft > 0 {
		r.workLeft--
		return false
	}
	r.workLeft = 63
	return r.w.tickWork(false)
}
