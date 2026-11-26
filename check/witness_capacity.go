package check

import (
	"cmp"
	"iter"
	"math/bits"
	"slices"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

// The counts and lives that reachable checks bound each key alone, so
// two keys could each count on the same free revision. In control seed
// 1529 of #485's sweep, ambiguous ops wrote 13 revisions in a row, and an
// order that left one key's versions short and another key present that a
// count needed gone failed only at that count, after every order of the
// revisions before it (#425, #499). roomReachable bounds them together.
//
// It is a relaxation of the search: a coordinate is one put of a key or
// one deletion of it; a writer that holds its revision, acked or resolved,
// and an acked revoke cover what they may cover for free; an ambiguous op
// is an action that covers, in one revision, every put and deletion it may
// make, in either branch, and may be taken again. The fewest actions that
// cover what the revisions ahead need is then at most the revisions a real
// order spends on them, so more than the free revisions proves the order
// cannot go on.

// maxRoomKeys and maxRoomNeed bound the problems roomReachable solves;
// larger ones pass.
const (
	maxRoomKeys = 12
	maxRoomNeed = 16
)

// room is the index of roomReachable.
type room struct {
	// ends are the revisions an obligation ends at: the creations and last
	// writes of the lives clients saw, and the revisions of their counts.
	ends []int64
	// byM and byC are the lives clients saw, by last write and creation.
	byM, byC []lifeOf
	// fixed are the revisions an acked or resolved writer holds; puts and
	// deletes, per key, the revisions at which such a writer or an acked
	// revoke may put or delete it, a revision once for each put; revokes,
	// per lease, those of its acked or resolved revokes.
	fixed         []int64
	puts, deletes [][]int64
	revokes       map[int64][]int64
	// attached are, per lease, the keys a put of the history puts on it,
	// and actions, per ambiguous op, the puts per key and the deletions it
	// may make, as one vector: puts of key k at k, its deletion at n+k.
	attached map[int64][]bool
	actions  map[int][]int
	// leasesOf are, per key, the leases a put of the history puts it on,
	// and revokesOf, per lease, its ambiguous revokes.
	leasesOf  [][]int64
	revokesOf map[int64][]int
}

// lifeOf is a life clients saw of key k, an index of the store's keys.
type lifeOf struct {
	k int
	life
}

func (r *revisionOrder) indexRoom() {
	s := r.w.live
	n := len(s.keys)
	if n > maxRoomKeys {
		return
	}
	x := &room{puts: make([][]int64, n), deletes: make([][]int64, n), revokes: map[int64][]int64{}, attached: map[int64][]bool{}, actions: map[int][]int{}, leasesOf: make([][]int64, n), revokesOf: map[int64][]int{}}
	r.room = x
	// The start's keys on leases are attached to them too.
	if st := r.w.h.Start; st != nil {
		for _, kv := range st.KVs {
			if r.indexStep() {
				return
			}
			if k := s.index(kv.Key); k >= 0 && kv.Lease != 0 {
				if x.attached[kv.Lease] == nil {
					x.attached[kv.Lease] = make([]bool, n)
				}
				if !x.attached[kv.Lease][k] {
					x.leasesOf[k] = append(x.leasesOf[k], kv.Lease)
				}
				x.attached[kv.Lease][k] = true
			}
		}
	}
	for _, req := range r.w.reqs {
		if r.indexStep() {
			return
		}
		for p := range r.indexPuts(req) {
			if k := s.index(p.Key); k >= 0 && p.Lease != 0 {
				if x.attached[p.Lease] == nil {
					x.attached[p.Lease] = make([]bool, n)
				}
				if !x.attached[p.Lease][k] {
					x.leasesOf[k] = append(x.leasesOf[k], p.Lease)
				}
				x.attached[p.Lease][k] = true
			}
		}
	}
	// The lives come by last write from note's slice, and by creation by
	// counting: sorting them grew with the history (#499). The counts and
	// marks are kept by revision from the history's first, as the order's
	// tables (#768); the lives created before it, which only the start's
	// keys have, are sorted, and come first.
	var marks []bool
	var before []int64
	var early []lifeOf
	mark := func(rev int64) {
		if rev < r.first {
			before = append(before, rev)
			return
		}
		marks = at(marks, rev-r.first)
		marks[rev-r.first] = true
	}
	counts := make([]int, r.limit-r.first+2)
	for _, ls := range r.noted {
		if r.indexStep() {
			return
		}
		for _, l := range ls {
			if r.indexStep() {
				return
			}
			x.byM = append(x.byM, lifeOf{l.k, l.life})
			mark(l.c)
			mark(l.m)
			if l.c < r.first {
				early = append(early, lifeOf{l.k, l.life})
				continue
			}
			counts[l.c-r.first+1]++
		}
	}
	indexSort(r, early, func(a, b lifeOf) int { return cmp.Compare(a.c, b.c) })
	counts[0] = len(early)
	for c := 1; c < len(counts); c++ {
		if r.indexStep() {
			return
		}
		counts[c] += counts[c-1]
	}
	x.byC = make([]lifeOf, len(x.byM))
	copy(x.byC, early)
	for _, l := range x.byM {
		if r.indexStep() {
			return
		}
		if l.c >= r.first {
			x.byC[counts[l.c-r.first]] = l
			counts[l.c-r.first]++
		}
	}
	indexSort(r, before, cmp.Compare[int64])
	lives := before[:0]
	for _, rev := range before {
		if r.indexStep() {
			return
		}
		if len(lives) == 0 || lives[len(lives)-1] != rev {
			lives = append(lives, rev)
		}
	}
	for i, ok := range marks {
		if r.indexStep() {
			return
		}
		if ok {
			lives = append(lives, r.first+int64(i))
		}
	}
	// A count's revision is the one its read asked for, which nothing
	// bounds by limit, so the counts' are merged in order, not marked: a
	// read at a far future revision must fail the check, not make a slice
	// as long as its revision.
	x.ends = r.unionRevs(lives, r.countRevs)
	// The acked writers' puts and deletions are those of indexCounts, in
	// either branch; the resolved writers' and the acked revokes' come on
	// top.
	for k := range n {
		if r.indexStep() {
			return
		}
		x.puts[k], x.deletes[k] = slices.Clone(r.putRevs[k]), slices.Clone(r.delRevs[k])
	}
	credit := func(rev int64, req history.Request) {
		if r.indexStep() {
			return
		}
		for p := range r.indexPuts(req) {
			if k := s.index(p.Key); k >= 0 {
				x.puts[k] = append(x.puts[k], rev)
			}
		}
		for _, k := range x.deletedWithStep(s, req, r.indexStep) {
			x.deletes[k] = append(x.deletes[k], rev)
		}
		if q := req.LeaseRevoke; q != nil {
			x.revokes[q.ID] = append(x.revokes[q.ID], rev)
		}
	}
	// The revisions an acked or resolved writer holds, in order.
	// held is kept by revision from the history's first, as writers.
	held := slices.Clone(r.writers)
	for rev, i := range r.resolved {
		if r.indexStep() {
			return
		}
		if _, ok := r.writer(rev); !ok {
			held = at(held, rev-r.first)
			held[rev-r.first] = -1
			credit(rev, r.w.reqs[i])
		}
	}
	for j, i := range held {
		if r.indexStep() {
			return
		}
		if i == 0 {
			continue
		}
		rev := r.first + int64(j)
		x.fixed = append(x.fixed, rev)
		if i > 0 && r.w.reqs[i-1].LeaseRevoke != nil {
			credit(rev, r.w.reqs[i-1])
		}
	}
	// An acked revoke may or may not write its header revision; what it
	// may delete, it deletes for free either way.
	for j, is := range r.revokes {
		if r.indexStep() {
			return
		}
		for _, i := range is {
			if r.indexStep() {
				return
			}
			credit(r.first+int64(j), r.w.reqs[i])
		}
	}
	for _, revs := range slices.Concat(x.puts, x.deletes) {
		if r.indexStep() {
			return
		}
		indexSort(r, revs, cmp.Compare[int64])
	}
	for _, revs := range x.revokes {
		if r.indexStep() {
			return
		}
		indexSort(r, revs, cmp.Compare[int64])
	}
	for _, i := range r.ambiguous {
		if r.indexStep() {
			return
		}
		a := make([]int, 2*n)
		for p := range r.indexPuts(r.w.reqs[i]) {
			if k := s.index(p.Key); k >= 0 {
				a[k]++
			}
		}
		for _, k := range x.deletedWithStep(s, r.w.reqs[i], r.indexStep) {
			a[n+k] = 1
		}
		x.actions[i] = a
		if q := r.w.reqs[i].LeaseRevoke; q != nil {
			x.revokesOf[q.ID] = append(x.revokesOf[q.ID], i)
		}
	}
}

// coverers returns the ambiguous ops that may cover a coordinate of need
// in s: a put of a key it needs put, or a deletion of one it needs gone,
// in order and once each. The ambiguous revokes of a busy history are its
// leases' expiries, so only those of a lease a key holds or a put puts it
// on count.
func (r *revisionOrder) coverers(s *state, need []int) iter.Seq[int] {
	return func(yield func(int) bool) {
		x := r.room
		n := len(s.keys)
		var local [2 * maxRoomKeys][]int
		lists := local[:0]
		count := 0
		add := func(ops []int) {
			if len(ops) > 0 {
				lists = append(lists, ops)
				count += len(ops)
			}
		}
		for k := range n {
			if r.step() {
				return
			}
			if need[k] > 0 {
				add(r.putters[k])
			}
			if need[n+k] > 0 {
				add(r.deleters[k])
				// A key holds a lease only from a put of the history, so
				// its lease now is one of these.
				for _, id := range x.leasesOf[k] {
					if r.capacityStep() {
						return
					}
					add(x.revokesOf[id])
				}
			}
		}
		if count > maxCovererSort {
			// Each input follows the history's op order (indexCounts,
			// indexRoom). Merge them without copying and sorting every
			// candidate in one uninterruptible library call.
			r.mergeCoverers(lists, count, yield)
			return
		}
		var out []int
		for _, ops := range lists {
			if r.capacityCharge(int64(len(ops))) {
				return
			}
			out = append(out, ops...)
		}
		// Keep the native sort for small inputs only. Its entire input
		// is bounded, and the charged n log n estimate is below one
		// stop-poll interval; large inputs use the polled merge above.
		if r.capacityCharge(int64(count) * int64(bits.Len(uint(count)))) {
			return
		}
		slices.Sort(out)
		if r.capacityCharge(int64(count)) {
			return
		}
		for _, i := range slices.Compact(out) {
			if !yield(i) {
				return
			}
		}
	}
}

const maxCovererSort = 2048

// mergeCoverers walks sorted op lists with a min-heap, yielding each op
// once. Keep the conservative n log n estimate of the old sort, but pay
// it per consumed candidate, not before the whole call. It covers the
// bounded heap walk too and keeps long-run budget sizing conservative.
// There is no candidate-sized scratch buffer or unbounded native sort.
func (r *revisionOrder) mergeCoverers(lists [][]int, count int, yield func(int) bool) {
	for p := len(lists)/2 - 1; p >= 0; p-- {
		if r.capacityCharge(int64(bits.Len(uint(len(lists))))) {
			return
		}
		covererDown(lists, p)
	}
	work := int64(bits.Len(uint(count)) + 2)
	last, have := 0, false
	for len(lists) > 0 {
		if r.capacityCharge(work) {
			return
		}
		i := lists[0][0]
		if !have || i != last {
			last, have = i, true
			if !yield(i) {
				return
			}
		}
		lists[0] = lists[0][1:]
		if len(lists[0]) == 0 {
			lists[0] = lists[len(lists)-1]
			lists = lists[:len(lists)-1]
		}
		covererDown(lists, 0)
	}
}

// A heap walk has at most bits.Len(len(lists)) levels, fewer than 64 on
// any supported target. Its caller charges that bound before each walk.
func covererDown(lists [][]int, p int) {
	for {
		child := 2*p + 1
		if child >= len(lists) {
			return
		}
		if child+1 < len(lists) && lists[child+1][0] < lists[child][0] {
			child++
		}
		if lists[p][0] <= lists[child][0] {
			return
		}
		lists[p], lists[child] = lists[child], lists[p]
		p = child
	}
}

// unionRevs returns the revisions of a and b, which are each in order and
// once each, in order and once each.
func (r *revisionOrder) unionRevs(a, b []int64) []int64 {
	out := make([]int64, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		if r.indexStep() {
			return nil
		}
		switch {
		case len(b) == 0 || len(a) > 0 && a[0] < b[0]:
			out, a = append(out, a[0]), a[1:]
		case len(a) == 0 || b[0] < a[0]:
			out, b = append(out, b[0]), b[1:]
		default:
			out, a, b = append(out, a[0]), a[1:], b[1:]
		}
	}
	return out
}

// between counts the revisions of revs, which are in order, after lo and
// up to hi, each as often as it is there.
func between(revs []int64, lo, hi int64) int {
	return sort.Search(len(revs), func(j int) bool { return revs[j] > hi }) - sort.Search(len(revs), func(j int) bool { return revs[j] > lo })
}

// roomReachable reports whether the revisions after s's and up to each
// end within countHorizon that no acked or resolved writer holds can still
// cover, once op skip is placed, what the order needs by then: the puts a
// life clients saw still lacks, a deletion of a key present now before a
// life of it they saw created later, and of each key present now that a
// count of zero needs gone.
func (r *revisionOrder) roomReachable(s *state, skip int) bool {
	if r.step() {
		return false
	}
	x := r.room
	if x == nil || r.w.slow {
		return true
	}
	now := s.rev
	first := sort.Search(len(x.ends), func(j int) bool { return x.ends[j] > now })
	for _, end := range x.ends[first:] {
		if r.step() {
			return false
		}
		// end is past now: the difference doesn't overflow (#768).
		if end-now > countHorizon {
			break
		}
		need := r.roomNeed(s, end)
		total := 0
		for _, v := range need {
			if r.capacityStep() {
				return false
			}
			total += v
		}
		if total == 0 || total > maxRoomNeed {
			continue
		}
		var actions [][]int
		add := func(a []int) {
			useful := false
			for j := range a {
				if r.capacityStep() {
					return
				}
				a[j] = min(a[j], need[j])
				useful = useful || a[j] > 0
			}
			if useful && !slices.ContainsFunc(actions, func(b []int) bool { return r.capacityStep() || slices.Equal(a, b) }) {
				actions = append(actions, a)
			}
		}
		for i := range r.coverers(s, need) {
			if r.capacityStep() {
				return false
			}
			if _, held := r.reserved[i]; i == skip || r.used[i] || r.lb[i] >= end || held {
				continue
			}
			add(slices.Clone(x.actions[i]))
		}
		free := int(end-now) - between(x.fixed, now, end)
		if r.w.stopped || fewestActions(need, actions, free, r.capacityStep) > free {
			return false
		}
	}
	return true
}

// roomNeed returns what the order needs by end from s, past what the
// acked and resolved writers may give: the puts of each key at k, and its
// deletion at n+k. Lives of one key take the most any of them needs, as a
// bound.
func (r *revisionOrder) roomNeed(s *state, end int64) []int {
	x := r.room
	n, now := len(s.keys), s.rev
	need := make([]int, 2*n)
	mustDelete := func(k int, by int64) {
		if s.kvs[k].Version == 0 || between(x.deletes[k], now, by) > 0 {
			return
		}
		if id := s.kvs[k].Lease; id != 0 && between(x.revokes[id], now, by) > 0 {
			return
		}
		need[n+k] = 1
	}
	life := func(l lifeOf) {
		if l.c > end || l.m <= now {
			return
		}
		if l.c > now {
			// A transaction may delete the key and create it again at c.
			mustDelete(l.k, l.c)
		}
		if l.m > end {
			if l.c > now {
				need[l.k] = max(need[l.k], 1-between(x.puts[l.k], l.c-1, l.c))
			}
			return
		}
		v, lo := int64(0), l.c-1
		if l.c <= now {
			v, lo = s.kvs[l.k].Version, now
		}
		need[l.k] = max(need[l.k], int(l.v-v)-between(x.puts[l.k], lo, l.m))
	}
	// The lives that end by end after now, and those created after now by
	// end: no other one needs anything by end.
	for _, l := range x.byM[sort.Search(len(x.byM), func(j int) bool { return x.byM[j].m > now }):] {
		if r.capacityStep() {
			return nil
		}
		if l.m > end {
			break
		}
		life(l)
	}
	for _, l := range x.byC[sort.Search(len(x.byC), func(j int) bool { return x.byC[j].c > now }):] {
		if r.capacityStep() {
			return nil
		}
		if l.c > end {
			break
		}
		if l.m > end {
			life(l)
		}
	}
	for _, rev := range r.countRevs[sort.Search(len(r.countRevs), func(j int) bool { return r.countRevs[j] > now }):] {
		if r.capacityStep() {
			return nil
		}
		if rev > end {
			break
		}
		for _, c := range r.counts[rev] {
			if r.capacityStep() {
				return nil
			}
			if c.n == 0 {
				for k := c.lo; k < c.hi; k++ {
					if r.capacityStep() {
						return nil
					}
					mustDelete(k, rev)
				}
			}
		}
	}
	return need
}

// fewestActions returns the fewest actions, each taken any number of
// times, whose sum covers need, or limit+1 if more. The first coordinate
// still needed must be covered by one of them, so it branches on those.
func fewestActions(need []int, actions [][]int, limit int, stop func() bool) int {
	memo := map[string]int{}
	var solve func([]int) int
	solve = func(d []int) int {
		if stop() {
			return limit + 1
		}
		key := make([]byte, len(d))
		first := -1
		for j, v := range d {
			if stop() {
				return limit + 1
			}
			key[j] = byte(v)
			if v > 0 && first < 0 {
				first = j
			}
		}
		if first < 0 {
			return 0
		}
		if v, ok := memo[string(key)]; ok {
			return v
		}
		best := limit + 1
		for _, a := range actions {
			if stop() {
				return limit + 1
			}
			if a[first] == 0 {
				continue
			}
			next := make([]int, len(d))
			for j, v := range d {
				if stop() {
					return limit + 1
				}
				next[j] = max(0, v-a[j])
			}
			best = min(best, 1+solve(next))
		}
		memo[string(key)] = best
		return best
	}
	return solve(need)
}

// deleted returns the keys req may delete, in order: those of a delete's
// range, in either branch, or for a revoke those that hold its lease in s
// or that a put of the history puts on it, which a later put may attach
// again.
func (x *room) deleted(s *state, req history.Request) []int {
	return x.deletedWithStep(s, req, func() bool { return false })
}

func (x *room) deletedWithStep(s *state, req history.Request, stop func() bool) []int {
	var ks []int
	if q := req.LeaseRevoke; q != nil {
		for k, kv := range s.kvs {
			if stop() {
				return nil
			}
			if kv.Lease == q.ID || x.attached[q.ID] != nil && x.attached[q.ID][k] {
				ks = append(ks, k)
			}
		}
		return ks
	}
	del := func(d *history.DeleteRequest) {
		lo, hi := s.span(d.Key, d.End)
		for k := lo; k < hi; k++ {
			if stop() {
				return
			}
			ks = append(ks, k)
		}
	}
	switch {
	case req.Delete != nil:
		del(req.Delete)
	case req.Txn != nil:
		for o := range req.Txn.Ops() {
			if stop() {
				return nil
			}
			if o.Delete != nil {
				del(o.Delete)
			}
		}
		sortWithStep(ks, cmp.Compare[int], stop)
		out := ks[:0]
		for _, k := range ks {
			if stop() {
				return nil
			}
			if len(out) == 0 || out[len(out)-1] != k {
				out = append(out, k)
			}
		}
		ks = out
	}
	return ks
}
