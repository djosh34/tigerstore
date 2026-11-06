package check

import (
	"cmp"
	"maps"
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
	for _, req := range r.w.reqs {
		for _, p := range putsIn(req) {
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
	for key, ls := range r.lives {
		k := s.index(key)
		if k < 0 {
			continue
		}
		for _, l := range ls {
			x.byM = append(x.byM, lifeOf{k, l.life})
			x.ends = append(x.ends, l.c, l.m)
		}
	}
	x.byC = slices.Clone(x.byM)
	slices.SortFunc(x.byM, func(a, b lifeOf) int { return cmp.Compare(a.m, b.m) })
	slices.SortFunc(x.byC, func(a, b lifeOf) int { return cmp.Compare(a.c, b.c) })
	x.ends = append(x.ends, r.countRevs...)
	slices.Sort(x.ends)
	x.ends = slices.Compact(x.ends)
	// The acked writers' puts and deletions are those of indexCounts, in
	// either branch; the resolved writers' and the acked revokes' come on
	// top.
	for k := range n {
		x.puts[k], x.deletes[k] = slices.Clone(r.putRevs[k]), slices.Clone(r.delRevs[k])
	}
	credit := func(rev int64, req history.Request) {
		for _, p := range putsIn(req) {
			if k := s.index(p.Key); k >= 0 {
				x.puts[k] = append(x.puts[k], rev)
			}
		}
		for _, k := range x.deleted(s, req) {
			x.deletes[k] = append(x.deletes[k], rev)
		}
		if q := req.LeaseRevoke; q != nil {
			x.revokes[q.ID] = append(x.revokes[q.ID], rev)
		}
	}
	for rev, i := range r.writers {
		if i == 0 {
			continue
		}
		x.fixed = append(x.fixed, int64(rev))
		if q := r.w.reqs[i-1].LeaseRevoke; q != nil {
			credit(int64(rev), r.w.reqs[i-1])
		}
	}
	for rev, i := range r.resolved {
		if _, ok := r.writer(rev); !ok {
			x.fixed = append(x.fixed, rev)
			credit(rev, r.w.reqs[i])
		}
	}
	// An acked revoke may or may not write its header revision; what it
	// may delete, it deletes for free either way.
	for rev, is := range r.revokes {
		for _, i := range is {
			credit(int64(rev), r.w.reqs[i])
		}
	}
	slices.Sort(x.fixed)
	for _, revs := range slices.Concat(x.puts, x.deletes, slices.Collect(maps.Values(x.revokes))) {
		slices.Sort(revs)
	}
	for _, i := range r.ambiguous {
		a := make([]int, 2*n)
		for _, p := range putsIn(r.w.reqs[i]) {
			if k := s.index(p.Key); k >= 0 {
				a[k]++
			}
		}
		for _, k := range x.deleted(s, r.w.reqs[i]) {
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
func (r *revisionOrder) coverers(s *state, need []int) []int {
	x := r.room
	n := len(s.keys)
	var out []int
	for k := range n {
		if need[k] > 0 {
			out = append(out, r.putters[k]...)
		}
		if need[n+k] > 0 {
			out = append(out, r.deleters[k]...)
			// A key holds a lease only from a put of the history, so its
			// lease now is one of these.
			for _, id := range x.leasesOf[k] {
				out = append(out, x.revokesOf[id]...)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
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
	x := r.room
	if x == nil || r.w.slow {
		return true
	}
	now := s.rev
	first := sort.Search(len(x.ends), func(j int) bool { return x.ends[j] > now })
	for _, end := range x.ends[first:] {
		if end > now+countHorizon {
			break
		}
		need := r.roomNeed(s, end)
		total := 0
		for _, v := range need {
			total += v
		}
		if total == 0 || total > maxRoomNeed {
			continue
		}
		var actions [][]int
		add := func(a []int) {
			useful := false
			for j := range a {
				a[j] = min(a[j], need[j])
				useful = useful || a[j] > 0
			}
			if useful && !slices.ContainsFunc(actions, func(b []int) bool { return slices.Equal(a, b) }) {
				actions = append(actions, a)
			}
		}
		for _, i := range r.coverers(s, need) {
			if _, held := r.reserved[i]; i == skip || r.used[i] || r.lb[i] >= end || held {
				continue
			}
			add(slices.Clone(x.actions[i]))
		}
		free := int(end-now) - between(x.fixed, now, end)
		if fewestActions(need, actions, free) > free {
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
		if l.m > end {
			break
		}
		life(l)
	}
	for _, l := range x.byC[sort.Search(len(x.byC), func(j int) bool { return x.byC[j].c > now }):] {
		if l.c > end {
			break
		}
		if l.m > end {
			life(l)
		}
	}
	for _, rev := range r.countRevs[sort.Search(len(r.countRevs), func(j int) bool { return r.countRevs[j] > now }):] {
		if rev > end {
			break
		}
		for _, c := range r.counts[rev] {
			if c.n == 0 {
				for k := c.lo; k < c.hi; k++ {
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
func fewestActions(need []int, actions [][]int, limit int) int {
	memo := map[string]int{}
	var solve func([]int) int
	solve = func(d []int) int {
		key := make([]byte, len(d))
		first := -1
		for j, v := range d {
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
			if a[first] == 0 {
				continue
			}
			next := make([]int, len(d))
			for j, v := range d {
				next[j] = max(0, v-a[j])
			}
			best = min(best, 1+solve(next))
		}
		memo[string(key)] = best
		return best
	}
	return solve(need)
}

// putsIn returns the puts of req, in both branches of a transaction.
func putsIn(req history.Request) []*history.PutRequest {
	if req.Put != nil {
		return []*history.PutRequest{req.Put}
	}
	var ps []*history.PutRequest
	if req.Txn != nil {
		for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
			if o.Put != nil {
				ps = append(ps, o.Put)
			}
		}
	}
	return ps
}

// deleted returns the keys req may delete, in order: those of a delete's
// range, in either branch, or for a revoke those that hold its lease in s
// or that a put of the history puts on it, which a later put may attach
// again.
func (x *room) deleted(s *state, req history.Request) []int {
	var ks []int
	if q := req.LeaseRevoke; q != nil {
		for k, kv := range s.kvs {
			if kv.Lease == q.ID || x.attached[q.ID] != nil && x.attached[q.ID][k] {
				ks = append(ks, k)
			}
		}
		return ks
	}
	del := func(d *history.DeleteRequest) {
		lo, hi := s.span(d.Key, d.End)
		for k := lo; k < hi; k++ {
			ks = append(ks, k)
		}
	}
	switch {
	case req.Delete != nil:
		del(req.Delete)
	case req.Txn != nil:
		for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
			if o.Delete != nil {
				del(o.Delete)
			}
		}
		slices.Sort(ks)
		ks = slices.Compact(ks)
	}
	return ks
}
