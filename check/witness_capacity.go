package check

import (
	"maps"
	"slices"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

// capacityIndex describes an optimistic covering problem for the revisions
// ahead. A coordinate is one put of a key, or one deletion of a key. Fixed
// writers cost no free revision; an action is the coverage of an unfixed one.
// Branches are combined, values and compares ignored, and actions reusable.
// Solving this relaxation can only undercount the revisions a witness needs.
type capacityIndex struct {
	ends, fixed   []int64
	puts, deletes [][]int64
	revokes       map[int64][]int64
	leases        map[int64]map[string]bool
	actions       map[int][]int
}

func (r *revisionOrder) indexCapacity() {
	s := r.w.live
	n := len(s.keys)
	if n > 12 {
		return
	}
	c := &capacityIndex{
		puts: make([][]int64, n), deletes: make([][]int64, n),
		revokes: map[int64][]int64{}, leases: map[int64]map[string]bool{}, actions: map[int][]int{},
	}
	r.capacity = c
	for _, req := range r.w.reqs {
		for _, p := range capacityPuts(req) {
			if p.Lease != 0 {
				if c.leases[p.Lease] == nil {
					c.leases[p.Lease] = map[string]bool{}
				}
				c.leases[p.Lease][p.Key] = true
			}
		}
	}
	for _, ls := range r.lives {
		for l := range ls {
			c.ends = append(c.ends, l.c, l.m)
		}
	}
	c.ends = append(c.ends, r.countRevs...)
	slices.Sort(c.ends)
	c.ends = slices.Compact(c.ends)
	fixed := maps.Clone(r.writers)
	for rev, i := range r.resolved {
		if _, ok := fixed[rev]; !ok {
			fixed[rev] = i
		}
	}
	for rev, i := range fixed {
		c.fixed = append(c.fixed, rev)
		for _, p := range capacityPuts(r.w.reqs[i]) {
			if k := s.index(p.Key); k >= 0 {
				c.puts[k] = append(c.puts[k], rev)
			}
		}
		for k := range s.keys {
			if c.deletesKey(s, r.w.reqs[i], k) {
				c.deletes[k] = append(c.deletes[k], rev)
			}
		}
		if q := r.w.reqs[i].LeaseRevoke; q != nil {
			c.revokes[q.ID] = append(c.revokes[q.ID], rev)
		}
	}
	// An acked revoke might write its header revision or might not. Let
	// its deletions satisfy obligations for free in either case.
	for rev, is := range r.revokes {
		for _, i := range is {
			req := r.w.reqs[i]
			c.revokes[req.LeaseRevoke.ID] = append(c.revokes[req.LeaseRevoke.ID], rev)
			for k := range s.keys {
				if c.deletesKey(s, req, k) {
					c.deletes[k] = append(c.deletes[k], rev)
				}
			}
		}
	}
	slices.Sort(c.fixed)
	for _, revs := range slices.Concat(c.puts, c.deletes) {
		slices.Sort(revs)
	}
	for _, revs := range c.revokes {
		slices.Sort(revs)
	}
	for _, i := range r.ambiguous {
		a := make([]int, 2*n)
		for _, p := range capacityPuts(r.w.reqs[i]) {
			if k := s.index(p.Key); k >= 0 {
				a[k]++
			}
		}
		for k := range s.keys {
			if c.deletesKey(s, r.w.reqs[i], k) {
				a[n+k] = 1
			}
		}
		c.actions[i] = a
	}
}

// capacityBetween counts entries after lo and up to hi. Duplicates count:
// a transaction may put the same key more than once at one revision.
func capacityBetween(revs []int64, lo, hi int64) int {
	a := sort.Search(len(revs), func(j int) bool { return revs[j] > lo })
	b := sort.Search(len(revs), func(j int) bool { return revs[j] > hi })
	return b - a
}

// capacityReachable asks whether unfixed revisions can cover the missing
// puts of observed lives and deletions required by future creations and
// empty ranges. Each horizon uses an independent relaxation; no scheduling
// or cross-horizon conclusion is assumed. Larger problems are left alone.
func (r *revisionOrder) capacityReachable(s *state, skip int) bool {
	c := r.capacity
	if c == nil || len(s.keys) > 12 {
		return true
	}
	n, now := len(s.keys), s.rev
	first := sort.Search(len(c.ends), func(j int) bool { return c.ends[j] > now })
	for _, end := range c.ends[first:] {
		if end > now+countHorizon {
			break
		}
		room := int(end-now) - capacityBetween(c.fixed, now, end)
		need := make([]int, 2*n)
		mustDelete := func(k int, by int64) {
			if s.kvs[k].Version == 0 || capacityBetween(c.deletes[k], now, by) > 0 {
				return
			}
			if id := s.kvs[k].Lease; id != 0 && capacityBetween(c.revokes[id], now, by) > 0 {
				return
			}
			need[n+k] = 1
		}
		for key, ls := range r.lives {
			k := s.index(key)
			if k < 0 {
				continue
			}
			for l := range ls {
				if l.c > end || l.m <= now {
					continue
				}
				if l.c > now {
					// Delete and recreate in the same transaction is legal.
					mustDelete(k, l.c)
				}
				if l.m > end {
					if l.c > now {
						need[k] = max(need[k], 1-capacityBetween(c.puts[k], l.c-1, l.c))
					}
					continue
				}
				v, lo := int64(0), l.c-1
				if l.c <= now {
					v, lo = s.kvs[k].Version, now
				}
				need[k] = max(need[k], int(l.v-v)-capacityBetween(c.puts[k], lo, l.m))
			}
		}
		j := sort.Search(len(r.countRevs), func(j int) bool { return r.countRevs[j] > now })
		for _, rev := range r.countRevs[j:] {
			if rev > end {
				break
			}
			for _, cnt := range r.counts[rev] {
				if cnt.n == 0 {
					for k := cnt.lo; k < cnt.hi; k++ {
						mustDelete(k, rev)
					}
				}
			}
		}
		total := 0
		for _, v := range need {
			total += v
		}
		if total == 0 || total > 16 {
			continue
		}
		actions := [][]int{}
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
		for _, i := range r.ambiguous {
			if i == skip || r.used[i] || r.lb[i] >= end {
				continue
			}
			if _, fixed := r.reserved[i]; fixed {
				continue
			}
			a := slices.Clone(c.actions[i])
			if q := r.w.reqs[i].LeaseRevoke; q != nil {
				for k, kv := range s.kvs {
					if kv.Version != 0 && kv.Lease == q.ID {
						a[n+k] = 1
					}
				}
			}
			add(a)
		}
		for rev, is := range r.revokes {
			if rev <= now || rev > end {
				continue
			}
			for _, i := range is {
				a := make([]int, 2*n)
				for k := range s.keys {
					if c.deletesKey(s, r.w.reqs[i], k) {
						a[n+k] = 1
					}
				}
				add(a)
			}
		}
		if capacityCost(need, actions, room) > room {
			return false
		}
	}
	return true
}

// capacityCost is the minimum number of reusable actions covering need,
// capped at limit+1. The first remaining obligation must be covered by
// some action; after choosing it, all coordinates it covers may decrease.
func capacityCost(need []int, actions [][]int, limit int) int {
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

func capacityPuts(req history.Request) []*history.PutRequest {
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

// deletesKey over-approximates revokes by the keys currently holding
// their lease or that any put in the history could attach to it. Testing
// only the current lease would be unsound: a later put may reattach it.
func (c *capacityIndex) deletesKey(s *state, req history.Request, k int) bool {
	if q := req.LeaseRevoke; q != nil {
		return s.kvs[k].Lease == q.ID || c.leases[q.ID][s.keys[k]]
	}
	del := func(d *history.DeleteRequest) bool { lo, hi := s.span(d.Key, d.End); return lo <= k && k < hi }
	if req.Delete != nil {
		return del(req.Delete)
	}
	if req.Txn != nil {
		for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
			if o.Delete != nil && del(o.Delete) {
				return true
			}
		}
	}
	return false
}
