package check

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/djosh34/tigerstore/history"
)

// fromRevisions builds the order from the revisions clients saw, revision
// by revision. The writer of a revision is the acked op that wrote it, or
// the ambiguous one whose value a client saw at it. A revision with
// neither is a gap: some ambiguous mutation, or an acked revoke, wrote it
// unseen. A depth-first search tries the candidates in call order,
// and goes back to the last gap when a later revision cannot be explained.
// A candidate must change exactly the keys that no client saw unchanged
// across the gap.
//
// Acked mutations that did not write their header revision (grants,
// compactions, revokes of leases without keys) follow its writer in call
// order. Ambiguous ones of those kinds go in only when a later answer
// needs them, after every answer that needs them left out.
func (w *witness) fromRevisions() error {
	r := &revisionOrder{
		w: w, writers: map[int64]int{}, revokes: map[int64][]int{}, others: map[int64][]int{}, seen: map[int64][]int{},
		lb: map[int]int64{}, alive: map[int64]int64{}, grants: map[int64]int{}, resolved: map[int64]int{}, reserved: map[int]int64{}, lives: map[string]map[life]int64{}, created: map[int64][]string{}, written: map[int64][]string{}, deleted: map[int64][]string{}, covered: map[string][]span{}, used: map[int]bool{}, ready: map[int]bool{},
		counts: map[int64][]count{},
	}
	w.search = r
	if err := r.index(); err != nil {
		return err
	}
	return r.search()
}

// revisionOrder is the state of fromRevisions.
type revisionOrder struct {
	w *witness
	// writers are the acked ops that wrote each revision, and revokes the
	// acked revokes at each header revision, which may have written it.
	writers map[int64]int
	revokes map[int64][]int
	// others are the acked mutations that changed no key, by header
	// revision, in call order. seen are the acked ops that changed
	// nothing, by header revision.
	others map[int64][]int
	seen   map[int64][]int
	// ambiguous are the ambiguous mutations, and lb the highest revision
	// returned before each was called: it writes no revision up to it.
	ambiguous []int
	lb        map[int]int64
	// alive is, per lease, the highest revision at which an answer shows
	// it in use: an acked put with it wrote that revision, or a key a
	// client saw still had it there. Its revoke writes a later revision,
	// unless the ID was granted more than once (grants).
	alive  map[int64]int64
	grants map[int64]int
	// returns and highest are the return times of the acked ops, in
	// order, and the highest header revision of those up to each; calls
	// and lowest their call times, in order, and the lowest header
	// revision of those from each on.
	returns, highest []int64
	calls, lowest    []int64
	// resolved are the ambiguous ops that a client saw write a revision,
	// and reserved the same by op: no other revision's candidate.
	resolved map[int64]int
	reserved map[int]int64
	// covered are, per key, the revisions at which a client saw it
	// unchanged.
	covered map[string][]span
	// lives are, per key, the lives clients saw it in: created at c, last
	// written at m with version v, still so at r. created, written and
	// deleted list, per revision, the keys a client saw it create, write
	// or delete: the writer of that revision must.
	lives                     map[string]map[life]int64
	created, written, deleted map[int64][]string
	// optional are the ambiguous mutations that change no key and that a
	// later answer needs, by the revision from which they may go, in call
	// order.
	optional []optional
	used     map[int]bool
	// mutations are the acked mutations by return time, which an
	// optional one must follow if they returned before its call, and
	// ready the optional ones whose mutations are all placed.
	mutations []int
	ready     map[int]bool
	// counts are, per revision, how many keys of a range clients saw
	// then, and countRevs those revisions in order. putRevs and delRevs
	// are, per key, the revisions whose acked writer may create or delete
	// it, in order, and putters and deleters the ambiguous mutations that
	// may; revokers are the revokes, which may delete any key (reachable).
	counts            map[int64][]count
	countRevs         []int64
	putRevs, delRevs  [][]int64
	putters, deleters [][]int
	revokers          []int
	// top is the highest revision any client saw.
	top int64
	// failure is the failure at the highest revision reached, and at its
	// revision.
	failure error
	at      int64
	// rev is the revision the search is at, stack its open gaps, and
	// backs how often it went back to one.
	rev   int64
	stack []*frame
	backs int
}

// progress says where the search is, for the report of a timeout.
func (r *revisionOrder) progress() string {
	var b strings.Builder
	fmt.Fprintf(&b, "the search was at revision %d of %d with %d gaps open and had gone back %d times", r.rev, r.top, len(r.stack), r.backs)
	for _, f := range r.stack[max(0, len(r.stack)-3):] {
		fmt.Fprintf(&b, "; gap %d had tried %d of its %d candidates", f.rev, f.next, len(f.cands))
		if f.next > 0 && f.next <= len(f.cands) {
			fmt.Fprintf(&b, ", the last %s", r.w.describe(f.cands[f.next-1]))
		}
	}
	if r.failure != nil {
		fmt.Fprintf(&b, "; the furthest it got was revision %d, where %v", r.at, r.failure)
	}
	return b.String()
}

// optional is an ambiguous mutation that changes no key and may go in
// from revision from on, once the first after acked mutations by return
// time are placed: those that returned before its call. A late one, an
// expiry that a rejection found gone by time gone, goes in as late as
// that allows: before the event of the first op called after gone, and
// by the end of revision by, the lowest header revision of an acked op
// called after gone.
type optional struct {
	op    int
	from  int64
	after int
	late  bool
	gone  int64
	by    int64
	// follow are acked ops that must be placed before it. reads are the
	// acked reads below a compaction's revision, which must still find
	// their answers in the order built before it goes in.
	follow []int
	reads  []int
}

// life is what one observation of a key shows: created at c, last
// written at m, with version v.
type life struct{ c, m, v int64 }

// count is what an answer showed of the keys lo to hi of the key set at
// revision rev: that n of them existed.
type count struct {
	rev    int64
	lo, hi int
	n      int64
}

// span is the revisions after lo up to hi.
type span struct{ lo, hi int64 }

// index sorts the ops into the maps of r.
func (r *revisionOrder) index() error {
	h := r.w.h
	values := map[[2]string][]int{}
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil {
			r.top = max(r.top, op.Resp.Revision)
		}
		switch {
		case op.Req.Range != nil && op.Req.Range.Serializable:
		case op.Resp != nil && bumps(op):
			if j, ok := r.writers[op.Resp.Revision]; ok {
				return fmt.Errorf("%s and op %d both wrote revision %d", r.w.describe(i), j, op.Resp.Revision)
			}
			r.writers[op.Resp.Revision] = i
		case op.Resp != nil && op.Req.LeaseRevoke != nil:
			r.revokes[op.Resp.Revision] = append(r.revokes[op.Resp.Revision], i)
		case op.Resp != nil && mutation(op):
			r.others[op.Resp.Revision] = append(r.others[op.Resp.Revision], i)
		case op.Resp != nil:
			r.seen[op.Resp.Revision] = append(r.seen[op.Resp.Revision], i)
		case op.Err.Ambiguous && op.Req.Mutates():
			if g := op.Req.LeaseGrant; g != nil && g.ID == 0 {
				// Nobody learned its ID, so no answer depends on it.
				continue
			}
			r.ambiguous = append(r.ambiguous, i)
			for _, kv := range putsOf(op.Req) {
				values[kv] = append(values[kv], i)
			}
		}
	}
	for _, ops := range r.others {
		r.byCall(ops)
	}
	r.lowerBounds()
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil && op.Req.Put != nil && op.Req.Put.Lease != 0 {
			r.alive[op.Req.Put.Lease] = max(r.alive[op.Req.Put.Lease], op.Resp.Revision)
		}
		if g := op.Req.LeaseGrant; g != nil {
			id := g.ID
			if op.Resp != nil && op.Resp.LeaseGrant != nil {
				id = op.Resp.LeaseGrant.ID
			}
			r.grants[id]++
		}
	}
	// What clients saw: a key unchanged over a span, and the writer of a
	// value, if they saw the value, and the leases still in use.
	saw := func(kv history.KeyValue, unchangedTo int64, valued bool) {
		if kv.Lease != 0 {
			r.alive[kv.Lease] = max(r.alive[kv.Lease], unchangedTo)
		}
		if kv.ModRevision < unchangedTo {
			r.covered[kv.Key] = append(r.covered[kv.Key], span{kv.ModRevision, unchangedTo})
		}
		r.note(kv, unchangedTo)
		r.top = max(r.top, kv.ModRevision)
		if _, ok := r.writers[kv.ModRevision]; ok || kv.ModRevision < 2 || !valued {
			return
		}
		ops := values[[2]string{kv.Key, kv.Value}]
		if len(ops) != 1 {
			return
		}
		if j, ok := r.resolved[kv.ModRevision]; ok && j != ops[0] {
			// Two values seen at one revision: let the search decide.
			r.resolved[kv.ModRevision] = -1
			return
		}
		r.resolved[kv.ModRevision] = ops[0]
	}
	for i := range h.Ops {
		observed(&h.Ops[i], saw)
	}
	for _, wt := range h.Watches {
		for _, resp := range wt.Responses {
			for _, e := range resp.Events {
				if e.PrevKV != nil {
					saw(*e.PrevKV, e.Revision()-1, true)
				}
				if e.Type == history.EventPut {
					saw(e.KV, e.Revision(), true)
				} else {
					r.deleted[e.Revision()] = append(r.deleted[e.Revision()], e.KV.Key)
				}
			}
		}
	}
	// An ambiguous op seen writing two revisions wrote neither for sure.
	revOf := map[int]int64{}
	for rev, i := range r.resolved {
		if i < 0 {
			delete(r.resolved, rev)
			continue
		}
		if other, ok := revOf[i]; ok {
			delete(r.resolved, rev)
			delete(r.resolved, other)
			continue
		}
		revOf[i] = rev
	}
	for rev, i := range r.resolved {
		r.reserved[i] = rev
	}
	for k, ss := range r.covered {
		r.covered[k] = merge(ss)
	}
	r.indexCounts()
	r.scheduleOptional()
	return nil
}

// countHorizon is how many revisions ahead reachable looks.
const countHorizon = 64

// indexCounts records the counts that acked reads and deletes showed, and
// which writers may change each key.
func (r *revisionOrder) indexCounts() {
	h, s := r.w.h, r.w.live
	add := func(rev int64, key, end string, n int64) {
		lo, hi := s.span(key, end)
		r.counts[rev] = append(r.counts[rev], count{rev: rev, lo: lo, hi: hi, n: n})
	}
	n := len(s.keys)
	r.putters, r.deleters, r.putRevs, r.delRevs = make([][]int, n), make([][]int, n), make([][]int64, n), make([][]int64, n)
	touch := func(i int, req history.Request) {
		acked := h.Ops[i].Resp != nil
		put := func(p *history.PutRequest) {
			if k := s.index(p.Key); k >= 0 {
				if acked {
					r.putRevs[k] = append(r.putRevs[k], h.Ops[i].Resp.Revision)
				} else {
					r.putters[k] = append(r.putters[k], i)
				}
			}
		}
		del := func(d *history.DeleteRequest) {
			lo, hi := s.span(d.Key, d.End)
			for k := lo; k < hi; k++ {
				if acked {
					r.delRevs[k] = append(r.delRevs[k], h.Ops[i].Resp.Revision)
				} else {
					r.deleters[k] = append(r.deleters[k], i)
				}
			}
		}
		switch {
		case req.Put != nil:
			put(req.Put)
		case req.Delete != nil:
			del(req.Delete)
		case req.Txn != nil:
			for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
				if o.Put != nil {
					put(o.Put)
				}
				if o.Delete != nil {
					del(o.Delete)
				}
			}
		case req.LeaseRevoke != nil:
			r.revokers = append(r.revokers, i)
		}
	}
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Req.Mutates() && (op.Resp != nil || op.Err.Ambiguous) {
			touch(i, r.w.reqs[i])
		}
		if op.Resp == nil {
			continue
		}
		rev := op.Resp.Revision
		switch {
		case op.Req.Range != nil && !op.Req.Range.Serializable:
			at := rev
			if op.Req.Range.Revision > 0 {
				at = op.Req.Range.Revision
			}
			add(at, op.Req.Range.Key, op.Req.Range.End, op.Resp.Range.Count)
		case op.Req.Delete != nil:
			at := rev
			if op.Resp.Delete.Deleted > 0 {
				at = rev - 1
			}
			add(at, op.Req.Delete.Key, op.Req.Delete.End, op.Resp.Delete.Deleted)
		}
	}
	for _, revs := range slices.Concat(r.putRevs, r.delRevs) {
		slices.Sort(revs)
	}
	r.countRevs = slices.Sorted(maps.Keys(r.counts))
}

// reachable reports whether the counts clients saw at the next revisions
// can still come true from s, the store right after its revision, once op
// skip is placed: in each range, the keys present that no writer left can
// delete are at most its count, and with the keys absent that a writer
// left can create, at least. Writers left are the acked writers of the
// revisions up to the count's, and the ambiguous mutations not yet placed
// that may come before it. A history whose writes no read sees for a long
// run of revisions made the search try their orders until a count far
// ahead failed them (#251).
//
// Only the counts of the next revision that has any, if within
// countHorizon, are checked, so that each candidate costs little.
func (r *revisionOrder) reachable(s *state, skip int) bool {
	next := sort.Search(len(r.countRevs), func(k int) bool { return r.countRevs[k] >= s.rev })
	if next == len(r.countRevs) || r.countRevs[next] > s.rev+countHorizon {
		return true
	}
	rev := r.countRevs[next]
	for _, c := range r.counts[rev] {
		var sure, maybe int64
		for k := c.lo; k < c.hi; k++ {
			present := s.kvs[k].Version != 0
			if present && !r.may(s, k, s.rev, rev, skip, false) {
				sure++
			}
			if present || r.may(s, k, s.rev, rev, skip, true) {
				maybe++
			}
		}
		if sure > c.n || maybe < c.n {
			return false
		}
	}
	return true
}

// may reports whether a writer may create key k, or delete it, after
// revision now and up to rev: an acked writer of one of those revisions,
// or an ambiguous mutation other than skip not yet placed whose lower
// bound is below rev. Any revoke may delete a key.
func (r *revisionOrder) may(s *state, k int, now, rev int64, skip int, create bool) bool {
	ok := func(i int) bool {
		op := &r.w.h.Ops[i]
		if op.Resp != nil {
			return op.Resp.Revision > now && op.Resp.Revision <= rev
		}
		if i == skip || r.used[i] || r.lb[i] >= rev {
			return false
		}
		if txn := op.Req.Txn; txn != nil {
			// A positive old mod revision cannot be regained. Once one
			// equality has become impossible, only the failure branch
			// can contribute to the future count. Counting success puts
			// here hides an irrecoverable deletion behind permutations
			// of all the remaining gaps (#281).
			for _, c := range txn.Compare {
				kv, _ := s.get(c.Key)
				if c.Target != history.TargetMod || c.Result != history.Equal || c.Number <= 0 || c.Number > now || kv.ModRevision == c.Number {
					continue
				}
				return slices.ContainsFunc(txn.Failure, func(o history.TxnOp) bool {
					if create {
						return o.Put != nil && o.Put.Key == s.keys[k]
					}
					if o.Delete == nil {
						return false
					}
					lo, hi := s.span(o.Delete.Key, o.Delete.End)
					return lo <= k && k < hi
				})
			}
		}
		return true
	}
	acked := func(revs []int64) bool {
		j := sort.Search(len(revs), func(j int) bool { return revs[j] > now })
		return j < len(revs) && revs[j] <= rev
	}
	if create {
		return acked(r.putRevs[k]) || slices.ContainsFunc(r.putters[k], ok)
	}
	return acked(r.delRevs[k]) || slices.ContainsFunc(r.deleters[k], ok) || slices.ContainsFunc(r.revokers, ok)
}

// note records what an observation of kv, unchanged up to revision to,
// shows of the key's life. One without a version, from a compare of the
// mod revision, shows only the revision of its last write.
func (r *revisionOrder) note(kv history.KeyValue, to int64) {
	if kv.ModRevision < 2 {
		return
	}
	if kv.Version == 0 {
		r.written[kv.ModRevision] = append(r.written[kv.ModRevision], kv.Key)
		return
	}
	l := life{kv.CreateRevision, kv.ModRevision, kv.Version}
	m := r.lives[kv.Key]
	if m == nil {
		m = map[life]int64{}
		r.lives[kv.Key] = m
	}
	if old, ok := m[l]; ok && old >= to {
		return
	}
	if _, ok := m[l]; !ok {
		r.created[l.c] = append(r.created[l.c], kv.Key)
		r.written[l.m] = append(r.written[l.m], kv.Key)
	}
	m[l] = to
}

func (r *revisionOrder) byCall(ops []int) {
	h := r.w.h
	slices.SortFunc(ops, func(a, b int) int { return cmp.Or(cmp.Compare(h.Ops[a].Call, h.Ops[b].Call), cmp.Compare(a, b)) })
}

// lowerBounds sets lb: for each ambiguous op, the highest header revision
// of an acked op that returned before its call.
func (r *revisionOrder) lowerBounds() {
	h := r.w.h
	var acked []int
	for i := range h.Ops {
		if op := &h.Ops[i]; op.Resp != nil && (op.Req.Range == nil || !op.Req.Range.Serializable) {
			acked = append(acked, i)
		}
	}
	slices.SortFunc(acked, func(a, b int) int { return cmp.Compare(h.Ops[a].Return, h.Ops[b].Return) })
	r.highest = make([]int64, len(acked))
	r.returns = make([]int64, len(acked))
	for k, i := range acked {
		r.returns[k] = h.Ops[i].Return
		r.highest[k] = h.Ops[i].Resp.Revision
		if k > 0 {
			r.highest[k] = max(r.highest[k], r.highest[k-1])
		}
	}
	byCall := slices.Clone(acked)
	slices.SortFunc(byCall, func(a, b int) int { return cmp.Compare(h.Ops[a].Call, h.Ops[b].Call) })
	r.calls = make([]int64, len(byCall))
	r.lowest = make([]int64, len(byCall))
	for k := len(byCall) - 1; k >= 0; k-- {
		i := byCall[k]
		r.calls[k], r.lowest[k] = h.Ops[i].Call, h.Ops[i].Resp.Revision
		if k+1 < len(byCall) {
			r.lowest[k] = min(r.lowest[k], r.lowest[k+1])
		}
	}
	for _, i := range r.ambiguous {
		r.lb[i] = r.boundAt(h.Ops[i].Call)
	}
}

// lowestAfter is the lowest header revision of an acked op called after
// time t, or the top revision.
func (r *revisionOrder) lowestAfter(t int64) int64 {
	n := sort.Search(len(r.calls), func(k int) bool { return r.calls[k] > t })
	if n == len(r.calls) {
		return r.top
	}
	return r.lowest[n]
}

// boundAt is the highest header revision of an acked op that returned
// before time t, or 1.
func (r *revisionOrder) boundAt(t int64) int64 {
	n := sort.Search(len(r.returns), func(k int) bool { return r.returns[k] >= t })
	if n == 0 {
		return 1
	}
	return r.highest[n-1]
}

// putsOf lists the key and value of every put a request can make.
func putsOf(req history.Request) [][2]string {
	var out [][2]string
	switch {
	case req.Put != nil:
		out = append(out, [2]string{req.Put.Key, req.Put.Value})
	case req.Txn != nil:
		for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
			if o.Put != nil {
				out = append(out, [2]string{o.Put.Key, o.Put.Value})
			}
		}
	}
	return out
}

// observed calls saw with every key an acked op saw, the revision up to
// which the key had not changed since, and whether the op saw its value:
// a keys-only read and a compare of the mod revision do not.
func observed(op *history.Op, saw func(kv history.KeyValue, unchangedTo int64, valued bool)) {
	if op.Resp == nil {
		return
	}
	rev := op.Resp.Revision
	// Up to which revision what the op read before its own write held.
	before := rev
	if bumps(op) {
		before = rev - 1
	}
	reads := func(q *history.RangeRequest, got *history.RangeResponse, at int64) {
		if q.Revision > 0 {
			at = q.Revision
		}
		for _, kv := range got.KVs {
			saw(kv, at, !q.KeysOnly)
		}
	}
	prev := func(kv *history.KeyValue) {
		if kv != nil {
			saw(*kv, before, true)
		}
	}
	switch {
	case op.Req.Range != nil:
		reads(op.Req.Range, op.Resp.Range, rev)
	case op.Req.Put != nil:
		prev(op.Resp.Put.PrevKV)
	case op.Req.Delete != nil:
		for k := range op.Resp.Delete.PrevKVs {
			prev(&op.Resp.Delete.PrevKVs[k])
		}
	case op.Req.Txn != nil:
		branch := op.Req.Txn.Success
		if !op.Resp.Txn.Succeeded {
			branch = op.Req.Txn.Failure
		}
		wrote := false
		for k, o := range branch {
			res := op.Resp.Txn.Results[k]
			switch {
			case o.Range != nil && !wrote:
				reads(o.Range, res.Range, before)
			case o.Put != nil:
				prev(res.Put.PrevKV)
				wrote = true
			case o.Delete != nil:
				for m := range res.Delete.PrevKVs {
					prev(&res.Delete.PrevKVs[m])
				}
				wrote = wrote || res.Delete.Deleted > 0
			}
		}
		if op.Resp.Txn.Succeeded {
			for _, c := range op.Req.Txn.Compare {
				if c.Target == history.TargetMod && c.Result == history.Equal && c.Number > 0 {
					saw(history.KeyValue{Key: c.Key, ModRevision: c.Number}, before, false)
				}
			}
		}
	}
}

// merge sorts spans and joins those that overlap.
func merge(ss []span) []span {
	slices.SortFunc(ss, func(a, b span) int { return cmp.Compare(a.lo, b.lo) })
	out := ss[:0]
	for _, s := range ss {
		if n := len(out); n > 0 && s.lo <= out[n-1].hi {
			out[n-1].hi = max(out[n-1].hi, s.hi)
			continue
		}
		out = append(out, s)
	}
	return out
}

// unchanged reports whether a client saw key unchanged at revision rev:
// no write at rev changed it.
func (r *revisionOrder) unchanged(key string, rev int64) bool {
	ss := r.covered[key]
	k := sort.Search(len(ss), func(k int) bool { return ss[k].hi >= rev })
	return k < len(ss) && ss[k].lo < rev
}

// scheduleOptional picks the ambiguous mutations that change no key and
// that a later rejection needs: a revoke of a lease that a later call
// found gone, a compaction that a later read found, a grant whose lease
// a later call used. Each goes after the last revision that needs it left
// out, and never before its call.
func (r *revisionOrder) scheduleOptional() {
	h := r.w.h
	// What the answers need of each lease and of compaction.
	// present is, per lease, the highest revision at which an answer
	// shows it in use, and gone the earliest return of a rejection that
	// found it gone.
	present, gone := map[int64]int64{}, map[int64]int64{}
	granted, revoked := map[int64]bool{}, map[int64]bool{}
	// compacted are the compacted rejections, by the revision they asked
	// for and their return; reads are the acked reads at a revision, and
	// compactions the acked compactions.
	type rejection struct{ rev, ret int64 }
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
			observed(op, func(kv history.KeyValue, to int64, _ bool) { note(kv.Lease, to) })
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
			for _, lease := range leasesOf(op.Req) {
				note(lease, rev)
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
			compacted = append(compacted, rejection{requestedRevision(op.Req), op.Return})
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
	slices.SortFunc(r.mutations, func(a, b int) int { return cmp.Compare(h.Ops[a].Return, h.Ops[b].Return) })
	revoking := map[int64]bool{}
	for _, i := range r.ambiguous {
		op := &h.Ops[i]
		// Not before the revision of the ops that returned before its
		// call; insertOptional also waits for their events.
		at := r.lb[i]
		// Not before the acked mutations that change no key and that
		// returned before call.
		call := op.Call
		o := optional{op: i}
		switch {
		case op.Req.LeaseRevoke != nil:
			id := op.Req.LeaseRevoke.ID
			// An acked revoke explains a lease found gone.
			goneAt, ok := gone[id]
			if !ok || revoked[id] || revoking[id] {
				continue
			}
			revoking[id] = true
			// As late as the answers allow, for an ambiguous put may
			// still have used the lease: a rejection that found the lease
			// gone, which may take effect up to its return, comes before
			// every op called after it returned. So the expiry goes in
			// before the event of the first such op, acked or not, and by
			// the end of the lowest header revision of an acked one,
			// which may have changed nothing. Where the first event is
			// depends on where the search puts the ambiguous ops, so
			// expire puts it in as the search goes, and a wrong guess is
			// undone with the rest. An expiry never returns, so real time
			// allows any later place.
			at = max(at, present[id])
			o.late, o.gone, o.by = true, goneAt, r.lowestAfter(goneAt)
		case op.Req.LeaseGrant != nil:
			id := op.Req.LeaseGrant.ID
			if _, ok := present[id]; !ok || granted[id] {
				continue
			}
		case op.Req.Compact != nil:
			// As late as the answers allow, as an expiry above: a
			// rejection that found the revision it asked for compacted
			// comes before every op called after it returned, so the
			// compaction goes in before the event of the first such op,
			// and by the end of the lowest header revision of an acked
			// one. It goes no earlier than the header revisions of the
			// acked reads below its revision, at which it can still come
			// after them: a rejection can need it between two reads of one
			// revision. They must find their answers before it, so it
			// waits for the mutations that returned before they were
			// called, and goes in only once the order built so far answers
			// them and the ops that returned before them (readsFit, #277).
			// And after the acked compactions to its revision or below, whose
			// answers it would turn into errors, but at their revision,
			// before the ops that follow them there (#258).
			c := op.Req.Compact.Revision
			goneAt := int64(math.MaxInt64)
			for _, rj := range compacted {
				if rj.rev <= c {
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

// maxSlotOrder bounds the acked mutations of one revision whose orders
// slotOrder tries.
const maxSlotOrder = 6

// slotOrder returns an order of ops, acked mutations that change no key
// at the current revision, in call order: that one if the model gives
// each its answer in it, else the first order that follows real time in
// which it does, else call order. Two compactions at one revision, for
// one, must go in the order of their revisions.
func (r *revisionOrder) slotOrder(ops []int) []int {
	if len(ops) < 2 || len(ops) > maxSlotOrder || r.answered(ops) {
		return ops
	}
	order := make([]int, 0, len(ops))
	used := make([]bool, len(ops))
	var try func() bool
	try = func() bool {
		if len(order) == len(ops) {
			return r.answered(order)
		}
		for k, i := range ops {
			if used[k] {
				continue
			}
			used[k] = true
			order = append(order, i)
			if try() {
				return true
			}
			order = order[:len(order)-1]
			used[k] = false
		}
		return false
	}
	if try() {
		return order
	}
	return ops
}

// answered reports whether ops, in this order after the live store,
// follow real time and get their answers from the model.
func (r *revisionOrder) answered(ops []int) bool {
	w := r.w
	s := w.live.clone()
	for k, i := range ops {
		for _, j := range ops[:k] {
			if w.h.Ops[i].Return < w.h.Ops[j].Call {
				return false
			}
		}
		if o := s.step(w.reqs[i], false); o.err != nil || !matches(o, w.h.Ops[i].Resp) {
			return false
		}
	}
	return true
}

// insertOptional puts in, at revision rev, every optional mutation that
// may go in by now and that the model takes without a write.
func (r *revisionOrder) insertOptional(rev int64) {
	w := r.w
	for _, o := range r.optional {
		if o.late || o.from > rev || r.used[o.op] || !r.isReady(o) {
			continue
		}
		s := w.live.clone()
		if out := s.step(w.reqs[o.op], false); out.err == nil && s.rev == rev {
			r.expire(w.h.Ops[o.op].Call, 0)
			w.apply(o.op, w.reqs[o.op])
			r.used[o.op] = true
		}
	}
}

// isReady reports whether the acked mutations that optional o must
// follow are all placed.
func (r *revisionOrder) isReady(o optional) bool {
	if r.ready[o.op] {
		return true
	}
	w := r.w
	unplaced := func(j int) bool { return !w.at[j].set }
	if slices.ContainsFunc(r.mutations[:o.after], unplaced) || slices.ContainsFunc(o.follow, unplaced) {
		return false
	}
	r.ready[o.op] = true
	return true
}

// readsFit reports whether the order built so far answers the acked reads
// that optional o must come after, with the ops that change nothing and
// that returned before the last of them was called, as observe would
// place them. A rejection among those can need another optional
// compaction first, which then goes in before o (#277, seed 8238917).
func (r *revisionOrder) readsFit(o optional) bool {
	if len(o.reads) == 0 {
		return true
	}
	w := r.w
	var call int64
	for _, k := range o.reads {
		call = max(call, w.h.Ops[k].Call)
	}
	ops := slices.Clone(o.reads)
	for i := range w.h.Ops {
		op := &w.h.Ops[i]
		if op.Return < call && !w.at[i].set && w.seen(i) && (op.Resp == nil || !mutation(op)) && !slices.Contains(o.reads, i) {
			ops = append(ops, i)
		}
	}
	return w.placeSeen(ops, false) == nil
}

// due lists the late optional mutations not yet placed that must go in
// before the event of an op called at time call, or by the end of
// revision rev; all of them if call is past every op. They come in call
// order: one called before a rejection returned may have to go in before
// one called after it, which ends the rejection's states.
func (r *revisionOrder) due(call, rev int64) []optional {
	var out []optional
	for _, o := range r.optional {
		if o.late && !r.used[o.op] && (o.gone < call || o.by <= rev) {
			out = append(out, o)
		}
	}
	slices.SortStableFunc(out, func(a, b optional) int { return cmp.Compare(r.w.h.Ops[a.op].Call, r.w.h.Ops[b.op].Call) })
	return out
}

// takes steps s through late optional o if it may go in now and the
// model takes it without a write.
func (r *revisionOrder) takes(s *state, o optional) bool {
	if o.from > s.rev || !r.isReady(o) || !r.readsFit(o) {
		return false
	}
	rev := s.rev
	out := s.step(r.w.reqs[o.op], false)
	return out.err == nil && s.rev == rev
}

// expire puts in the late optional mutations due before the event of an
// op called at time call, or by the end of revision rev. One that cannot
// go in now stays out, and the answer that needs it fails.
func (r *revisionOrder) expire(call, rev int64) {
	for _, o := range r.due(call, rev) {
		r.putIn(o, r.takes)
	}
}

// putIn puts in late optional o if take says it may go in now. A
// compaction is called at its own time, which can be after the rejection
// that an expiry must precede, so the expiries due before the event of an
// op called when it was go in first, as for any other op. An expiry is
// called when its lease is granted, so nothing goes in before it.
func (r *revisionOrder) putIn(o optional, take func(*state, optional) bool) {
	w := r.w
	if r.used[o.op] {
		return
	}
	if w.h.Ops[o.op].Req.Compact != nil {
		for _, e := range r.due(w.h.Ops[o.op].Call, 0) {
			if w.h.Ops[e.op].Req.LeaseRevoke != nil && !r.used[e.op] && r.takes(w.live.clone(), e) {
				w.apply(e.op, w.reqs[e.op])
				r.used[e.op] = true
			}
		}
	}
	if take(w.live.clone(), o) {
		w.apply(o.op, w.reqs[o.op])
		r.used[o.op] = true
	}
}

// expireEarly puts in the late optional mutations due by the end of
// revision rev before the acked ops of rev that change no key. Those write
// nothing that an ambiguous put of the lease could come after, and an
// answer called after the rejection that found the lease gone may have to
// come before them, such as a read below a compaction of rev (#237). One
// that an answer at rev shows in use waits for the end of rev.
func (r *revisionOrder) expireEarly(rev int64) {
	w := r.w
	for _, o := range r.due(0, rev) {
		if l := w.h.Ops[o.op].Req.LeaseRevoke; l != nil && r.alive[l.ID] >= rev {
			continue
		}
		r.putIn(o, r.takes)
	}
}

// expireEnd puts in, past every revision a client saw, the late optional
// mutations not yet placed. Unlike expire, it lets them write: no answer
// shows the revisions they write. An expiry of a lease that still has
// keys deletes them, and when no client saw a revision after it, it can
// go in nowhere else (#237).
func (r *revisionOrder) expireEnd() {
	w := r.w
	for _, o := range r.due(math.MaxInt64, math.MaxInt64) {
		r.putIn(o, func(s *state, o optional) bool {
			return o.from <= s.rev && r.isReady(o) && r.readsFit(o) && s.step(w.reqs[o.op], false).err == nil
		})
	}
}

// leasesOf lists the leases the puts of a request use.
func leasesOf(req history.Request) []int64 {
	var out []int64
	switch {
	case req.Put != nil && req.Put.Lease != 0:
		out = append(out, req.Put.Lease)
	case req.Txn != nil:
		for _, o := range slices.Concat(req.Txn.Success, req.Txn.Failure) {
			if o.Put != nil && o.Put.Lease != 0 {
				out = append(out, o.Put.Lease)
			}
		}
	}
	return out
}

// mark is a point of the search to go back to.
type mark struct {
	events, appended, placed int
	live                     *state
	used                     map[int]bool
}

// frame is a gap of the search: its revision, the candidates to write it,
// and the next one to try.
type frame struct {
	rev   int64
	cands []int
	next  int
	mark  mark
}

func (r *revisionOrder) save() mark {
	w := r.w
	return mark{events: len(w.events), appended: len(w.appended), placed: len(w.placed), live: w.live.clone(), used: maps.Clone(r.used)}
}

func (r *revisionOrder) restore(m mark) {
	w := r.w
	w.events, w.states = w.events[:m.events], w.states[:m.events+1]
	for k := len(w.appended) - 1; k >= m.appended; k-- {
		key := w.appended[k]
		w.past[key] = w.past[key][:len(w.past[key])-1]
	}
	w.appended = w.appended[:m.appended]
	for _, i := range w.placed[m.placed:] {
		w.at[i] = place{}
	}
	w.placed = w.placed[:m.placed]
	// What observe placed belongs to the order given up.
	for i, p := range w.at {
		if p.seen {
			w.at[i] = place{}
		}
	}
	w.live = m.live.clone()
	w.kvsRev = -1
	clear(r.ready)
	r.used = maps.Clone(m.used)
}

// dead notes why the search cannot go on at revision rev.
func (r *revisionOrder) dead(rev int64, err error) bool {
	if r.failure == nil || rev >= r.at {
		r.failure, r.at = err, rev
	}
	return false
}

// search builds the order revision by revision. Once every revision has
// a writer, it places the ops that changed nothing and checks real time;
// if either fails, the search goes on with the next candidate of the last
// gap, as after a revision it cannot explain.
func (r *revisionOrder) search() error {
	w := r.w
	if !r.finish(1) {
		return r.failure
	}
	for rev := int64(2); ; {
		r.rev = rev
		if w.tick() {
			return errWitnessTimeout
		}
		var ok bool
		if rev > r.top {
			r.expireEnd()
			err := w.observe()
			if err == nil {
				err = w.verify()
			}
			if err == nil {
				return nil
			}
			if errors.Is(err, errWitnessTimeout) {
				return err
			}
			ok = r.dead(rev, err)
		} else {
			ok = r.write(rev) && r.finish(rev)
		}
		if ok {
			rev++
			continue
		}
		// Back to the last gap, for its next candidate.
		if len(r.stack) == 0 {
			return r.failure
		}
		f := r.stack[len(r.stack)-1]
		r.restore(f.mark)
		r.backs++
		rev = f.rev
	}
}

// write places the writer of rev, the next candidate if rev is a gap.
func (r *revisionOrder) write(rev int64) bool {
	w := r.w
	if i, ok := r.writers[rev]; ok {
		r.expire(w.h.Ops[i].Call, 0)
		o := w.apply(i, w.reqs[i])
		if o.err != nil || !matches(o, w.h.Ops[i].Resp) {
			return r.dead(rev, fmt.Errorf("acked %s, which wrote revision %d, gets %s from the model in the order so far; the order around it: %s", w.describe(i), rev, outputOf(o), w.around(len(w.events))))
		}
		return true
	}
	if i, ok := r.resolved[rev]; ok && !r.used[i] && r.lb[i] < rev {
		if r.fits(i, rev) {
			r.expire(w.h.Ops[i].Call, 0)
			w.apply(i, w.reqs[i])
			r.used[i] = true
			return true
		}
		return r.dead(rev, fmt.Errorf("%s, whose value a client saw at revision %d, cannot write it in the order so far; the order around it: %s", w.describe(i), rev, w.around(len(w.events))))
	}
	var f *frame
	if n := len(r.stack); n > 0 && r.stack[n-1].rev == rev {
		f = r.stack[n-1]
	} else {
		f = &frame{rev: rev, mark: r.save(), cands: r.candidates(rev)}
		r.stack = append(r.stack, f)
	}
	for ; f.next < len(f.cands); f.next++ {
		i := f.cands[f.next]
		if r.used[i] || !r.fits(i, rev) {
			continue
		}
		r.expire(w.h.Ops[i].Call, 0)
		w.apply(i, w.reqs[i])
		r.used[i] = true
		f.next++
		return true
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r.dead(rev, fmt.Errorf("no acked op wrote revision %d, and no ambiguous mutation or acked revoke can have written it in the order so far; the order before it: %s", rev, w.around(len(w.events))))
}

// candidates are the ops that may write gap rev: the acked revokes at it
// first, then the ambiguous mutations that no answer puts after it and
// that no client saw write another revision, earliest call first. When a
// fault hides a long prefix of writes, this tries the puts before later
// deletes that may depend on them. Reversing that order can exhaust the
// search on permutations before it reaches the first observed revision.
// Ambiguous revokes come last: a lease expiry is called when its lease
// is granted, so by call it would come first in every gap, and the
// search could try every order of the gaps after a wrong one before it
// found the delete that wrote it (seed 59113215 on #238). Call order is
// only a preference: the search still tries other orders, and verifies
// every response and real-time constraint before passing.
func (r *revisionOrder) candidates(rev int64) []int {
	h := r.w.h
	out := slices.Clone(r.revokes[rev])
	var amb []int
	for _, i := range r.ambiguous {
		if at, ok := r.reserved[i]; ok && at != rev {
			continue
		}
		if l := h.Ops[i].Req.LeaseRevoke; l != nil && r.grants[l.ID] <= 1 && r.alive[l.ID] >= rev {
			// An answer shows the lease in use at rev or later (#237).
			continue
		}
		if !r.used[i] && r.lb[i] < rev {
			amb = append(amb, i)
		}
	}
	revoke := func(i int) int {
		if h.Ops[i].Req.LeaseRevoke != nil {
			return 1
		}
		return 0
	}
	slices.SortFunc(amb, func(a, b int) int {
		return cmp.Or(cmp.Compare(revoke(a), revoke(b)), cmp.Compare(h.Ops[a].Call, h.Ops[b].Call), cmp.Compare(a, b))
	})
	return append(out, amb...)
}

// fits reports whether op i can write revision rev next, after the
// expiries due before it: the model says it writes exactly rev, it changes
// no key that a client saw unchanged at rev, and an acked revoke gets its
// answer.
func (r *revisionOrder) fits(i int, rev int64) bool {
	w := r.w
	s := w.live.clone()
	for _, e := range r.due(w.h.Ops[i].Call, 0) {
		if t := s.clone(); r.takes(t, e) {
			s = t
		}
	}
	o := s.step(w.reqs[i], true)
	if o.err != nil || s.rev != rev {
		return false
	}
	if op := &w.h.Ops[i]; op.Resp != nil && !matches(o, op.Resp) {
		return false
	}
	for _, e := range o.events {
		if r.unchanged(e.KV.Key, rev) {
			return false
		}
		// A key it changes inside a life a client saw stays in that
		// life: created then, with no more versions than seen.
		for l, to := range r.lives[e.KV.Key] {
			if l.c <= rev && rev <= to {
				kv, ok := s.get(e.KV.Key)
				if !ok || kv.CreateRevision != l.c || kv.Version > l.v {
					return false
				}
			}
		}
	}
	// What clients saw happen at rev must happen.
	for _, k := range r.created[rev] {
		if kv, ok := s.get(k); !ok || kv.CreateRevision != rev {
			return false
		}
	}
	for _, k := range r.written[rev] {
		if kv, ok := s.get(k); !ok || kv.ModRevision != rev {
			return false
		}
	}
	for _, k := range r.deleted[rev] {
		if _, ok := s.get(k); ok || !slices.ContainsFunc(o.events, func(e history.Event) bool { return e.KV.Key == k }) {
			return false
		}
	}
	return r.reachable(s, i)
}

// finish places what follows the writer of rev: the acked mutations that
// changed no key at it, in call order, and the optional ones as early as
// the model takes them without a write, and checks that every acked op
// that changed nothing at rev has a state that answers it.
func (r *revisionOrder) finish(rev int64) bool {
	w := r.w
	r.insertOptional(rev)
	r.expireEarly(rev)
	var acked []int
	for _, i := range slices.Concat(r.others[rev], r.revokes[rev]) {
		if !r.used[i] {
			acked = append(acked, i)
		}
	}
	r.byCall(acked)
	for _, i := range r.slotOrder(acked) {
		if r.used[i] {
			continue
		}
		r.expire(w.h.Ops[i].Call, 0)
		o := w.apply(i, w.reqs[i])
		if o.err != nil || !matches(o, w.h.Ops[i].Resp) {
			return r.dead(rev, fmt.Errorf("acked %s gets %s from the model in the order so far; the order around it: %s", w.describe(i), outputOf(o), w.around(len(w.events))))
		}
		r.used[i] = true
		r.insertOptional(rev)
	}
	r.expire(0, rev)
	first := len(w.states) - 1
	for first > 0 && w.states[first-1].rev == rev {
		first--
	}
	for _, i := range r.seen[rev] {
		if _, ok := w.find(i, first, len(w.states)-1); !ok {
			return r.dead(rev, w.unanswered(i, first, len(w.states)-1, first, len(w.states)-1))
		}
	}
	return true
}
