package check

import (
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// scanOrder is a revisionOrder of n ops with random lower bounds, along
// which used only grows, with a restore now and then, as along a search.
type scanOrder struct {
	r   *revisionOrder
	rng *rand.Rand
}

func newScanOrder(seed uint64, n int, op func(rng *rand.Rand) history.Op) *scanOrder {
	rng := rand.New(rand.NewPCG(seed, 521))
	h := &history.History{}
	for range n {
		h.Ops = append(h.Ops, op(rng))
	}
	w := newWitness(h, time.Now().Add(time.Hour))
	r := &revisionOrder{w: w, used: make([]bool, n), lb: make([]int64, n)}
	for i := range n {
		r.lb[i] = rng.Int64N(50)
	}
	return &scanOrder{r, rng}
}

// step uses an op not used yet, or goes back to an earlier set of used
// ops as restore does.
func (o *scanOrder) step() {
	r := o.r
	if o.rng.IntN(8) == 0 {
		for i := range r.used {
			if r.used[i] && o.rng.IntN(2) == 0 {
				r.used[i] = false
			}
		}
		r.dues, r.revokePrefix = nil, 0
		return
	}
	r.used[o.rng.IntN(len(r.used))] = true
}

// TestMayRevokeMatchesScan compares mayRevoke with a scan of every
// ambiguous revoke: one other than skip, not used yet, whose lower bound
// is below rev (#499).
func TestMayRevokeMatchesScan(t *testing.T) {
	o := newScanOrder(1, 300, func(rng *rand.Rand) history.Op {
		op := history.Op{Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}}}
		if rng.IntN(3) == 0 {
			op.Resp = &history.Response{}
		}
		return op
	})
	r := o.r
	for i := range r.w.h.Ops {
		r.revokers = append(r.revokers, i)
	}
	for step := range 5000 {
		o.step()
		rev, skip := o.rng.Int64N(60), o.rng.IntN(len(r.used))-1
		want := slices.ContainsFunc(r.revokers, func(i int) bool {
			return r.w.h.Ops[i].Resp == nil && i != skip && !r.used[i] && r.lb[i] < rev
		})
		if got := r.mayRevoke(rev, skip); got != want {
			t.Fatalf("step %d: mayRevoke(%d, %d) = %v, the scan says %v", step, rev, skip, got, want)
		}
	}
}

// TestDueIndexMatchesScan compares lateDue and earlyDue with a scan of
// every optional mutation, in r.optional's order: the late ones not used
// yet that are gone before call or due by rev, and the early ones not used
// yet that may go in at rev (#499).
func TestDueIndexMatchesScan(t *testing.T) {
	o := newScanOrder(2, 300, func(*rand.Rand) history.Op {
		return history.Op{Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}}}
	})
	r := o.r
	for i := range r.w.h.Ops {
		opt := optional{op: i, late: o.rng.IntN(2) == 0}
		if opt.late {
			opt.gone, opt.by = o.rng.Int64N(100), o.rng.Int64N(60)
		} else {
			opt.from = o.rng.Int64N(60)
		}
		r.optional = append(r.optional, opt)
	}
	ops := func(os []optional) []int {
		var out []int
		for _, o := range os {
			out = append(out, o.op)
		}
		return out
	}
	for step := range 5000 {
		o.step()
		call, rev := o.rng.Int64N(110), o.rng.Int64N(60)
		var late, early []int
		for _, opt := range r.optional {
			switch {
			case r.used[opt.op]:
			case opt.late && (opt.gone < call || opt.by <= rev):
				late = append(late, opt.op)
			case !opt.late && opt.from <= rev:
				early = append(early, opt.op)
			}
		}
		if got := ops(r.lateDue(call, rev)); !slices.Equal(got, late) {
			t.Fatalf("step %d: lateDue(%d, %d) = %v, the scan says %v", step, call, rev, got, late)
		}
		if got := ops(r.earlyDue(rev)); !slices.Equal(got, early) {
			t.Fatalf("step %d: earlyDue(%d) = %v, the scan says %v", step, rev, got, early)
		}
	}
}

// TestVersionsAtMatchesSearch compares versions.at, which starts where its
// last lookup ended, with a binary search of every version, along random
// adds, drops as restore does, and lookups at any revision.
func TestVersionsAtMatchesSearch(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 521))
	v := versions{}
	var all []history.KeyValue
	rev := int64(1)
	for step := range 20_000 {
		switch n := rng.IntN(10); {
		case n < 4:
			rev += rng.Int64N(3)
			kv := history.KeyValue{Key: "a", ModRevision: rev, Version: rng.Int64N(3)}
			v.add([]history.Event{{KV: kv}})
			all = append(all, kv)
		case n == 4 && len(all) > 0:
			v.drop("a")
			all = all[:len(all)-1]
			if len(all) > 0 {
				rev = all[len(all)-1].ModRevision
			}
		}
		at := rng.Int64N(rev + 3)
		if rng.IntN(2) == 0 {
			at = rev - rng.Int64N(3)
		}
		i := sort.Search(len(all), func(j int) bool { return all[j].ModRevision > at })
		want, wantOK := history.KeyValue{}, i > 0 && all[i-1].Version != 0
		if wantOK {
			want = all[i-1]
		}
		if got, ok := v.at("a", at); ok != wantOK || got != want {
			t.Fatalf("step %d: at(%d) = %v %v, the search says %v %v", step, at, got, ok, want, wantOK)
		}
	}
}
