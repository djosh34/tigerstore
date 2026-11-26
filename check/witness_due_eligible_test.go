package check

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// The #876 profile spends most of its time revisiting due expiries whose
// from revision is still ahead. Warm queries must not walk that future set,
// whether asked by call, by revision, or during a trial gap writer.
func TestFutureDueWorkScales(t *testing.T) {
	for _, n := range []int{128, 8192} {
		for _, mode := range []string{"expire", "early", "fits"} {
			t.Run(fmt.Sprintf("%s/%d", mode, n), func(t *testing.T) {
				h := &history.History{}
				for i := range n {
					h.Ops = append(h.Ops, history.Op{Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: int64(i + 1)}}})
				}
				h.Ops = append(h.Ops, history.Op{Call: 2, Req: history.Request{Put: &history.PutRequest{Key: "key", Value: "value"}}})
				r := newRevisionOrder(newWitness(h, time.Now().Add(time.Minute)))
				for i := range n {
					r.optional = append(r.optional, optional{op: i, late: true, from: 10_000, gone: 1, by: 1})
				}
				query := func() {
					switch mode {
					case "expire":
						r.expire(2, 0)
					case "early":
						r.expireEarly(1)
					case "fits":
						if !r.fits(n, 2) {
							t.Fatal("future expiries changed the fitting put")
						}
					}
				}
				query() // Build and warm the retained sets outside the bound.
				before := r.w.steps
				for range 128 {
					query()
				}
				if work := r.w.steps - before; work > 4 {
					t.Fatalf("128 warmed queries visited future expiries: %d weighted steps, want <=4 independent of %d candidates", work, n)
				}
				if r.w.stopped || r.w.live.rev != 1 {
					t.Fatal("future-only queries stopped or changed the order")
				}
			})
		}
	}
}

func TestEligibleDueMatchesReference(t *testing.T) {
	o := newScanOrder(876, 257, func(rng *rand.Rand) history.Op {
		return history.Op{Call: rng.Int64N(20), Req: history.Request{Compact: &history.CompactRequest{Revision: 1 + rng.Int64N(30)}}}
	})
	r := o.r
	for i := range r.used {
		r.optional = append(r.optional, optional{op: i, late: true, from: o.rng.Int64N(80) - 10, gone: o.rng.Int64N(90) - 10, by: o.rng.Int64N(60) - 10})
	}
	for step := range 2000 {
		if o.rng.IntN(8) == 0 {
			clear(r.used)
			r.w.live.compacted = 0
			r.dues = nil
			if r.dueWork != nil {
				r.dueWork.valid = false
			}
		} else {
			r.use(o.rng.IntN(len(r.used)))
			r.w.live.compacted += o.rng.Int64N(2)
		}
		call, rev, from := o.rng.Int64N(100)-10, o.rng.Int64N(70)-10, o.rng.Int64N(100)-20
		switch step % 4 {
		case 0:
			call = 0
		case 1:
			rev = 0
		case 2:
			from = math.MaxInt64
		case 3:
			from = math.MinInt64
		}
		want := slices.DeleteFunc(referenceDue(r, call, rev), func(o optional) bool { return o.from > from })
		var got []optional
		for candidate := range r.dueAt(call, rev, from) {
			// Moving all thresholds in a nested query must not change the
			// outer snapshot or tied-call order, including its eligibility.
			for range r.dueAt(90-call, 60-rev, -from) {
				break
			}
			got = append(got, candidate)
		}
		if !slices.EqualFunc(got, want, func(a, b optional) bool { return a.op == b.op }) {
			t.Fatalf("step %d, dueAt(%d, %d, %d): got %v, want %v", step, call, rev, from, got, want)
		}
	}
}

func TestEligibleDueStopsWhileMovingThreshold(t *testing.T) {
	for _, forward := range []bool{false, true} {
		t.Run(fmt.Sprint("forward=", forward), func(t *testing.T) {
			r := dueShape(150_000)
			bound, next := int64(10_000), int64(0)
			if forward {
				bound, next = next, bound
			}
			for range r.dueAt(1000, 1, bound) {
			}
			calls := armSearchStop(r)
			for range r.dueAt(1000, 1, next) {
			}
			assertSearchStopped(t, r, calls)
		})
	}
}

func TestEndExpiryCanEnableLaterRevision(t *testing.T) {
	h := &history.History{Start: &history.Start{Rev: 1, Leases: []int64{1, 2}, KVs: []history.KeyValue{
		{Key: "a", Value: "a", Version: 1, CreateRevision: 1, ModRevision: 1, Lease: 1},
		{Key: "b", Value: "b", Version: 1, CreateRevision: 1, ModRevision: 1, Lease: 2},
	}}, Ops: []history.Op{
		{Call: 1, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}}},
		{Call: 2, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 2}}},
	}}
	r := newRevisionOrder(newWitness(h, time.Now().Add(time.Minute)))
	r.optional = []optional{{op: 0, late: true, from: 1}, {op: 1, late: true, from: 2}}
	// Warm the filtered path at revision 1 first. End expiry must widen its
	// eligibility snapshot: the first deletion enables the second expiry.
	r.expire(3, 0)
	if r.used[0] || r.used[1] {
		t.Fatal("ordinary expiry wrote a revision")
	}
	r.expireEnd()
	if r.w.stopped || !r.used[0] || !r.used[1] || r.w.live.rev != 3 {
		t.Fatalf("end expiry lost an enabled revision: used=%v, revision=%d", r.used, r.w.live.rev)
	}
}
