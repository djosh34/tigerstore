package check

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// referenceDue is due before #857, including the dueIndex query and the
// stable sort at every call. Charge its previously hidden work too, so the
// scale test compares work bounds rather than depending on the host's clock.
func referenceDue(r *revisionOrder, call, rev int64) []optional {
	var out []optional
	for _, o := range r.lateDue(call, rev) {
		if r.step() {
			return nil
		}
		if o.late && !r.used[o.op] && (o.gone < call || o.by <= rev) {
			if c := r.w.reqs[o.op].Compact; c != nil && c.Revision <= r.w.live.compacted {
				continue
			}
			out = append(out, o)
		}
	}
	slices.SortStableFunc(out, func(a, b optional) int {
		r.step()
		return cmp.Compare(r.w.calls[a.op], r.w.calls[b.op])
	})
	return out
}

func TestDueWorkMatchesReference(t *testing.T) {
	o := newScanOrder(857, 257, func(rng *rand.Rand) history.Op {
		return history.Op{Call: rng.Int64N(20), Req: history.Request{Compact: &history.CompactRequest{Revision: 1 + rng.Int64N(30)}}}
	})
	r := o.r
	for i := range r.used {
		r.optional = append(r.optional, optional{op: i, late: true, gone: o.rng.Int64N(90) - 10, by: o.rng.Int64N(60) - 10})
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
		call, rev := o.rng.Int64N(100)-10, o.rng.Int64N(70)-10
		switch step % 3 {
		case 0:
			call = 0
		case 1:
			rev = 0
		}
		want := referenceDue(r, call, rev)
		got := slices.Collect(r.due(call, rev))
		if !slices.EqualFunc(got, want, func(a, b optional) bool { return a.op == b.op }) {
			t.Fatalf("step %d, due(%d, %d): got %v, want %v", step, call, rev, got, want)
		}
		// A nested query moves both thresholds, but must not change the
		// outer query's remaining words or its stable order of tied calls.
		var nested []optional
		for candidate := range r.due(call, rev) {
			for range r.due(90-call, 60-rev) {
				break
			}
			nested = append(nested, candidate)
		}
		if !slices.EqualFunc(nested, want, func(a, b optional) bool { return a.op == b.op }) {
			t.Fatalf("step %d: nested query changed outer results", step)
		}
	}
}

func TestDueWorkRestoresAndSnapshots(t *testing.T) {
	o := newScanOrder(858, 130, func(*rand.Rand) history.Op {
		return history.Op{Req: history.Request{Compact: &history.CompactRequest{Revision: 10}}}
	})
	r := o.r
	for i := range r.used {
		r.optional = append(r.optional, optional{op: i, late: true, gone: 1, by: 1})
	}
	want := referenceDue(r, 2, 2)
	var got []optional
	for candidate := range r.due(2, 2) {
		if len(got) == 0 {
			r.w.live.compacted = 10
			// This inner query removes every compaction from future
			// queries, but must not remove the outer snapshot's tail.
			if nested := slices.Collect(r.due(2, 2)); len(nested) != 0 {
				t.Fatal("nested query kept redundant compactions")
			}
		}
		got = append(got, candidate)
	}
	if !slices.EqualFunc(got, want, func(a, b optional) bool { return a.op == b.op }) {
		t.Fatal("a nested compaction changed the outer snapshot")
	}
	r.w.live.compacted = -1
	mark := r.save()
	retained := r.dueWork
	r.use(0)
	r.restore(mark)
	if r.dueWork != retained {
		t.Fatal("restore discarded the sorted candidates and buffers")
	}
	got = slices.Collect(r.due(2, 2))
	if !slices.EqualFunc(got, want, func(a, b optional) bool { return a.op == b.op }) {
		t.Fatal("restore did not bring back used and compacted candidates")
	}
}

// dueShape models the hot path of a long prefix of open gaps: many late
// expiries are due but still cannot go in (their from is ahead). Trying the
// gaps' candidates asks by time, and finishing a revision asks by revision.
// Calls are deliberately out of optional order and tied. The same sets stay
// due while the search tries the next candidate; the old code queried and
// sorted all of them again. Keeping frames open must not retain a copy of
// these sets per frame.
func dueShape(n int) *revisionOrder {
	h := &history.History{}
	for i := range n {
		h.Ops = append(h.Ops, history.Op{Call: int64((i * 521) % 997), Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: int64(i + 1)}}})
	}
	r := newRevisionOrder(newWitness(h, time.Now().Add(time.Hour)))
	for i := range n {
		r.optional = append(r.optional, optional{op: i, late: true, from: 10_000, gone: 1, by: 1})
	}
	for rev := range 200 {
		r.stack = append(r.stack, &frame{rev: int64(rev + 2)})
	}
	return r
}

func TestDueWorkScale(t *testing.T) {
	const n, queries, budget = 4096, 256, 30_000
	for _, old := range []bool{false, true} {
		t.Run(fmt.Sprint("reference=", old), func(t *testing.T) {
			r := dueShape(n)
			r.w.maxSteps = budget
			start := time.Now()
			for q := range queries {
				call, rev := int64(1000+q), int64(0)
				if q%2 == 0 {
					call, rev = 0, int64(q+1)
				}
				count := 0
				if old {
					count = len(referenceDue(r, call, rev))
				} else {
					for range r.due(call, rev) {
						count++
					}
				}
				if r.w.stopped {
					break
				}
				if count != n {
					t.Fatalf("query %d returned %d, want %d", q, count, n)
				}
			}
			if r.w.stopped != old {
				t.Fatalf("reference=%v: stopped=%v, steps=%d", old, r.w.stopped, r.w.steps)
			}
			t.Logf("%d candidates, %d queries, %d open gaps: %s, %d steps", n, queries, len(r.stack), time.Since(start), r.w.steps)
		})
	}
	r := dueShape(n)
	for range r.due(1000, 0) {
	}
	allocs := testing.AllocsPerRun(100, func() {
		for range r.due(1000, 0) {
		}
	})
	if allocs > 2 {
		t.Fatalf("steady due allocated %.1f times per query, want at most 2 small iterator allocations", allocs)
	}
}

// TestDueGapHistoryScale also runs the whole search, rather than only its
// due kernel: thousands of expiries followed by hundreds of unseen writes.
// All the gaps remain open until the final read verifies the order.
func TestDueGapHistoryScale(t *testing.T) {
	const leases, gaps = 4096, 256
	h := dueGapHistory(t, leases, gaps)
	// BenchmarkWitnessBudgetHistories runs this same history and allocation
	// measurement on base 37e51817: 2,189,414,600 B/op. The bound is below
	// that reference, with room above the retained path's 2,184,659,568.
	// Restoring the old per-call candidate allocation must break it.
	const bytesBound = 2_187_000_000
	var w *witness
	var searchErr error
	result := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			w = newWitness(h, time.Now().Add(time.Minute))
			w.maxSteps = 1_000_000
			if searchErr = w.fromRevisions(); searchErr != nil {
				b.Fatalf("%v after %d steps; %s", searchErr, w.steps, w.search.progress())
			}
		}
	})
	if searchErr != nil {
		t.Fatal(searchErr)
	}
	if bytes := result.AllocedBytesPerOp(); bytes > bytesBound {
		t.Fatalf("search allocated %d bytes, bound %d; old reference 2,189,414,600", bytes, bytesBound)
	}
	if len(w.search.stack) != gaps || len(w.search.optional) != leases {
		t.Fatalf("got %d gaps and %d expiries", len(w.search.stack), len(w.search.optional))
	}
	t.Logf("searched %d ops, %d revisions, %d open gaps, %d expiries in %s, %d steps", len(h.Ops), gaps, len(w.search.stack), leases, result.T, w.steps)
	t.Logf("whole search allocated %d B/op, below %d; base exceeded that bound", result.AllocedBytesPerOp(), bytesBound)
}

func BenchmarkDueWork(b *testing.B) {
	for _, old := range []bool{false, true} {
		b.Run(fmt.Sprint("reference=", old), func(b *testing.B) {
			r := dueShape(4096)
			for range r.due(1000, 0) {
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if old {
					_ = referenceDue(r, 1000, 0)
				} else {
					for range r.due(1000, 0) {
					}
				}
			}
		})
	}
}
