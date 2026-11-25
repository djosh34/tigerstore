package check

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// indexHistory has n writers, acked compactions, historical reads, compacted
// rejections and ambiguous compactions. The acked compactions overlap the
// rejections, so none explains them in every order. Every optional compaction
// needs large prefixes of the acked reads and compactions.
func indexHistory(n int) *history.History {
	h := &history.History{}
	for i := range n {
		rev := int64(i + 2)
		h.Ops = append(
			h.Ops,
			history.Op{Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "k"}}, Resp: &history.Response{Revision: rev, Put: &history.PutResponse{}}},
			history.Op{Call: 2, Return: 5, Req: history.Request{Compact: &history.CompactRequest{Revision: rev}}, Resp: &history.Response{Revision: int64(n + 1), Compact: &history.CompactResponse{}}},
			history.Op{Call: 2, Return: 5, Req: history.Request{Range: &history.RangeRequest{Key: "k", Revision: rev}}, Resp: &history.Response{Revision: int64(n + 1), Range: &history.RangeResponse{}}},
			history.Op{Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "k", Revision: rev}}, Err: errCompacted},
			history.Op{Call: 2, Return: 5, Req: history.Request{Compact: &history.CompactRequest{Revision: int64(n + 1)}}, Err: errUnavailable},
		)
	}
	for i := range h.Ops {
		h.Ops[i].Client = i
	}
	return h
}

func TestRevisionIndexBudget(t *testing.T) {
	h := indexHistory(20_000)
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprint("stop=", stop), func(t *testing.T) {
			calls := 0
			opts := []Option{MaxSteps(4)}
			if stop {
				opts = []Option{StopWhen(func() bool {
					calls++
					return calls == 2
				})}
			}
			start := time.Now()
			err := Witnessed(h, FromRevisions, time.Hour, opts...)
			if !errors.Is(err, ErrWitnessTimeout) || !strings.Contains(err.Error(), "the revision index was being built") {
				t.Fatalf("want timeout in index, got %v", err)
			}
			if stop && calls != 2 {
				t.Fatalf("stop hook called %d times, want 2", calls)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("index cancellation took %s", elapsed)
			}
		})
	}
}

func TestRevisionIndexDeadline(t *testing.T) {
	w := newWitness(indexHistory(20_000), time.Now().Add(-time.Hour))
	r := newRevisionOrder(w)
	if err := r.index(); !errors.Is(err, ErrWitnessTimeout) || w.steps != 1024 {
		t.Fatalf("expired deadline: %v after %d steps", err, w.steps)
	}
}

func TestRevisionIndexLargeTransactionBudget(t *testing.T) {
	ops := make([]history.TxnOp, 10_000)
	for k := range ops {
		ops[k].Range = &history.RangeRequest{Key: "k"}
	}
	h := &history.History{Ops: []history.Op{{
		Req: history.Request{Txn: &history.TxnRequest{Success: ops}}, Err: errUnavailable,
	}}}
	w := newWitness(h, time.Now().Add(time.Hour))
	w.maxSteps = 1
	if err := newRevisionOrder(w).index(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("large transaction bypassed index budget: %v", err)
	}
}

func TestRevisionIndexSharesSearchBudget(t *testing.T) {
	b := newBuilder(t, "k")
	for range 100 {
		b.put(0, "k", "v")
	}
	w := newWitness(b.done(), time.Now().Add(time.Hour))
	r := newRevisionOrder(w)
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	if w.steps == 0 {
		t.Fatal("index used no steps")
	}
	w.maxSteps = w.steps
	if err := r.search(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("search did not share index budget: %v", err)
	}
	if len(w.events) != 0 {
		t.Fatal("search placed events after exhausting the index budget")
	}
}

func TestRevisionIndexScale(t *testing.T) {
	const n = 10_000
	w := newWitness(indexHistory(n), time.Now().Add(time.Hour))
	// This work bound, unlike a wall-clock assertion, also holds under -race.
	w.maxSteps = 200 * n / 256
	r := newRevisionOrder(w)
	start := time.Now()
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	t.Logf("indexed %d ops in %s, %d steps", len(w.h.Ops), time.Since(start), w.steps)
	if len(r.optional) != n {
		t.Fatalf("got %d optional compactions, want %d", len(r.optional), n)
	}
	first, last := r.optional[0], r.optional[n-1]
	if len(first.reads) != n-1 || len(first.follow) != n || &first.reads[0] != &last.reads[0] || &first.follow[0] != &last.follow[0] {
		t.Fatal("optional compactions do not share their constraint prefixes")
	}
	// Running out near the end, rather than in the first pass, must stop too.
	limited := newWitness(w.h, time.Now().Add(time.Hour))
	limited.maxSteps = w.steps * 3 / 4
	if err := newRevisionOrder(limited).index(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("late index budget: %v", err)
	}
}

func BenchmarkRevisionIndex(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			h := indexHistory(n)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				w := newWitness(h, time.Now().Add(time.Hour))
				if err := newRevisionOrder(w).index(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// optionalConstraints compares sets of predecessors. Their storage order is
// immaterial: isReady tests all follow ops, and placeSeen sorts reads by call.
func optionalConstraints(os []optional) []optional {
	out := slices.Clone(os)
	for k := range out {
		out[k].reads, out[k].follow = slices.Clone(out[k].reads), slices.Clone(out[k].follow)
		slices.Sort(out[k].reads)
		slices.Sort(out[k].follow)
	}
	return out
}

func compareOptional(t *testing.T, h *history.History) *revisionOrder {
	t.Helper()
	makeOrder := func() *revisionOrder {
		r := newRevisionOrder(newWitness(h, time.Now().Add(time.Hour)))
		for i := range h.Ops {
			if op := &h.Ops[i]; op.Resp == nil && op.Err.Ambiguous && op.Req.Mutates() {
				r.ambiguous = append(r.ambiguous, i)
			}
		}
		r.lowerBounds()
		r.top = 20
		return r
	}
	got, want := makeOrder(), makeOrder()
	got.scheduleOptional()
	want.scheduleOptionalReference()
	if !reflect.DeepEqual(optionalConstraints(got.optional), optionalConstraints(want.optional)) || !slices.Equal(slices.Sorted(slices.Values(got.mutations)), slices.Sorted(slices.Values(want.mutations))) {
		t.Fatalf("constraints differ\ngot  %+v\nwant %+v", got.optional, want.optional)
	}
	return got
}

func TestOptionalIndexCompactionRules(t *testing.T) {
	for _, tc := range []struct {
		name        string
		read        bool
		rev, c      int64
		start       *history.Start
		ackedReturn int64
		want        bool
	}{
		{name: "read equal", read: true, rev: 3, c: 3},
		{name: "read below", read: true, rev: 2, c: 3, want: true},
		{name: "compact equal", rev: 3, c: 3, want: true},
		{name: "compact above", rev: 4, c: 3},
		{name: "acked before", read: true, rev: 2, c: 3, ackedReturn: 9},
		{name: "acked equal call", read: true, rev: 2, c: 3, ackedReturn: 10, want: true},
		{name: "start explains read", read: true, rev: 2, c: 3, start: &history.Start{Rev: 3}},
		{name: "start read equal", read: true, rev: 3, c: 4, start: &history.Start{Rev: 3}, want: true},
		{name: "start compact equal", rev: 3, c: 4, start: &history.Start{Rev: 3}},
		{name: "transaction names zero", rev: 0, c: 3, start: &history.Start{Rev: 3}, want: true},
		{name: "max revision read equal", read: true, rev: math.MaxInt64, c: math.MaxInt64},
		{name: "max revision compact equal", rev: math.MaxInt64, c: math.MaxInt64, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := history.Request{Compact: &history.CompactRequest{Revision: tc.rev}}
			if tc.read {
				req = history.Request{Range: &history.RangeRequest{Key: "k", Revision: tc.rev}}
			} else if tc.rev == 0 {
				req = history.Request{Txn: &history.TxnRequest{}}
			}
			h := &history.History{Start: tc.start, Ops: []history.Op{
				{Call: 1, Return: 20, Req: history.Request{Compact: &history.CompactRequest{Revision: tc.c}}, Err: errUnavailable},
				{Call: 10, Return: 12, Req: req, Err: errCompacted},
			}}
			if tc.ackedReturn != 0 {
				h.Ops = append(h.Ops, history.Op{Call: 1, Return: tc.ackedReturn, Req: history.Request{Compact: &history.CompactRequest{Revision: tc.c}}, Resp: &history.Response{Revision: 5, Compact: &history.CompactResponse{}}})
			}
			r := compareOptional(t, h)
			if (len(r.optional) != 0) != tc.want {
				t.Fatalf("got %d optionals, want present=%v", len(r.optional), tc.want)
			}
		})
	}
}

func TestOptionalIndexLeaseRules(t *testing.T) {
	for _, ackedRevoke := range []bool{false, true} {
		t.Run(fmt.Sprint("acked revoke=", ackedRevoke), func(t *testing.T) {
			b := newBuilder(t, "k")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 11, TTL: 10}})
			put := b.do(0, history.Request{Put: &history.PutRequest{Key: "k", Lease: 11}})
			revoke := b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 11}})
			rejected := b.do(0, history.Request{Put: &history.PutRequest{Key: "k", Lease: 11}})
			duplicate := b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 11}})
			h := b.done()
			for _, i := range []int{grant, duplicate} {
				h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
			}
			if !ackedRevoke {
				h.Ops[revoke].Resp, h.Ops[revoke].Err = nil, errUnavailable
			}
			r := compareOptional(t, h)
			want := 2
			if ackedRevoke {
				want = 1
			}
			if len(r.optional) != want || r.optional[0].op != grant {
				t.Fatalf("lease in use must need its ambiguous grant: %+v", r.optional)
			}
			if !ackedRevoke {
				o := r.optional[1]
				if o.op != revoke || !o.late || o.from < h.Ops[put].Resp.Revision || o.gone != h.Ops[rejected].Return {
					t.Fatalf("lease gone must need one expiry after its last use: %+v", o)
				}
			}
		})
	}
}

func TestIndexSortMatchesStableSort(t *testing.T) {
	type item struct{ key, position int }
	rng := rand.New(rand.NewPCG(849, 2))
	for _, n := range []int{0, 1, 2, 63, 64, 1000} {
		got := make([]item, n)
		for k := range got {
			got[k] = item{rng.IntN(10), k}
		}
		want := slices.Clone(got)
		compare := func(a, b item) int { return a.key - b.key }
		slices.SortStableFunc(want, compare)
		r := newRevisionOrder(newWitness(&history.History{}, time.Now().Add(time.Hour)))
		indexSort(r, got, compare)
		if !slices.Equal(got, want) {
			t.Fatalf("sort of %d items differs", n)
		}
	}
	calls := 0
	got := make([]int, 1000)
	for k := range got {
		got[k] = len(got) - k
	}
	sortWithStep(got, func(a, b int) int { return a - b }, func() bool {
		calls++
		return calls >= 100
	})
	if calls != 100 {
		t.Fatalf("sort did not stop immediately: %d calls", calls)
	}
}

func TestOptionalIndexRandomReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(849, 1))
	for trial := range 200 {
		b := newBuilder(t, "k", "m")
		for range 100 {
			id, rev := int64(rng.IntN(4)+1), int64(rng.IntN(12))
			var req history.Request
			switch rng.IntN(9) {
			case 0:
				req.Put = &history.PutRequest{Key: "k", Value: "v", Lease: id}
			case 1:
				req.Put = &history.PutRequest{Key: "m", Value: "v"}
			case 2:
				req.LeaseGrant = &history.LeaseGrantRequest{ID: id, TTL: 10}
			case 3:
				req.LeaseRevoke = &history.LeaseRevokeRequest{ID: id}
			case 4, 5:
				req.Compact = &history.CompactRequest{Revision: rev}
			case 6, 7:
				req.Range = &history.RangeRequest{Key: "k", Revision: rev}
			case 8:
				req.Txn = &history.TxnRequest{
					Compare: []history.Compare{{Key: "m", Target: history.TargetMod, Result: history.Equal, Number: rev}},
					Success: []history.TxnOp{{Put: &history.PutRequest{Key: "k", Lease: id}}},
					Failure: []history.TxnOp{{Put: &history.PutRequest{Key: "m", Lease: id + 1}}},
				}
			}
			b.do(0, req)
		}
		h := b.done()
		for i := range h.Ops {
			op := &h.Ops[i]
			if op.Req.Mutates() && rng.IntN(3) == 0 {
				op.Resp, op.Err = nil, errUnavailable
			}
			// Exercise strict return-before-call, ties, overlapping calls,
			// and ops whose history order differs from revision order.
			op.Call = int64(rng.IntN(30))
			op.Return = op.Call + int64(rng.IntN(10)+1)
		}
		if trial%2 == 0 {
			h.Start = &history.Start{Rev: int64(rng.IntN(8) + 1)}
		}
		t.Run(strconv.Itoa(trial), func(t *testing.T) { compareOptional(t, h) })
	}
}
