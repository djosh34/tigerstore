package check

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func TestBusyOptionalIndexMatchesScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 1))
	h := &history.History{Ops: make([]history.Op, 1000)}
	w := &witness{h: h, reqs: make([]history.Request, 1000), live: &state{compacted: -1}}
	r := &revisionOrder{w: w, used: map[int]bool{}}
	for i := range h.Ops {
		h.Ops[i].Call = rng.Int64N(100)
		r.optional = append(r.optional, optional{op: i, late: rng.IntN(2) == 0, from: rng.Int64N(100), gone: rng.Int64N(100), by: rng.Int64N(100)})
		if rng.IntN(3) == 0 {
			w.reqs[i].Compact = &history.CompactRequest{Revision: rng.Int64N(100)}
		}
	}
	for trial := range 500 {
		if trial%40 == 0 {
			// A backtrack restores used and compaction; throw away the index.
			clear(r.used)
			w.live.compacted = -1
			r.deadlines = nil
		}
		r.used[rng.IntN(len(h.Ops))] = true
		w.live.compacted = max(w.live.compacted, rng.Int64N(100))
		call, rev := rng.Int64N(110), rng.Int64N(110)
		for _, late := range []bool{false, true} {
			var want []optional
			for _, o := range r.optional {
				if r.used[o.op] || o.late != late {
					continue
				}
				if late {
					if o.gone >= call && o.by > rev {
						continue
					}
					if c := w.reqs[o.op].Compact; c != nil && c.Revision <= w.live.compacted {
						continue
					}
				} else if o.from > rev {
					continue
				}
				want = append(want, o)
			}
			got := r.indexedOptional(call, rev, late)
			if !slices.EqualFunc(got, want, func(a, b optional) bool { return a.op == b.op }) {
				t.Fatalf("trial %d late=%v: got %d want %d", trial, late, len(got), len(want))
			}
		}
	}
}

// The short reproduction: future optional mutations must not each be
// visited at each revision. The old scan performs n*n visits here.
func TestBusyFutureOptionals(t *testing.T) {
	const n = 4096
	r := &revisionOrder{w: &witness{}, used: map[int]bool{}}
	for i := range n {
		r.optional = append(r.optional, optional{op: i, late: true, gone: math.MaxInt64, by: math.MaxInt64})
	}
	for i := range n {
		if got := r.indexedOptional(int64(i), int64(i), true); len(got) != 0 {
			t.Fatal("future optional due")
		}
	}
	if r.diagIndexVisits != n {
		t.Fatalf("visited %d nodes, want %d", r.diagIndexVisits, n)
	}
}

func TestBusyEventTimesMatchScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 2))
	h := &history.History{}
	times := newEventTimes()
	var ops []int
	for range 3000 {
		if len(ops) > 0 && rng.IntN(5) == 0 {
			n := rng.IntN(len(ops))
			ops = ops[:n]
			times.truncate(n)
		} else {
			op := -1
			if rng.IntN(5) > 0 {
				op = len(h.Ops)
				o := history.Op{Call: rng.Int64N(1000), Return: rng.Int64N(1000)}
				if rng.IntN(3) > 0 {
					o.Resp = &history.Response{}
				}
				h.Ops = append(h.Ops, o)
			}
			times.append(h, op)
			ops = append(ops, op)
		}
		call, ret := rng.Int64N(1000), rng.Int64N(1000)
		lo, hi := 0, len(ops)
		for k, i := range ops {
			if i < 0 {
				continue
			}
			o := h.Ops[i]
			if o.Resp != nil && o.Return < call {
				lo = k + 1
			}
			if o.Call > ret {
				hi = min(hi, k)
			}
		}
		if got := times.after(call); got != lo {
			t.Fatalf("after=%d want %d", got, lo)
		}
		if got := times.before(ret); got != hi {
			t.Fatalf("before=%d want %d", got, hi)
		}
	}
}
