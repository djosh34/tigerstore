package main

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestHistBuckets(t *testing.T) {
	prev := -1
	for v := range int64(1 << 20) {
		i := histIndex(v)
		if i < prev || i > prev+1 {
			t.Fatalf("index of %d is %d after %d", v, i, prev)
		}
		if histLow(i) > v || histLow(i+1) <= v {
			t.Fatalf("%d is outside bucket %d [%d, %d)", v, i, histLow(i), histLow(i+1))
		}
		prev = i
	}
	for _, v := range []int64{1 << 40, 1<<62 + 12345, math.MaxInt64} {
		i := histIndex(v)
		if i >= histBuckets || histLow(i) > v {
			t.Fatalf("%d maps to bucket %d with low %d", v, i, histLow(i))
		}
	}
}

func TestHistQuantiles(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var h hist
	var vals []int64
	for range 100000 {
		v := int64(r.ExpFloat64() * 2e6) // around 2 ms
		vals = append(vals, v)
		h.record(v)
	}
	slices.Sort(vals)
	for _, q := range []float64{0, 0.5, 0.9, 0.99, 0.999, 1} {
		want := vals[max(int(math.Ceil(q*float64(len(vals))))-1, 0)]
		got := h.quantile(q)
		if math.Abs(float64(got-want)) > 0.01*float64(want)+1 {
			t.Errorf("q%v = %d, want %d within 1%%", q, got, want)
		}
	}
	if h.min != vals[0] || h.max != vals[len(vals)-1] {
		t.Errorf("min/max %d/%d, want %d/%d", h.min, h.max, vals[0], vals[len(vals)-1])
	}

	var a, b hist
	for i, v := range vals {
		if i%2 == 0 {
			a.record(v)
		} else {
			b.record(v)
		}
	}
	a.merge(&b)
	if a.counts != h.counts || a.n != h.n || a.min != h.min || a.max != h.max {
		t.Error("merging two halves differs from recording all")
	}
}
