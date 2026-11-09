package k8ssize

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestBuckets(t *testing.T) {
	var count, total int
	for i, b := range Buckets {
		if b.Count <= 0 || b.Size < 1 || b.Size > 33365 {
			t.Fatalf("bad row: %+v", b)
		}
		if i > 0 && Buckets[i-1].Resource == b.Resource && Buckets[i-1].Size >= b.Size {
			t.Fatalf("rows out of order at %+v", b)
		}
		count += b.Count
		total += b.Size * b.Count
	}
	if count != 6345 || total != 9253583 || len(Resources()) != 27 {
		t.Fatalf("histogram totals %d objects, %d bytes, %d resources", count, total, len(Resources()))
	}
	r := rand.New(rand.NewPCG(1, 2))
	const samples = 400000
	draws := make([]int, len(Buckets))
	for range samples {
		draws[Draw(r, "")]++
	}
	for i, b := range Buckets {
		probability := float64(b.Count) / float64(count)
		want := samples * probability
		tolerance := 6*math.Sqrt(want*(1-probability)) + 1
		if math.Abs(float64(draws[i])-want) > tolerance {
			t.Fatalf("%s %d: %d draws, want %.0f", b.Resource, b.Size, draws[i], want)
		}
		for range 20 {
			if Buckets[Draw(r, b.Resource)].Resource != b.Resource {
				t.Fatal("resource draw crossed resources")
			}
		}
	}
}

// TestDrawWalks checks that a draw picks the row a walk of the rows in
// order picks for the same random number, so a seed draws the same
// sizes as before.
func TestDrawWalks(t *testing.T) {
	walk := func(r *rand.Rand, resource string) int {
		total := 0
		for _, b := range Buckets {
			if resource == "" || resource == b.Resource {
				total += b.Count
			}
		}
		n := r.IntN(total)
		for i, b := range Buckets {
			if resource == "" || resource == b.Resource {
				n -= b.Count
				if n < 0 {
					return i
				}
			}
		}
		return -1
	}
	a, b := rand.New(rand.NewPCG(3, 4)), rand.New(rand.NewPCG(3, 4))
	for _, resource := range append(Resources(), "") {
		for range 2000 {
			if got, want := Draw(a, resource), walk(b, resource); got != want {
				t.Fatalf("%q: drew row %d, a walk drew %d", resource, got, want)
			}
		}
	}
	for row, bucket := range Buckets {
		if got, want := DrawLike(a, row), walk(b, bucket.Resource); got != want {
			t.Fatalf("like row %d: drew row %d, a walk drew %d", row, got, want)
		}
	}
}
