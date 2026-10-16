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
