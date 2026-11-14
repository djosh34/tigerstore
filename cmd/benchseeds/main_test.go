package main

import (
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// shapeOf is the shape loadgen's k8s replay draws for seed.
func shapeOf(t *testing.T, dir, s string) string {
	t.Helper()
	seed, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	d, err := k8strace.DrawSeed(dir, seed, 1)
	if err != nil {
		t.Fatal(err)
	}
	return d.Trace.Shape
}

// TestQuickGridSeedsAreCrdHeavy draws what bench.yml's quick grid draws,
// 6 seeds of which 6 must draw crd-heavy: every seed lands on crd-heavy,
// as loadgen's k8s replay draws it.
func TestQuickGridSeedsAreCrdHeavy(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "recordings")
	for i := range 3 {
		rng := rand.New(rand.NewPCG(uint64(i), 7))
		seeds, err := draw(dir, "crd-heavy", 6, 6, rng.Uint64)
		if err != nil {
			t.Fatal(err)
		}
		if len(seeds) != 6 {
			t.Fatalf("%d seeds, want 6", len(seeds))
		}
		for _, s := range seeds {
			if got := shapeOf(t, dir, s); got != "crd-heavy" {
				t.Fatalf("seed %s of %v draws %s, not crd-heavy", s, seeds, got)
			}
		}
	}
}

// TestDrawsAtLeastMin draws 8 seeds with -min 4, as benchseeds' defaults
// do for a run that mixes shapes: at least 4 land on crd-heavy, the rest
// on anything; and a shape with no recording is refused.
func TestDrawsAtLeastMin(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "recordings")
	rng := rand.New(rand.NewPCG(1, 2))
	seeds, err := draw(dir, "crd-heavy", 8, 4, rng.Uint64)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, s := range seeds {
		if shapeOf(t, dir, s) == "crd-heavy" {
			matched++
		}
	}
	if len(seeds) != 8 || matched < 4 {
		t.Fatalf("%d seeds, %d draw crd-heavy, want 8 and at least 4", len(seeds), matched)
	}
	if _, err := draw(dir, "no-such-shape", 8, 4, rng.Uint64); err == nil {
		t.Fatal("a shape with no recording was drawn")
	}
}
