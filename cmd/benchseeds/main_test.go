package main

import (
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// TestDrawsTheShape draws 8 seeds of which at least 4 draw crd-heavy, as
// loadgen's k8s replay draws them, and refuses a shape with no recording.
func TestDrawsTheShape(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "recordings")
	rng := rand.New(rand.NewPCG(1, 2))
	seeds, err := draw(dir, "crd-heavy", 8, 4, rng.Uint64)
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) != 8 {
		t.Fatalf("%d seeds, want 8", len(seeds))
	}
	matched := 0
	for _, s := range seeds {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		d, err := k8strace.DrawSeed(dir, seed, 1)
		if err != nil {
			t.Fatal(err)
		}
		if d.Trace.Shape == "crd-heavy" {
			matched++
		}
	}
	if matched < 4 {
		t.Fatalf("%d of %v draw crd-heavy, want at least 4", matched, seeds)
	}
	if _, err := draw(dir, "no-such-shape", 8, 4, rng.Uint64); err == nil {
		t.Fatal("a shape with no recording was drawn")
	}
}
