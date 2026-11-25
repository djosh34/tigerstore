package main

import "testing"

// Each node's GOMAXPROCS is drawn from the cluster's seed, from
// cmd/tigerstore's least up to the runner's cpus: the same for a seed,
// different across seeds, and the least on a runner with fewer (#502).
func TestEachNodesCoresAreDrawnFromTheSeed(t *testing.T) {
	const cpus = 16
	seen := map[int]bool{}
	for seed := range uint64(200) {
		p := drawProcs(seed, cpus)
		if p != drawProcs(seed, cpus) {
			t.Fatalf("seed %d drew %v, then %v", seed, p, drawProcs(seed, cpus))
		}
		for _, n := range p {
			if n < minProcs || n > cpus {
				t.Fatalf("seed %d drew %d processors, want %d to %d", seed, n, minProcs, cpus)
			}
			seen[n] = true
		}
	}
	if !seen[minProcs] || !seen[cpus] || len(seen) != cpus-minProcs+1 {
		t.Fatalf("200 seeds drew %d of the %d counts from %d to %d", len(seen), cpus-minProcs+1, minProcs, cpus)
	}
	if p := drawProcs(7, 1); p != [3]int{minProcs, minProcs, minProcs} {
		t.Fatalf("a runner of one cpu drew %v, want %d each", p, minProcs)
	}
}
