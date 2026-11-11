package sim

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestKubeCacheSlots runs the short Kubernetes-shaped load with a memory
// budget so small that the value cache drops values all the time and its
// slab slots take new ones, and with compactions, so keys move between
// one version in their record and several in an array (#602). The
// checkers compare every value a client read with the history. A cache
// hit that was not copied out of its slot passes here, as the answer goes
// out before the slot is taken again; TestAnswerKeepsCachedValue in
// internal/core catches that. It runs with no random faults, so every run
// commits enough to compact: under the race detector a seed does not
// follow gosim's schedule, and one drew faults that let the cluster commit
// almost nothing, and no compaction (#618).
func TestKubeCacheSlots(t *testing.T) {
	simrun.Seeds(t, 1, 2, func(t *testing.T, seed uint64) Stats {
		cfg := kubeConfig(seed, false)
		cfg.Faults = Faults{DiskBudget: cfg.Faults.DiskBudget, DiskCapacity: cfg.Faults.DiskCapacity}
		cfg.Duration = 40 * Second
		cfg.CompactEvery = 5 * Second
		cfg.MemoryBytes = 256 << 10
		var hits, misses int64
		res, err := runWith(t, cfg, func(s *Sim) error {
			for _, n := range s.live() {
				m := n.proc.core.Metrics()
				hits += m.CacheHits
				misses += m.CacheMisses
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		compactions := 0
		for _, op := range res.Log {
			if _, ok := op.(*pb.CompactionRequest); ok {
				compactions++
			}
		}
		checkers(t, res)
		if hits == 0 || misses == 0 || compactions == 0 {
			t.Fatalf("seed %d did too little to test anything: %d cache hits, %d cold reads, %d compactions in the log", seed, hits, misses, compactions)
		}
		t.Logf("cache hits %d, cold reads %d, compactions %d; the checkers passed", hits, misses, compactions)
		return res.Stats
	}, nil)
}
