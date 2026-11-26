package sim

import "testing"

// The report names its threshold, and counts images, not their forks.
func TestAgedImageLeaseReport(t *testing.T) {
	ran, images := map[string]int{}, map[uint64]bool{}
	for seed, leases := range []int{0, 999, 1000, 2999, 3000, 12000} {
		countAgedImage(ran, images, uint64(seed), leases)
		countAgedImage(ran, images, uint64(seed), leases)
	}
	if got := ran["aged images with at least 3,000 leases"]; got != 2 {
		t.Errorf("images with at least 3,000 leases: %d, want 2; report %v", got, ran)
	}
	if ran["aged images"] != 6 || ran["live leases at aged images"] != 19998 {
		t.Errorf("images or live leases counted more than once: %v", ran)
	}
}

// The lease variant is drawn last: the objects, duration, voters, memory,
// hot keys and tight-disk draw stay those of the older aging seeds.
func TestAgingLeaseDraw(t *testing.T) {
	for _, want := range []struct {
		seed     uint64
		objects  int
		duration int64
		nodes    int
		memory   int64
		grow     bool
		leases   int
	}{
		{1, 2822, 4780860009620, 3, 482396535, true, 0},
		{2, 5503, 1231051863158, 5, 0, false, 8695},
		{3, 3167, 171944261069, 3, 0, false, 0},
		{4, 1171, 354733816375, 3, 565948953, false, 0},
		{5, 14899, 1123946167322, 1, 0, false, 0},
		{6, 6435, 1710955910123, 5, 2068127493, true, 0},
		{7, 14794, 2171057698682, 5, 2502018350, false, 0},
		{8, 602, 1688652440109, 1, 0, false, 0},
	} {
		cfg := agingConfig(want.seed)
		if cfg.Objects != want.objects || cfg.Duration != want.duration || cfg.Nodes != want.nodes || cfg.MemoryBytes != want.memory || cfg.HotKeys || cfg.Faults.GrowDisk != want.grow || cfg.ImageLeases != want.leases {
			t.Errorf("seed %d: objects %d, duration %d, voters %d, memory %d, hot %t, grow %t, leases %d; want %+v",
				want.seed, cfg.Objects, cfg.Duration, cfg.Nodes, cfg.MemoryBytes, cfg.HotKeys, cfg.Faults.GrowDisk, cfg.ImageLeases, want)
		}
	}
}
