package sim

import "testing"

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
