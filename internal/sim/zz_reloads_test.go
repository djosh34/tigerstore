package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

func TestSnapshotsPerSeed(t *testing.T) {
	simrun.Seeds(t, 280, 20, func(t *testing.T, seed uint64) Stats {
		res := run(t, DefaultConfig(seed))
		t.Logf("RELOADS seed %d snapshots %d reloads %d", seed, res.Stats.Snapshots, res.Stats.Reloads)
		return res.Stats
	}, nil)
}
