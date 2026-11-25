package sim

import (
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestATightDiskHoldsItsCheckpointsAndRecovers runs DefaultConfig's load
// on disks too small for the store's second checkpoint, with no other
// fault: each node finds no room, holds its checkpoints with NOSPACE
// raised and stays up, its log ring filling behind the checkpoint it
// cannot take, until an operator grows the disks and disarms the alarm
// (Faults.GrowDisk). The run must then converge and pass every checker,
// writes taken again after the disarm (#769, #846's ruling, Opus's review
// of #846 on a ring that fills while checkpoints wait).
func TestATightDiskHoldsItsCheckpointsAndRecovers(t *testing.T) {
	if simrun.Race {
		t.Skip("the disks' room is counted under gosim")
	}
	simrun.Each(t, seedRange(3800, 2), func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{DiskBudget: 32 << 10, DiskCapacity: 20 << 10, GrowDisk: true}
		// Time for the disk to fill, for the operator, and for the writes
		// taken again after the disarm.
		cfg.Duration, cfg.Heal = 40*Second, 20*Second
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		t.Logf("seed %d: %d ok, NoRoom %d, NoSpace %d, DiskGrowths %d, Disarms %d, OutOfSpace %d, checkpoints %d, log cuts %d",
			seed, st.OK, st.NoRoom, st.NoSpace, st.DiskGrowths, st.Disarms, st.OutOfSpace, st.Checkpoints, st.LogCuts)
		if st.NoRoom == 0 || st.DiskGrowths != 1 || st.Disarms == 0 || st.OutOfSpace != 0 {
			t.Fatalf("seed %d: %d checkpoints held for room, %d growths, %d disarms, %d nodes out of space: the disk must fill, the nodes stay up, and the operator grow it and disarm",
				seed, st.NoRoom, st.DiskGrowths, st.Disarms, st.OutOfSpace)
		}
		// NOSPACE's refusals count for the liveness rule as answers; the
		// writes must be taken again once the alarm is disarmed.
		if !slices.ContainsFunc(res.History, func(r *Record) bool { return r.Call > st.DisarmedAt && r.Err == nil && isWrite(r.req) }) {
			t.Fatalf("seed %d: no write succeeded after the disarm at %s", seed, seconds(st.DisarmedAt))
		}
		return st
	}, nil)
}
