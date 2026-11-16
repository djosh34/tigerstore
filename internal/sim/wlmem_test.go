//go:build simio || sim

package sim

import (
	"runtime"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wl"
)

// The memory of a wl node of the simulator (wl.SimFootprint), which only
// a build of wl's seam for the simulator has: the tag sim, which gosim's
// translation sets, or simio natively (scripts/sim.sh).

// A wl node of the simulator makes at most 128 MiB at its start, Go's
// memory under gosim, made again at each start: a run of three nodes, its
// restarts and its cold restarts, four runs at once on a campaign's
// runner, fit it. At 64 MiB rings and the real build's tables, a calm seed
// held 3.8 GB live, and the campaign's runners were killed.
func TestAWLNodeOfTheSimulatorIsSmall(t *testing.T) {
	const most = 128 << 20
	if got := wl.SimFootprint(0); got > most {
		t.Fatalf("a wl node of the simulator makes %d MiB at its start, past %d MiB", got>>20, most>>20)
	}
}

// A wl node that stops is let go: a seed's starts do not add up on Go's
// heap, where each makes wl.SimFootprint again under gosim. Campaign
// 43842206701 on 2be0c825 lost every runner at its first seeds: one seed
// held 3.7 GB live, as the simulator kept each process's last turn, whose
// values (TurnState.LogValues then) held the whole node. A seed with a
// crash every second starts its nodes again many times; at its end the
// heap must hold less than the nodes it ran at once and as much again.
func TestWLStoppedNodesAreLetGo(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		cfg.Faults = Faults{CrashEvery: Second, MaxDown: Second}
		res := run(t, cfg)
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		most := 2 * uint64(len(res.nodes)) * uint64(wl.SimFootprint(0))
		t.Logf("%d crashes; the heap holds %d MiB, at most %d MiB", res.Stats.Crashes, ms.HeapAlloc>>20, most>>20)
		if res.Stats.Crashes < 2*len(res.nodes) {
			t.Fatalf("%d crashes, too few to start the nodes again past the heap's limit", res.Stats.Crashes)
		}
		if ms.HeapAlloc > most {
			t.Fatalf("after %d crashes the heap holds %d MiB, past %d MiB: the simulator keeps stopped nodes", res.Stats.Crashes, ms.HeapAlloc>>20, most>>20)
		}
		return res.Stats
	}, nil)
}
