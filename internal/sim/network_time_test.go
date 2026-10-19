package sim

import (
	"flag"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/check"
)

var networkScenario = flag.String("sim.network", "leader-incoming", "round-2 network schedule: leader-incoming, follower-island, leader-flap, peer-slow-leader, minority-bridge")

// TestNetworkTime combines leader-targeted faults with leases, compared
// two-key transactions, compaction, snapshots and watches of both start
// modes. Short runs omit leases and must also pass Porcupine.
func TestNetworkTime(t *testing.T) {
	for _, short := range []bool{false, true} {
		name := "leases"
		if short {
			name = "porcupine"
		}
		t.Run(name, func(t *testing.T) {
			n := *numSeeds
			if n == 0 {
				n = 4
			}
			for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
				t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
					cfg := DefaultConfig(seed)
					cfg.NetworkTime = *networkScenario
					if cfg.NetworkTime == "minority-bridge" {
						cfg.Voters, cfg.Clients = 5, 10
					}
					cfg.Faults = Faults{MaxDelay: 10 * Millisecond, DriftPerMille: 100, Checkpoint: true}
					cfg.Duration, cfg.Heal = 24*Second, 12*Second
					cfg.PinClients, cfg.LongWatches, cfg.LeaseCycle = true, true, true
					if short {
						cfg.Duration, cfg.Heal = 7*Second, 8*Second
						cfg.Clients, cfg.NoLeases = 2, true
						cfg.PinClients = false
					}
					res := run(t, cfg)
					checkers(t, res)
					if res.Stats.OK < 100 || res.Stats.Snapshots == 0 {
						t.Fatalf("too little workload or no snapshot: %+v", res.Stats)
					}
					if short {
						h, err := res.Checkable()
						if err != nil {
							t.Fatal(err)
						}
						if !check.ShortEnough(h) {
							t.Fatalf("Porcupine history too large: %d operations", len(h.Ops))
						}
						if err := agree(h); err != nil {
							t.Fatal(err)
						}
					}
					t.Logf("scenario %s: %+v", cfg.NetworkTime, res.Stats)
				})
			}
		})
	}
}
