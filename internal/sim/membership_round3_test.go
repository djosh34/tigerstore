package sim

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/check"
)

func TestMembershipRound3(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 4
	}
	cleaned := 0
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cfg := membershipConfig(seed)
			cfg.Duration = 90 * Second
			cfg.MembershipRound3 = true
			cfg.PinClients = true
			// Alternate dense cleaning traffic with a small history on
			// which Porcupine must agree with both witness modes.
			if seed%2 == 0 {
				cfg.ClientThink = 200 * Millisecond
			}
			cfg.Faults = Faults{MaxDelay: 10 * Millisecond, SlowDisk: 5, Checkpoint: true, SlowFetch: true}
			a, err := runWith(t, cfg, func(s *Sim) error {
				var leader uint64
				for i, n := range s.live() {
					got := n.proc.core.Leader()
					if i == 0 {
						leader = got
					}
					if got == 0 || got > 3 || got != leader {
						return fmt.Errorf("node %d names leader %d, want original voter %d", n.id, got, leader)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for i, count := range a.Stats.MembershipMixes {
				if count != 1 {
					t.Fatalf("boundary %d ran %d fault mixes, want 1", i+1, count)
				}
			}
			if a.Stats.Crashes < 6 || a.Stats.Compactions == 0 || a.Stats.Snapshots == 0 {
				t.Fatalf("missing crash, compaction or snapshot: %+v", a.Stats)
			}
			cleaned += a.Stats.ErasedAtRemoval
			checkers(t, a)
			h, err := a.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ClientThink > 0 && !check.ShortEnough(h) {
				t.Fatalf("the thin workload has %d operations, more than Porcupine's %d", len(h.Ops), check.ShortOps)
			}
			if err := agree(h); err != nil {
				t.Fatal(err)
			}
			t.Logf("%+v", a.Stats)
		})
	}
	// Removal depends on the values still live in each segment. It must
	// happen across a campaign, not in every individual schedule.
	if n >= 16 && cleaned == 0 {
		t.Fatal("no seed reached a synced segment removal")
	}
	t.Logf("%d seeds reached a synced segment removal", cleaned)
}
