package sim

import (
	"fmt"
	"strconv"
	"testing"
)

func TestMembershipRound3(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 4
	}
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cfg := membershipConfig(seed)
			cfg.MembershipRound3 = true
			cfg.PinClients = true
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
			if a.Stats.Crashes < 6 || a.Stats.Compactions == 0 || a.Stats.Snapshots == 0 || a.Stats.ErasedAtRemoval == 0 {
				t.Fatalf("missing crash, compaction, snapshot or log cleaning: %+v", a.Stats)
			}
			checkers(t, a)
			h, err := a.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			if err := agree(h); err != nil {
				t.Fatal(err)
			}
			t.Logf("%+v", a.Stats)
		})
	}
}
