package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"testing"
)

// TestCampaignKindShares checks the shares by cost from seed 1, at the
// campaign's 2,404 seeds (#502), longer prefixes and across a full cycle.
// These are kind lookups only: no seed runs in the simulator.
func TestCampaignKindShares(t *testing.T) {
	for _, seeds := range []uint64{2404, 4808, 10_000, 38_461, 76_922} {
		t.Run(strconv.FormatUint(seeds, 10), func(t *testing.T) {
			counts := map[string]int{}
			for seed := uint64(1); seed <= seeds; seed++ {
				counts[campaignKind(seed)]++
			}
			cost := map[string]int{
				kindFresh: counts[kindFresh] * costFresh,
				kindAged:  counts[kindAged] * costImage,
				kindLong:  counts[kindLong] * costLong,
			}
			total := cost[kindFresh] + cost[kindAged] + cost[kindLong]
			for _, k := range []struct {
				kind  string
				share int
			}{{kindFresh, shareFresh}, {kindAged, shareAged}, {kindLong, shareLong}} {
				share := 100 * float64(cost[k.kind]) / float64(total)
				t.Logf("seeds 1 to %d: %s %d seeds, cost %d of %d (%.2f%%)", seeds, k.kind, counts[k.kind], cost[k.kind], total, share)
				if delta := 100*cost[k.kind] - k.share*total; delta < -3*total || delta > 3*total {
					t.Errorf("%s is %.2f%% by cost, want within 3 points of %d%%", k.kind, share, k.share)
				}
			}
		})
	}
}

// TestCampaignKindDeterministic checks that seeds looked up alone, in a
// walk from 1, and in reverse order take the same kinds.
func TestCampaignKindDeterministic(t *testing.T) {
	seeds := []uint64{1500, 390, 1052, 1, 1424, 2180, 2404, 10_000}
	alone := map[uint64]string{}
	for _, seed := range seeds {
		alone[seed] = campaignKind(seed)
	}
	for seed := uint64(1); seed <= 10_000; seed++ {
		if got := campaignKind(seed); alone[seed] != "" && got != alone[seed] {
			t.Errorf("seed %d: walk gives %s, alone gives %s", seed, got, alone[seed])
		}
	}
	for _, seed := range slices.Backward(seeds) {
		if got := campaignKind(seed); got != alone[seed] {
			t.Errorf("seed %d: reverse order gives %s, alone gives %s", seed, got, alone[seed])
		}
	}
	for seed := uint64(0); seed <= 16; seed++ {
		if got := campaignKind(seed); got != kindFresh {
			t.Errorf("seed %d: starts with %s, want fresh", seed, got)
		}
	}
	// Large seed numbers must not need a walk or overflow the lookup.
	for _, seed := range []uint64{1 << 32, 1 << 63, ^uint64(0)} {
		equivalent := (seed-1)%uint64(len(campaignKinds)) + 1
		if got, want := campaignKind(seed), campaignKind(equivalent); got != want {
			t.Errorf("seed %d: gives %s, same place in the cycle gives %s", seed, got, want)
		}
	}
	// Large seeds at an aged and a long place of the cycle (seeds 390 and
	// 1,052's): a lookup truncated to 32 bits would give fresh there.
	for _, c := range []struct {
		seed uint64
		want string
	}{{^uint64(0) - 3308, kindAged}, {^uint64(0) - 2646, kindLong}} {
		if got := campaignKind(c.seed); got != c.want {
			t.Errorf("seed %d: gives %s, want %s", c.seed, got, c.want)
		}
	}
}

// TestCampaignKindCycleCloses checks that the allocation's deficits
// return to zero within campaignCycle's bound, so its table is one whole
// cycle and not a walk cut short.
func TestCampaignKindCycleCloses(t *testing.T) {
	if n := len(campaignKinds); n >= maxCampaignCycle {
		t.Fatalf("the allocation's deficits did not return to zero within %d seeds: the costs or shares need a cycle that closes", n)
	}
}

// TestCampaignKindRunningCost checks the table and its repeat against a
// walk that keeps the costs themselves, including the fixed tie-break.
func TestCampaignKindRunningCost(t *testing.T) {
	kinds := [...]string{kindFresh, kindAged, kindLong}
	costs := [...]int{costFresh, costImage, costLong}
	shares := [...]int{shareFresh, shareAged, shareLong}
	var spent [3]int
	total := 0
	for seed := 1; seed <= 2*len(campaignKinds)+1; seed++ {
		kind, largest := 0, 0
		for k := range kinds {
			deficit := shares[k]*(total+costs[k]) - 100*(spent[k]+costs[k])
			if k == 0 || deficit > largest {
				kind, largest = k, deficit
			}
		}
		if got := campaignKind(uint64(seed)); got != kinds[kind] {
			t.Fatalf("seed %d: table gives %s, running cost gives %s", seed, got, kinds[kind])
		}
		spent[kind] += costs[kind]
		total += costs[kind]
		if seed == len(campaignKinds) {
			for k := range kinds {
				if spent[k]*100 != shares[k]*total {
					t.Errorf("%s: cycle's cost %d is not %d%% of %d", kinds[k], spent[k], shares[k], total)
				}
			}
		}
	}
}

// campaignCycleSHA256 is the SHA-256 of one cycle of campaignKind, a
// letter a seed (f, a or l) from seed 1. scripts/codex-campaign.mjs
// plans Codex Cloud campaigns with its own copy of the allocation and
// checks it against the same hash; a change to the costs or shares
// changes both.
const campaignCycleSHA256 = "b84598f8d62599e2f2b3331b667910dea986eb2aeeab2a38635def15acaea592"

// TestCampaignKindCycleHash checks that the cycle is the one
// scripts/codex-campaign.mjs plans with.
func TestCampaignKindCycleHash(t *testing.T) {
	letters := make([]byte, len(campaignKinds))
	for i, kind := range campaignKinds {
		letters[i] = kind[0]
	}
	sum := sha256.Sum256(letters)
	if got := hex.EncodeToString(sum[:]); got != campaignCycleSHA256 {
		t.Errorf("the cycle's SHA-256 is %s, want %s: update campaignCycleSHA256 here and the copy of the allocation in scripts/codex-campaign.mjs", got, campaignCycleSHA256)
	}
}
