//go:build !sim

package sim

import (
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"testing"
)

// campaignLongSeeds shares the simulator's allocator, never runs a seed,
// and stops before producing more than an Actions matrix can hold.
func campaignLongSeeds(test string, first, seeds uint64) []uint64 {
	longs := []uint64{}
	if test != "TestCampaign" && test != "TestCampaignShapeSeeds" {
		return longs
	}
	for i := range seeds {
		seed := first + i
		if test == "TestCampaignShapeSeeds" || campaignKind(seed) == kindLong {
			longs = append(longs, seed)
			if len(longs) > 256 {
				break
			}
		}
	}
	return longs
}

// TestCampaignPlan writes only the long seeds for the workflow's Python
// planner. No gosim build or simulator run is needed, even for long seeds.
func TestCampaignPlan(t *testing.T) {
	path := os.Getenv("SIM_CAMPAIGN_LONG_SEEDS")
	if path == "" {
		t.Skip("set SIM_CAMPAIGN_LONG_SEEDS to write the planner's long seeds")
	}
	first, err := strconv.ParseUint(os.Getenv("FIRST"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	seeds, err := strconv.ParseUint(os.Getenv("SEEDS"), 10, 64)
	if err != nil || seeds == 0 || seeds-1 > ^uint64(0)-first {
		t.Fatalf("invalid seed range: FIRST=%q SEEDS=%q", os.Getenv("FIRST"), os.Getenv("SEEDS"))
	}
	data, err := json.Marshal(campaignLongSeeds(os.Getenv("TEST"), first, seeds))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignLongSeeds(t *testing.T) {
	for _, c := range []struct {
		test         string
		first, seeds uint64
		want         int
	}{
		{"TestCampaign", 1, 2404, 1},
		{"TestCampaign", 1, 4808, 2},
		{"TestCampaign", 1052, 1, 1},
		{"TestCampaignShapeSeeds", 17, 4, 4},
		{"TestCampaignShapeSeeds", 1, 100_000, 257},
		{"TestSeeds", 1, 100_000, 0},
	} {
		longs := campaignLongSeeds(c.test, c.first, c.seeds)
		if len(longs) != c.want {
			t.Errorf("%s %d+%d: got long seeds %v, want %d", c.test, c.first, c.seeds, longs, c.want)
		}
		if c.test == "TestCampaign" && len(longs) > 0 && longs[0] != 1052 {
			t.Errorf("first long seed = %d, want 1052", longs[0])
		}
		if c.test == "TestCampaign" && c.seeds == 4808 && !slices.Equal(longs, []uint64{1052, 3546}) {
			t.Errorf("planner fixture's long seeds = %v, want [1052 3546]", longs)
		}
	}
}
