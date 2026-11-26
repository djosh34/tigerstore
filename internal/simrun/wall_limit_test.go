//go:build !sim

package simrun

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestResolveWallLimit(t *testing.T) {
	const long = 160 * time.Minute
	for _, tc := range []struct {
		test, env string
		policy    time.Duration
		want      time.Duration
	}{
		{"TestSeeds", "", 0, OrdinaryWallLimit},
		{"TestBusySeeds", "", 0, OrdinaryWallLimit},
		{"TestAgedForks", "", 0, OrdinaryWallLimit},
		{"TestCampaign", "", 0, OrdinaryWallLimit},
		{"TestCampaign", "", long, long},
		{"TestCampaignShapeSeeds", "", long, long},
		{"TestKubernetesLong", "", 0, 165 * time.Minute},
		{"TestKubernetesLong", "", long, long},
		{"TestSeeds", "2m", 0, 2 * time.Minute},
		{"TestCampaign", "2m", long, 2 * time.Minute},
		{"TestKubernetesLong", "2m", 0, 2 * time.Minute},
	} {
		t.Run(fmt.Sprintf("%s/env=%s/policy=%s", tc.test, tc.env, tc.policy), func(t *testing.T) {
			got, err := resolveWallLimit(tc.test, tc.env, tc.policy)
			if err != nil || got != tc.want {
				t.Fatalf("limit=%s, err=%v; want %s", got, err, tc.want)
			}
			if got+wallGrace != tc.want+10*time.Minute {
				t.Fatal("non-yielding kill grace changed")
			}
		})
	}
	for _, env := range []string{"bad", "0", "-1m"} {
		if _, err := resolveWallLimit("TestCampaign", env, long); err == nil {
			t.Fatalf("accepted invalid SIM_WALL %q", env)
		}
	}
	if _, err := resolveWallLimit("TestSeeds", "", -time.Second); err == nil {
		t.Fatal("accepted a negative per-seed limit")
	}
}

func TestSeedWallPolicyDoesNotChangeEnvironment(t *testing.T) {
	selectLimit := func(seed uint64) time.Duration {
		if seed == 7 {
			return 160 * time.Minute
		}
		return 0
	}
	opts := seedOptionsOf([]SeedOption{WithWallLimit(selectLimit)})
	if opts.wallLimit(7) != 160*time.Minute || opts.wallLimit(8) != 0 {
		t.Fatal("per-seed selector changed another seed's default")
	}
	r := Run{Test: "TestCampaign", Seed: 7, Env: []string{"SIM_SEED=7", "SIM_SEEDS=1"}}
	before, err := runEnv(r.Env)
	if err != nil {
		t.Fatal(err)
	}
	r.WallLimit = opts.wallLimit(r.Seed)
	after, err := runEnv(r.Env)
	if err != nil || !slices.Equal(before, after) {
		t.Fatalf("policy changed the run's environment: before %v, after %v, err %v", before, after, err)
	}
	if got, err := resolveWallLimit(r.Test, "", r.WallLimit); err != nil || got != r.WallLimit {
		t.Fatalf("driver ignored the selected limit: %s, %v", got, err)
	}
	if seedOptionsOf(nil).wallLimit != nil {
		t.Fatal("a run without a policy acquired a new limit")
	}
}
