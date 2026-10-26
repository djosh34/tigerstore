package sim

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// envInt is the integer in $name, or def.
func envInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

// TestAgedForks (#449, exp branch only) ages a cluster once with the
// long profile of the Kubernetes-shaped load, then runs forks from its
// image (aged.go), each with every checker:
//
//   - SIM_AGE_OBJECTS objects (1,000) for SIM_AGE_MINUTES virtual
//     minutes (2), under the long profile's faults, or almost none with
//     SIM_AGE_CALM=1; the aging run is checked only with SIM_AGE_CHECK=1;
//   - SIM_FORKS forks (2) of SIM_FORK_SECONDS virtual seconds (120)
//     each, under the default faults, with a compaction every minute.
//
// Fork i of harness seed s has seed s<<16 + i. SIM_FORK_FIRST (0) is the
// first fork's i, so that fork i replays alone after the aging run with
// SIM_FORK_FIRST=i SIM_FORKS=1.
func TestAgedForks(t *testing.T) {
	if simrun.Race {
		t.Skip("runs under gosim (#439)")
	}
	simrun.Seeds(t, 1, 1, func(t *testing.T, seed uint64) Stats {
		objects := envInt(t, "SIM_AGE_OBJECTS", 1000)
		age := kubeConfig(seed, true)
		age.Objects = objects
		age.Duration = int64(envInt(t, "SIM_AGE_MINUTES", 2)) * 60 * Second
		age.Faults.DiskBudget = max(16<<20, int64(objects)*8<<10)
		age.Faults.DiskCapacity = int(2 * age.Faults.DiskBudget)
		// A damaged disk or a torn crash image fences its node for good,
		// and a fork needs all three: the aging run leaves those to the
		// forks.
		age.Faults.Corrupt, age.Faults.Rot, age.noFence = false, false, true
		if os.Getenv("SIM_AGE_CALM") == "1" {
			spreadFaults(&age.Faults, 1000)
		}
		checked := os.Getenv("SIM_AGE_CHECK") == "1"
		age.skipCheck = !checked
		var img *image
		t.Run("age", func(t *testing.T) {
			res, err := runWith(t, age, func(s *Sim) error {
				var err error
				img, err = s.capture()
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if checked {
				checkers(t, res)
			}
			t.Logf("aged seed %d for %d min, checked %v: %+v", seed, age.Duration/60/Second, checked, res.Stats)
		})
		if img == nil {
			t.Fatal("the aging run left no image")
		}
		t.Logf("image: %d entries applied, %d writes, revision %d, compacted at %d, %d base calls, %d bytes on the three disks",
			img.applied, img.entries, img.rev, img.compacted, len(img.base), img.bytes)
		for _, f := range img.files {
			t.Logf("image %s", f)
		}
		var total Stats
		first := envInt(t, "SIM_FORK_FIRST", 0)
		for i := first; i < first+envInt(t, "SIM_FORKS", 2); i++ {
			t.Run(fmt.Sprintf("fork%d", i), func(t *testing.T) {
				cfg := kubeConfig(seed<<16+uint64(i), false)
				cfg.Clients, cfg.V3Clients = 16, 0
				cfg.Objects = objects
				cfg.Duration = int64(envInt(t, "SIM_FORK_SECONDS", 120)) * Second
				cfg.CompactEvery = 60 * Second
				cfg.Faults.DiskBudget = age.Faults.DiskBudget
				cfg.Faults.DiskCapacity = age.Faults.DiskCapacity
				cfg.from = img
				res := run(t, cfg)
				checkers(t, res)
				st := res.Stats
				t.Logf("fork %d (seed %d): %d calls after the base, %d log writes, final revision %d: %+v",
					i, cfg.Seed, len(res.History)-len(img.base), len(res.Log), res.FinalRev, st)
				total.OK += st.OK
				total.Failed += st.Failed
				total.Ambiguous += st.Ambiguous
			})
		}
		return total
	}, nil)
}
