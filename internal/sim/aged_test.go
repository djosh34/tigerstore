package sim

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// agingConfig is the aging run of an image's seed (#465, #474): the
// Kubernetes-shaped load on 500 to 5,000 objects for 2 to 30 simulated
// minutes, calm (faults 1,000 times further apart) or faulted (2 to 8
// times), on 1, 3 or 5 voters, half with a memory budget. Its crash
// images keep whole writes and no disk is damaged on purpose, so that
// every node is up for the image; its forks keep those faults.
func agingConfig(seed uint64) Config {
	rng := rand.New(rand.NewPCG(seed, 0x61676564))
	cfg := kubeConfig(seed, false)
	cfg.Objects = int(logUniform(rng, 500, 5000))
	cfg.Duration = logUniform(rng, 2*60*Second, 30*60*Second)
	cfg.CompactEvery = 60 * Second
	if rng.IntN(2) == 0 {
		spreadFaults(&cfg.Faults, 1000)
	} else {
		spreadFaults(&cfg.Faults, float64(2+rng.IntN(7)))
	}
	cfg.Faults.Corrupt, cfg.Faults.Rot, cfg.noFence = false, false, true
	cfg.Faults.DiskBudget = max(16<<20, int64(cfg.Objects)*8<<10)
	cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	cfg.Nodes = pick(rng, scenarioSizes)
	if rng.IntN(2) == 0 {
		cfg.MemoryBytes = logUniform(rng, 256<<20, 4<<30)
	}
	return cfg
}

// forkConfig is fork seed's run from an image aged by aging: the small
// keys' load beside the image's store (agedRequest), DefaultConfig's
// faults, an operator who replaces fenced nodes, and 2 to 10 simulated
// minutes of faults.
func forkConfig(aging Config, seed uint64) Config {
	rng := rand.New(rand.NewPCG(seed, 0x666f726b))
	cfg := DefaultConfig(seed)
	cfg.Nodes, cfg.MemoryBytes, cfg.SegmentBytes = aging.Nodes, aging.MemoryBytes, aging.SegmentBytes
	cfg.Faults.DiskBudget, cfg.Faults.DiskCapacity = aging.Faults.DiskBudget, aging.Faults.DiskCapacity
	cfg.Faults.Replace = true
	cfg.Duration = 2*60*Second + rng.Int64N(8*60*Second)
	return cfg
}

// forkSlot is how far apart the forks of one image start in simulated
// time: fork i starts i slots after its image, whatever ran before it,
// so that with gosim's random stream reseeded from its seed it plays out
// the same alone as after the forks before it.
const forkSlot = 3 * time.Hour

// forksPerImage is how many forks an image's seed runs.
const forksPerImage = 4

// TestAgedForks ages a cluster for each of $SIM_SEEDS seeds from
// $SIM_SEED (agingConfig), then runs forksPerImage forks of it
// (forkConfig), every checker on each, starting from the image's store
// (history.Start). $SIM_FORK=k runs only fork k after the aging, as its
// replay command does: the gosim run keeps its image's seed, from which
// the aging plays out the same. It skips unless $SIM_SEEDS is set: an
// image takes minutes to build.
func TestAgedForks(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: an image takes minutes to build")
	}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		return agedForks(t, seed, agingConfig(seed), forksPerImage, forkConfig)
	}, nil)
}

// TestAgedFork builds a small image, 300 objects for 40 s, and runs one
// fork of 30 s from it, every checker on it, as CI's short case.
func TestAgedFork(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		aging := agingConfig(seed)
		aging.Objects, aging.Duration = 300, 40*Second
		return agedForks(t, seed, aging, 1, func(aging Config, seed uint64) Config {
			cfg := forkConfig(aging, seed)
			cfg.Duration = 30 * Second
			return cfg
		})
	}, nil)
}

// agedForks runs aging, takes its image and runs forks of it, and
// returns their stats added up.
func agedForks(t *testing.T, seed uint64, aging Config, forks int, fork func(Config, uint64) Config) Stats {
	t.Helper()
	only := -1
	if v := os.Getenv("SIM_FORK"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("SIM_FORK=%q: %v", v, err)
		}
		only = k
	}
	var img *image
	start := time.Now()
	res, err := runWith(t, aging, func(s *Sim) error {
		var err error
		img, err = s.capture()
		return err
	})
	if err != nil {
		t.Fatalf("aging seed %d: %v", seed, err)
	}
	t.Logf("aging seed %d: %d objects, %s, %d voters, memory budget %d: %d writes to revision %d, %d keys and %d leases at the image; %+v",
		seed, aging.Objects, seconds(aging.Duration), aging.voters(), aging.MemoryBytes, img.ops, img.start.Rev, len(img.start.KVs), len(img.start.Leases), res.Stats)
	var total Stats
	for i := range forks {
		if only >= 0 && i != only {
			continue
		}
		t.Run(fmt.Sprintf("fork%d", i), func(t *testing.T) {
			wait := time.Until(start.Add(time.Duration(i+1) * forkSlot))
			if wait < 0 {
				t.Fatalf("fork %d starts %s after its slot", i, -wait)
			}
			// gosim's limit of virtual time must outlast the wait.
			simrun.BubbleFor(t, wait+time.Hour, func(*testing.T) { time.Sleep(wait) })
			cfg := fork(aging, seed*1000+uint64(i))
			cfg.from = img
			simrun.Reseed(cfg.Seed)
			res := run(t, cfg)
			checkers(t, res)
			st := res.Stats
			t.Logf("fork %d (seed %d) of image %d: %d ok, %d failed, %d ambiguous, %d writes after the image, %d fenced, %d replaced, %d cold restarts",
				i, cfg.Seed, seed, st.OK, st.Failed, st.Ambiguous, len(res.Log)-res.base, st.Fenced, st.Replaced, st.ColdRestarts)
			if forks > 1 {
				t.Logf("fork %d alone: SIM_FORK=%d %s", i, i, replayCommand(fmt.Sprintf("%s/%d", strings.Split(t.Name(), "/")[0], seed), seed))
			}
			total.OK += st.OK
			total.Failed += st.Failed
			total.Ambiguous += st.Ambiguous
		})
	}
	return total
}
