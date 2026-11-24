package sim

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// agingConfig is the aging run of an image's seed (#465, #474): the
// Kubernetes-shaped load on 500 to 20,000 objects for 2 simulated minutes
// to 2 hours, which writes up to millions of revisions, calm (faults
// 1,000 times further apart) or faulted (2 to 8 times), on 1, 3 or 5
// voters, half with a memory budget, a quarter with hot keys. Its crash
// images keep whole writes and no disk is damaged on purpose, so that
// every node is up for the image; its forks keep those faults.
func agingConfig(seed uint64) Config {
	rng := rand.New(rand.NewPCG(seed, 0x61676564))
	cfg := kubeConfig(seed, false)
	cfg.Objects = int(logUniform(rng, 500, 20_000))
	cfg.Duration = logUniform(rng, 2*60*Second, 2*3600*Second)
	cfg.CompactEvery = 60 * Second
	if rng.IntN(2) == 0 {
		spreadFaults(&cfg.Faults, 1000)
	} else {
		spreadFaults(&cfg.Faults, float64(2+rng.IntN(7)))
	}
	cfg.Faults.Corrupt, cfg.Faults.Rot, cfg.noFence, cfg.longCerts = false, false, true, true
	cfg.Faults.DiskBudget = max(16<<20, int64(cfg.Objects)*8<<10)
	cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	cfg.Nodes = pick(rng, scenarioSizes)
	if rng.IntN(2) == 0 {
		cfg.MemoryBytes = logUniform(rng, 256<<20, 4<<30)
	}
	cfg.HotKeys = rng.IntN(4) == 0
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

// forkStart is when fork i of image starts, after the image was taken:
// at a time fixed by the image's seed, whatever ran before it, so that
// with gosim's random stream reseeded from its seed it plays out the
// same alone as after the forks before it. Between forks the whole
// cluster is down for 3 hours and more: a few hours, days to months, or
// one to ten years, an idle stretch after which the clock has jumped
// ahead for the time-driven paths (#474). Nothing runs meanwhile, so
// years cost nothing.
func forkStart(image uint64, i int) time.Duration {
	var at time.Duration
	for j := range i + 1 {
		rng := rand.New(rand.NewPCG(image, 0x676170+uint64(j)))
		gap := 3 * time.Hour
		switch k := rng.IntN(10); {
		case k < 5:
			gap += time.Duration(rng.Int64N(int64(6 * time.Hour)))
		case k < 8:
			gap += time.Duration(logUniform(rng, int64(24*time.Hour), int64(180*24*time.Hour)))
		default:
			gap += time.Duration(logUniform(rng, int64(365*24*time.Hour), int64(10*365*24*time.Hour)))
		}
		at += gap
	}
	return at
}

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
			wait := time.Until(start.Add(forkStart(seed, i)))
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
			t.Logf("fork %d (seed %d) of image %d, %s after it: %d ok, %d failed, %d ambiguous, %d writes after the image, %d fenced, %d replaced, %d cold restarts",
				i, cfg.Seed, seed, forkStart(seed, i), st.OK, st.Failed, st.Ambiguous, len(res.Log)-res.base, st.Fenced, st.Replaced, st.ColdRestarts)
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

// TestAgedBulk measures an image bulk-loaded past its memory budget
// (#775): 160 MB of values under a budget of 137 MiB, the least three wl
// voters that bootstrap their cluster keep (#760), then one fork of it,
// every checker on
// each. It logs the gosim run's peak RSS and the runner's free memory, on
// the driver's side. It skips unless $SIM_SEEDS is set: it runs on
// Actions (sim-campaign.yml), not on a small host.
func TestAgedBulk(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: an image of hundreds of MB runs on Actions")
	}
	var before int64
	if !simrun.InGosim {
		before = memAvailable(t)
	}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		aging := agingConfig(seed)
		aging.Nodes, aging.Objects, aging.Duration = 3, 500, 2*60*Second
		aging.MemoryBytes, aging.BulkBytes = 137<<20, 160_000_000
		aging.Faults.DiskBudget = 2 * aging.BulkBytes
		aging.Faults.DiskCapacity = int(2 * aging.Faults.DiskBudget)
		return agedForks(t, seed, aging, 1, func(aging Config, seed uint64) Config {
			cfg := forkConfig(aging, seed)
			cfg.Duration = 2 * 60 * Second
			return cfg
		})
	}, func(seed uint64, _ Stats) bool {
		var ru syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &ru); err != nil {
			t.Error(err)
		}
		t.Logf("seed %d: peak RSS of a gosim run %d MiB; free memory %d MiB before, %d MiB after", seed, ru.Maxrss>>10, before>>20, memAvailable(t)>>20)
		return false
	})
}

// memAvailable is the runner's MemAvailable, in bytes.
func memAvailable(t *testing.T) int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Error(err)
		return 0
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), 10, 64)
			if err != nil {
				t.Error(err)
			}
			return kb << 10
		}
	}
	return 0
}
