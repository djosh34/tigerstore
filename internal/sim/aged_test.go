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
// every node is up for the image; its forks keep those faults. A disk
// holds two checkpoints of the store the load can reach, with a margin
// (fitDisk). One seed in five keeps the disk of before, twice the budget,
// which holds less once there are 1,500 objects or so: its nodes age
// under NOSPACE, holding their checkpoints (#769), until an operator
// grows the disks and disarms (Faults.GrowDisk), and the aging must
// recover (#846's ruling). One seed in four finishes by granting 3,000
// to 12,000 leases of 120 s, with keys on a quarter of them, so the image
// holds thousands of live leases (#502). The forks renew, revoke and
// expire them; a new leader gives each its whole TTL after the idle gap.
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
	fitDisk(&cfg, kubeQuota(cfg.Objects))
	cfg.Nodes = pick(rng, scenarioSizes)
	if rng.IntN(2) == 0 {
		cfg.MemoryBytes = logUniform(rng, 256<<20, 4<<30)
	}
	cfg.HotKeys = rng.IntN(4) == 0
	// Drawn last, so that the draws before stay those of every image
	// aged before.
	if rng.IntN(5) == 0 {
		cfg.Faults.DiskCapacity, cfg.Quota, cfg.Faults.GrowDisk = int(2*cfg.Faults.DiskBudget), 0, true
	}
	// Drawn after the disk variant too: the older draws do not move.
	if rng.IntN(4) == 0 {
		cfg.ImageLeases = 3000 + rng.IntN(9001)
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

// forkStart is when fork i of image starts, after the image was taken:
// at a time fixed by the image's seed, whatever ran before it. Under
// gosim each fork starts in a run of its own from the image's file
// (#794); in one process, under the race detector, gosim's random stream
// is reseeded from the fork's seed. Between forks the whole
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
// (history.Start). Under gosim the aging saves the image to a file, and
// each fork runs from it in a gosim run of its own (simrun.ImageSeeds),
// so a fork replays alone: $SIM_FORK=k ages the image again, checks it is
// the same by its SHA-256 ($SIM_IMAGE_SHA256, which the fork's replay
// line names), and runs only fork k from it (#794). It skips unless
// $SIM_SEEDS is set: an image takes minutes to build.
func TestAgedForks(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: an image takes minutes to build")
	}
	simrun.ImageSeeds(t, 1, 0, forksPerImage, func(t *testing.T, seed uint64) Stats {
		return agedForks(t, seed, agingConfig(seed), forksPerImage, forkConfig)
	}, nil)
}

// TestAgedFork builds a small image, 300 objects for 40 s, and runs one
// fork of 30 s from it, every checker on it, as CI's short case.
func TestAgedFork(t *testing.T) {
	simrun.ImageEach(t, []uint64{1}, 1, func(t *testing.T, seed uint64) Stats {
		aging := agingConfig(seed)
		aging.Objects, aging.Duration = 300, 40*Second
		return agedForks(t, seed, aging, 1, func(aging Config, seed uint64) Config {
			cfg := forkConfig(aging, seed)
			cfg.Duration = 30 * Second
			return cfg
		})
	}, nil)
}

// TestAgedLeases builds an image with 3,000 live leases and keys on a
// quarter of them, then runs one short fork, every checker on both runs.
// The calm fork keeps some alive, revokes some and expires thousands.
func TestAgedLeases(t *testing.T) {
	simrun.ImageEach(t, []uint64{1}, 1, func(t *testing.T, seed uint64) Stats {
		aging := agingConfig(seed)
		aging.Nodes, aging.Objects, aging.Duration = 1, 300, 5*Second
		aging.ImageLeases = 3000
		st := agedForks(t, seed, aging, 1, func(aging Config, seed uint64) Config {
			cfg := forkConfig(aging, seed)
			cfg.Duration = 150 * Second
			cfg.Faults = Faults{DiskBudget: aging.Faults.DiskBudget, DiskCapacity: aging.Faults.DiskCapacity}
			return cfg
		})
		if st.ImageLeases < aging.ImageLeases {
			t.Errorf("image holds %d live leases, want at least %d", st.ImageLeases, aging.ImageLeases)
		}
		if save, _ := forkMode(t, 1); !save {
			if st.ImageKeepAlives == 0 || st.ImageRevokes == 0 || st.ImageExpiries < 2000 {
				t.Errorf("imaged leases: %d keepalives, %d client revokes, %d expiries; want renewals, revokes and thousands of expiries", st.ImageKeepAlives, st.ImageRevokes, st.ImageExpiries)
			}
		}
		return st
	}, nil)
}

// agedForks runs aging, takes its image and runs forks of it, and
// returns their stats added up.
func agedForks(t *testing.T, seed uint64, aging Config, forks int, fork func(Config, uint64) Config) Stats {
	t.Helper()
	save, only := forkMode(t, forks)
	if save && !simrun.InGosim {
		t.Skip("SIM_FORK=image saves an image only under gosim")
	}
	after := func(i int) time.Duration { return forkStart(seed, i) }
	if only >= 0 && simrun.InGosim {
		return runForks(t, seed, aging, savedImage(t), forks, only, fork, after)
	}
	// The aging runs in a subtest of its own, whose end lets its run go:
	// the forks keep the image alone (#792's re-check).
	var img *image
	aging.replay = replayCommand(fmt.Sprintf("%s/%d", strings.Split(t.Name(), "/")[0], seed), seed)
	if !t.Run("aging", func(t *testing.T) {
		res, err := runWith(t, aging, func(s *Sim) error {
			var err error
			img, err = s.capture()
			if err == nil && len(img.start.Leases) < aging.ImageLeases {
				return fmt.Errorf("image holds %d live leases, want at least %d", len(img.start.Leases), aging.ImageLeases)
			}
			return err
		})
		if err != nil {
			t.Fatalf("aging seed %d: %v", seed, err)
		}
		// The aging run passes every checker too: a fork's history starts
		// from its image's store, and no longer holds the aging's. A tight
		// disk that filled must have recovered (#846's ruling). Check
		// that before checkers releases a long run's history, without
		// stopping the independent checks if recovery failed.
		if aging.Faults.GrowDisk {
			if err := grownRecovery(res, false); err != nil {
				t.Errorf("seed %d: %v", seed, err)
			}
		}
		checkers(t, res)
		res.Stats.ImageLeases = len(img.start.Leases)
		t.Logf("aging seed %d: %d objects, %s, %d voters, memory budget %d: %d writes to revision %d, %d keys and %d leases at the image; %+v",
			seed, aging.Objects, seconds(aging.Duration), aging.voters(), aging.MemoryBytes, img.ops, img.start.Rev, len(img.start.KVs), len(img.start.Leases), res.Stats)
	}) {
		t.FailNow()
	}
	img.at = time.Now()
	if save {
		return saveImage(t, img)
	}
	return runForks(t, seed, aging, img, forks, only, fork, after)
}

// forkMode is what $SIM_FORK asks of a test of images (simrun.ImageSeeds):
// to make the image and save it (image), to run one fork k of it (k), or
// with it unset, to make the image and run every fork in this process, as
// under the race detector.
func forkMode(t *testing.T, forks int) (save bool, only int) {
	t.Helper()
	v := os.Getenv("SIM_FORK")
	switch v {
	case "":
		return false, -1
	case "image":
		return true, -1
	}
	k, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("SIM_FORK=%q: %v", v, err)
	}
	if k < 0 || k >= forks {
		t.Fatalf("SIM_FORK=%d: the image has forks 0 to %d", k, forks-1)
	}
	return false, k
}

// saveImage saves img to the run's output "image", a file the driver
// hands each fork's run as its input (simrun.ImageSeeds). It streams to
// the file, which alone holds the whole image.
func saveImage(t *testing.T, img *image) Stats {
	t.Helper()
	w, closeOutput, err := simrun.CreateOutput("image")
	if err != nil {
		t.Fatalf("the image's file: %v", err)
	}
	err = img.save(w)
	if cerr := closeOutput(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("saving the image: %v", err)
	}
	return Stats{ImageLeases: len(img.start.Leases)}
}

// savedImage is the image the driver gave this fork's run as its input
// "image" (saveImage).
func savedImage(t *testing.T) *image {
	t.Helper()
	r, size, closeInput, err := simrun.OpenInput("image")
	if err != nil {
		t.Fatalf("the image's file: %v", err)
	}
	img, err := loadImage(r, size)
	if cerr := closeInput(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("loading the image: %v", err)
	}
	return img
}

// runForks runs forks of img, an image of seed made for base, each at
// after(i) after the image, or only fork only if it is not -1, and
// returns their stats added up.
func runForks(t *testing.T, seed uint64, base Config, img *image, forks, only int, fork func(Config, uint64) Config, after func(i int) time.Duration) Stats {
	t.Helper()
	// The idle gaps count from when the image was taken, in this process
	// or the one that saved it. Under gosim the clock jumps to each
	// fork's slot; under the race detector each run keeps a clock of its
	// own (synctest's bubble), and no gap reaches a fork.
	start := img.at
	total := Stats{ImageLeases: len(img.start.Leases)}
	for i := range forks {
		if only >= 0 && i != only {
			continue
		}
		t.Run(fmt.Sprintf("fork%d", i), func(t *testing.T) {
			// Logged first, so that a fork that fails says how to run it
			// again: under gosim every fork runs in a run of its own from
			// its image's file, and replays alone, as SIM_FORK=k has the
			// driver make the image and run fork k from it (#794). The
			// failure's own replay line is that one too (Config.replay).
			image := fmt.Sprintf("SIM_FORK=%d %s", i, replayCommand(fmt.Sprintf("%s/%d", strings.Split(t.Name(), "/")[0], seed), seed))
			t.Logf("fork %d runs again with: %s", i, image)
			wait := time.Until(start.Add(after(i)))
			if wait < 0 {
				t.Fatalf("fork %d starts %s after its slot", i, -wait)
			}
			// gosim's limit of virtual time must outlast the wait.
			simrun.BubbleFor(t, wait+time.Hour, func(*testing.T) { time.Sleep(wait) })
			cfg := fork(base, seed*1000+uint64(i))
			cfg.from, cfg.replay = img, image
			simrun.Reseed(cfg.Seed)
			res := run(t, cfg)
			checkers(t, res)
			st := res.Stats
			t.Logf("fork %d (seed %d) of image %d, %s after it: %d ok, %d failed, %d ambiguous, %d writes after the image, %d fenced, %d replaced, %d cold restarts; imaged leases: %d keepalives, %d client revokes, %d expiries; hashes %s",
				i, cfg.Seed, seed, after(i), st.OK, st.Failed, st.Ambiguous, len(res.Log)-res.base, st.Fenced, st.Replaced, st.ColdRestarts, st.ImageKeepAlives, st.ImageRevokes, st.ImageExpiries, runHashes(res))
			total.OK += st.OK
			total.Failed += st.Failed
			total.Ambiguous += st.Ambiguous
			total.ImageKeepAlives += st.ImageKeepAlives
			total.ImageRevokes += st.ImageRevokes
			total.ImageExpiries += st.ImageExpiries
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
	simrun.ImageSeeds(t, 1, 0, 1, func(t *testing.T, seed uint64) Stats {
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

// TestBulkLoad loads eight values of 256 KiB, a Txn of four at a time,
// before the clients, and runs every checker on the run: the bulk
// client's calls follow one another (history.Validate), and the values
// are in the log and the store. Its disk budget sizes the log's ring to
// 8 MiB, and its disks hold twice the budget, as TestAgedBulk's do: the
// default ring of 1 MiB holds no Txn of four values, whose append then
// waits for room for good, and the default disk fills with the values'
// checkpoints (#769). TestAgedBulk is the same load at hundreds of MB, on
// Actions.
func TestBulkLoad(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Duration, cfg.BulkBytes = 10*Second, 8*bulkValue
		cfg.Faults.DiskBudget = 16 << 20
		cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats
	}, nil)
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
