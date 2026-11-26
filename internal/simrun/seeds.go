package simrun

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// SeedOption changes the native driver's policy for a set of seeds, not
// the environment or random stream of any simulated run.
type SeedOption func(*seedOptions)

type seedOptions struct {
	wallLimit func(uint64) time.Duration
}

// WithWallLimit selects each seed's real-time backstop. Zero keeps the
// default, and SIM_WALL still overrides it. For image seeds it applies to
// both the image and its forks. The selector runs only in the driver.
func WithWallLimit(limit func(uint64) time.Duration) SeedOption {
	return func(o *seedOptions) { o.wallLimit = limit }
}

func seedOptionsOf(opts []SeedOption) seedOptions {
	var o seedOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// FailedSeed lets a seed report a particular outcome even if its test
// failed. The body must return its result; a fatal failure saves none.
// Only a result that returns true is passed to add after a failed run.
type FailedSeed interface {
	ReportFailedSeed() bool
}

// Seeds runs body on each seed of the test's set, in a subtest named
// after the seed, and passes what body returns to add, if set, which
// reports whether to stop. The set is $SIM_SEEDS seeds, n by default, from
// $SIM_SEED, first by default. A seed that the package's
// known-failures.txt lists for the test is skipped.
//
// scripts/sim.sh runs each seed as a gosim run of its own, with gosim's
// seed equal to the harness seed and the planted bug of $SIM_PLANTED,
// and $SIM_TWICE seeds of the set twice; each of those must give the same
// hashes both times, or both runs are saved. Under gosim the test runs its
// seed, saves what body returned for the driver, and Seeds reports false:
// the driver applies the test's rules over the set. Under the race
// detector the seeds run here. A failed gosim run is left out unless its
// returned result implements FailedSeed and asks to be reported.
func Seeds[T any](t *testing.T, first uint64, n int, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts ...SeedOption) bool {
	t.Helper()
	first = envUint(t, "SIM_SEED", first)
	n = int(envUint(t, "SIM_SEEDS", uint64(n)))
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = first + uint64(i)
	}
	set := []string{fmt.Sprintf("SIM_SEED=%d", first), fmt.Sprintf("SIM_SEEDS=%d", n)}
	return each(t, seeds, set, noForks, body, add, seedOptionsOf(opts))
}

// noForks gives no seed forks: each is a gosim run of its own.
func noForks(uint64) int { return 0 }

// ImageSeeds is Seeds for a test whose seed makes an image and runs forks
// of it, forks in all (#794). The driver runs each seed as a gosim run
// that makes the image and saves its file, with $SIM_FORK=image, then a
// gosim run for each fork, or only for fork $SIM_FORK, with $SIM_FORK=k,
// which loads the file (OpenInput): every fork starts in a simulation of
// its own, in sequence as alone, and replays alone. body does what
// $SIM_FORK says; add gets each fork's result. Under the race detector
// the seeds run here, as body does with $SIM_FORK unset: the image and
// its forks in one process.
func ImageSeeds[T any](t *testing.T, first uint64, n, forks int, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts ...SeedOption) bool {
	t.Helper()
	return ImageSeedsBy(t, first, n, func(uint64) int { return forks }, body, add, opts...)
}

// ImageSeedsBy is ImageSeeds for a test whose seeds are of kinds: a seed
// forksOf gives forks makes an image and runs that many forks of it, as
// under ImageSeeds, and one it gives none runs as under Seeds, as
// TestCampaign's aged seeds and its others.
func ImageSeedsBy[T any](t *testing.T, first uint64, n int, forksOf func(seed uint64) int, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts ...SeedOption) bool {
	t.Helper()
	first = envUint(t, "SIM_SEED", first)
	n = int(envUint(t, "SIM_SEEDS", uint64(n)))
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = first + uint64(i)
	}
	set := []string{fmt.Sprintf("SIM_SEED=%d", first), fmt.Sprintf("SIM_SEEDS=%d", n)}
	return each(t, seeds, set, forksOf, body, add, seedOptionsOf(opts))
}

// ImageEach is ImageSeeds over fixed seeds of the test's own, as Each is.
func ImageEach[T any](t *testing.T, seeds []uint64, forks int, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts ...SeedOption) bool {
	t.Helper()
	if One() {
		seed := envUint(t, "SIM_SEED", 0)
		seeds = slices.DeleteFunc(slices.Clone(seeds), func(s uint64) bool { return s != seed })
		if len(seeds) == 0 {
			t.Skipf("seed %d is not one of this test's", seed)
		}
	}
	return each(t, seeds, nil, func(uint64) int { return forks }, body, add, seedOptionsOf(opts))
}

// Each is Seeds over fixed seeds of the test's own. When $SIM_SEEDS is 1,
// as in the replay of one seed, it runs only the seed $SIM_SEED, if it is
// one of them.
func Each[T any](t *testing.T, seeds []uint64, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts ...SeedOption) bool {
	t.Helper()
	if One() {
		seed := envUint(t, "SIM_SEED", 0)
		seeds = slices.DeleteFunc(slices.Clone(seeds), func(s uint64) bool { return s != seed })
		if len(seeds) == 0 {
			t.Skipf("seed %d is not one of this test's", seed)
		}
	}
	return each(t, seeds, nil, noForks, body, add, seedOptionsOf(opts))
}

// One reports whether the tests run one seed, as a replay does
// ($SIM_SEEDS is 1). A test's rules over its whole set do not apply then.
func One() bool { return os.Getenv("SIM_SEEDS") == "1" }

// envUint is the number in the variable name, or def when it is not set.
func envUint(t *testing.T, name string, def uint64) uint64 {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return n
}
