package simrun

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
)

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
// detector the seeds run here.
func Seeds[T any](t *testing.T, first uint64, n int, body func(*testing.T, uint64) T, add func(uint64, T) bool) bool {
	t.Helper()
	first = envUint(t, "SIM_SEED", first)
	n = int(envUint(t, "SIM_SEEDS", uint64(n)))
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = first + uint64(i)
	}
	set := []string{fmt.Sprintf("SIM_SEED=%d", first), fmt.Sprintf("SIM_SEEDS=%d", n)}
	return each(t, seeds, set, body, add)
}

// Each is Seeds over fixed seeds of the test's own. When $SIM_SEEDS is 1,
// as in the replay of one seed, it runs only the seed $SIM_SEED, if it is
// one of them.
func Each[T any](t *testing.T, seeds []uint64, body func(*testing.T, uint64) T, add func(uint64, T) bool) bool {
	t.Helper()
	if One() {
		seed := envUint(t, "SIM_SEED", 0)
		seeds = slices.DeleteFunc(slices.Clone(seeds), func(s uint64) bool { return s != seed })
	}
	return each(t, seeds, nil, body, add)
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
