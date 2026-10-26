// Package simrun runs the simulator's tests (docs/testing.md).
//
// Under gosim, which translates the tests with the sim build tag, a test
// runs on gosim's scheduler and clock, which the run's seed decides: a
// seed plays out the same way every time. scripts/sim.sh starts a test
// natively; the test then runs each of its seeds as a gosim run of its
// own (Drive, Do) and writes what the runs saved to $SIM_OUT. Under the
// race detector a test runs natively instead, in testing/synctest
// bubbles, as the race-sim job does. Plain go test skips it.
package simrun

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// A Run is one gosim run of a test.
type Run struct {
	// Test is the name of a top-level test of the package.
	Test string
	// Seed is gosim's seed: the harness seed, or 1 for a test that runs
	// fixed seeds.
	Seed uint64
	// Env holds the variables, as KEY=VALUE, that the test reads its
	// seeds, planted bug and profile from: SIM_SEED, SIM_SEEDS,
	// SIM_PLANTED and SIM_LONG.
	Env []string
}

// Result is what a run left.
type Result struct {
	Failed bool
	// Artifacts holds what the run saved: hashes, the line per run of
	// its trace, history and disk hashes; seed.json, what the seed's test
	// passes to its driver; and under sim-out/, the files of a run to
	// keep.
	Artifacts map[string][]byte
}

// Replay is the command that runs the tests that pattern selects in the
// package in dir again under gosim, with env. A planted bug needs the
// planted build tag.
func Replay(dir, pattern string, env []string) string {
	env = slices.Clone(env)
	if slices.ContainsFunc(env, func(v string) bool { return strings.HasPrefix(v, "SIM_PLANTED=") }) {
		env = append(env, "TAGS=planted")
	}
	slices.Sort(env)
	return strings.TrimSpace(strings.Join(env, " ")+" scripts/sim.sh "+dir) + " '" + pattern + "'"
}

// Long reports whether the tests run their long profiles ($SIM_LONG is 1),
// as the nightly kube-sim workflow does.
func Long() bool { return os.Getenv("SIM_LONG") == "1" }

// passedOn are the variables besides the seeds that the driver passes to
// each gosim run, which gets no other: the planted bug and the profile.
var passedOn = []string{"SIM_PLANTED", "SIM_LONG", "EXP441_OUT", "EXP441_MINUTES"}

// Pattern is the -run pattern of the test or subtest name. A subtest
// named after a test function, such as TestSeeds's deliberate scenario,
// runs as that test.
func Pattern(name string) string {
	parts := Top(strings.Split(name, "/"))
	for i, p := range parts {
		parts[i] = "^" + regexp.QuoteMeta(p) + "$"
	}
	return strings.Join(parts, "/")
}

// Top drops the parts of a test name before the last one named after a
// test function.
func Top(parts []string) []string {
	for i := len(parts) - 1; i > 0; i-- {
		if strings.HasPrefix(parts[i], "Test") {
			return parts[i:]
		}
	}
	return parts
}

// WriteFiles writes each of files, by its path under dir.
func WriteFiles(dir string, files map[string][]byte) error {
	var errs []error
	for name, b := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			errs = append(errs, err)
			continue
		}
		errs = append(errs, os.WriteFile(path, b, 0o644))
	}
	return errors.Join(errs...)
}
