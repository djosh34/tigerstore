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
	"fmt"
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
	// Env holds the variables of runVars, as KEY=VALUE, that the run
	// reads its seeds and planted variant from, and whether it keeps its
	// files. Do passes the others of runVars empty.
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

// runVars are the only variables a gosim run gets, and it always gets
// all of them, in this order, empty when not set. A run is its commit,
// test and seed, and for TestPlanted its planted variant, whatever else
// the environment holds. gosim's maps draw from the run's one random
// stream for each entry past the fourth, and the run copies its
// environment into such a map when it first reads a variable, so one
// variable more or less would change every later choice (#482).
var runVars = []string{"SIM_SEED", "SIM_SEEDS", "SIM_PLANTED", "SIM_CONTROL", "SIM_KEEP"}

// passedOn are the variables of runVars besides the seeds that the
// driver takes from its own environment for each run: the planted bug,
// and whether to run its seed without the bug.
var passedOn = []string{"SIM_PLANTED", "SIM_CONTROL"}

// runEnv is env with each variable of runVars, in their order, and
// those that env does not set empty. A variable that is not one of them
// is an error.
func runEnv(env []string) ([]string, error) {
	values := map[string]string{}
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		if !slices.Contains(runVars, name) {
			return nil, fmt.Errorf("%w: %s", errRunVar, name)
		}
		values[name] = value
	}
	out := make([]string, len(runVars))
	for i, name := range runVars {
		out[i] = name + "=" + values[name]
	}
	return out, nil
}

var errRunVar = errors.New("a gosim run takes only the variables " + strings.Join(runVars, ", "))

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
