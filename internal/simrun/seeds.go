package simrun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Seeds runs body on each seed of the set, in a subtest named after the
// seed, and passes what body returns to add, if set, which reports
// whether to stop. The set is $SIM_SEEDS seeds, n by default, from
// $SIM_SEED, first by default. A seed that the package's
// known-failures.txt lists for the test is skipped.
//
// scripts/sim.sh runs each seed as a gosim run of its own, with gosim's
// seed equal to the harness seed and the planted bug of $SIM_PLANTED,
// and $SIM_TWICE seeds of the set twice; each of those must give the same
// hashes both times. Under gosim the test runs its seed,
// saves what body returned for the driver, and Seeds reports false: the
// driver applies the test's rules over the set. Under the race detector
// the seeds run here.
func Seeds[T any](t *testing.T, first uint64, n int, body func(*testing.T, uint64) T, add func(uint64, T) bool) bool {
	t.Helper()
	first = envUint(t, "SIM_SEED", first)
	n = int(envUint(t, "SIM_SEEDS", uint64(n)))
	if add == nil {
		add = func(uint64, T) bool { return false }
	}
	if InGosim {
		for seed := first; seed < first+uint64(n); seed++ {
			t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
				b, err := json.Marshal(body(t, seed))
				if err != nil {
					t.Fatal(err)
				}
				Artifact("seed.json", b)
			})
		}
		return false
	}
	here := Here(t)
	known := knownFailures(t)
	twice := int(envUint(t, "SIM_TWICE", 0))
	test := Top(strings.Split(t.Name(), "/"))[0]
	var env []string
	if bug := os.Getenv("SIM_PLANTED"); bug != "" {
		env = append(env, "SIM_PLANTED="+bug)
	}
	anyFailed := false
	if !here {
		// A rule over the set can fail the test with no seed failed; the
		// whole set then runs again.
		t.Cleanup(func() {
			if t.Failed() && !anyFailed {
				set := append([]string{fmt.Sprintf("SIM_SEED=%d", first), fmt.Sprintf("SIM_SEEDS=%d", n)}, env...)
				t.Logf("replay: %s", Replay(dir(t), Pattern(t.Name()), set))
			}
		})
	}
	for i := range n {
		seed := first + uint64(i)
		stop := false
		anyFailed = !t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			if bug, ok := known[knownSeed{test, seed}]; ok {
				// Printed also without -test.v, which shows no skip.
				msg := fmt.Sprintf("known failure: seed %d of %s fails because of %s (known-failures.txt)", seed, test, bug)
				if _, err := fmt.Fprintln(os.Stdout, msg); err != nil {
					t.Error(err)
				}
				t.Skip(msg)
			}
			if here {
				stop = add(seed, body(t, seed))
				return
			}
			env := append([]string{fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1"}, env...)
			r := Do(t, Run{Test: test, Seed: seed, Env: env})
			if r.Failed {
				return
			}
			t.Logf("hashes: %s", r.Artifacts["hashes"])
			if twice > 0 && i%max(1, n/twice) == 0 {
				again := Do(t, Run{Test: test, Seed: seed, Env: env})
				switch {
				case again.Failed:
				case len(r.Artifacts["hashes"]) == 0:
					t.Errorf("seed %d left no hashes to compare", seed)
				case string(again.Artifacts["hashes"]) != string(r.Artifacts["hashes"]):
					t.Errorf("seed %d played out differently the second time:\n%s\n%s\nreplay: %s", seed,
						r.Artifacts["hashes"], again.Artifacts["hashes"], Replay(dir(t), Pattern(t.Name()), append(env, "SIM_TWICE=1")))
				}
			}
			var v T
			if err := json.Unmarshal(r.Artifacts["seed.json"], &v); err != nil {
				t.Fatalf("seed %d left no result: %v", seed, err)
			}
			stop = add(seed, v)
		}) || anyFailed
		if stop {
			break
		}
	}
	return true
}

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

// A knownSeed is a seed of a test.
type knownSeed struct {
	test string
	seed uint64
}

// knownFailures are the seeds in the package's known-failures.txt, if it
// has one, which fail because of an open bug issue, with the issue.
func knownFailures(t *testing.T) map[knownSeed]string {
	t.Helper()
	known := map[knownSeed]string{}
	f, err := os.Open("known-failures.txt")
	if errors.Is(err, fs.ErrNotExist) {
		return known
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	lines := bufio.NewScanner(f)
	for line := 1; lines.Scan(); line++ {
		text, _, _ := strings.Cut(lines.Text(), "#")
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
		var seed uint64
		if len(fields) == 3 {
			seed, err = strconv.ParseUint(fields[1], 10, 64)
		} else {
			err = errBadLine
		}
		if err != nil {
			t.Fatalf("known-failures.txt:%d: %v: want a test, a seed and an issue", line, err)
		}
		known[knownSeed{fields[0], seed}] = fields[2]
	}
	if err := lines.Err(); err != nil {
		t.Fatal(err)
	}
	return known
}

var errBadLine = errors.New("not three fields")
