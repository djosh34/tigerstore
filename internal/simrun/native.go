//go:build !sim

package simrun

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jellevandenhooff/gosim/metatesting"
)

// InGosim reports whether the tests run under gosim.
const InGosim = false

// Bubble runs f, which runs one simulation, in a synctest bubble, on whose
// clock the nodes run.
func Bubble(t *testing.T, f func(*testing.T)) { synctest.Test(t, f) }

// BubbleFor is Bubble: natively a simulation has no timeout of virtual
// time.
func BubbleFor(t *testing.T, _ time.Duration, f func(*testing.T)) { synctest.Test(t, f) }

// Artifact does nothing natively.
func Artifact(string, []byte) {}

// AfterSeed runs f at once natively.
func AfterSeed(f func()) { f() }

// Here reports whether t runs its body in this process, as under the race
// detector. It reports false when scripts/sim.sh started t, which then
// runs its seeds in gosim with Do, and skips t otherwise.
func Here(t *testing.T) bool {
	t.Helper()
	if Race {
		return true
	}
	if os.Getenv("GOSIMTOOL") == "" {
		t.Skipf("the simulator runs under gosim: %s", Replay(dir(t), Pattern(t.Name()), nil))
	}
	return false
}

// passed are the variables of passedOn that are set, as KEY=VALUE.
func passed() []string {
	var env []string
	for _, name := range passedOn {
		if v := os.Getenv(name); v != "" {
			env = append(env, name+"="+v)
		}
	}
	return env
}

// Drive runs t's whole test, which runs the one harness seed seed, as a
// gosim run with that seed, the planted bug of $SIM_PLANTED and the
// profile of $SIM_LONG, and
// reports true when scripts/sim.sh started it; the caller then returns.
// It reports false when t runs its body here (Here).
func Drive(t *testing.T, seed uint64) bool {
	t.Helper()
	if Here(t) {
		return false
	}
	r := Do(t, Run{Test: Top(strings.Split(t.Name(), "/"))[0], Seed: seed, Env: passed()})
	keep(t, r.Artifacts, "")
	return true
}

// Do runs r in gosim. A failed run fails t, with the run's log, its
// hashes and the command that runs it again; with -test.v Do logs a
// passed run's log too, and how long each run took.
func Do(t *testing.T, r Run) *Result {
	t.Helper()
	pkg, _ := pkgPath(t)
	mt := metatesting.ForOtherPackage(t, pkg, tags...)
	start := time.Now()
	res, err := mt.RunQuiet(&metatesting.RunConfig{Test: r.Test, Seed: int64(r.Seed), ExtraEnv: r.Env})
	if err != nil {
		t.Fatalf("running %s in gosim: %v", r.Test, err)
	}
	if testing.Verbose() {
		t.Logf("the gosim run of seed %d took %v", r.Seed, time.Since(start).Round(time.Second))
	}
	if res.Failed {
		t.Errorf("%s failed in gosim with seed %d: %s\n%s", r.Test, r.Seed, res.Err, readable(res.LogOutput))
		t.Logf("hashes:\n%s", res.Artifacts["hashes"])
		t.Logf("replay: %s", Replay(dir(t), Pattern(t.Name()), r.Env))
	} else if testing.Verbose() {
		t.Logf("%s", readable(res.LogOutput))
	}
	return &Result{Failed: res.Failed, Artifacts: res.Artifacts}
}

// readable is a run's log as go test prints it: gosim writes what the
// test logged as JSON lines, with the file and line it logged from.
func readable(out []byte) string {
	var b strings.Builder
	for line := range strings.SplitAfterSeq(string(out), "\n") {
		var entry struct {
			Msg    string
			Source struct {
				File string
				Line int
			}
			// A panic that ended the run has its value and stack.
			Panic     string
			Traceback []string
		}
		if json.Unmarshal([]byte(line), &entry) != nil || entry.Msg == "" {
			// Not a log line: === RUN and --- PASS are text.
			b.WriteString(line)
			continue
		}
		fmt.Fprintf(&b, "%s:%d: %s\n", filepath.Base(entry.Source.File), entry.Source.Line, entry.Msg)
		if entry.Panic != "" {
			fmt.Fprintf(&b, "panic: %s\n", entry.Panic)
		}
		for _, l := range entry.Traceback {
			fmt.Fprintf(&b, "\t%s\n", l)
		}
	}
	return b.String()
}

// keep writes the files a run saved under sim-out/ to $SIM_OUT, each
// run's in a directory with suffix after its name, if set.
func keep(t *testing.T, artifacts map[string][]byte, suffix string) {
	t.Helper()
	for _, d := range keepQuiet(t, artifacts, suffix) {
		t.Logf("the run is saved in %s", d)
	}
}

// keepQuiet is keep, which returns the directories it wrote instead of
// logging them.
func keepQuiet(t *testing.T, artifacts map[string][]byte, suffix string) []string {
	t.Helper()
	files := map[string][]byte{}
	runs := map[string]bool{}
	for name, b := range artifacts {
		rel, ok := strings.CutPrefix(name, "sim-out/")
		if !ok {
			continue
		}
		run, path, _ := strings.Cut(rel, "/")
		if suffix != "" {
			// replay.txt names the run's directory, which gets the
			// suffix too.
			if path == "replay.txt" {
				b = bytes.ReplaceAll(b, []byte("$SIM_OUT/"+run+"/"), []byte("$SIM_OUT/"+run+"-"+suffix+"/"))
			}
			run += "-" + suffix
		}
		files[filepath.Join(run, path)] = b
		runs[run] = true
	}
	if len(files) == 0 {
		return nil
	}
	out := os.Getenv("SIM_OUT")
	if out == "" {
		t.Logf("set SIM_OUT to save the files of %v", slices.Sorted(maps.Keys(runs)))
		return nil
	}
	if err := WriteFiles(out, files); err != nil {
		t.Errorf("saving the run in %s: %v", out, err)
		return nil
	}
	var dirs []string
	for _, run := range slices.Sorted(maps.Keys(runs)) {
		dirs = append(dirs, filepath.Join(out, run))
	}
	return dirs
}

// each runs the seeds: here under the race detector, otherwise each as a
// gosim run of its own (Seeds). set holds the variables that pick the
// whole set again.
func each[T any](t *testing.T, seeds []uint64, set []string, body func(*testing.T, uint64) T, add func(uint64, T) bool) bool {
	t.Helper()
	if add == nil {
		add = func(uint64, T) bool { return false }
	}
	here := Here(t)
	known := knownFailures(t)
	twice := int(envUint(t, "SIM_TWICE", 0))
	test := Top(strings.Split(t.Name(), "/"))[0]
	passedEnv := passed()
	anyFailed := false
	if !here {
		// A rule over the set can fail the test with no seed failed; the
		// whole set then runs again.
		t.Cleanup(func() {
			if t.Failed() && !anyFailed {
				t.Logf("replay: %s", Replay(dir(t), Pattern(t.Name()), append(set, passedEnv...)))
			}
		})
	}
	for i, seed := range seeds {
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
			env := append([]string{fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1"}, passedEnv...)
			var r *Result
			if twice > 0 && i%max(1, len(seeds)/twice) == 0 {
				r = twoRuns(t, Run{Test: test, Seed: seed, Env: env})
			} else {
				r = Do(t, Run{Test: test, Seed: seed, Env: env})
				keep(t, r.Artifacts, "")
			}
			if r.Failed {
				return
			}
			t.Logf("hashes: %s", r.Artifacts["hashes"])
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

// twoRuns runs r twice, and fails t unless both give the same hashes.
// With SIM_KEEP each run saves its files, which are kept, as first and
// second, only when they differ or a run failed. The first run's files
// go to $SIM_OUT before the second run starts, so this process holds one
// run's files at a time, which for a long profile take gigabytes; they
// are removed again unless the runs differ.
func twoRuns(t *testing.T, r Run) *Result {
	t.Helper()
	r.Env = append(r.Env, "SIM_KEEP=1")
	first := Do(t, r)
	if first.Failed {
		keep(t, first.Artifacts, "")
		return first
	}
	saved := keepQuiet(t, first.Artifacts, "first")
	hashes := first.Artifacts["hashes"]
	first.Artifacts = map[string][]byte{"hashes": hashes, "seed.json": first.Artifacts["seed.json"]}
	second := Do(t, r)
	differ := false
	switch {
	case second.Failed:
		keep(t, second.Artifacts, "")
	case len(hashes) == 0:
		t.Errorf("seed %d left no hashes to compare", r.Seed)
	case string(hashes) != string(second.Artifacts["hashes"]):
		differ = true
		for _, d := range saved {
			t.Logf("the run is saved in %s", d)
		}
		keep(t, second.Artifacts, "second")
		env := slices.DeleteFunc(slices.Clone(r.Env), func(v string) bool { return v == "SIM_KEEP=1" })
		t.Errorf("seed %d played out differently the second time:\n%s\n%s\nreplay: %s", r.Seed,
			hashes, second.Artifacts["hashes"], Replay(dir(t), Pattern(t.Name()), append(env, "SIM_TWICE=1")))
	}
	if !differ {
		for _, d := range saved {
			if err := os.RemoveAll(d); err != nil {
				t.Error(err)
			}
		}
	}
	return first
}

// pkgPath is the import path of the package t tests, and that of its
// module: a test binary's main package is the package's path with .test
// after it.
func pkgPath(t *testing.T) (pkg, module string) {
	t.Helper()
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("the test binary has no build information")
	}
	return strings.TrimSuffix(bi.Path, ".test"), bi.Main.Path
}

// dir is the directory of the package t tests, from the module root.
func dir(t *testing.T) string {
	t.Helper()
	pkg, module := pkgPath(t)
	return "./" + strings.TrimPrefix(pkg, module+"/")
}

// Reseed does nothing natively.
func Reseed(uint64) {}
