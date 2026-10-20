//go:build !sim

package simrun

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/jellevandenhooff/gosim/metatesting"
)

// InGosim reports whether the tests run under gosim.
const InGosim = false

// Bubble runs f, which runs one simulation, in a synctest bubble, on whose
// clock the nodes run.
func Bubble(t *testing.T, f func(*testing.T)) { synctest.Test(t, f) }

// Artifact does nothing natively.
func Artifact(string, []byte) {}

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

// Drive runs t's whole test as one gosim run with seed 1, with the planted
// bug of $SIM_PLANTED, and reports true when scripts/sim.sh started it;
// the caller then returns. It reports false when t runs its body here
// (Here).
func Drive(t *testing.T) bool {
	t.Helper()
	if Here(t) {
		return false
	}
	var env []string
	if bug := os.Getenv("SIM_PLANTED"); bug != "" {
		env = append(env, "SIM_PLANTED="+bug)
	}
	Do(t, Run{Test: Top(strings.Split(t.Name(), "/"))[0], Seed: 1, Env: env})
	return true
}

// Do runs r in gosim. A failed run fails t, with the run's log, its
// hashes and the command that runs it again; with -test.v Do logs a
// passed run's log too. Do writes the files the run kept to $SIM_OUT.
func Do(t *testing.T, r Run) *Result {
	t.Helper()
	pkg, _ := pkgPath(t)
	mt := metatesting.ForOtherPackage(t, pkg, tags...)
	res, err := mt.RunQuiet(&metatesting.RunConfig{Test: r.Test, Seed: int64(r.Seed), ExtraEnv: r.Env})
	if err != nil {
		t.Fatalf("running %s in gosim: %v", r.Test, err)
	}
	keep(t, res.Artifacts)
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
		}
		if json.Unmarshal([]byte(line), &entry) != nil || entry.Msg == "" {
			// Not a log line: === RUN and --- PASS are text.
			b.WriteString(line)
			continue
		}
		fmt.Fprintf(&b, "%s:%d: %s\n", filepath.Base(entry.Source.File), entry.Source.Line, entry.Msg)
	}
	return b.String()
}

// keep writes the files a run saved under sim-out/ to $SIM_OUT.
func keep(t *testing.T, artifacts map[string][]byte) {
	t.Helper()
	out := os.Getenv("SIM_OUT")
	saved := map[string]bool{}
	for name, b := range artifacts {
		rel, ok := strings.CutPrefix(name, "sim-out/")
		if !ok {
			continue
		}
		run, _, _ := strings.Cut(rel, "/")
		if out == "" {
			saved[run] = false
			continue
		}
		path := filepath.Join(out, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Errorf("saving the run: %v", err)
			return
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Errorf("saving the run: %v", err)
			return
		}
		saved[run] = true
	}
	for run, ok := range saved {
		if ok {
			t.Logf("the run is saved in %s", filepath.Join(out, run))
		} else {
			t.Logf("set SIM_OUT to save the files of run %s", run)
		}
	}
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
