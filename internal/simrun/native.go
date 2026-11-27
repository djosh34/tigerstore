//go:build !sim

package simrun

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
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

// WallLimitPassed reports false natively, where go test's -timeout bounds
// a run.
func WallLimitPassed() bool { return false }

// CPUTime is the CPU time the process has used so far, on all of its
// threads.
func CPUTime() (time.Duration, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}

// TimerFirings is 0 natively, where nothing counts the timers that fire.
func TimerFirings() uint64 { return 0 }

// Aside runs f: natively there is no run's random stream to keep.
func Aside(f func()) { f() }

// Stacks returns every goroutine's stack.
func Stacks() []byte {
	buf := make([]byte, 1<<20)
	for {
		if n := runtime.Stack(buf, true); n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

// Artifact does nothing natively.
func Artifact(string, []byte) {}

// Reseed does nothing: natively Go's scheduler decides the order of the
// work, and no seed replays.
func Reseed(uint64) {}

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
// gosim run with that seed and the planted bug of $SIM_PLANTED, and
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
	env, err := runEnv(r.Env)
	if err != nil {
		t.Fatalf("running %s in gosim: %v", r.Test, err)
	}
	limit := wallLimit(t, r.WallLimit)
	if r.WallLimit > 0 && testing.Verbose() {
		t.Logf("seed %d real-time backstop %s, non-yield kill %s", r.Seed, limit, limit+wallGrace)
	}
	start := time.Now()
	res, err := mt.RunQuiet(&metatesting.RunConfig{Test: r.Test, Seed: int64(r.Seed), ExtraEnv: env, WallLimit: limit, WallKill: limit + wallGrace, Inputs: r.Inputs, Outputs: r.Outputs})
	if err != nil {
		t.Fatalf("running %s in gosim with seed %d: %v\nreplay: %s", r.Test, r.Seed, err, Replay(dir(t), Pattern(t.Name()), append(slices.Clone(r.Env), r.ReplayEnv...)))
	}
	keepBlackbox(filepath.Join("sim-out", strings.ReplaceAll(t.Name(), "/", "_")), res)
	if testing.Verbose() {
		t.Logf("the gosim run of seed %d took %v", r.Seed, time.Since(start).Round(time.Second))
	}
	if res.Failed {
		t.Errorf("%s failed in gosim with seed %d: %s\n%s", r.Test, r.Seed, res.Err, readable(res.LogOutput))
		t.Logf("hashes:\n%s", res.Artifacts["hashes"])
		t.Logf("replay: %s", Replay(dir(t), Pattern(t.Name()), append(slices.Clone(r.Env), r.ReplayEnv...)))
	} else if testing.Verbose() {
		t.Logf("%s", readable(res.LogOutput))
	}
	return &Result{Failed: res.Failed, Artifacts: res.Artifacts}
}

// keepBlackbox adds to res's artifacts, as the files of the run in
// directory run, what a run that gosim stopped short had saved: the trace
// and nodes its watchdog saved last, at most 10 simulated seconds old,
// and the stack of every goroutine, or the log with the panic. A run gosim
// ended past its limit of real time never yielded again, and in a run that
// panicked gosim stops every goroutine at once, so neither gets to write
// its calls, history or disks, which a replay does not save either: it
// stops the same way (#499). A file the run saved itself, as one that
// panicked after its report, stays.
func keepBlackbox(run string, res *metatesting.RunResult) {
	files := map[string][]byte{}
	switch {
	case res.Ended:
		files["stacks.txt"] = res.Artifacts["gosim-stacks.txt"]
	case res.Err == gosimruntime.ErrPaniced.Error():
		files["log.txt"] = []byte(readable(res.LogOutput))
	default:
		return
	}
	for _, name := range []string{"trace.txt", "nodes.txt"} {
		if b, ok := res.Artifacts["blackbox/"+name]; ok {
			files[name] = b
		}
	}
	if res.Artifacts == nil {
		res.Artifacts = map[string][]byte{}
	}
	for name, b := range files {
		path := filepath.Join(run, name)
		if _, ok := res.Artifacts[path]; !ok {
			res.Artifacts[path] = b
		}
	}
}

// wallGrace is how long past its limit of real time a run that never
// yields gets before gosim ends it.
const wallGrace = 10 * time.Minute

// wallLimit is each run's real-time backstop: SIM_WALL overrides its
// per-seed policy, otherwise the default is OrdinaryWallLimit, or 165 for a test
// named *Long. A run past it stops with its report and stacks; one that
// never yields is ended wallGrace later (#499). The limit is not passed
// into the simulated environment and changes no run before it passes.
func wallLimit(t *testing.T, configured time.Duration) time.Duration {
	t.Helper()
	d, err := resolveWallLimit(Top(strings.Split(t.Name(), "/"))[0], os.Getenv("SIM_WALL"), configured)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func resolveWallLimit(test, override string, configured time.Duration) (time.Duration, error) {
	if override != "" {
		d, err := time.ParseDuration(override)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("SIM_WALL: %q is not a positive duration", override)
		}
		return d, nil
	}
	if configured < 0 {
		return 0, fmt.Errorf("run wall limit %s is negative", configured)
	}
	if configured > 0 {
		return configured, nil
	}
	if strings.HasSuffix(test, "Long") {
		// Keep the existing kube-sim limit and its margin below the
		// 180 minutes kube-sim.sh gives that test binary.
		return 165 * time.Minute, nil
	}
	return OrdinaryWallLimit, nil
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
// gosim run of its own (Seeds), or, for a seed forksOf gives forks, as an
// image and its forks (ImageSeeds). set holds the variables that pick the
// whole set again.
func each[T any](t *testing.T, seeds []uint64, set []string, forksOf func(uint64) int, body func(*testing.T, uint64) T, add func(uint64, T) bool, opts seedOptions) bool {
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
		bug, knownFailure := known[knownSeed{test, seed}]
		anyFailed = !seedSubtest(t, test, seed, knownFailure, func(t *testing.T) {
			if knownFailure {
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
			double := doubleSeed(i, len(seeds), twice)
			run := Run{Test: test, Seed: seed, Env: env}
			if opts.wallLimit != nil {
				run.WallLimit = opts.wallLimit(seed)
			}
			if forks := forksOf(seed); forks > 0 {
				stop = imageAndForks(t, run, forks, double, add)
				return
			}
			r := part[T](t, run, double)
			if r.Failed {
				stop = addFailedSeed(seed, r.Artifacts["seed.json"], add)
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

// driverVerdict writes directly, not through testing's buffered subtest
// log. A later killed seed must not erase the verdicts of finished seeds.
func driverVerdict(t *testing.T, part, name, status string) {
	t.Helper()
	if _, err := fmt.Fprintf(os.Stdout, "simrun: %s %s %s\n", part, name, status); err != nil {
		t.Error(err)
	}
}

// seedSubtest reports after t.Run returns, including its cleanups. Go's
// own PASS/FAIL lines may remain buffered until the parent test ends.
func seedSubtest(t *testing.T, test string, seed uint64, knownFailure bool, body func(*testing.T)) bool {
	t.Helper()
	ran, skipped := false, false
	passed := t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
		ran = true
		t.Cleanup(func() { skipped = t.Skipped() })
		body(t)
	})
	if !ran {
		return passed // A filtered-out subtest is not a completed seed.
	}
	status := "PASS"
	if !passed {
		status = "FAIL"
	} else if skipped {
		status = "SKIP"
		if knownFailure {
			status += " known-failure"
		}
	}
	part := "seed"
	if os.Getenv("SIM_FORK") != "" {
		// Image-only and single-fork replays are not whole-seed results,
		// even when all the work selected by the replay passed.
		part = "partial"
	}
	driverVerdict(t, part, fmt.Sprintf("%s/%d", test, seed), status)
	return passed
}

// doubleSeed selects exactly min(n, twice) evenly spaced seeds, starting
// with the first. Integer division's old stride could select extra seeds.
func doubleSeed(i, n, twice int) bool {
	twice = min(n, max(0, twice))
	return twice > 0 && i*twice%n < twice
}

// addFailedSeed passes a failed seed's result to add only when it asks
// to be reported (FailedSeed). A fatal failure may have saved no result.
func addFailedSeed[T any](seed uint64, data []byte, add func(uint64, T) bool) bool {
	if v, report := failedSeed[T](data); report {
		return add(seed, v)
	}
	return false
}

// failedSeed decodes a saved result and reports whether it opts in to
// being counted even though its run failed (FailedSeed).
func failedSeed[T any](data []byte) (v T, report bool) {
	if err := json.Unmarshal(data, &v); err != nil {
		return v, false
	}
	f, ok := any(v).(FailedSeed)
	return v, ok && f.ReportFailedSeed()
}

// errNoInput is OpenInput's error outside gosim, which gives a run no
// inputs.
var errNoInput = errors.New("simrun: a run has inputs and outputs only under gosim")

// OpenInput opens the run's input name (Run.Inputs). Only a gosim run has
// any.
func OpenInput(string) (io.ReaderAt, int64, func() error, error) { return nil, 0, nil, errNoInput }

// CreateOutput makes the run's output name (Run.Outputs). Only a gosim run
// has any.
func CreateOutput(string) (io.Writer, func() error, error) { return nil, nil, errNoInput }

// part runs r once, keeping its files, or twice if double (twoRuns).
func part[T any](t *testing.T, r Run, double bool) *Result {
	t.Helper()
	if double {
		return twoRuns[T](t, r)
	}
	res := Do(t, r)
	keep(t, res.Artifacts, "")
	return res
}

// imageAndForks runs seed r of a test of images (ImageSeeds) as one gosim
// run that makes the image and saves its file (SIM_FORK=image), then one
// for each fork, or only for fork $SIM_FORK, which loads that file
// (OpenInput): each fork starts in a simulation of its own, which no
// other fork ran in before, as alone (#794). Every gosim run of the test
// binary starts afresh, as each seed of a campaign does. $SIM_FORK=image
// makes the image only. It checks the image's SHA-256 against
// $SIM_IMAGE_SHA256, if set, as a fork's replay line sets it, and calls
// add with each fork's result, as under the race detector add gets each
// seed's. With double, the image and each fork run twice and must give
// the same image and hashes.
func imageAndForks[T any](t *testing.T, r Run, forks int, double bool, add func(uint64, T) bool) bool {
	t.Helper()
	only, imageOnly := -1, false
	switch v := os.Getenv("SIM_FORK"); v {
	case "":
	case "image":
		imageOnly = true
	default:
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 || k >= forks {
			t.Fatalf("SIM_FORK=%q: the image has forks 0 to %d", v, forks-1)
		}
		only = k
	}
	env := slices.DeleteFunc(slices.Clone(r.Env), func(v string) bool { return strings.HasPrefix(v, "SIM_FORK=") })
	r.Env = append(slices.Clone(env), "SIM_FORK=image")
	path, hash, ok := runImage(t, r, double)
	if !ok || imageOnly {
		return false
	}
	// Keep the verified image with this diagnostic's streamed history.
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	imageOut, err := os.Create(filepath.Join(os.Getenv("SIM_OUT"), "image.bin"))
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(imageOut, input)
	closeErr, inputErr := imageOut.Close(), input.Close()
	if err := errors.Join(copyErr, closeErr, inputErr); err != nil {
		t.Fatal(err)
	}
	proof, _ := json.Marshal(map[string]any{"actual_image_sha256": hash, "expected_image_sha256": os.Getenv("SIM_IMAGE_SHA256"), "matched": true})
	if err := os.WriteFile(filepath.Join(os.Getenv("SIM_OUT"), "image-proof.json"), proof, 0o644); err != nil {
		t.Fatal(err)
	}
	stop := false
	for k := range forks {
		if only >= 0 && k != only {
			continue
		}
		fork := Run{
			Test: r.Test, Seed: r.Seed, WallLimit: r.WallLimit, Env: append(slices.Clone(env), fmt.Sprintf("SIM_FORK=%d", k)),
			Inputs: map[string]string{"image": path}, ReplayEnv: []string{"SIM_IMAGE_SHA256=" + hash},
		}
		// NEVER MERGE: stream the single diagnostic history to a driver-owned
		// file, rather than duplicating gigabytes in the gosim artifact map.
		if r.Seed != 5 || k != 0 || double {
			t.Fatal("diagnostic requires seed 5, fork 0, once")
		}
		fork.Outputs = map[string]string{"diagnostic-history": filepath.Join(os.Getenv("SIM_OUT"), "history.json")}
		stop = imageFork(t, fork, k, double, add) || stop
	}
	return stop
}

// imageFork reports each completed fork separately, without presenting a
// fork's result as the verdict of the seed that owns all the forks.
func imageFork[T any](t *testing.T, r Run, k int, double bool, add func(uint64, T) bool) bool {
	t.Helper()
	status := "FAIL"
	defer func() { driverVerdict(t, "fork", fmt.Sprintf("%s/%d/%d", r.Test, r.Seed, k), status) }()
	res := part[T](t, r, double)
	if res.Failed {
		// A failed fork's result still counts where it asks to (FailedSeed,
		// a missed quorum loss); its verdict stays FAIL.
		return addFailedSeed(r.Seed, res.Artifacts["seed.json"], add)
	}
	t.Logf("fork %d hashes: %s", k, res.Artifacts["hashes"])
	var v T
	if err := json.Unmarshal(res.Artifacts["seed.json"], &v); err != nil {
		t.Fatalf("fork %d of seed %d left no result: %v", k, r.Seed, err)
	}
	stop := add(r.Seed, v)
	status = "PASS"
	return stop
}

// runImage runs r, an image's run, and writes the image it saved to a file
// of t's: it returns its path and SHA-256, which it checks against
// $SIM_IMAGE_SHA256 if set. It reports false if the run failed. With
// double it runs r again, which must save the same image and hashes. The
// caller keeps no copy of the image's bytes in memory.
func runImage(t *testing.T, r Run, double bool) (path, hash string, ok bool) {
	t.Helper()
	// ok stays false on a fatal exit, including an image-SHA mismatch.
	// Report only after the image's full validation and replay checks.
	defer func() {
		status := "FAIL"
		if ok {
			status = "PASS"
		}
		driverVerdict(t, "image", fmt.Sprintf("%s/%d", r.Test, r.Seed), status)
	}()
	// The run writes its image straight to a file of t's (CreateOutput),
	// which holds it whole only on disk: as an artifact a large image
	// came back in memory several times over (#794's TestAgedBulk).
	path = filepath.Join(t.TempDir(), "image")
	r.Outputs = map[string]string{"image": path}
	img := Do(t, r)
	keep(t, img.Artifacts, "")
	if img.Failed {
		return "", "", false
	}
	hash, size := fileHash(t, path)
	if size == 0 {
		t.Fatalf("seed %d saved no image", r.Seed)
	}
	t.Logf("image of seed %d: %d bytes, SHA-256 %s", r.Seed, size, hash)
	if double {
		hashes := img.Artifacts["hashes"]
		r.Outputs = map[string]string{"image": path + "-again"}
		again := Do(t, r)
		if again.Failed {
			keep(t, again.Artifacts, "")
			return "", "", false
		}
		hash2, _ := fileHash(t, path+"-again")
		if err := os.Remove(path + "-again"); err != nil {
			t.Error(err)
			return "", "", false
		}
		if hash2 != hash || !bytes.Equal(again.Artifacts["hashes"], hashes) {
			t.Errorf("the image of seed %d played out differently the second time: SHA-256 %s, then %s\n%s\n%s\nreplay: %s",
				r.Seed, hash, hash2, hashes, again.Artifacts["hashes"], Replay(dir(t), Pattern(t.Name()), append(slices.Clone(r.Env), "SIM_TWICE=1")))
			return "", "", false
		}
	}
	if want := os.Getenv("SIM_IMAGE_SHA256"); want != "" && want != hash {
		t.Fatalf("the image of seed %d has SHA-256 %s, not %s: it is not the image the fork ran from", r.Seed, hash, want)
	}
	return path, hash, true
}

// fileHash is the SHA-256 and size of the file at path, read a piece at a
// time; a missing file has size 0.
func fileHash(t *testing.T, path string) (string, int64) {
	t.Helper()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", 0
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

// twoRuns runs r twice, and fails t unless both give the same hashes.
// With SIM_KEEP each run saves its files, which are kept, as first and
// second, only when they differ or a run failed. The first run's files
// go to $SIM_OUT before the second run starts, so this process holds one
// run's files at a time, which for a long profile take gigabytes; they
// are removed again unless the runs differ.
func twoRuns[T any](t *testing.T, r Run) *Result {
	t.Helper()
	return twoRunsWith[T](t, r, Do)
}

// twoRunsWith is twoRuns with the runner supplied, so its result
// selection can be tested without starting deliberately failed runs.
func twoRunsWith[T any](t *testing.T, r Run, run func(*testing.T, Run) *Result) *Result {
	t.Helper()
	r.Env = append(r.Env, "SIM_KEEP=1")
	first := run(t, r)
	if first.Failed {
		keep(t, first.Artifacts, "")
		return first
	}
	saved := keepQuiet(t, first.Artifacts, "first")
	hashes := first.Artifacts["hashes"]
	first.Artifacts = map[string][]byte{"hashes": hashes, "seed.json": first.Artifacts["seed.json"]}
	second := run(t, r)
	differ := false
	switch {
	case second.Failed:
		first.Failed = true
		keep(t, second.Artifacts, "")
	case len(hashes) == 0:
		first.Failed = true
		t.Errorf("seed %d left no hashes to compare", r.Seed)
	case string(hashes) != string(second.Artifacts["hashes"]):
		first.Failed = true
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
				first.Failed = true
				t.Error(err)
			}
		}
	}
	// The second run may have reached a verdict the first did not. Keep
	// that opted-in failure for the callback, but leave ordinary failures
	// selected as before. Fresh seeds and forks take this same path.
	if second.Failed {
		if _, report := failedSeed[T](second.Artifacts["seed.json"]); report {
			return second
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
