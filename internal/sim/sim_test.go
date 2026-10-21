package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// The simulator's tests read their settings from the environment, which
// reaches them under gosim too, where they get no flags:
//   - SIM_SEED and SIM_SEEDS: the first seed of a test that runs a set of
//     seeds (simrun.Seeds), and how many. TestSeeds, TestLinearizable and
//     TestMembershipFaults run 16 from seed 1 by default.
//   - SIM_TWICE: how many seeds of the set scripts/sim.sh runs twice; each
//     must give the same trace, history and disk hashes both times.
//   - SIM_OUT: the directory where a failed run saves its trace, how it
//     left the nodes, its history, its disks and the command that runs it
//     again.
//   - SIM_PLANTED: the planted bug TestPlanted switches on.

// run runs one seed and fails the test if it breaks a rule.
func run(t *testing.T, cfg Config) *Result {
	t.Helper()
	res, err := runSeed(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// lastRun is the last run of each test, by its *testing.T.
var lastRun sync.Map

// runSeed runs one seed. If the test fails with no later run, the run
// explains itself (explain).
func runSeed(t *testing.T, cfg Config) (*Result, error) {
	t.Helper()
	return runWith(t, cfg, nil)
}

// runWith is runSeed with runInspect's inspect.
func runWith(t *testing.T, cfg Config, inspect func(*Sim) error) (*Result, error) {
	t.Helper()
	var res *Result
	var err error
	simrun.Bubble(t, func(*testing.T) { res, err = runInspect(cfg, inspect) })
	if simrun.InGosim && res != nil {
		hashLines = append(hashLines, fmt.Sprintf("seed %d: %s", cfg.Seed, hashes(res)))
		simrun.Artifact("hashes", []byte(strings.Join(hashLines, "\n")+"\n"))
		// The driver's double runs keep both runs' files, for when they
		// differ, once the seed's last simulation is over.
		if os.Getenv("SIM_KEEP") != "" {
			cmd := replayCommand(t.Name(), cfg.Seed)
			simrun.AfterSeed(func() { save(t, cmd, res) })
		}
	}
	me := new(int)
	lastRun.Store(t, me)
	t.Cleanup(func() {
		if last, _ := lastRun.Load(t); last == me {
			lastRun.Delete(t)
			if t.Failed() {
				explain(t, cfg, res)
			}
		}
	})
	return res, err
}

// hashLines are the hashes of each run of the test, in order.
var hashLines []string

// hashes are the hashes of a run's trace, its history and each node's
// disk, now and right after its last crash. Under gosim, two runs of the
// same seed must give the same.
func hashes(res *Result) string {
	sum := func(b []byte) string {
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:8])
	}
	var out strings.Builder
	fmt.Fprintf(&out, "trace %s history %s", sum([]byte(strings.Join(res.Trace, "\n"))), sum(historyBytes(res)))
	for _, n := range res.nodes {
		var disk strings.Builder
		for _, files := range []map[string][]byte{n.disk.Files(), n.crashImage} {
			for _, name := range slices.Sorted(maps.Keys(files)) {
				fmt.Fprintf(&disk, "%s %d %s\n", name, len(files[name]), files[name])
			}
			disk.WriteString("\n")
		}
		fmt.Fprintf(&out, " node%d %s", n.id, sum([]byte(disk.String())))
	}
	return out.String()
}

// historyBytes is every call and every watch response of a run, with its exact
// times, for its hash.
func historyBytes(res *Result) []byte {
	var b []byte
	marshal := func(m proto.Message) {
		if m == nil {
			b = append(b, "nil\n"...)
			return
		}
		bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			b = fmt.Appendf(b, "%T: %v\n", m, err)
			return
		}
		b = fmt.Appendf(b, "%T %d %x\n", m, len(bytes), bytes)
	}
	for _, r := range res.History {
		b = fmt.Appendf(b, "call %d %d %d %d %v %v\n", r.Client, r.Node, r.Call, r.Return, r.Err, r.Ambiguous)
		marshal(r.req)
		marshal(r.Resp)
	}
	for _, w := range res.Watches {
		b = fmt.Appendf(b, "watch %q %q %v %v %d\n", w.Key, w.End, w.PrevKV, w.ProgressNotify, w.Start)
		for _, r := range w.Responses {
			b = fmt.Appendf(b, "at %d\n", r.Time)
			marshal(r.Resp)
		}
	}
	return b
}

// explainSim explains a scenario that drives s itself, as explain does a
// run: its trace, the nodes, their hashes, its saved files and the
// command that runs it again. It goes before s shuts down.
func explainSim(t *testing.T, cfg Config, s *Sim) {
	t.Helper()
	s.mu.Lock()
	res := &Result{Clients: cfg.Clients + cfg.V3Clients, History: s.history, Stats: s.stats, nodes: s.nodes}
	s.mu.Unlock()
	res.Report = s.report()
	s.mu.Lock()
	res.Trace = slices.Clone(s.trace)
	s.mu.Unlock()
	if simrun.InGosim {
		hashLines = append(hashLines, fmt.Sprintf("seed %d: %s", cfg.Seed, hashes(res)))
		simrun.Artifact("hashes", []byte(strings.Join(hashLines, "\n")+"\n"))
	}
	explain(t, cfg, res)
}

// explain logs what a failed run did, how it left each node, its hashes
// and the command that runs its seed again, and saves the run (save).
func explain(t *testing.T, cfg Config, res *Result) {
	cmd := replayCommand(t.Name(), cfg.Seed)
	if res == nil {
		t.Logf("replay: %s", cmd)
		return
	}
	t.Logf("trace of seed %d:\n%s", cfg.Seed, strings.Join(res.Trace, "\n"))
	t.Logf("the nodes at the end of seed %d:\n%s", cfg.Seed, res.Report)
	t.Logf("hashes of seed %d: %s", cfg.Seed, hashes(res))
	save(t, cmd, res)
	t.Logf("replay: %s", cmd)
}

// save keeps a run's files (runFiles) in $SIM_OUT, under the test's name.
// Under gosim the driver writes them there.
func save(t *testing.T, cmd string, res *Result) {
	name := strings.ReplaceAll(t.Name(), "/", "_")
	if simrun.InGosim {
		for path, body := range runFiles(filepath.Join("$SIM_OUT", name), cmd, res) {
			simrun.Artifact(filepath.Join("sim-out", name, path), []byte(body))
		}
		return
	}
	out := os.Getenv("SIM_OUT")
	if out == "" {
		return
	}
	dir := filepath.Join(out, name)
	files := map[string][]byte{}
	for path, body := range runFiles(dir, cmd, res) {
		files[path] = []byte(body)
	}
	if err := simrun.WriteFiles(dir, files); err != nil {
		t.Logf("saving the run in %s: %v", dir, err)
		return
	}
	t.Logf("the run is saved in %s", dir)
}

// seedRange is the n seeds from first.
func seedRange(first uint64, n int) []uint64 {
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = first + uint64(i)
	}
	return seeds
}

// runFiles are a run's trace, node report, stats, hashes, history, disks
// and replay command, by path, for a directory dir of their own.
func runFiles(dir, cmd string, res *Result) map[string]string {
	files := map[string]string{
		"trace.txt":  strings.Join(res.Trace, "\n") + "\n",
		"nodes.txt":  res.Report,
		"replay.txt": cmd + "\n",
		"stats.txt":  fmt.Sprintf("%+v\n", res.Stats),
		"hashes.txt": hashes(res) + "\n",
		// Every call, as the clients saw it, also when the run failed
		// before its checks built the committed log that history.json
		// needs.
		"calls.txt": calls(res),
	}
	h, err := res.Checkable()
	switch {
	case !res.logged:
		files["replay.txt"] += "no history.json: the run failed before its checks built the committed log, which the checkers need; calls.txt has the calls\n"
	case err != nil:
		files["replay.txt"] += fmt.Sprintf("no history.json: %v\n", err)
	default:
		b, err := json.MarshalIndent(h, "", "  ")
		if err != nil {
			files["replay.txt"] += fmt.Sprintf("no history.json: %v\n", err)
			break
		}
		files["history.json"] = string(b) + "\n"
		checkers := "witness-log,witness-revisions"
		if check.ShortEnough(h) {
			checkers += ",linearizable"
		}
		files["replay.txt"] += "go run ./cmd/checkhistory -checkers " + checkers + " " + filepath.Join(dir, "history.json") + "\n"
	}
	// Each node's last disk calls and its files now and right after its
	// last crash, and the disks a failed cold restart started from, with
	// that restart's disk calls (#274).
	for _, n := range res.nodes {
		d := filepath.Join("disks", fmt.Sprintf("node%d", n.id))
		files[filepath.Join(d, "calls.txt")] = strings.Join(n.diskLog, "\n") + "\n"
		addFiles(files, filepath.Join(d, "now"), n.disk.Files())
		addFiles(files, filepath.Join(d, "after-crash"), n.crashImage)
	}
	if c := res.cold; c != nil {
		for i, d := range c.disks {
			name := fmt.Sprintf("node%d", i+1)
			if i == c.erased {
				name += "-erased"
			}
			addFiles(files, filepath.Join("cold", fmt.Sprintf("copy-at-%dms", c.at/Millisecond), name), d.Files())
		}
		for _, n := range c.nodes {
			files[filepath.Join("cold", fmt.Sprintf("node%d-calls.txt", n.id))] = strings.Join(n.diskLog, "\n") + "\n"
		}
	}
	return files
}

// calls is every call of a run, as the clients saw it.
func calls(res *Result) string {
	var b strings.Builder
	for _, r := range res.History {
		answer := fmt.Sprint(r.Resp)
		if r.Err != nil {
			answer = fmt.Sprintf("error %v, ambiguous %v", r.Err, r.Ambiguous)
		}
		fmt.Fprintf(&b, "client %d, node %d, %s to %s: %T %v -> %s\n", r.Client, r.Node+1, seconds(r.Call), seconds(r.Return), r.req, r.req, answer)
	}
	return b.String()
}

// addFiles puts each of a disk's files into files, under dir.
func addFiles(files map[string]string, dir string, disk map[string][]byte) {
	for name, b := range disk {
		files[filepath.Join(dir, name)] = string(b)
	}
}

// activePlanted is the planted bug TestPlanted switched on, if any.
var activePlanted string

// replayCommand is the command that runs test name's run of seed again:
// scripts/sim.sh, or go test natively under the race detector. A subtest
// named after the seed is one of a set (simrun.Seeds, simrun.Each).
func replayCommand(name string, seed uint64) string {
	parts := simrun.Top(strings.Split(name, "/"))
	var env []string
	if len(parts) > 1 && parts[1] == strconv.FormatUint(seed, 10) {
		env = append(env, fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1")
	}
	if activePlanted != "" {
		env = append(env, "SIM_PLANTED="+activePlanted)
	}
	pattern := simrun.Pattern(name)
	if !simrun.Race {
		return simrun.Replay("./internal/sim", pattern, env)
	}
	cmd := strings.Join(append(env, "go test -race -v"), " ")
	if activePlanted != "" {
		cmd += " -tags planted"
	}
	return cmd + fmt.Sprintf(" -count=1 ./internal/sim/ -run '%s'", pattern)
}

// TestSeeds runs every seed with the checkers. A failing seed prints the
// command that runs it again.
func TestSeeds(t *testing.T) {
	var total Stats
	var took effects
	ran := 0
	if !simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) Stats {
		a := run(t, DefaultConfig(seed))
		st := a.Stats
		if st.Compactions == 0 || st.Revokes == 0 || st.OK < 200 || len(a.Log) < 50 {
			t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
		}
		checkers(t, a)
		t.Logf("%+v, %d committed writes", st, len(a.Log))
		return st
	}, func(seed uint64, st Stats) bool {
		took.add(seed, map[string]bool{
			"crash": st.Crashes > 0, "link change": st.LinkChanges > 0, "two cold restarts": st.ColdRestarts >= 2,
			"disk fault": st.DiskFaults > 0, "pause": st.Pauses > 0, "link stall": st.LinkStalls > 0,
			"half-open link": st.HalfOpens > 0, "gray node": st.GrayNodes > 0, "gray client": st.GrayStreams > 0,
			"kill": st.Kills > 0 || simrun.Race, "clientv3 call": st.V3OK > 0, "link fault": slices.ContainsFunc(st.Netem[:], func(n int) bool { return n > 0 }),
			"connection reset": st.Resets > 0, "slow watcher": st.SlowWatchers > 0,
		})
		ran++
		total.Fenced += st.Fenced
		total.Silences += st.Silences
		total.Asymmetric += st.Asymmetric
		total.RecoveryCrashes += st.RecoveryCrashes
		total.ErasedAtRoot += st.ErasedAtRoot
		total.ErasedAtRemoval += st.ErasedAtRemoval
		total.Disk.TornWrites += st.Disk.TornWrites
		total.Disk.Reordered += st.Disk.Reordered
		total.TornRefusals += st.TornRefusals
		total.Packs += st.Packs
		total.OutOfSpace += st.OutOfSpace
		return false
	}) {
		return
	}
	// Random faults can end a disk stall before the leader goes silent
	// (#218), so each shard also runs the deliberate scenario.
	if ran >= 64 {
		t.Run("TestStalledLeaderIsReplaced", TestStalledLeaderIsReplaced)
	}
	// Corruption, a link 10 times slower than its other direction and the
	// other common events are rare in one run, but a set of seeds must see
	// each.
	if ran >= 64 && !t.Failed() && (total.Fenced == 0 || total.Asymmetric == 0 ||
		total.RecoveryCrashes == 0 || total.ErasedAtRoot == 0 || total.ErasedAtRemoval == 0 ||
		total.Disk.TornWrites == 0 || total.Disk.Reordered == 0 || total.Packs == 0 || total.OutOfSpace == 0) {
		t.Fatalf("%d seeds: %d fenced nodes, %d asymmetric links, %d crashes in recovery, %d disks erased after a root, %d after a removal, %d writes a crash tore, %d crashes that kept a write and lost an earlier one, %d packs, %d nodes out of space; each must happen",
			ran, total.Fenced, total.Asymmetric, total.RecoveryCrashes, total.ErasedAtRoot, total.ErasedAtRemoval, total.Disk.TornWrites, total.Disk.Reordered, total.Packs, total.OutOfSpace)
	}
	t.Logf("%d seeds: %d writes a crash tore, %d crashes kept a write and lost an earlier one, %d nodes refused their torn crash image",
		ran, total.Disk.TornWrites, total.Disk.Reordered, total.TornRefusals)
	// A stalled leader that goes silent is rarer: about 0.9% of seeds had
	// one when it handed over instead (#218). Over 2000 seeds even 0.7%
	// leaves less than one chance in a million of none. CI's shards of 400
	// do not reach that, the seed campaign does.
	if ran >= 2000 && !t.Failed() && total.Silences == 0 {
		t.Fatalf("%d seeds: no stalled leader went silent; one must", ran)
	}
	t.Logf("%d seeds: %d stalled leaders went silent", ran, total.Silences)
	took.check(t)
}

// effects counts, over a set of runs, the runs in which each fault took
// effect. A fault that took effect in too few of them fails the test,
// not the run it missed, and names the seeds it missed. With all set,
// every run must show each fault.
type effects struct {
	runs   int
	all    bool
	took   map[string]int
	missed map[string][]uint64
}

// add counts the run of seed, with whether each fault took effect in it.
func (e *effects) add(seed uint64, took map[string]bool) {
	if e.took == nil {
		e.took, e.missed = map[string]int{}, map[string][]uint64{}
	}
	e.runs++
	for name, ok := range took {
		n := e.took[name]
		if ok {
			n++
		} else {
			e.missed[name] = append(e.missed[name], seed)
		}
		e.took[name] = n
	}
}

// check fails t unless each fault took effect in atLeast of the seeds,
// or in all of them, and logs the counts. A failure names up to five
// seeds that missed the fault, with the command that runs each again.
func (e *effects) check(t *testing.T) {
	t.Helper()
	least := atLeast(e.runs)
	if e.all {
		least = e.runs
	}
	var counts []string
	for _, name := range slices.Sorted(maps.Keys(e.took)) {
		n := e.took[name]
		counts = append(counts, fmt.Sprintf("%s %d", name, n))
		if n >= least {
			continue
		}
		missed := e.missed[name][:min(5, len(e.missed[name]))]
		t.Errorf("%s took effect in %d of %d seeds, want %d at least; it missed seeds %v", name, n, e.runs, least, missed)
		for _, seed := range missed {
			t.Logf("replay: %s", replayCommand(t.Name()+"/"+strconv.FormatUint(seed, 10), seed))
		}
	}
	t.Logf("of %d seeds, the faults took effect in: %s", e.runs, strings.Join(counts, ", "))
}

// checkers runs the independent checkers of package check on a run:
// durability, revisions and watches. Linearizability runs on the smaller
// histories of TestLinearizable.
func checkers(t *testing.T, res *Result) {
	t.Helper()
	if os.Getenv("SIM_PLANTED") == "diag369" {
		t.Fatal("diagnosis #369: save the history before checkers")
	}
	if err := independent(res); err != nil {
		t.Fatal(err)
	}
}

// Under gosim the clock stands still while a check computes, so the
// checks are bounded by the steps of their search too. A typical check of
// a run takes 4,000 to 10,000 steps. The witness checker's bound is about
// a minute of its steps natively; Porcupine's search steps cost more.
var (
	witnessSteps   = check.MaxSteps(50_000_000)
	porcupineSteps = check.MaxSteps(1_000_000)
)

// independent runs the durability, revision and watch checkers of package
// check on a run.
func independent(res *Result) error {
	h, err := res.Checkable()
	if err != nil {
		return err
	}
	var errs []error
	if err := check.Durable(h); err != nil {
		errs = append(errs, fmt.Errorf("durability: %w", err))
	}
	if err := check.Revisions(h); err != nil {
		errs = append(errs, fmt.Errorf("revisions: %w", err))
	}
	if err := check.Watches(h); err != nil {
		errs = append(errs, fmt.Errorf("watches: %w", err))
	}
	for _, mode := range []check.WitnessMode{check.FromLog, check.FromRevisions} {
		if err := check.Witnessed(h, mode, time.Minute, witnessSteps); err != nil {
			errs = append(errs, fmt.Errorf("witness: %w", err))
		}
	}
	return errors.Join(errs...)
}

// agree runs the witness checker in both modes on a run's history, and
// Porcupine too if the history is short enough for it
// (check.ShortEnough). All of them must pass; a history that one passes
// and another fails is a checker bug.
func agree(h *history.History) error {
	names := []string{"witness from the log", "witness from the revisions"}
	errs := []error{check.Witnessed(h, check.FromLog, time.Minute, witnessSteps), check.Witnessed(h, check.FromRevisions, time.Minute, witnessSteps)}
	if check.ShortEnough(h) {
		names = append(names, "linearizability")
		errs = append(errs, check.Linearizable(h, 3*time.Minute, porcupineSteps))
	}
	failed := 0
	var verdicts []string
	for i, err := range errs {
		v := "passes"
		if err != nil {
			failed++
			v = err.Error()
		}
		verdicts = append(verdicts, names[i]+" "+v)
	}
	switch failed {
	case 0:
		return nil
	case len(errs):
		return fmt.Errorf("%s: %w", strings.Join(names, ", "), errors.Join(errs...))
	}
	return fmt.Errorf("the checkers disagree: %s", strings.Join(verdicts, "; "))
}

// linearizableConfig is a short run whose history Porcupine can check:
// 5 virtual seconds with the same faults and 4 clients, one of them a
// client of etcd's Go client, then 8 s of healing, without leases.
func linearizableConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Duration = 5 * Second
	cfg.Heal = 8 * Second
	cfg.Clients, cfg.V3Clients = 3, 1
	cfg.NoLeases = true
	return cfg
}

// TestLinearizable checks short runs with Porcupine and the witness
// checker, which must agree. Not finishing in time is a failure. Nearly
// every run must be short enough for Porcupine (check.ShortEnough).
func TestLinearizable(t *testing.T) {
	n, short := 0, 0
	if !simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) bool {
		res := run(t, linearizableConfig(seed))
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := agree(h); err != nil {
			t.Fatal(err)
		}
		t.Logf("%d ops checked in %v", len(h.Ops), time.Since(start))
		return check.ShortEnough(h)
	}, func(_ uint64, ok bool) bool {
		n++
		if ok {
			short++
		}
		return false
	}) {
		return
	}
	if least := atLeast(n); !t.Failed() && short < least {
		t.Fatalf("Porcupine ran on %d of %d histories, want %d at least: the others were longer than %d operations", short, n, least, check.ShortOps)
	}
}

// atLeast is how many of n runs must show something that most runs show:
// 95% of them, rounded down.
func atLeast(n int) int { return n * 95 / 100 }

// TestStalledLeaderIsReplaced stalls the leader's disk for good at 5 s,
// with no other fault. The leader would go on heartbeating, so it must go
// silent for the others to elect another (#233): core.StallTicks
// election timeouts after the oldest append it has not persisted, which
// is the first one the stall holds, one queued before the stall or the
// first after it. It must go silent by then, plus two ticks, as the core
// counts ticks, and every call to a healthy node that starts 10 election
// timeouts after the stall must succeed. A leader can propose nothing for
// a while after the stall, as when it serves a snapshot (#260).
func TestStalledLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	simrun.Each(t, []uint64{1, 2, 3}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.StallLeaderAt = 5 * Second
		cfg.LiveFrom = cfg.StallLeaderAt + 10*election
		cfg.Duration = 25 * Second
		res := run(t, cfg)
		if len(res.Silences) == 0 || res.Stats.Silences == 0 {
			t.Fatalf("seed %d: the stalled leader never went silent", seed)
		}
		if res.HeldAppendAt == 0 {
			t.Fatalf("seed %d: the stall held no append of the leader", seed)
		}
		limit := res.HeldAppendAt + core.StallTicks*election + 2*TickTime
		if at := res.Silences[0]; at < cfg.StallLeaderAt || at > limit {
			t.Fatalf("seed %d: the stalled leader went silent at %d ms, want between the stall at %d ms and %d ms, %d election timeouts and two ticks after its oldest held append at %d ms",
				seed, at/Millisecond, cfg.StallLeaderAt/Millisecond, limit/Millisecond, core.StallTicks, res.HeldAppendAt/Millisecond)
		}
		checkers(t, res)
		t.Logf("seed %d: stalled leader went silent at %d ms, deadline %d ms after its oldest held append at %d ms", seed, res.Silences[0]/Millisecond, limit/Millisecond, res.HeldAppendAt/Millisecond)
		return struct{}{}
	}, nil)
}

// TestAllLinkMasks holds each of the 64 sets of directed links between the
// three nodes for 3 virtual seconds, from 2 s on, with no other fault,
// then heals. Every rule and checker must hold, and the cluster must serve
// again 2 s after healing.
func TestAllLinkMasks(t *testing.T) {
	simrun.Each(t, seedRange(1000, 64), func(t *testing.T, seed uint64) struct{} {
		mask := seed - 1000
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{HoldMask: mask, HoldMaskAt: 2 * Second}
		cfg.Duration = 5 * Second
		cfg.Heal = 8 * Second
		cfg.Clients = 4
		cfg.NoLeases = true
		res := run(t, cfg)
		// The mask went in at 2 s and, with no other link fault, held
		// until healing at 5 s.
		if res.Stats.LinkChanges != 1 {
			t.Fatalf("mask %06b: %d link changes, want the one", mask, res.Stats.LinkChanges)
		}
		checkers(t, res)
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := agree(h); err != nil {
			t.Fatalf("mask %06b: %v", mask, err)
		}
		return struct{}{}
	}, nil)
}

// TestSwitchingLinks switches to a random set of directed links about
// every 300 ms for 10 virtual seconds, then heals. Two of the three seeds
// at least must switch 10 times.
func TestSwitchingLinks(t *testing.T) {
	var took effects
	if !simrun.Each(t, []uint64{2001, 2002, 2003}, func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{SwitchEvery: 300 * Millisecond}
		cfg.Duration = 10 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.LinkChanges
	}, func(seed uint64, changes int) bool {
		took.add(seed, map[string]bool{"ten switches": changes >= 10})
		return false
	}) {
		return
	}
	took.check(t)
}

// faultCase is a fault alone, with the count that shows it took effect.
type faultCase struct {
	name   string
	faults Faults
	// cfg, if set, changes the run's configuration further.
	cfg   func(*Config)
	count func(Stats) int
}

// faultCases are the cases of TestEachFaultTakesEffect.
func faultCases() []faultCase {
	crashes := Faults{CrashEvery: Second, MaxDown: Second}
	// Crashes while directory operations are not synced yet: inside
	// recovery, which removes and renames files, and at a checkpoint or
	// removal step.
	unsynced := Faults{CrashEvery: 500 * Millisecond, MaxDown: 500 * Millisecond, RecoveryCrash: true, StepCrash: true}
	// The damage needs sealed segments, which take a few seconds of
	// writes.
	long := func(c *Config) { c.Duration = 16 * Second }
	longer := func(c *Config) { c.Duration = 24 * Second }
	damage := func(k damageKind, count func(Stats) int) faultCase {
		return faultCase{k.String(), Faults{CrashEvery: Second, MaxDown: Second, Corrupt: true}, func(c *Config) {
			c.damageKinds = []damageKind{k}
			long(c)
		}, count}
	}
	cases := []faultCase{
		{"link stall", Faults{LinkStallEvery: Second}, nil, func(st Stats) int { return st.LinkStalls }},
		{"pause", Faults{PauseEvery: Second}, nil, func(st Stats) int { return st.Pauses }},
		{"half-open link", Faults{HalfOpenEvery: Second}, nil, func(st Stats) int { return st.HalfOpens }},
		{"link delays", Faults{SwitchEvery: 200 * Millisecond, MaxDelay: 5 * Millisecond}, nil, func(st Stats) int { return st.Asymmetric }},
		{"gray node", Faults{GrayEvery: Second}, nil, func(st Stats) int { return st.GrayNodes }},
		{"gray client", Faults{GrayClient: true}, nil, func(st Stats) int { return st.GrayStreams }},
		{"crash in recovery", Faults{CrashEvery: 2 * Second, MaxDown: Second, RecoveryCrash: true}, nil, func(st Stats) int { return st.RecoveryCrashes }},
		{"follower held down", Faults{HoldDown: true}, nil, func(st Stats) int { return st.Snapshots }},
		{"damaged snapshot", Faults{}, func(c *Config) { *c = damagedSnapshotConfig(c.Seed) }, func(st Stats) int { return st.SnapshotFlips }},
		{"crash during a fetch", Faults{HoldDown: true, FetchCrash: true}, nil, func(st Stats) int { return min(st.FetchCrashes, st.Reloads) }},
		{"crash at a checkpoint step", Faults{StepCrash: true}, nil, func(st Stats) int { return st.StepCrashes }},
		{"rotten value", Faults{Rot: true}, nil, func(st Stats) int { return st.Rots }},
		{"kill", crashes, nil, func(st Stats) int { return st.Kills }},
		// One kill in three, of half the crashes, has a power loss after it.
		{"power loss after a kill", crashes, long, func(st Stats) int { return st.KillLosses }},
		{"lost directory operation", unsynced, long, func(st Stats) int { return st.Disk.LostDirOps }},
		{"failed sync", Faults{DiskErrorEvery: Second}, nil, func(st Stats) int { return st.Disk.FailedSyncs }},
		{"EIO", Faults{DiskErrorEvery: Second}, nil, func(st Stats) int { return st.IOErrors }},
		{"file system down", Faults{FSDownEvery: Second}, nil, func(st Stats) int { return min(st.FSDowns, st.DiskFaults) }},
		{"read-only file system", Faults{FSDownEvery: Second}, nil, func(st Stats) int { return st.ReadOnlyStops }},
		{"client link down", Faults{LinkEvery: Second}, nil, func(st Stats) int { return st.ClientLinksDown }},
		{"full disk", Faults{DiskFullEvery: Second}, nil, func(st Stats) int { return st.OutOfSpace }},
		{"cleaning within the budget", Faults{DiskBudget: 32 << 10, DiskCapacity: 128 << 10}, nil, func(st Stats) int { return st.Packs }},
		damage(flippedRoot, func(st Stats) int { return min(st.Damage[flippedRoot], st.Fenced) }),
		damage(misdirected, func(st Stats) int { return min(st.Damage[misdirected], st.Fenced) }),
		damage(lostWrite, func(st Stats) int { return min(st.Damage[lostWrite], st.Fenced) }),
		damage(badSector, func(st Stats) int { return min(st.Damage[badSector], st.Fenced, st.Disk.BadReads) }),
		damage(misread, func(st Stats) int { return min(st.Damage[misread], st.MisreadRefusals) }),
		{"connection reset", Faults{ResetEvery: Second}, nil, func(st Stats) int { return st.Resets }},
		{"full send buffer", Faults{LinkStallEvery: 500 * Millisecond}, nil, func(st Stats) int { return st.BlockedWrites }},
		// A stall past the write timeout, one in four, on a link that
		// carries something.
		{"write deadline", Faults{LinkStallEvery: 300 * Millisecond}, longer, func(st Stats) int { return st.WriteTimeouts }},
		{"slow watcher", Faults{SlowWatcherEvery: Second}, nil, func(st Stats) int { return st.WatchersCut }},
	}
	for i := range numNetem {
		k := netemKind(i)
		cases = append(cases, faultCase{
			k.String(),
			Faults{NetemEvery: 300 * Millisecond},
			func(c *Config) { c.netemKinds = []netemKind{k} }, func(st Stats) int { return st.Netem[k] },
		})
	}
	return cases
}

// TestEachFaultTakesEffect runs each fault alone for 8 virtual seconds,
// on three seeds, and requires that it happened in two of them at least.
// The seeds of case i are 3000+10i to 3002+10i, each a gosim run of its
// own. A follower held down for up to 4 s needs about a second after its
// restart before its first fetch, which the stream faults damage only
// before healing.
func TestEachFaultTakesEffect(t *testing.T) {
	if simrun.Race {
		// Under the race detector the cases take half an hour.
		t.Skip("the faults' effects are counted under gosim")
	}
	cases := faultCases()
	var seeds []uint64
	for i := range cases {
		seeds = append(seeds, seedRange(3000+10*uint64(i), 3)...)
	}
	took := make([]effects, len(cases))
	if !simrun.Each(t, seeds, func(t *testing.T, seed uint64) bool {
		c := cases[(seed-3000)/10]
		cfg := DefaultConfig(seed)
		cfg.Faults = c.faults
		cfg.Duration = 8 * Second
		cfg.Heal = 8 * Second
		if c.cfg != nil {
			c.cfg(&cfg)
		}
		res := run(t, cfg)
		checkers(t, res)
		t.Logf("%s: %+v", c.name, res.Stats)
		return c.count(res.Stats) > 0
	}, func(seed uint64, effect bool) bool {
		i := (seed - 3000) / 10
		took[i].add(seed, map[string]bool{cases[i].name: effect})
		return false
	}) {
		return
	}
	for i := range cases {
		took[i].check(t)
	}
}

// TestDamagedLastRecord damages the last record of a node's newest
// segment where that loses only a new term or vote (lastRecord): the node
// must start again with the term and vote it held durable (#357).
func TestDamagedLastRecord(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3500, 3), func(t *testing.T, seed uint64) bool {
		cfg := DefaultConfig(seed)
		// A power loss comes right after a node synced a new term or
		// vote alone, so that its newest record holds it.
		cfg.Faults = Faults{Corrupt: true, VoteCrash: true, CrashEvery: Second, MaxDown: Second}
		cfg.Duration = 8 * Second
		cfg.Heal = 8 * Second
		cfg.damageKinds = []damageKind{lastRecord}
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.Damage[lastRecord] > 0
	}, func(seed uint64, effect bool) bool {
		took.add(seed, map[string]bool{lastRecord.String(): effect})
		return false
	}) {
		return
	}
	took.check(t)
}

// TestPowerLossAfterKill makes every crash a kill, which keeps the page
// cache, followed by a power loss 0.1 to 3 s later, while the node is
// down or after it started again. What the restarted node read and held
// durable must survive the power loss: before #361's fix, recovery did
// not sync what it read from the page cache.
func TestPowerLossAfterKill(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3600, 16), func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{CrashEvery: Second, MaxDown: Second}
		cfg.killLosses = true
		cfg.Duration = 10 * Second
		cfg.Heal = 8 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.KillLosses
	}, func(seed uint64, losses int) bool {
		took.add(seed, map[string]bool{"power loss after a kill": losses > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// TestCheckpointCreateFails makes every disk fault EIO on the creation of
// a checkpoint file, which takes 50 ms, so that the later parts of the
// checkpoint queue up meanwhile. The node stops, and must not write them
// into a new file without the first part (#363), which the run's check
// of every completed checkpoint finds.
func TestCheckpointCreateFails(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3700, 4), func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{DiskErrorEvery: 500 * Millisecond}
		cfg.ckptCreateFaults = true
		cfg.Duration = 8 * Second
		cfg.Heal = 8 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.CheckpointCreateErrors
	}, func(seed uint64, n int) bool {
		took.add(seed, map[string]bool{"EIO on a checkpoint's creation": n > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// TestFailedSyncThenRestart makes every disk fault a failed sync, after
// which the node starts again with its page cache kept, where the bytes
// the sync did not write stay readable, and no later sync writes them.
// Recovery must write them again: with segments of 256 bytes, which end
// every few writes, a power loss later would find a hole in a sealed
// segment (#364).
func TestFailedSyncThenRestart(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3800, 8), func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{DiskErrorEvery: 500 * Millisecond, CrashEvery: 2 * Second, MaxDown: Second}
		cfg.syncFaultExits = true
		cfg.SegmentBytes = 256
		cfg.Duration = 10 * Second
		cfg.Heal = 8 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.Disk.FailedSyncs
	}, func(seed uint64, n int) bool {
		took.add(seed, map[string]bool{"failed sync": n > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// damagedSnapshotConfig holds a follower down until it fetches a snapshot,
// and damages the first stream, for 8 virtual seconds and 8 of healing:
// the case "damaged snapshot" of TestEachFaultTakesEffect, and B22's.
func damagedSnapshotConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{HoldDown: true, FlipSnapshot: true}
	cfg.Duration = 8 * Second
	cfg.Heal = 8 * Second
	return cfg
}

// loneHeaderConfig is #295's path: a follower held down past the leader's
// window starts again on a fresh segment, whose header waits in the log's
// buffer, and fetches a snapshot. A crash at a step of the install, before
// its sync, can keep the records written after the header and lose the
// write of the header.
func loneHeaderConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{HoldDown: true, InstallCrash: true}
	cfg.Duration = 10 * Second
	cfg.Heal = 8 * Second
	// Segments of 64 bytes end after nearly every write, so the install
	// often comes on a fresh segment.
	cfg.SegmentBytes = 64
	return cfg
}

// TestLoneSegmentHeader runs #295's scenario (loneHeaderConfig) on 32
// seeds, which must all pass, and most of which crash during an install;
// planted bug B32, which writes the header alone, must fail it.
func TestLoneSegmentHeader(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(6001, 32), func(t *testing.T, seed uint64) Stats {
		res := run(t, loneHeaderConfig(seed))
		checkers(t, res)
		return res.Stats
	}, func(seed uint64, st Stats) bool {
		took.add(seed, map[string]bool{"crash during an install": st.InstallCrashes > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// TestCleanerSettles runs 8 default seeds, with their disk budget, and
// requires each to make fewer than 1,000 packs: a run makes 30 to 130.
// Before #362's fix, the cleaner copied a pack whose small values were all
// live again on every pass, 10,000 to 47,000 times a run, and the roots
// it wrote filled the disk once the clients stopped.
func TestCleanerSettles(t *testing.T) {
	simrun.Each(t, seedRange(7001, 8), func(t *testing.T, seed uint64) int {
		res := run(t, DefaultConfig(seed))
		if res.Stats.Packs >= 1000 {
			t.Fatalf("seed %d: the cleaner made %d packs", seed, res.Stats.Packs)
		}
		return res.Stats.Packs
	}, nil)
}

// slowFetchConfig holds a follower down until it needs a snapshot, and
// starts half its fetches up to 2 s late.
func slowFetchConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{HoldDown: true, SlowFetch: true}
	cfg.Duration = 10 * Second
	cfg.Heal = 8 * Second
	return cfg
}

// TestSlowFetch runs slowFetchConfig. Meanwhile the leader may make a
// newer checkpoint durable while it still keeps the one the snapshot
// named, which it must then serve, or give up on the snapshot and remove
// that checkpoint, after which the follower starts again from its log and
// gets another snapshot. And the follower, applying its log after the
// restart, may finish a checkpoint while the install is on its way to
// disk, whose root must not follow the install's (#182). Every run must
// pass the checkers, which recover every node's disk at the end, and
// across the seeds each of the three must happen.
func TestSlowFetch(t *testing.T) {
	var older, failed, during int
	// Each of the three happens in about one seed in 20 to 50; the search
	// stops once all did.
	if !simrun.Each(t, seedRange(5001, 200), func(t *testing.T, seed uint64) Stats {
		res := run(t, slowFetchConfig(seed))
		checkers(t, res)
		return res.Stats
	}, func(_ uint64, st Stats) bool {
		older += st.OlderCheckpoints
		failed += st.ServeFailures
		during += st.CheckpointsDuringInstall
		return older > 0 && failed > 0 && during > 0
	}) {
		return
	}
	if !simrun.One() && !t.Failed() && (older == 0 || failed == 0 || during == 0) {
		t.Fatalf("%d snapshots installed from an older checkpoint, %d fetches the leader could not serve, %d checkpoints finished during an install; each must happen", older, failed, during)
	}
}

// TestHalfOpenLink makes the link from the leader to a follower half-open
// at 5 s, with no other fault, until the peer write timeout fails it 5 s
// later. The leader and the other follower are a quorum, so every call to
// them must succeed and take under a second; the cut-off follower must
// serve again 2 s after the connection fails. A call that a node answers
// with "server stopped" as it starts again from its log after a snapshot
// install is left out, as the liveness rule does: the simulator's window
// of 16 bytes (tinyLimits) makes a follower that falls one entry behind
// install a snapshot, which under 12 clients happens every 10 to 20 ms
// (#266).
func TestHalfOpenLink(t *testing.T) {
	simrun.Each(t, []uint64{4001, 4002, 4003}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		// Calls to the cut-off follower wait up to the client timeout for a
		// leader (#105). With clients that pick a node per call, all of
		// them could wait there at once and leave the healthy pair idle
		// (#279), so four stay on each node.
		cfg.Clients, cfg.PinClients = 12, true
		cfg.HalfOpenAt = 5 * Second
		cfg.LiveFrom = cfg.HalfOpenAt + halfOpenTimeout + 2*Second
		cfg.Duration = 15 * Second
		res := run(t, cfg)
		if len(res.halfOpens) != 1 {
			t.Fatalf("seed %d: %d half-open links, want 1", seed, len(res.halfOpens))
		}
		h := res.halfOpens[0]
		healthy := 0
		for _, r := range res.History {
			if r.Call < h.start || r.Call >= h.end || r.Node == h.to || errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed during the half-open link: %v", seed, r.Node+1, r.Call/Millisecond, r.Err)
			}
			if r.Return-r.Call >= Second {
				t.Fatalf("seed %d: call to node %d at %d ms took %d ms", seed, r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
			}
			healthy++
		}
		if healthy < 20 {
			t.Fatalf("seed %d: only %d calls to the healthy pair during the half-open link", seed, healthy)
		}
		return struct{}{}
	}, nil)
}

// TestDiskFaultAfterHealing cuts every link before the first election, so
// no node appends during the faults: the guaranteed disk fault must fire
// after healing, and the liveness rule must still hold.
func TestDiskFaultAfterHealing(t *testing.T) {
	if simrun.Drive(t, 6001) {
		return
	}
	cfg := DefaultConfig(6001)
	cfg.Faults = Faults{DiskErrorEvery: 1000 * Second, HoldMask: 0, HoldMaskAt: 1}
	cfg.Duration = 5 * Second
	cfg.Heal = 8 * Second
	res := run(t, cfg)
	if res.Stats.DiskFaults != 1 {
		t.Fatalf("%d disk faults, want the guaranteed one", res.Stats.DiskFaults)
	}
	checkers(t, res)
}

// TestGrayClient runs only the gray client for 5 s: it reconnects every
// 50 to 300 ms and opens 10 watches each time, and never reads. Every
// call of the other clients during that time must succeed in under a
// second.
func TestGrayClient(t *testing.T) {
	if simrun.Drive(t, 7001) {
		return
	}
	cfg := DefaultConfig(7001)
	cfg.Faults = Faults{GrayClient: true}
	cfg.Duration = 5 * Second
	res := run(t, cfg)
	if res.Stats.GrayStreams < 10 {
		t.Fatalf("the gray client opened only %d streams", res.Stats.GrayStreams)
	}
	calls := 0
	for _, r := range res.History {
		if r.Call >= cfg.Duration || r.Call < Second {
			continue
		}
		if errors.Is(r.Err, rpctypes.ErrGRPCStopped) || res.nodes[r.Node].hungOnReload(r) {
			// With no other fault, only a start from the log after a
			// snapshot install, which the run's tiny window makes
			// frequent, answers that, or makes clientv3 try a read
			// again, which can time out (#365).
			continue
		}
		if r.Err != nil && !isRejection(r.Err) {
			t.Fatalf("call to node %d at %d ms failed beside the gray client: %v", r.Node+1, r.Call/Millisecond, r.Err)
		}
		if took := r.Return - r.Call; took >= Second {
			t.Fatalf("call to node %d at %d ms took %d ms beside the gray client", r.Node+1, r.Call/Millisecond, took/Millisecond)
		}
		calls++
	}
	if calls < 100 {
		t.Fatalf("only %d calls beside the gray client", calls)
	}
}

// TestColdRestartWaitsForLastCommit runs seed 736998125, whose cold
// restart at 17,821 ms ended with a lease expiry committed on the leader
// 1 ms before the end and not yet on the follower. The check must wait for
// the two to agree instead of failing. A change to the simulator's random
// draws can move this seed off that edge; the test then passes without
// testing it.
func TestColdRestartWaitsForLastCommit(t *testing.T) {
	if simrun.Drive(t, 736998125) {
		return
	}
	run(t, DefaultConfig(736998125))
}

// TestRotInAnEntryRecord runs seed 1320469980, whose rotten value on node
// 3 is in entry 16. At 11,799 ms the node reads that entry's whole record
// for the term of its checkpoint root, and stops on the record's crc
// instead of the value's checksum. That is the stop the run expects. A
// change to the simulator's random draws can move this seed off that edge;
// the test then passes without testing it.
func TestRotInAnEntryRecord(t *testing.T) {
	if simrun.Drive(t, 1320469980) {
		return
	}
	res := run(t, DefaultConfig(1320469980))
	checkers(t, res)
}

// TestSnapshotFromCorruptLeader runs seeds until one in which a damaged
// node fails to serve a snapshot on its own damage and is then fenced, as
// when a node serves a snapshot from a damaged old record. Serving it must
// stop that node, so the follower can catch up from a healthy node instead
// of reloading on every fetch (#201, campaign seed 5038431). Every run
// must pass the checkers.
func TestSnapshotFromCorruptLeader(t *testing.T) {
	fenced := false
	if !simrun.Each(t, seedRange(1, 200), func(t *testing.T, seed uint64) Stats {
		res := run(t, DefaultConfig(seed))
		checkers(t, res)
		return res.Stats
	}, func(seed uint64, st Stats) bool {
		if st.FencedAfterServe > 0 {
			fenced = true
			t.Logf("seed %d: %+v", seed, st)
		}
		return fenced
	}) {
		return
	}
	if !simrun.One() && !t.Failed() && !fenced {
		t.Fatal("no run fenced a node after it failed a serve on its own damage")
	}
}

// TestAmbiguousWritesInTheLog runs seed 1320469923, whose history has 8
// ambiguous writes, all in the committed log, in its first 7.3 s. Each
// one Porcupine had to fork into took effect or not, at any time after
// its call, and it did not finish in a minute. With their revisions and
// times from the log it takes milliseconds. A change to the simulator's
// random draws can move this seed off that edge; the test then passes
// without testing it.
func TestAmbiguousWritesInTheLog(t *testing.T) {
	if simrun.Drive(t, 1320469923) {
		return
	}
	res := run(t, linearizableConfig(1320469923))
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	if err := agree(h); err != nil {
		t.Fatal(err)
	}
}

// TestWitnessGaps runs seeds 1789179577 and 1789179677, whose first
// revisions no acked write holds: revisions 16 to 30 of the first all
// came from ambiguous writes, and only later reads show which. The
// log-free witness check searched for an order of them for longer than a
// minute until it used what the reads show of each key's life: created
// at one revision, last written at another, with a version (#197). A
// change to the simulator's random draws can move these seeds off that
// edge; the test then passes without testing it. Seed 1797098110 has a
// lease expiry that the log-free check put in too early, before an
// ambiguous put that used the lease.
func TestWitnessGaps(t *testing.T) {
	simrun.Each(t, []uint64{1789179577, 1789179677, 1797098110}, func(t *testing.T, seed uint64) struct{} {
		res := run(t, DefaultConfig(seed))
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := check.Witnessed(h, check.FromRevisions, time.Minute, witnessSteps); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		return struct{}{}
	}, nil)
}

// TestEndWaitsForCatchUp is #269's case: a follower starts again just
// before the end, behind what committed while it was down, so the end
// finds it behind the others. The run must let it catch up, then pass
// every check, as strict as ever. A seed whose run committed nothing
// meanwhile ends with it caught up, so two of the four must lag.
func TestEndWaitsForCatchUp(t *testing.T) {
	lagged := 0
	if !simrun.Each(t, []uint64{7001, 7002, 7003, 7004}, func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.LagAtEnd = true
		res := run(t, cfg)
		checkers(t, res)
		t.Logf("seed %d: the nodes ran %d s more after the end", seed, res.Stats.Settles)
		return res.Stats.Settles
	}, func(_ uint64, settles int) bool {
		if settles > 0 {
			lagged++
		}
		return false
	}) {
		return
	}
	if !simrun.One() && !t.Failed() && lagged < 2 {
		t.Fatalf("%d of 4 seeds ended with a node behind; the test needs two", lagged)
	}
}

// TestSaveRunKeepsDisks checks that a saved run has each node's disk
// calls and files, and the files of a crashed node right after its crash
// (#274).
func TestSaveRunKeepsDisks(t *testing.T) {
	if simrun.Drive(t, 11) {
		return
	}
	cfg := DefaultConfig(11)
	cfg.Duration = 6 * Second
	res := run(t, cfg)
	files := runFiles("out", "replay", res)
	calls := files[filepath.Join("disks", "node1", "calls.txt")]
	if !strings.Contains(calls, " ms sync 0") || !strings.Contains(calls, " ms write 0") {
		t.Fatalf("node 1's disk calls: %.200q", calls)
	}
	saved := func(dir string) bool {
		for path := range files {
			if strings.HasPrefix(path, dir+string(filepath.Separator)) {
				return true
			}
		}
		return false
	}
	if !saved(filepath.Join("disks", "node1", "now")) {
		t.Fatal("node 1's segments are not saved")
	}
	crashed := 0
	for _, n := range res.nodes {
		if n.crashImage != nil {
			crashed++
			if !saved(filepath.Join("disks", fmt.Sprintf("node%d", n.id), "after-crash")) {
				t.Fatalf("node %d crashed, but its files after the crash are not saved", n.id)
			}
		}
	}
	if crashed == 0 {
		t.Fatal("no node crashed")
	}
}
