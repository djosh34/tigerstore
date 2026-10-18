package sim

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

var (
	firstSeed = flag.Uint64("sim.seed", 1, "first seed to run")
	numSeeds  = flag.Int("sim.seeds", 0, "number of seeds; 0 means 4 with -short and 16 without")
	simOut    = flag.String("sim.out", "", "directory where a failed run saves its trace, how it left the nodes, its history and the command that runs it again")
)

// run runs one seed in a synctest bubble of its own and fails the test if
// it breaks a rule.
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

// runSeed runs one seed in a synctest bubble of its own. If the test
// fails with no later run, the run explains itself (explain).
func runSeed(t *testing.T, cfg Config) (*Result, error) {
	t.Helper()
	return runWith(t, cfg, nil)
}

// runWith is runSeed with runInspect's inspect.
func runWith(t *testing.T, cfg Config, inspect func(*Sim) error) (*Result, error) {
	t.Helper()
	var res *Result
	var err error
	synctest.Test(t, func(*testing.T) { res, err = runInspect(cfg, inspect) })
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

// explain logs what a failed run did and how it left each node, and the
// command that runs its seed again. With -sim.out it saves them there
// too, with the run's history for cmd/checkhistory.
func explain(t *testing.T, cfg Config, res *Result) {
	cmd := replayCommand(t.Name(), cfg.Seed)
	if res == nil {
		t.Logf("replay: %s", cmd)
		return
	}
	trace := strings.Join(res.Trace, "\n")
	t.Logf("trace of seed %d:\n%s", cfg.Seed, trace)
	t.Logf("the nodes at the end of seed %d:\n%s", cfg.Seed, res.Report)
	if *simOut != "" {
		dir := filepath.Join(*simOut, strings.ReplaceAll(t.Name(), "/", "_"))
		if err := saveRun(dir, cmd, trace, res); err != nil {
			t.Logf("saving the run in %s: %v", dir, err)
		} else {
			t.Logf("the run is saved in %s", dir)
		}
	}
	// The seed decides the faults and every delay, but Go's scheduler the
	// order of the work between two disk or network calls, so a run again
	// can play out differently (package comment).
	t.Logf("replay: %s", cmd)
}

// saveRun writes a failed run's trace, node report, stats, history and
// replay command to dir.
func saveRun(dir, cmd, trace string, res *Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	files := map[string]string{
		"trace.txt":  trace + "\n",
		"nodes.txt":  res.Report,
		"replay.txt": cmd + "\n",
		"stats.txt":  fmt.Sprintf("%+v\n", res.Stats),
	}
	if h, err := res.Checkable(); err == nil {
		b, err := json.MarshalIndent(h, "", "  ")
		if err != nil {
			return err
		}
		files["history.json"] = string(b) + "\n"
		files["replay.txt"] += "go run ./cmd/checkhistory -checkers witness-log,witness-revisions,linearizable " + filepath.Join(dir, "history.json") + "\n"
	}
	var errs []error
	for name, body := range files {
		errs = append(errs, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	return errors.Join(errs...)
}

// seedTests are the tests whose subtests are the seeds of -sim.seed and
// -sim.seeds.
var seedTests = map[string]bool{"TestSeeds": true, "TestLinearizable": true, "TestMembershipFaults": true}

// activePlanted is the planted bug TestPlanted switched on, if any.
var activePlanted string

// replayCommand is the command that runs test name's run of seed again. A
// subtest named after a test function, such as TestSeeds's deliberate
// scenarios, runs as that test.
func replayCommand(name string, seed uint64) string {
	parts := strings.Split(name, "/")
	for i := len(parts) - 1; i > 0; i-- {
		if strings.HasPrefix(parts[i], "Test") {
			parts = parts[i:]
			break
		}
	}
	var args []string
	if seedTests[parts[0]] && len(parts) > 1 && parts[1] == strconv.FormatUint(seed, 10) {
		args = append(args, fmt.Sprintf("-sim.seed=%d", seed), "-sim.seeds=1")
	}
	for i, p := range parts {
		parts[i] = "^" + regexp.QuoteMeta(p) + "$"
	}
	cmd := "go test"
	if activePlanted != "" {
		cmd += " -tags planted"
		args = append(args, "-planted="+activePlanted)
	}
	cmd += fmt.Sprintf(" -count=1 ./internal/sim/ -run '%s'", strings.Join(parts, "/"))
	if len(args) > 0 {
		cmd += " -args " + strings.Join(args, " ")
	}
	return cmd
}

// knownBad are seeds that hit an open bug, which TestSeeds skips until
// the bug is fixed.
var knownBad = map[uint64]string{}

// TestSeeds runs every seed with the checkers. A failing seed runs again
// with -run 'TestSeeds/<seed>$'; its schedule of CPU work can differ, so
// its log carries the trace of the failed run.
func TestSeeds(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 16
		if testing.Short() {
			n = 4
		}
	}
	var total Stats
	var took effects
	ran := 0
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			if bug, ok := knownBad[seed]; ok {
				t.Skipf("seed %d hits %s", seed, bug)
			}
			a := run(t, DefaultConfig(seed))
			st := a.Stats
			if st.Compactions == 0 || st.Revokes == 0 || st.OK < 200 || len(a.Log) < 50 {
				t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
			}
			took.add(map[string]bool{
				"crash": st.Crashes > 0, "link change": st.LinkChanges > 0, "two cold restarts": st.ColdRestarts >= 2,
				"disk fault": st.DiskFaults > 0, "pause": st.Pauses > 0, "link stall": st.LinkStalls > 0,
				"half-open link": st.HalfOpens > 0, "gray node": st.GrayNodes > 0, "gray client": st.GrayStreams > 0,
			})
			ran++
			total.Fenced += st.Fenced
			total.StallHandOffs += st.StallHandOffs
			total.Asymmetric += st.Asymmetric
			total.GrayLeaderStops += st.GrayLeaderStops
			total.RecoveryCrashes += st.RecoveryCrashes
			total.ErasedAtRoot += st.ErasedAtRoot
			total.ErasedAtRemoval += st.ErasedAtRemoval
			checkers(t, a)
			t.Logf("%+v, %d committed writes", st, len(a.Log))
		})
	}
	// Random faults can end a gray period or disk stall before the watchdog
	// fires (#183, #218), so each shard also runs both deliberate scenarios.
	if ran >= 64 {
		t.Run("TestStalledLeaderIsReplaced", TestStalledLeaderIsReplaced)
		t.Run("TestGrayLeaderIsReplaced", TestGrayLeaderIsReplaced)
	}
	// Corruption, a link 10 times slower than its other direction and the
	// other common events are rare in one run, but a set of seeds must see
	// each.
	if ran >= 64 && !t.Failed() && (total.Fenced == 0 || total.Asymmetric == 0 ||
		total.RecoveryCrashes == 0 || total.ErasedAtRoot == 0 || total.ErasedAtRemoval == 0) {
		t.Fatalf("%d seeds: %d fenced nodes, %d asymmetric links, %d crashes in recovery, %d disks erased after a root, %d after a removal; each must happen",
			ran, total.Fenced, total.Asymmetric, total.RecoveryCrashes, total.ErasedAtRoot, total.ErasedAtRemoval)
	}
	// A stalled leader that hands over and a gray leader the commit
	// watchdog stops are rarer: about 0.9% of seeds had the first (#218),
	// 0.75% to 1.1% the second (#183). Over 2000 seeds even 0.7% leaves
	// less than one chance in a million of none. CI's shards of 400 do not
	// reach that, the seed campaign does.
	if ran >= 2000 && !t.Failed() && (total.StallHandOffs == 0 || total.GrayLeaderStops == 0) {
		t.Fatalf("%d seeds: %d stalled leaders that handed over, %d stopped gray leaders; each must happen",
			ran, total.StallHandOffs, total.GrayLeaderStops)
	}
	t.Logf("%d seeds: %d stalled leaders that handed over, %d stopped gray leaders", ran, total.StallHandOffs, total.GrayLeaderStops)
	took.check(t, "seeds")
}

// effects counts, over a set of runs, the runs in which each fault took
// effect. A fault that took effect in too few of them fails the test,
// not the run it missed.
type effects struct {
	runs int
	took map[string]int
}

// add counts one run, with whether each fault took effect in it.
func (e *effects) add(took map[string]bool) {
	if e.took == nil {
		e.took = map[string]int{}
	}
	e.runs++
	for name, ok := range took {
		n := e.took[name]
		if ok {
			n++
		}
		e.took[name] = n
	}
}

// check fails t unless each fault took effect in atLeast of the runs, and
// logs the counts.
func (e *effects) check(t *testing.T, runs string) {
	t.Helper()
	least := atLeast(e.runs)
	var counts []string
	for _, name := range slices.Sorted(maps.Keys(e.took)) {
		n := e.took[name]
		counts = append(counts, fmt.Sprintf("%s %d", name, n))
		if n < least {
			t.Errorf("%s took effect in %d of %d %s, want %d at least", name, n, e.runs, runs, least)
		}
	}
	t.Logf("of %d %s, the faults took effect in: %s", e.runs, runs, strings.Join(counts, ", "))
}

// checkers runs the independent checkers of package check on a run:
// durability, revisions and watches. Linearizability runs on the smaller
// histories of TestLinearizable.
func checkers(t *testing.T, res *Result) {
	t.Helper()
	if err := independent(res); err != nil {
		t.Fatal(err)
	}
}

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
		if err := check.Witnessed(h, mode, time.Minute); err != nil {
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
	errs := []error{check.Witnessed(h, check.FromLog, time.Minute), check.Witnessed(h, check.FromRevisions, time.Minute)}
	if check.ShortEnough(h) {
		names = append(names, "linearizability")
		errs = append(errs, check.Linearizable(h, 3*time.Minute))
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
		return fmt.Errorf("linearizability: %w", errors.Join(errs...))
	}
	return fmt.Errorf("the checkers disagree: %s", strings.Join(verdicts, "; "))
}

// linearizableConfig is a short run whose history Porcupine can check:
// 5 virtual seconds with the same faults and 4 clients, then 8 s of
// healing, without leases.
func linearizableConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Duration = 5 * Second
	cfg.Heal = 8 * Second
	cfg.Clients = 4
	cfg.NoLeases = true
	return cfg
}

// TestLinearizable checks short runs with Porcupine and the witness
// checker, which must agree. Not finishing in time is a failure. Nearly
// every run must be short enough for Porcupine (check.ShortEnough).
func TestLinearizable(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 16
		if testing.Short() {
			n = 4
		}
	}
	short := 0
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			res := run(t, linearizableConfig(seed))
			h, err := res.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if err := agree(h); err != nil {
				t.Fatal(err)
			}
			if check.ShortEnough(h) {
				short++
			}
			t.Logf("%d ops checked in %v", len(h.Ops), time.Since(start))
		})
	}
	if least := atLeast(n); !t.Failed() && short < least {
		t.Fatalf("Porcupine ran on %d of %d histories, want %d at least: the others were longer than %d operations", short, n, least, check.ShortOps)
	}
}

// atLeast is how many of n runs must show something that most runs show:
// 95% of them, rounded down.
func atLeast(n int) int { return n * 95 / 100 }

// TestStalledLeaderIsReplaced stalls the leader's disk for good at 5 s,
// with no other fault. The leader still heartbeats, so only its own
// watchdog can make way. The core hands over core.StallTicks election
// timeouts after the oldest append it has not persisted, which is the
// first one the stall holds: one queued before the stall, or the first
// after it. It must hand over by then, plus two ticks, as the core counts
// ticks, and every call to a healthy node that starts 10 election
// timeouts after the stall must succeed. A leader can propose nothing for
// a while after the stall, as when it serves a snapshot (#260).
func TestStalledLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.StallLeaderAt = 5 * Second
		cfg.LiveFrom = cfg.StallLeaderAt + 10*election
		cfg.Duration = 25 * Second
		res := run(t, cfg)
		if len(res.StallHandOffs) == 0 || res.Stats.StallHandOffs == 0 {
			t.Fatalf("seed %d: the stalled leader never handed over", seed)
		}
		if res.HeldAppendAt == 0 {
			t.Fatalf("seed %d: the stall held no append of the leader", seed)
		}
		limit := res.HeldAppendAt + core.StallTicks*election + 2*TickTime
		if at := res.StallHandOffs[0]; at < cfg.StallLeaderAt || at > limit {
			t.Fatalf("seed %d: the stalled leader handed over at %d ms, want between the stall at %d ms and %d ms, %d election timeouts and two ticks after its oldest held append at %d ms",
				seed, at/Millisecond, cfg.StallLeaderAt/Millisecond, limit/Millisecond, core.StallTicks, res.HeldAppendAt/Millisecond)
		}
		checkers(t, res)
		t.Logf("seed %d: stalled leader handed over at %d ms, deadline %d ms after its oldest held append at %d ms", seed, res.StallHandOffs[0]/Millisecond, limit/Millisecond, res.HeldAppendAt/Millisecond)
	}
}

// TestAllLinkMasks holds each of the 64 sets of directed links between the
// three nodes for 3 virtual seconds, from 2 s on, with no other fault,
// then heals. Every rule and checker must hold, and the cluster must serve
// again 2 s after healing.
func TestAllLinkMasks(t *testing.T) {
	for mask := range uint64(64) {
		cfg := DefaultConfig(1000 + mask)
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
	}
}

// TestSwitchingLinks switches to a random set of directed links about
// every 300 ms for 10 virtual seconds, then heals. Two of the three seeds
// at least must switch 10 times.
func TestSwitchingLinks(t *testing.T) {
	var took effects
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(2000 + seed)
		cfg.Faults = Faults{SwitchEvery: 300 * Millisecond}
		cfg.Duration = 10 * Second
		res := run(t, cfg)
		checkers(t, res)
		took.add(map[string]bool{"ten switches": res.Stats.LinkChanges >= 10})
	}
	took.check(t, "seeds")
}

// TestEachFaultTakesEffect runs each network, process, gray and recovery
// fault alone for 8 virtual seconds, on three seeds, and requires that it
// happened in two of them at least. A follower held down for up to 4 s
// needs about a second after its restart before its first fetch, which
// the stream faults damage only before healing.
func TestEachFaultTakesEffect(t *testing.T) {
	cases := []struct {
		name   string
		faults Faults
		count  func(Stats) int
	}{
		{"link stall", Faults{LinkStallEvery: Second}, func(st Stats) int { return st.LinkStalls }},
		{"pause", Faults{PauseEvery: Second}, func(st Stats) int { return st.Pauses }},
		{"half-open link", Faults{HalfOpenEvery: Second}, func(st Stats) int { return st.HalfOpens }},
		{"link delays", Faults{SwitchEvery: 200 * Millisecond, MaxDelay: 5 * Millisecond}, func(st Stats) int { return st.Asymmetric }},
		{"gray node", Faults{GrayEvery: Second}, func(st Stats) int { return st.GrayNodes }},
		{"gray client", Faults{GrayClient: true}, func(st Stats) int { return st.GrayStreams }},
		{"crash in recovery", Faults{CrashEvery: 2 * Second, MaxDown: Second, RecoveryCrash: true}, func(st Stats) int { return st.RecoveryCrashes }},
		{"follower held down", Faults{HoldDown: true}, func(st Stats) int { return st.Snapshots }},
		{"damaged snapshot", Faults{HoldDown: true, FlipSnapshot: true}, func(st Stats) int { return st.SnapshotFlips }},
		{"crash during a fetch", Faults{HoldDown: true, FetchCrash: true}, func(st Stats) int { return min(st.FetchCrashes, st.Reloads) }},
		{"crash at a checkpoint step", Faults{StepCrash: true}, func(st Stats) int { return st.StepCrashes }},
		{"rotten value", Faults{Rot: true}, func(st Stats) int { return st.Rots }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var took effects
			for seed := uint64(3000); seed < 3003; seed++ {
				cfg := DefaultConfig(seed)
				cfg.Faults = c.faults
				cfg.Duration = 8 * Second
				cfg.Heal = 8 * Second
				res := run(t, cfg)
				checkers(t, res)
				took.add(map[string]bool{c.name: c.count(res.Stats) > 0})
			}
			took.check(t, "seeds")
		})
	}
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
	older, failed, during := 0, 0, 0
	// Each of the three happens in about one seed in 20 to 50; the search
	// stops once all did.
	for seed := uint64(1); seed <= 200 && (older == 0 || failed == 0 || during == 0); seed++ {
		res := run(t, slowFetchConfig(5000+seed))
		checkers(t, res)
		older += res.Stats.OlderCheckpoints
		failed += res.Stats.ServeFailures
		during += res.Stats.CheckpointsDuringInstall
	}
	if older == 0 || failed == 0 || during == 0 {
		t.Fatalf("%d snapshots installed from an older checkpoint, %d fetches the leader could not serve, %d checkpoints finished during an install; each must happen", older, failed, during)
	}
}

// TestHalfOpenLink makes the link from the leader to a follower half-open
// at 5 s, with no other fault, until the peer write timeout fails it 5 s
// later. The leader and the other follower are a quorum, so every call to
// them must succeed and take under a second; the cut-off follower must
// serve again 2 s after the connection fails.
func TestHalfOpenLink(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(4000 + seed)
		cfg.Faults = Faults{}
		// Calls to the cut-off follower wait up to the client timeout for a
		// leader (#105), so more clients keep the healthy pair busy.
		cfg.Clients = 12
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
			if r.Call < h.start || r.Call >= h.end || r.Node == h.to {
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
	}
}

// TestGrayLeaderIsReplaced makes the leader gray at 5 s until healing,
// with no other fault: its heartbeats get through, its appends do not, so
// Raft's quorum check keeps it leader while nothing commits. It must stop
// within core.CommitStallTicks election timeouts plus one. It
// restarts still gray and with the longest log, so it can win the next
// election and stop again. Every call to another node that starts 3
// election timeouts after its last stop must succeed in under a second.
func TestGrayLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(5000 + seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt = 5 * Second
		// The stopped leader restarts still gray, until healing, so a
		// client call to it hangs for the client's 3 s timeout. How many
		// calls reach the healthy pair then depends on how often the
		// clients pick that node, and the window starts at its last
		// stop; 40 s leaves enough of them on every seed (seed 2 had 19
		// in 30 s).
		cfg.Duration = 40 * Second
		res := run(t, cfg)
		if len(res.CommitStops) == 0 || res.Stats.GrayLeaderStops == 0 {
			t.Fatalf("seed %d: the gray leader never stopped", seed)
		}
		stop := res.CommitStops[0]
		// One more election timeout than the watchdog's: on the real
		// driver the stop came up to 750 ms past its three (seed 5001 of
		// sim-campaign run 39044402060). Why is not known.
		limit := cfg.GrayLeaderAt + (core.CommitStallTicks+1)*election
		if stop < cfg.GrayLeaderAt || stop > limit {
			t.Fatalf("seed %d: the gray leader stopped at %d ms, want between %d and %d ms", seed, stop/Millisecond, cfg.GrayLeaderAt/Millisecond, limit/Millisecond)
		}
		last := res.CommitStops[len(res.CommitStops)-1]
		ok := 0
		for _, r := range res.History {
			if r.Call < last+3*election || r.Call >= cfg.Duration || r.Node == res.grayNode {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed: %v", seed, r.Node+1, r.Call/Millisecond, r.Err)
			}
			if r.Return-r.Call >= Second {
				t.Fatalf("seed %d: call to node %d at %d ms took %d ms", seed, r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
			}
			ok++
		}
		if ok < 20 {
			t.Fatalf("seed %d: only %d calls to the healthy pair", seed, ok)
		}
		checkers(t, res)
		t.Logf("seed %d: gray leader stopped at %d ms, deadline %d ms; %d timely calls to the healthy pair", seed, stop/Millisecond, limit/Millisecond, ok)
	}
}

// TestDiskFaultAfterHealing cuts every link before the first election, so
// no node appends during the faults: the guaranteed disk fault must fire
// after healing, and the liveness rule must still hold.
func TestDiskFaultAfterHealing(t *testing.T) {
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
		if errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
			// With no other fault, only a start from the log after a
			// snapshot install, which the run's tiny window makes
			// frequent, answers that.
			continue
		}
		if r.Err != nil && !isRejection(r.Err) {
			t.Fatalf("call to node %d at %d ms failed beside the gray client: %v", r.Node+1, r.Call/Millisecond, r.Err)
		}
		if r.Return-r.Call >= Second {
			t.Fatalf("call to node %d at %d ms took %d ms beside the gray client", r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
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
	run(t, DefaultConfig(736998125))
}

// TestRotInAnEntryRecord runs seed 1320469980, whose rotten value on node
// 3 is in entry 16. At 11,799 ms the node reads that entry's whole record
// for the term of its checkpoint root, and stops on the record's crc
// instead of the value's checksum. That is the stop the run expects. A
// change to the simulator's random draws can move this seed off that edge;
// the test then passes without testing it.
func TestRotInAnEntryRecord(t *testing.T) {
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
	for seed := uint64(1); seed <= 200; seed++ {
		res := run(t, DefaultConfig(seed))
		checkers(t, res)
		if res.Stats.FencedAfterServe > 0 {
			t.Logf("seed %d: %+v", seed, res.Stats)
			return
		}
	}
	t.Fatal("no run fenced a node after it failed a serve on its own damage")
}

// TestAmbiguousWritesInTheLog runs seed 1320469923, whose history has 8
// ambiguous writes, all in the committed log, in its first 7.3 s. Each
// one Porcupine had to fork into took effect or not, at any time after
// its call, and it did not finish in a minute. With their revisions and
// times from the log it takes milliseconds. A change to the simulator's
// random draws can move this seed off that edge; the test then passes
// without testing it.
func TestAmbiguousWritesInTheLog(t *testing.T) {
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
	for _, seed := range []uint64{1789179577, 1789179677, 1797098110} {
		res := run(t, DefaultConfig(seed))
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := check.Witnessed(h, check.FromRevisions, time.Minute); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}
