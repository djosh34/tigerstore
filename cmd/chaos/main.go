// Command chaos runs one seeded real-process chaos run against a fresh
// 3-node TLS tigerstore cluster. Every client-to-node and node-to-node
// link goes through its own seeded fault proxy (package chaos/proxy).
// Three stock clientv3 clients, one pinned to each node, record Put, CAS
// Txn, DeleteRange and Range calls, a reader on each node records Range
// calls, and three watches with previous values record events. Meanwhile
// the run injects each fault of the schedule once, in a seeded order:
// SIGKILL and restart, SIGSTOP and SIGCONT of the leader and of a
// follower, and proxy faults. Then it
// heals, waits until the cluster converges, reads the committed log from
// the stopped nodes' WALs, and runs the Linearizable, Watches, Durable
// and Revisions checkers on the history.
//
// The run fails if a fault was not installed or did not heal, if a node
// exited on its own, if the cluster did not converge, or if a checker
// fails. Its directory keeps the seed, schedule, fault evidence, node
// logs, data directories and history either way, and nodes.txt, how each
// node stood at the end. A failure prints how each node stood then and
// the command that runs the seed again.
//
// Whether each fault took effect, and overlapped a write, is counted over
// many runs: -tally reads the faults.json of every run under its
// arguments and fails if a fault took effect in fewer than three in four
// of the runs it was installed in.
//
//	go build -o /tmp/tigerstore ./cmd/tigerstore
//	go run ./cmd/chaos -tigerstore /tmp/tigerstore -seed 1 -out /tmp/chaos
//	go run ./cmd/chaos -tally /tmp/chaos
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

type config struct {
	Seed           uint64        `json:"seed"`
	Out            string        `json:"out"`
	Tigerstore     string        `json:"tigerstore"`
	Faults         []string      `json:"faults"`
	Rate           int           `json:"rate"`
	RequestTimeout time.Duration `json:"request_timeout"`
	WatchTimeout   time.Duration `json:"watch_timeout"`
	Quiet          time.Duration `json:"quiet"`
	CheckerTimeout time.Duration `json:"checker_timeout"`
	// Env runs every node on a disk of its own, in a cgroup of its own,
	// with netem on the links and 1 to 10 ms between nodes, through
	// sudo -n; LazyFS is its binary, for the faults that need it.
	Env      bool   `json:"env"`
	LazyFS   string `json:"lazyfs,omitempty"`
	DiskSize int64  `json:"disk_size,omitempty"`
}

func main() {
	var c config
	var faults, recheck string
	flag.Uint64Var(&c.Seed, "seed", 0, "seed of the schedule, the proxies and the workload (required)")
	flag.StringVar(&c.Out, "out", "chaos-out", "directory for evidence; the run writes to <out>/seed-<seed>")
	flag.StringVar(&c.Tigerstore, "tigerstore", "", "tigerstore binary (required)")
	flag.StringVar(&faults, "faults", strings.Join(allFaults, ","), "faults to inject, comma separated")
	flag.IntVar(&c.Rate, "rate", 10, "calls per second per client")
	flag.DurationVar(&c.RequestTimeout, "request-timeout", 2*time.Second, "deadline of one client call")
	flag.DurationVar(&c.WatchTimeout, "watch-timeout", 20*time.Second, "time for watches to reach the final revision")
	flag.DurationVar(&c.Quiet, "quiet", 8*time.Second, "wait after the heal, longer than the server's request timeout, before the final reads")
	flag.DurationVar(&c.CheckerTimeout, "checker-timeout", time.Minute, "linearizability checker timeout")
	flag.StringVar(&recheck, "check", "", "only rerun the checkers on this saved history.json, writing results next to it")
	tallyRuns := flag.Bool("tally", false, "only count, over the runs under the directories given as arguments, the runs in which each fault took effect")
	flag.BoolVar(&c.Env, "env", false, "run each node on a disk and in a cgroup of its own, with netem and 1 to 10 ms between nodes (needs sudo -n), and add the faults of that environment")
	flag.StringVar(&c.LazyFS, "lazyfs", "", "LazyFS binary; with -env, every disk runs under it")
	flag.Int64Var(&c.DiskSize, "disk-size", 512<<20, "with -env, the bytes of each node's disk")
	flag.Parse()
	if recheck != "" {
		os.Exit(checkSaved(recheck, c.CheckerTimeout))
	}
	if *tallyRuns {
		logf := func(format string, args ...any) { _, _ = fmt.Fprintf(os.Stdout, format+"\n", args...) }
		ok, err := tally(flag.Args(), logf)
		if err != nil {
			logf("tally: %v", err)
			os.Exit(2)
		}
		if !ok {
			os.Exit(1)
		}
		return
	}
	c.Faults = strings.Split(faults, ",")
	if c.Env && faults == strings.Join(allFaults, ",") {
		c.Faults = append(c.Faults, envFaultsFor(c.LazyFS != "")...)
	}
	if c.Seed == 0 || c.Tigerstore == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	dir := filepath.Join(c.Out, fmt.Sprintf("seed-%d", c.Seed))
	err := run(c, dir)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "FAIL seed %d: %v\nevidence in %s\nreplay: %s\n", c.Seed, err, dir, replayCommand(c))
		os.Exit(1)
	}
	_, _ = fmt.Fprintf(os.Stdout, "PASS seed %d, evidence in %s\n", c.Seed, dir)
}

// replayCommand is the command that runs c again, with every flag that
// shapes the run.
func replayCommand(c config) string {
	cmd := fmt.Sprintf("go run ./cmd/chaos -tigerstore <binary> -seed %d -faults %s -rate %d -request-timeout %v -watch-timeout %v -quiet %v -checker-timeout %v",
		c.Seed, strings.Join(c.Faults, ","), c.Rate, c.RequestTimeout, c.WatchTimeout, c.Quiet, c.CheckerTimeout)
	if c.Env {
		cmd += fmt.Sprintf(" -env -disk-size %d", c.DiskSize)
		if c.LazyFS != "" {
			cmd += " -lazyfs <LazyFS binary>"
		}
	}
	return cmd
}

func save(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// verdict is the outcome of a run, saved as verdict.json.
type verdict struct {
	Seed    uint64   `json:"seed"`
	Passed  bool     `json:"passed"`
	Errors  []string `json:"errors,omitempty"`
	Seconds float64  `json:"seconds"`
}

// stepError is the failure of one step of a run.
type stepError struct {
	step string
	err  error
}

func (e *stepError) Error() string { return e.step + ": " + e.err.Error() }
func (e *stepError) Unwrap() error { return e.err }

func run(c config, dir string) (err error) {
	for _, f := range c.Faults {
		if slices.Contains(envFaults, f) && !c.Env {
			return fmt.Errorf("fault %q needs -env", f)
		}
		if slices.Contains(withoutLazyFS, f) && c.LazyFS != "" {
			return fmt.Errorf("fault %q runs only without -lazyfs, which would hide it", f)
		}
		if slices.Contains(withLazyFS, f) && c.LazyFS == "" {
			return fmt.Errorf("fault %q needs -lazyfs", f)
		}
		if !slices.Contains(allFaults, f) && !slices.Contains(envFaults, f) && !slices.Contains(networkTimeFaults, f) {
			return fmt.Errorf("unknown fault %q; known: %s", f, strings.Join(slices.Concat(allFaults, envFaults), ","))
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	logFile, err := os.Create(filepath.Join(dir, "chaos.log"))
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	begin := time.Now()
	logf := func(format string, args ...any) {
		line := fmt.Sprintf("%8.3fs ", time.Since(begin).Seconds()) + fmt.Sprintf(format, args...) + "\n"
		_, _ = os.Stdout.WriteString(line)
		_, _ = logFile.WriteString(line)
	}
	var errs []error
	fail := func(step string, e error) {
		if e != nil {
			logf("%s: %v", step, e)
			errs = append(errs, &stepError{step, e})
		}
	}
	defer func() {
		v := verdict{Seed: c.Seed, Passed: len(errs) == 0, Seconds: time.Since(begin).Seconds()}
		for _, e := range errs {
			v.Errors = append(v.Errors, e.Error())
		}
		err = errors.Join(append(errs, save(filepath.Join(dir, "verdict.json"), v))...)
	}()
	steps := plan(c.Seed, c.Faults)
	fail("save config", save(filepath.Join(dir, "config.json"), c))
	fail("save schedule", save(filepath.Join(dir, "schedule.json"), steps))
	logf("seed %d, %d steps: %v", c.Seed, len(steps), c.Faults)

	var e *environment
	var data [3]string
	var extra []string
	if c.Env {
		if e, err = newEnvironment(dir, c.Seed, c.LazyFS, c.DiskSize); err != nil {
			fail("environment", err)
			return
		}
		defer func() { fail("close the environment", e.close()) }()
		data, extra = e.data(), nodeArgs
	}
	cl, err := newCluster(dir, c.Tigerstore, c.Seed, data, extra)
	if err != nil {
		fail("cluster", err)
		return
	}
	defer func() { fail("close proxies", cl.closeProxies()) }()
	if e != nil {
		cl.started = func(i, pid int) error { return e.groups[i].Add(pid) }
		if ne := e.network(cl, c.Seed); ne != nil {
			fail("netem", ne)
			return
		}
		logf("one-way delays between nodes: %v", e.latency)
		fail("save latency", save(filepath.Join(dir, "latency.json"), e.latency))
	}
	ctx := context.Background()
	for _, n := range cl.nodes {
		if e := cl.start(n); e != nil {
			fail("start", e)
			logf("the nodes now:\n%s", cl.report(ctx))
			fail("stop", cl.stop())
			return
		}
	}
	if e := cl.ready(ctx, 30*time.Second); e != nil {
		fail("ready", e)
		logf("the nodes now:\n%s", cl.report(ctx))
		fail("stop", cl.stop())
		return
	}
	logf("cluster ready, member IDs %v", cl.ids)
	cs, err := dialClients(cl, c.RequestTimeout)
	if err != nil {
		fail("clients", err)
		fail("stop", cl.stop())
		return
	}
	wcfg := workloadConfig{seed: c.Seed, rate: c.Rate, keys: 6, valueSize: 256, requestTimeout: c.RequestTimeout, watchTimeout: c.WatchTimeout, quiet: c.Quiet}
	t, err := startTraffic(wcfg, cs)
	if err != nil {
		fail("traffic", err)
		fail("close clients", closeClients(cs))
		fail("stop", cl.stop())
		return
	}
	r := &runner{c: cl, t: t, e: e, start: t.r.origin, logf: logf}
	var archived <-chan error
	stopArchive := func() {}
	if e != nil {
		actx, cancel := context.WithCancel(ctx)
		archived, stopArchive = e.archive(actx, dir), cancel
	}
	_ = pause(ctx, 2*time.Second)
	records, schedErr := r.run(ctx, steps)
	fail("schedule", schedErr)
	fail("save faults", save(filepath.Join(dir, "faults.json"), records))
	fail("heal", r.healAll())
	// Stop the random calls first, so the nodes' revisions can settle.
	t.stopClients()
	logf("schedule done, clients stopped, waiting for convergence")
	fail("convergence", cl.converged(ctx, 30*time.Second))
	fail("finish", t.finish())
	fail("close clients", closeClients(cs))
	// How each node stands before the stop, for a failure here or in the
	// checks.
	nodes := cl.report(ctx)
	fail("save the nodes", os.WriteFile(filepath.Join(dir, "nodes.txt"), []byte(nodes), 0o644))
	defer func() {
		if len(errs) > 0 {
			logf("the nodes before the stop:\n%s", nodes)
		}
	}()
	fail("stop", cl.stop())
	for _, u := range cl.unexpectedExits() {
		fail("unexpected exit", errors.New(u))
	}
	events := map[string]any{}
	for _, l := range cl.links {
		ev, dropped := l.Proxy.Events()
		events[l.Name] = map[string]any{"stats": l.Proxy.Stats(), "events": ev, "events_not_kept": dropped}
	}
	fail("save proxy events", save(filepath.Join(dir, "proxy-events.json"), events))

	h := &t.r.h
	var dirs []string
	for i := range 3 {
		dirs = append(dirs, filepath.Join(dir, "data", fmt.Sprintf("node%d", i+1)))
	}
	if e != nil {
		stopArchive()
		fail("archive the logs", <-archived)
		fail("keep data", e.keep(dir))
		for i := range dirs {
			d, me := merged(dir, i)
			fail("merge the archived log", me)
			dirs[i] = d
		}
	}
	log, walEvidence, logErr := committedLog(dirs)
	h.Log = log
	fail("save WAL evidence", save(filepath.Join(dir, "wal.json"), walEvidence))
	fail("save history", save(filepath.Join(dir, "history.json"), h))
	fail("save watch responses", save(filepath.Join(dir, "watch-responses.json"), t.r.diagnostics))
	fail("committed log", logErr)
	logf("history: %d ops, %d log requests, target %d", len(h.Ops), len(h.Log), h.Target)
	fail("checks", runChecks(h, c.CheckerTimeout, dir, logf))
	faultsOverlapWrites(records, h)
	fail("save faults", save(filepath.Join(dir, "faults.json"), records))
	return
}

type checkerResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	// Skipped says why the checker did not run.
	Skipped string `json:"skipped,omitempty"`
	Error   string `json:"error,omitempty"`
	// CheckerError is the checker's own finding, without the shared
	// preconditions (coverage floor, target fence, validation) that Error
	// also holds.
	CheckerError string  `json:"checker_error,omitempty"`
	Seconds      float64 `json:"seconds"`
}

// runChecks runs the checkers of package check on h: Porcupine on the
// history without the ambiguous writes the log does not have, if that is
// short enough for it (check.ShortEnough), the witness checker in both
// modes on the whole history, the watch, durability and revision checkers.
// Each one fails too if the history is incomplete: no committed log, no
// watches, or a target that the log does not end at.
func runChecks(h *history.History, timeout time.Duration, dir string, logf func(string, ...any)) error {
	var missing error
	if len(h.Ops) == 0 || len(h.Log) == 0 || len(h.Watches) == 0 || h.Target <= 0 {
		missing = errors.New("incomplete history: need ops, a committed log, watches and a positive target")
	}
	if rev, e := finalRevision(h.Log); e != nil || rev != h.Target {
		missing = errors.Join(missing, fmt.Errorf("the committed log ends at revision %d, the target is %d", rev, h.Target), e)
	}
	missing = errors.Join(missing, h.Validate())
	cov, covErr := coverage(h)
	missing = errors.Join(missing, covErr, save(filepath.Join(dir, "coverage.json"), cov))
	lin, pruned, err := h.WithoutLostWrites()
	missing = errors.Join(missing, err)
	logf("linearizability input leaves out %d ambiguous writes that did not take effect", pruned)
	var results []checkerResult
	var errs []error
	short := check.ShortEnough(lin)
	for _, ch := range []struct {
		name string
		fn   func() error
	}{
		{"Linearizable", func() error { return check.Linearizable(lin, timeout) }},
		{"Witnessed (log)", func() error { return check.Witnessed(h, check.FromLog, timeout) }},
		{"Witnessed (revisions)", func() error { return check.Witnessed(h, check.FromRevisions, timeout) }},
		{"Watches", func() error { return check.Watches(h) }},
		{"Durable", func() error { return check.Durable(h) }},
		{"Revisions", func() error { return check.Revisions(h) }},
	} {
		if ch.name == "Linearizable" && !short {
			skipped := fmt.Sprintf("%d operations, more than the %d Porcupine gets", len(lin.Ops), check.ShortOps)
			logf("checker Linearizable skipped: %s", skipped)
			results = append(results, checkerResult{Name: ch.name, Skipped: skipped})
			continue
		}
		start := time.Now()
		own := ch.fn()
		err := errors.Join(missing, own)
		res := checkerResult{Name: ch.name, Passed: err == nil, Seconds: time.Since(start).Seconds()}
		if own != nil {
			res.CheckerError = own.Error()
		}
		if err != nil {
			res.Error = err.Error()
			errs = append(errs, fmt.Errorf("%s: %w", ch.name, err))
		}
		logf("checker %s passed=%v in %.3fs", ch.name, res.Passed, res.Seconds)
		results = append(results, res)
	}
	return errors.Join(append(errs, save(filepath.Join(dir, "checks.json"), results))...)
}

// checkSaved reruns the checkers on a history a run saved.
func checkSaved(path string, timeout time.Duration) int {
	b, err := os.ReadFile(path)
	h := new(history.History)
	if err == nil {
		err = json.Unmarshal(b, h)
	}
	logf := func(format string, args ...any) { _, _ = fmt.Fprintf(os.Stdout, format+"\n", args...) }
	if err == nil {
		err = runChecks(h, timeout, filepath.Dir(path), logf)
	}
	if err != nil {
		logf("FAIL %s: %v", path, err)
		return 1
	}
	logf("PASS %s", path)
	return 0
}
