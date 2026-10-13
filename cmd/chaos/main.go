// Command chaos runs one seeded real-process chaos run against a fresh
// 3-node TLS tigerstore cluster. Every client-to-node and node-to-node
// link goes through its own seeded fault proxy (package chaos/proxy).
// Three stock clientv3 clients, one pinned to each node, record Put, CAS
// Txn, DeleteRange and Range calls, and three watches with previous
// values record events. Meanwhile the run injects each fault of the
// schedule once, in a seeded order: SIGKILL and restart, SIGSTOP and
// SIGCONT of the leader and of a follower, and proxy faults. Then it
// heals, waits until the cluster converges, reads the committed log from
// the stopped nodes' WALs, and runs the Linearizable, Watches, Durable
// and Revisions checkers on the history.
//
// The run fails if a fault was not installed, had no effect or did not
// heal, if a node exited on its own, if the cluster did not converge, or
// if a checker fails. Its directory keeps the seed, schedule, fault
// evidence, node logs, data directories and history either way.
//
//	go build -o /tmp/tigerstore ./cmd/tigerstore
//	go run ./cmd/chaos -tigerstore /tmp/tigerstore -seed 1 -out /tmp/chaos
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
	flag.Parse()
	if recheck != "" {
		os.Exit(checkSaved(recheck, c.CheckerTimeout))
	}
	c.Faults = strings.Split(faults, ",")
	if c.Seed == 0 || c.Tigerstore == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	dir := filepath.Join(c.Out, fmt.Sprintf("seed-%d", c.Seed))
	err := run(c, dir)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "FAIL seed %d: %v\nevidence in %s\nreproduce: go run ./cmd/chaos -tigerstore <binary> -seed %d -faults %s\n", c.Seed, err, dir, c.Seed, faults)
		os.Exit(1)
	}
	_, _ = fmt.Fprintf(os.Stdout, "PASS seed %d, evidence in %s\n", c.Seed, dir)
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

func run(c config, dir string) (err error) {
	for _, f := range c.Faults {
		known := false
		for _, a := range allFaults {
			known = known || a == f
		}
		if !known {
			return fmt.Errorf("unknown fault %q; known: %s", f, strings.Join(allFaults, ","))
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
	fail := func(what string, e error) {
		if e != nil {
			logf("%s: %v", what, e)
			errs = append(errs, fmt.Errorf("%s: %w", what, e))
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

	cl, err := newCluster(dir, c.Tigerstore, c.Seed)
	if err != nil {
		fail("cluster", err)
		return
	}
	defer func() { fail("close proxies", cl.closeProxies()) }()
	for _, n := range cl.nodes {
		if e := cl.start(n); e != nil {
			fail("start", e)
			fail("stop", cl.stop())
			return
		}
	}
	ctx := context.Background()
	if e := cl.ready(ctx, 30*time.Second); e != nil {
		fail("ready", e)
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
	r := &runner{c: cl, start: t.r.origin, logf: logf}
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
	log, walEvidence, logErr := committedLog(dirs)
	h.Log = log
	fail("save WAL evidence", save(filepath.Join(dir, "wal.json"), walEvidence))
	fail("save history", save(filepath.Join(dir, "history.json"), h))
	fail("save watch responses", save(filepath.Join(dir, "watch-responses.json"), t.r.diagnostics))
	fail("committed log", logErr)
	logf("history: %d ops, %d log requests, target %d", len(h.Ops), len(h.Log), h.Target)
	fail("checks", runChecks(h, c.CheckerTimeout, dir, logf))
	fail("fault overlap", faultsOverlapWrites(records, h))
	return
}

type checkerResult struct {
	Name    string  `json:"name"`
	Passed  bool    `json:"passed"`
	Error   string  `json:"error,omitempty"`
	Seconds float64 `json:"seconds"`
}

// runChecks runs the four checkers of package check on h. Each one fails
// too if the history is incomplete: no committed log, no watches, or a
// target that the log does not end at.
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
	lin, pruned := withoutLostWrites(h)
	logf("linearizability input leaves out %d ambiguous writes that did not take effect", pruned)
	var results []checkerResult
	var errs []error
	for _, ch := range []struct {
		name string
		fn   func() error
	}{
		{"Linearizable", func() error { return check.Linearizable(lin, timeout) }},
		{"Watches", func() error { return check.Watches(h) }},
		{"Durable", func() error { return check.Durable(h) }},
		{"Revisions", func() error { return check.Revisions(h) }},
	} {
		start := time.Now()
		err := errors.Join(missing, ch.fn())
		res := checkerResult{Name: ch.name, Passed: err == nil, Seconds: time.Since(start).Seconds()}
		if err != nil {
			res.Error = err.Error()
			errs = append(errs, fmt.Errorf("%s: %w", ch.name, err))
		}
		logf("checker %s passed=%v in %.3fs", ch.name, res.Passed, res.Seconds)
		results = append(results, res)
	}
	return errors.Join(append(errs, save(filepath.Join(dir, "checks.json"), results))...)
}

// withoutLostWrites returns a copy of h without the ambiguous writes that
// never took effect, and how many it left out. Porcupine must keep an
// ambiguous write open until the end of the history, and with a few dozen
// of them its search did not finish in a minute.
//
// A write took effect only if it is in the committed log, and the log
// holds every write that succeeded (Durable checks that). So when the log
// holds a request no more often than successful calls sent it, no
// ambiguous call with that request took effect. Most workload writes carry
// a value no other call sends, which makes this exact for them. Leaving
// out an ambiguous call is safe whatever the log says: it can only make
// the check stricter, as a read that saw the call's effect then fails.
func withoutLostWrites(h *history.History) (*history.History, int) {
	key := func(q history.Request) string {
		b, err := json.Marshal(q)
		if err != nil {
			return fmt.Sprint(q)
		}
		return string(b)
	}
	inLog := map[string]int{}
	for _, q := range h.Log {
		inLog[key(q)]++
	}
	for _, op := range h.Ops {
		if op.Err == nil && op.Req.Mutates() {
			inLog[key(op.Req)]--
		}
	}
	out := *h
	out.Ops = nil
	pruned := 0
	for _, op := range h.Ops {
		if op.Err != nil && op.Err.Ambiguous && op.Req.Mutates() && inLog[key(op.Req)] <= 0 {
			pruned++
			continue
		}
		out.Ops = append(out.Ops, op)
	}
	return &out, pruned
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
