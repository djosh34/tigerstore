package main

import (
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

const deadline = "2026-10-13T21:50:00Z"

type config struct {
	endpoints, ca, cert, key, out, receipt, history, backend, dataDirs, stopped, stop string
	duration, requestTimeout, watchTimeout, checkerTimeout, quiet                     time.Duration
	seed                                                                              uint64
	clients, rate, keys, valueSize                                                    int
	statusOnly                                                                        bool
	listChecks, serializable                                                          bool
	burstArm, burstRelease, burstReady, burstHealed                                   string
	interval                                                                          time.Duration
	burstWrites                                                                       int
	readArm, readRelease, readReady, triggerFile                                      string
}

func main() {
	if err := runCLI(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCLI() error {
	if len(os.Args) < 2 {
		return errors.New("expected workload|status|verify|log|check")
	}
	until, err := time.Parse(time.RFC3339, deadline)
	if err != nil {
		return err
	}
	if !time.Now().Before(until) {
		return errors.New("deadline reached; no new work")
	}
	c := config{}
	f := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	f.StringVar(&c.endpoints, "endpoints", "", "pinned endpoint CSV")
	f.StringVar(&c.ca, "cacert", "", "trusted CA")
	f.StringVar(&c.cert, "cert", "", "client certificate")
	f.StringVar(&c.key, "key", "", "client private key")
	f.StringVar(&c.out, "out", "", "output path")
	f.StringVar(&c.receipt, "receipt", "", "terminal receipt")
	f.StringVar(&c.history, "history", "", "history JSON input")
	f.StringVar(&c.backend, "backend", "tigerstore", "tigerstore or etcd")
	f.StringVar(&c.dataDirs, "data-dirs", "", "stopped node data-directory CSV")
	f.StringVar(&c.stopped, "stopped-file", "", "orchestrator shutdown marker")
	f.StringVar(&c.stop, "stop-file", "", "end workload when marker exists")
	f.DurationVar(&c.duration, "duration", 65*time.Second, "workload duration")
	f.DurationVar(&c.requestTimeout, "request-timeout", 2*time.Second, "per request deadline")
	f.DurationVar(&c.watchTimeout, "watch-timeout", 20*time.Second, "watch finalization deadline")
	f.DurationVar(&c.checkerTimeout, "checker-timeout", 30*time.Second, "linearizability deadline")
	f.Uint64Var(&c.seed, "seed", 49, "workload seed")
	f.IntVar(&c.clients, "clients", 3, "3..256 sequential clients spread across three endpoints; checker coverage floor")
	f.BoolVar(&c.statusOnly, "status-only", false, "Status RPC only, without a linearizable read")
	f.BoolVar(&c.listChecks, "list-checks", false, "seed 32 keys and run checked concurrent RangeStream LISTs")
	f.BoolVar(&c.serializable, "serializable", false, "serializable LIST during leaderless faults")
	f.StringVar(&c.burstArm, "burst-arm-file", "", "controller arms the bounded write burst")
	f.StringVar(&c.burstRelease, "burst-release-file", "", "controller releases all burst workers after fault installation")
	f.StringVar(&c.burstReady, "burst-ready-file", "", "all workers ready marker")
	f.IntVar(&c.burstWrites, "burst-writes", 0, "unpaced sequential puts per client in the bounded burst, 0..16")
	f.StringVar(&c.burstHealed, "burst-healed-file", "", "after burst, read only until controller heals the disk")
	f.DurationVar(&c.interval, "interval", 0, "paced per-client interval; zero uses rate")
	f.StringVar(&c.readArm, "read-arm-file", "", "controller arms a synchronized read burst")
	f.StringVar(&c.readRelease, "read-release-file", "", "controller releases the read burst under ENOSPC")
	f.StringVar(&c.readReady, "read-ready-file", "", "all read workers ready marker")
	f.StringVar(&c.triggerFile, "trigger-file", "", "JSON endpoint for exactly one separately recorded trigger Put")
	f.IntVar(&c.rate, "rate", 5, "maximum operations per second per client")
	f.IntVar(&c.keys, "keys", 6, "hot keys, 2 to 8")
	f.IntVar(&c.valueSize, "value-size", 1024, "mutation value bytes")
	f.DurationVar(&c.quiet, "quiet", 8*time.Second, "healthy quiet suffix before terminal reads")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	if len(f.Args()) > 0 {
		return fmt.Errorf("unexpected arguments %v", f.Args())
	}
	switch os.Args[1] {
	case "list-watch-probe":
		return listWatchProbe(c)
	case "status":
		return networkStatus(c)
	case "workload", "run":
		return workload(c)
	case "verify", "inspect":
		return verify(c)
	case "log":
		return generateLog(c)
	case "check":
		return runChecks(c)
	default:
		return fmt.Errorf("unknown mode %q", os.Args[1])
	}
}

func save(path string, v any) error {
	if path == "" {
		return errors.New("missing output path")
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", append(b, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func load(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func csv(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

type checkerResult struct {
	Name    string  `json:"name"`
	Passed  bool    `json:"passed"`
	Error   string  `json:"error,omitempty"`
	Seconds float64 `json:"seconds"`
}

func runChecks(c config) error {
	h := new(history.History)
	loadErr := load(c.history, h)
	var missing error
	if loadErr != nil {
		missing = fmt.Errorf("oracle construction: unreadable history: %w", loadErr)
	}
	if len(h.Ops) == 0 || len(h.Log) == 0 || len(h.Watches) == 0 || h.Target <= 0 {
		missing = errors.Join(missing, errors.New("missing oracle: require nonempty ops, committed log, watches and positive target"))
	}
	if rev, e := finalRevision(h.Log); e != nil || rev != h.Target {
		missing = errors.Join(missing, fmt.Errorf("oracle target fence: replay revision %d target %d: %v", rev, h.Target, e))
	}
	missing = errors.Join(missing, h.Validate())
	cov, covErr := coverage(h, c.clients)
	if c.out != "" {
		if e := save(c.out+".coverage.json", cov); e != nil {
			missing = errors.Join(missing, e)
		}
	}
	missing = errors.Join(missing, covErr)
	var results []checkerResult
	var errs []error
	for _, ch := range []struct {
		name string
		fn   func() error
	}{
		{"Watches", func() error { return check.Watches(h) }},
		{"Durable", func() error { return check.Durable(h) }},
		{"Revisions", func() error { return check.Revisions(h) }},
		{"Linearizable", func() error { return check.Linearizable(h, c.checkerTimeout) }},
	} {
		start := time.Now()
		err := errors.Join(missing, ch.fn())
		r := checkerResult{Name: ch.name, Passed: err == nil, Seconds: time.Since(start).Seconds()}
		if err != nil {
			r.Error = err.Error()
			errs = append(errs, fmt.Errorf("%s: %w", ch.name, err))
		}
		results = append(results, r)
		if c.out != "" {
			if e := save(c.out, results); e != nil {
				errs = append(errs, e)
			}
		}
		b, e := json.Marshal(r)
		if e != nil {
			return e
		}
		fmt.Println(string(b))
	}
	if c.out != "" {
		errs = append(errs, save(c.out, results))
	}
	return errors.Join(errs...)
}
