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

const deadline = "2026-10-13T22:00:00Z"

type config struct {
	endpoints, ca, cert, key, out, receipt, history, backend, dataDirs, stopped, stop, scenario string
	duration, requestTimeout, watchTimeout, checkerTimeout, quiet                               time.Duration
	seed                                                                                        uint64
	clients, rate, keys, valueSize, watchers                                                    int
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
	f.StringVar(&c.scenario, "scenario", "", "lease, compaction or watchers")
	f.IntVar(&c.watchers, "watchers", 96, "concurrent logical watchers")
	f.DurationVar(&c.duration, "duration", 65*time.Second, "workload duration")
	f.DurationVar(&c.requestTimeout, "request-timeout", 2*time.Second, "per request deadline")
	f.DurationVar(&c.watchTimeout, "watch-timeout", 20*time.Second, "watch finalization deadline")
	f.DurationVar(&c.checkerTimeout, "checker-timeout", 30*time.Second, "linearizability deadline")
	f.Uint64Var(&c.seed, "seed", 49, "workload seed")
	f.IntVar(&c.clients, "clients", 3, "one sequential client per endpoint")
	f.IntVar(&c.rate, "rate", 5, "maximum operations per second per client")
	f.IntVar(&c.keys, "keys", 6, "hot keys, 6 to 8")
	f.IntVar(&c.valueSize, "value-size", 1024, "mutation value bytes")
	f.DurationVar(&c.quiet, "quiet", 8*time.Second, "healthy quiet suffix before terminal reads")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	if len(f.Args()) > 0 {
		return fmt.Errorf("unexpected arguments %v", f.Args())
	}
	switch os.Args[1] {
	case "status":
		return networkStatus(c)
	case "workload", "run":
		if c.scenario != "" {
			return semantics(c)
		}
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
	cov, covErr := coverage(h)
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
		{"Linearizable", func() error { return check.Linearizable(h, c.checkerTimeout) }},
		{"Watches", func() error { return check.Watches(h) }},
		{"Durable", func() error { return check.Durable(h) }},
		{"Revisions", func() error { return check.Revisions(h) }},
	} {
		start := time.Now()
		err := errors.Join(missing, ch.fn())
		r := checkerResult{Name: ch.name, Passed: err == nil, Seconds: time.Since(start).Seconds()}
		if err != nil {
			r.Error = err.Error()
			errs = append(errs, fmt.Errorf("%s: %w", ch.name, err))
		}
		results = append(results, r)
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
