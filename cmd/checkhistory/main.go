// Command checkhistory runs the checkers of package check on recorded
// histories: files in the history package's JSON, gzipped if their name
// ends in .gz. It prints one JSON line per file and checker, and exits 1
// if a checker fails or does not finish within its timeout.
//
//	checkhistory [-timeout 10m] [-checkers witness-log,witness-revisions] file...
//
// The checkers are witness-log and witness-revisions (check.Witnessed in
// each mode), linearizable (Porcupine), watches, durable and revisions.
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

// verdict is one checker's answer on one file.
type verdict struct {
	File       string              `json:"file"`
	Checker    string              `json:"checker"`
	Ops        int                 `json:"ops"`
	Clients    int                 `json:"clients"`
	Passed     bool                `json:"passed"`
	Seconds    float64             `json:"seconds"`
	Error      string              `json:"error,omitempty"`
	Variant    string              `json:"variant,omitempty"`
	Metrics    *check.WitnessStats `json:"metrics,omitempty"`
	AllocBytes uint64              `json:"alloc_bytes,omitempty"`
}

func main() {
	timeout := flag.Duration("timeout", 10*time.Minute, "how long each checker may take on one file")
	names := flag.String("checkers", "witness-log,witness-revisions", "comma-separated checkers to run")
	parallel := flag.Bool("parallel", false, "also run the checkers again at once, each in a goroutine (#441)")
	metrics := flag.Bool("witness-stats", false, "print revision witness work counters")
	variant := flag.String("witness-variant", "baseline", "baseline, checkpoints, indexed, both, prefilter, fast, optional, scoped, tuned, retired, best, undo, or ultimate")
	cpu := flag.String("cpuprofile", "", "CPU profile of checking (one file/checker only)")
	flag.Parse()
	var stats check.WitnessStats
	var lastProgress time.Time
	progress := func(s *check.WitnessStats) {
		if *metrics && time.Since(lastProgress) >= 10*time.Second {
			b, _ := json.Marshal(s)
			fmt.Fprintf(os.Stderr, "WITNESS_PROGRESS %s\n", b)
			lastProgress = time.Now()
		}
	}
	checkers := map[string]func(*history.History) error{
		"witness-log": func(h *history.History) error { return check.Witnessed(h, check.FromLog, *timeout) },
		"witness-revisions": func(h *history.History) error {
			stats = check.WitnessStats{}
			return check.Witnessed(h, check.FromRevisions, *timeout, check.WitnessMetrics(&stats), check.RevisionSearch(*variant), check.WitnessProgress(progress))
		},
		"linearizable": func(h *history.History) error { return check.Linearizable(h, *timeout) },
		"watches":      check.Watches,
		"durable":      check.Durable,
		"revisions":    check.Revisions,
	}
	var run []string
	for name := range strings.SplitSeq(*names, ",") {
		if _, ok := checkers[name]; !ok {
			fmt.Fprintf(os.Stderr, "checkhistory: unknown checker %q\n", name)
			os.Exit(2)
		}
		run = append(run, name)
	}
	out := json.NewEncoder(os.Stdout)
	failed := false
	for _, file := range flag.Args() {
		readStart := time.Now()
		h, err := read(file)
		fmt.Fprintf(os.Stderr, "read %s in %.1f s\n", file, time.Since(readStart).Seconds())
		if err != nil {
			failed = true
			if err := out.Encode(verdict{File: file, Checker: "read", Error: err.Error()}); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			continue
		}
		clients := map[int]bool{}
		for _, op := range h.Ops {
			clients[op.Client] = true
		}
		for _, name := range run {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			if *cpu != "" {
				f, e := os.Create(*cpu)
				if e != nil {
					panic(e)
				}
				defer f.Close()
				if e := pprof.StartCPUProfile(f); e != nil {
					panic(e)
				}
			}
			start := time.Now()
			err := checkers[name](h)
			v := verdict{File: file, Checker: name, Ops: len(h.Ops), Clients: len(clients), Passed: err == nil, Seconds: time.Since(start).Seconds()}
			if *cpu != "" {
				pprof.StopCPUProfile()
			}
			runtime.ReadMemStats(&after)
			if *metrics && name == "witness-revisions" {
				v.Variant, v.Metrics, v.AllocBytes = *variant, &stats, after.TotalAlloc-before.TotalAlloc
			}
			if err != nil {
				failed = true
				v.Error = err.Error()
			}
			if err := out.Encode(v); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
		if *parallel {
			start := time.Now()
			var wg sync.WaitGroup
			var mu sync.Mutex
			for _, name := range run {
				wg.Go(func() {
					s := time.Now()
					err := checkers[name](h)
					mu.Lock()
					defer mu.Unlock()
					fmt.Fprintf(os.Stderr, "parallel %s: %.1f s, passed %v\n", name, time.Since(s).Seconds(), err == nil)
				})
			}
			wg.Wait()
			fmt.Fprintf(os.Stderr, "parallel all: %.1f s\n", time.Since(start).Seconds())
		}
	}
	if failed {
		os.Exit(1)
	}
}

// read loads one history file.
func read(file string) (*history.History, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read only
	var r io.Reader = f
	if strings.HasSuffix(file, ".gz") {
		z, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		r = z
	}
	var h history.History
	if err := json.NewDecoder(r).Decode(&h); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return &h, nil
}
