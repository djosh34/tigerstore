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
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

// verdict is one checker's answer on one file.
type verdict struct {
	File    string  `json:"file"`
	Checker string  `json:"checker"`
	Ops     int     `json:"ops"`
	Clients int     `json:"clients"`
	Passed  bool    `json:"passed"`
	Seconds float64 `json:"seconds"`
	Error   string  `json:"error,omitempty"`
}

func main() {
	timeout := flag.Duration("timeout", 10*time.Minute, "how long each checker may take on one file")
	names := flag.String("checkers", "witness-log,witness-revisions", "comma-separated checkers to run")
	cpuprofile := flag.String("cpuprofile", "", "exp only: write a CPU profile of the checks")
	flag.Parse()
	checkers := map[string]func(*history.History) error{
		"witness-log": func(h *history.History) error { return check.Witnessed(h, check.FromLog, *timeout) },
		"witness-revisions": func(h *history.History) error {
			err := check.Witnessed(h, check.FromRevisions, *timeout)
			return err
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
		h, err := read(file)
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
			if *cpuprofile != "" {
				runtime.GC()
				runtime.MemProfileRate = 64 << 10
				f, err := os.Create(*cpuprofile)
				if err != nil {
					log.Fatal(err)
				}
				if err := pprof.StartCPUProfile(f); err != nil {
					log.Fatal(err)
				}
			}
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			start := time.Now()
			err := checkers[name](h)
			runtime.ReadMemStats(&m1)
			fmt.Fprintf(os.Stderr, "check alloc %d MB, %d mallocs, %d GCs, live after %d MB, gc cpu %.2f\n", (m1.TotalAlloc-m0.TotalAlloc)>>20, m1.Mallocs-m0.Mallocs, m1.NumGC-m0.NumGC, m1.HeapAlloc>>20, m1.GCCPUFraction)
			if *cpuprofile != "" {
				pprof.StopCPUProfile()
				mf, _ := os.Create(*cpuprofile + ".mem")
				_ = pprof.Lookup("allocs").WriteTo(mf, 0)
				mf.Close()
			}
			v := verdict{File: file, Checker: name, Ops: len(h.Ops), Clients: len(clients), Passed: err == nil, Seconds: time.Since(start).Seconds()}
			if err != nil {
				failed = true
				v.Error = err.Error()
			}
			if err := out.Encode(v); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
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
