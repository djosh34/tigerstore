// Command benchreport turns the loadgen results written by bench/run.sh into
// Markdown tables with min, median and max of every per-run statistic.
//
//	benchreport <dir>
//
// <dir> holds one directory per benchmark, each with files named
// <target>-<max|rate>-<rep>.json.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// result is the part of a loadgen result the tables use.
type result struct {
	Valid       bool    `json:"valid"`
	OfferedRate float64 `json:"offered_rate"`
	Throughput  float64 `json:"throughput"`
	Latency     struct {
		Mean float64 `json:"mean"`
		P90  float64 `json:"p90"`
		Max  float64 `json:"max"`
		P50  float64 `json:"p50"`
		P99  float64 `json:"p99"`
		P999 float64 `json:"p999"`
	} `json:"latency_us"`
	Unsent           int64     `json:"unsent"`
	ServerCPUUsPerOp float64   `json:"server_cpu_us_per_op"`
	ServerCores      []float64 `json:"server_cores_by_node"`
	ClientCPUUsPerOp float64   `json:"client_cpu_us_per_op"`
	Watch            *struct {
		EventsPerS          float64 `json:"events_per_s"`
		ServerCPUUsPerEvent float64 `json:"server_cpu_us_per_event"`
		Latency             struct {
			Mean float64 `json:"mean"`
			P90  float64 `json:"p90"`
			P999 float64 `json:"p999"`
			Max  float64 `json:"max"`
			P50  float64 `json:"p50"`
			P99  float64 `json:"p99"`
		} `json:"latency_us"`
	} `json:"watch"`
}

// runs holds the repetitions of one benchmark, target and mode.
type runs []result

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: benchreport <dir>")
		os.Exit(2)
	}
	if err := report(os.Stdout, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "benchreport:", err)
		os.Exit(1)
	}
}

func report(w io.Writer, dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*", "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("no results in " + dir)
	}
	type key struct{ bench, target, mode string }
	all := map[key]runs{}
	var benches, targets []string
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".json")
		parts := strings.Split(name, "-")
		if len(parts) != 3 {
			return fmt.Errorf("%s: want <target>-<mode>-<rep>.json", f)
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var r result
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if !r.Valid {
			return fmt.Errorf("%s: invalid result; a comparison without it would be wrong", f)
		}
		k := key{filepath.Base(filepath.Dir(f)), parts[0], parts[1]}
		all[k] = append(all[k], r)
		if !slices.Contains(benches, k.bench) {
			benches = append(benches, k.bench)
		}
		if !slices.Contains(targets, k.target) {
			targets = append(targets, k.target)
		}
	}
	// Every target ran the same runs, or the comparison is unfair.
	for k, rs := range all {
		for _, t := range targets {
			if n := len(all[key{k.bench, t, k.mode}]); n != len(rs) {
				return fmt.Errorf("%s %s: %s has %d runs, %s has %d", k.bench, k.mode, k.target, len(rs), t, n)
			}
		}
	}
	slices.SortFunc(benches, benchOrder)
	// etcd first, as the baseline.
	slices.SortStableFunc(targets, func(a, b string) int {
		return boolCmp(b == "etcd", a == "etcd")
	})

	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	each := func(f func(bench, target, mode string, rs runs)) {
		for _, bench := range benches {
			for _, target := range targets {
				for _, mode := range []string{"max", "rate"} {
					rs := all[key{bench, target, mode}]
					if len(rs) > 0 {
						f(bench, target, mode, rs)
					}
				}
			}
		}
	}
	p("| Benchmark | Store | Mode | Ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/op | Generator CPU us/op | Runs |\n")
	p("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	each(func(bench, target, mode string, rs runs) {
		p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, target, mode,
			spread(rs, func(r result) float64 { return r.Throughput }),
			ms(rs, func(r result) float64 { return r.Latency.P50 }),
			ms(rs, func(r result) float64 { return r.Latency.P90 }),
			ms(rs, func(r result) float64 { return r.Latency.P99 }),
			ms(rs, func(r result) float64 { return r.Latency.P999 }),
			ms(rs, func(r result) float64 { return r.Latency.Mean }),
			ms(rs, func(r result) float64 { return r.Latency.Max }),
			num(rs, func(r result) float64 { return r.ServerCPUUsPerOp }, "%.1f"),
			num(rs, func(r result) float64 { return r.ClientCPUUsPerOp }, "%.1f"), len(rs))
	})
	p("\nStatistics are min / median / max over the repetitions, not pooled percentiles. Runs is the repetition count. At max every stream is busy. Open loop uses half of etcd's median max rate. Rate-only mixed-k8s first measures one max calibration per store; an explicit rate override is possible. Latency starts at the scheduled arrival time.\n")

	hasWatches := false
	each(func(_, _, _ string, rs runs) { hasWatches = hasWatches || rs[0].Watch != nil })
	if hasWatches {
		p("\n| Watch benchmark | Store | Mode | Events/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/event | Runs |\n")
		p("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		each(func(bench, target, mode string, rs runs) {
			if rs[0].Watch == nil {
				return
			}
			p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, target, mode,
				spread(rs, watchField(func(r result) float64 { return r.Watch.EventsPerS })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.P50 })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.P90 })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.P99 })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.P999 })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.Mean })),
				ms(rs, watchField(func(r result) float64 { return r.Watch.Latency.Max })),
				watchCPU(rs), len(rs))
		})
		p("\nEvent latency runs from the mutation's scheduled send time to the event's arrival at the watcher.\n")
	}

	p("\n| Benchmark | Store | Mode | Server cores per node | Offered ops/s | Unsent | Runs |\n")
	p("|---|---|---|---|---|---|---|\n")
	each(func(bench, target, mode string, rs runs) {
		p("| %s | %s | %s | %s | %s | %s | %d |\n", bench, target, mode, cores(rs),
			spread(rs, func(r result) float64 { return r.OfferedRate }),
			num(rs, func(r result) float64 { return float64(r.Unsent) }, "%.0f"), len(rs))
	})
	return nil
}

func watchCPU(rs runs) string {
	for _, r := range rs {
		if r.Watch == nil || r.Watch.ServerCPUUsPerEvent == 0 {
			return "-"
		}
	}
	return num(rs, watchField(func(r result) float64 { return r.Watch.ServerCPUUsPerEvent }), "%.1f")
}

func watchField(f func(result) float64) func(result) float64 {
	return func(r result) float64 {
		if r.Watch == nil {
			return 0
		}
		return f(r)
	}
}

// benchOrder sorts by workload in the order of the issue, then by size.
func benchOrder(a, b string) int {
	order := []string{"put", "txn", "delete", "range", "list", "watch", "mixed"}
	wa, sa, _ := strings.Cut(a, "-")
	wb, sb, _ := strings.Cut(b, "-")
	if c := slices.Index(order, wa) - slices.Index(order, wb); c != 0 {
		return c
	}
	return len(sa) - len(sb)
}

func boolCmp(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

func median(rs runs, f func(result) float64) (med, lo, hi float64) {
	v := make([]float64, len(rs))
	for i, r := range rs {
		v[i] = f(r)
	}
	slices.Sort(v)
	n := len(v)
	med = v[n/2]
	if n%2 == 0 {
		med = (v[n/2-1] + v[n/2]) / 2
	}
	return med, v[0], v[n-1]
}

func num(rs runs, f func(result) float64, format string) string {
	if len(rs) == 0 {
		return "-"
	}
	m, lo, hi := median(rs, f)
	return fmt.Sprintf(format, lo) + " / " + fmt.Sprintf(format, m) + " / " + fmt.Sprintf(format, hi)
}

func ms(rs runs, f func(result) float64) string {
	return num(rs, func(r result) float64 { return f(r) / 1000 }, "%.2f")
}

func spread(rs runs, f func(result) float64) string {
	if len(rs) == 0 {
		return "-"
	}
	m, lo, hi := median(rs, f)
	return fmt.Sprintf("%s / %s / %s", thousands(lo), thousands(m), thousands(hi))
}

func cores(rs runs) string {
	if len(rs) == 0 || len(rs[0].ServerCores) == 0 {
		return "-"
	}
	var s []string
	for i := range rs[0].ServerCores {
		s = append(s, num(rs, func(r result) float64 {
			if i < len(r.ServerCores) {
				return r.ServerCores[i]
			}
			return 0
		}, "%.2f"))
	}
	return strings.Join(s, "; ")
}

// thousands formats v rounded to an integer with comma separators.
func thousands(v float64) string {
	s := fmt.Sprintf("%.0f", v)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 && s[i-1] != '-' {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
