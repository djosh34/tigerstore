// Command benchreport turns the loadgen results written by bench/run.sh into
// Markdown tables: the median of the repetitions, with the range of
// throughput.
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
	OfferedRate float64 `json:"offered_rate"`
	Throughput  float64 `json:"throughput"`
	Latency     struct {
		P50  float64 `json:"p50"`
		P99  float64 `json:"p99"`
		P999 float64 `json:"p999"`
	} `json:"latency_us"`
	Unsent             int64     `json:"unsent"`
	ServerCPUUsPerOp   float64   `json:"server_cpu_us_per_op"`
	ServerCores        []float64 `json:"server_cores_by_node"`
	ClientCPUUsPerOp   float64   `json:"client_cpu_us_per_op"`
	Watch              *struct {
		EventsPerS float64 `json:"events_per_s"`
		Latency    struct {
			P50 float64 `json:"p50"`
			P99 float64 `json:"p99"`
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
		k := key{filepath.Base(filepath.Dir(f)), parts[0], parts[1]}
		all[k] = append(all[k], r)
		if !slices.Contains(benches, k.bench) {
			benches = append(benches, k.bench)
		}
		if !slices.Contains(targets, k.target) {
			targets = append(targets, k.target)
		}
	}
	slices.SortFunc(benches, benchOrder)
	// etcd first, as the baseline.
	slices.SortStableFunc(targets, func(a, b string) int {
		return boolCmp(b == "etcd", a == "etcd")
	})

	var watches []string
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("| Benchmark | Store | Max ops/s | p50 / p99 ms at max | Server CPU us/op at max | Open-loop ops/s | p50 / p99 / p99.9 ms open loop | Server CPU us/op open loop | Runs |\n")
	p("|---|---|---|---|---|---|---|---|---|\n")
	for _, bench := range benches {
		for _, target := range targets {
			mx, rt := all[key{bench, target, "max"}], all[key{bench, target, "rate"}]
			if len(mx) == 0 && len(rt) == 0 {
				continue
			}
			if strings.HasPrefix(bench, "watch") {
				watches = append(watches, bench)
			}
			p("| %s | %s | %s | %s / %s | %s | %s | %s / %s / %s | %s | %d + %d |\n",
				bench, target,
				spread(mx, func(r result) float64 { return r.Throughput }),
				ms(mx, func(r result) float64 { return r.Latency.P50 }),
				ms(mx, func(r result) float64 { return r.Latency.P99 }),
				num(mx, func(r result) float64 { return r.ServerCPUUsPerOp }, "%.1f"),
				spread(rt, func(r result) float64 { return r.Throughput }),
				ms(rt, func(r result) float64 { return r.Latency.P50 }),
				ms(rt, func(r result) float64 { return r.Latency.P99 }),
				ms(rt, func(r result) float64 { return r.Latency.P999 }),
				num(rt, func(r result) float64 { return r.ServerCPUUsPerOp }, "%.1f"),
				len(mx), len(rt))
		}
	}
	p("\nMedians of the runs; throughput shows the lowest and highest run in brackets. Open loop offers half of etcd's median max throughput, with latency counted from each request's scheduled send time.\n")

	if len(watches) > 0 {
		p("\n| Watch benchmark | Store | Events/s at max | Event p50 / p99 ms at max | Events/s open loop | Event p50 / p99 ms open loop |\n")
		p("|---|---|---|---|---|---|\n")
		for _, bench := range slices.Compact(watches) {
			for _, target := range targets {
				mx, rt := all[key{bench, target, "max"}], all[key{bench, target, "rate"}]
				if len(mx) == 0 && len(rt) == 0 {
					continue
				}
				p("| %s | %s | %s | %s / %s | %s | %s / %s |\n", bench, target,
					spread(mx, watchField(func(r result) float64 { return r.Watch.EventsPerS })),
					ms(mx, watchField(func(r result) float64 { return r.Watch.Latency.P50 })),
					ms(mx, watchField(func(r result) float64 { return r.Watch.Latency.P99 })),
					spread(rt, watchField(func(r result) float64 { return r.Watch.EventsPerS })),
					ms(rt, watchField(func(r result) float64 { return r.Watch.Latency.P50 })),
					ms(rt, watchField(func(r result) float64 { return r.Watch.Latency.P99 })))
			}
		}
		p("\nEvent latency runs from the put's scheduled send time to the event's arrival at the watcher.\n")
	}

	p("\n| Benchmark | Store | Server cores per node at max | Generator CPU us/op at max | Open-loop arrivals left unsent |\n")
	p("|---|---|---|---|---|\n")
	for _, bench := range benches {
		for _, target := range targets {
			mx, rt := all[key{bench, target, "max"}], all[key{bench, target, "rate"}]
			if len(mx) == 0 && len(rt) == 0 {
				continue
			}
			p("| %s | %s | %s | %s | %s |\n", bench, target, cores(mx),
				num(mx, func(r result) float64 { return r.ClientCPUUsPerOp }, "%.1f"),
				num(rt, func(r result) float64 { return float64(r.Unsent) }, "%.0f"))
		}
	}
	return nil
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
	order := []string{"put", "txn", "delete", "range", "watch"}
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
	m, _, _ := median(rs, f)
	return fmt.Sprintf(format, m)
}

func ms(rs runs, f func(result) float64) string {
	return num(rs, func(r result) float64 { return f(r) / 1000 }, "%.2f")
}

func spread(rs runs, f func(result) float64) string {
	if len(rs) == 0 {
		return "-"
	}
	m, lo, hi := median(rs, f)
	return fmt.Sprintf("%s (%s to %s)", thousands(m), thousands(lo), thousands(hi))
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
	return strings.Join(s, " / ")
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
