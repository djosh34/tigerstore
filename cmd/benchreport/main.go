// Command benchreport turns the loadgen results written by bench/run.sh into
// Markdown tables with min, median and max of every per-run statistic, and
// checks the bars of the latency bench.
//
//	benchreport <dir>
//
// <dir> holds one directory rtt-<ms> per round-trip time between nodes, each
// with one directory per benchmark, with files named
// <target>-<mode>-<rep>.json, mode max, rate, or the layouts tmpfs and alone
// that only tigerstore targets run. A run that failed has
// <target>-<mode>-<rep>.failed with its cause, and its result, if any, is
// left out. A diskbench.jsonl next to them adds the table of ceilings.
//
// After the tables it exits 1 if a tigerstore run failed or a tigerstore
// target missed its bar at an RTT above 0. A failed etcd run is reported
// and is no error: where etcd has no max result, tigerstore's stands alone.
package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// errInvalidResult is a result that loadgen marked invalid in a run that
// did not fail, errUnequalRuns targets that ran a bench a different number
// of times, errRunFailed a tigerstore run that failed and errBarMissed a
// tigerstore target below its bar.
var (
	errInvalidResult = errors.New("invalid result")
	errUnequalRuns   = errors.New("unequal runs")
	errRunFailed     = errors.New("tigerstore run failed")
	errBarMissed     = errors.New("bar missed")
)

// bars are the least median max throughput of every tigerstore target over
// etcd's, at every RTT above 0 (#404).
var bars = map[string]float64{"put-1k": 10, "txn-1k": 10, "mixed-k8s": 2.5}

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
	DiskBytesPerOp   float64   `json:"disk_bytes_per_op"`
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

// key names the runs of one target in one mode of one benchmark at one RTT.
type key struct{ rtt, bench, target, mode string }

// failure is a failed run: its repetition and the cause bench/run.sh wrote.
type failure struct{ rep, cause string }

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
	files, err := filepath.Glob(filepath.Join(dir, "rtt-*", "*", "*.json"))
	if err != nil {
		return err
	}
	failedFiles, err := filepath.Glob(filepath.Join(dir, "rtt-*", "*", "*.failed"))
	if err != nil {
		return err
	}
	if len(files)+len(failedFiles) == 0 {
		return errors.New("no results in " + dir)
	}
	all := map[key]runs{}
	failed := map[key][]failure{}
	var rtts, benches, targets []string
	add := func(l *[]string, v string) {
		if !slices.Contains(*l, v) {
			*l = append(*l, v)
		}
	}
	parse := func(f, ext string) (key, string, error) {
		parts := strings.Split(strings.TrimSuffix(filepath.Base(f), ext), "-")
		if len(parts) != 3 {
			return key{}, "", fmt.Errorf("%s: want <target>-<mode>-<rep>%s", f, ext)
		}
		benchDir := filepath.Dir(f)
		k := key{strings.TrimPrefix(filepath.Base(filepath.Dir(benchDir)), "rtt-"), filepath.Base(benchDir), parts[0], parts[1]}
		if _, err := strconv.ParseFloat(k.rtt, 64); err != nil {
			return key{}, "", fmt.Errorf("%s: want a directory rtt-<ms>", f)
		}
		add(&rtts, k.rtt)
		add(&benches, k.bench)
		add(&targets, k.target)
		return k, parts[2], nil
	}
	for _, f := range failedFiles {
		k, rep, err := parse(f, ".failed")
		if err != nil {
			return err
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		failed[k] = append(failed[k], failure{rep, strings.TrimSpace(string(b))})
	}
	for _, f := range files {
		k, _, err := parse(f, ".json")
		if err != nil {
			return err
		}
		if _, err := os.Stat(strings.TrimSuffix(f, ".json") + ".failed"); err == nil {
			continue
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
			return fmt.Errorf("%s: %w in a run that did not fail", f, errInvalidResult)
		}
		all[k] = append(all[k], r)
	}
	// Every target ran the same runs, failed or not, or the comparison is
	// unfair. etcd runs no layouts.
	count := func(k key) int { return len(all[k]) + len(failed[k]) }
	for _, k := range append(slices.Collect(maps.Keys(all)), slices.Collect(maps.Keys(failed))...) {
		for _, t := range targets {
			if t == "etcd" && slices.Contains(layouts, k.mode) {
				continue
			}
			other := key{k.rtt, k.bench, t, k.mode}
			if count(other) != count(k) {
				return fmt.Errorf("%w: RTT %s ms %s %s: %s has %d runs, %s has %d", errUnequalRuns, k.rtt, k.bench, k.mode, k.target, count(k), t, count(other))
			}
		}
	}
	slices.SortFunc(rtts, func(a, b string) int {
		x, _ := strconv.ParseFloat(a, 64)
		y, _ := strconv.ParseFloat(b, 64)
		return cmp.Compare(x, y)
	})
	slices.SortFunc(benches, benchOrder)
	// etcd first, as the baseline.
	slices.SortStableFunc(targets, func(a, b string) int {
		return boolCmp(b == "etcd", a == "etcd")
	})

	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	throughput := func(r result) float64 { return r.Throughput }

	p("| Benchmark | Store | RTT ms | Max ops/s | Max over etcd | Rate ops/s | p50 ms | p99 ms | Failed runs |\n")
	p("|---|---|---|---|---|---|---|---|---|\n")
	var problems []error
	for _, bench := range benches {
		for _, target := range targets {
			for _, rtt := range rtts {
				maxRuns, rateRuns := all[key{rtt, bench, target, "max"}], all[key{rtt, bench, target, "rate"}]
				var nRuns, nFailed int
				for _, mode := range append([]string{"max", "rate"}, layouts...) {
					k := key{rtt, bench, target, mode}
					nRuns += count(k)
					nFailed += len(failed[k])
				}
				if nRuns == 0 {
					continue
				}
				over := "-"
				etcdMax := all[key{rtt, bench, "etcd", "max"}]
				if target != "etcd" && len(maxRuns) > 0 {
					got := medianOf(maxRuns, throughput)
					bar, hasBar := bars[bench]
					hasBar = hasBar && rtt != "0"
					switch {
					case len(etcdMax) > 0:
						ratio := got / medianOf(etcdMax, throughput)
						over = fmt.Sprintf("%.1fx", ratio)
						if hasBar && ratio < bar {
							over += fmt.Sprintf(", below %gx", bar)
							problems = append(problems, fmt.Errorf("%w: %s at RTT %s ms, %s: %.1fx etcd's max throughput, the bar is %gx", errBarMissed, bench, rtt, target, ratio, bar))
						}
					case len(failed[key{rtt, bench, "etcd", "max"}]) > 0:
						over = "etcd failed"
					}
				}
				if target != "etcd" && nFailed > 0 {
					problems = append(problems, fmt.Errorf("%w: %s at RTT %s ms, %s: %d of %d runs", errRunFailed, bench, rtt, target, nFailed, nRuns))
				}
				p("| %s | %s | %s | %s | %s | %s | %s | %s | %d of %d |\n", bench, target, rtt,
					medianText(maxRuns, throughput, thousands), over,
					medianText(rateRuns, func(r result) float64 { return r.OfferedRate }, thousands),
					medianText(rateRuns, func(r result) float64 { return r.Latency.P50 / 1000 }, twoDecimals),
					medianText(rateRuns, func(r result) float64 { return r.Latency.P99 / 1000 }, twoDecimals),
					nFailed, nRuns)
			}
		}
	}
	p("\nMedians over the runs that did not fail. RTT is the round trip between nodes; clients reach the nodes without delay. Max over etcd is the store's median max throughput over etcd's at the same RTT. The rate is half of etcd's median max at that RTT, the same for every store, or the first RTT's rate where etcd has no max result. Failed runs counts every mode and layout.\n")
	p("\nBars at every RTT above 0: put-1k and txn-1k at least 10x etcd's max throughput, mixed-k8s at least 2.5x. A failed etcd run is reported and fails nothing; a failed tigerstore run or a missed bar fails the bench.\n")

	var causes []string
	for _, k := range sortedKeys(failed, rtts, benches, targets) {
		for _, f := range failed[k] {
			causes = append(causes, fmt.Sprintf("- RTT %s ms, %s, %s %s %s: %s\n", k.rtt, k.bench, k.target, k.mode, f.rep, f.cause))
		}
	}
	if len(causes) > 0 {
		p("\nFailed runs (node logs are in the artifact, <target>-<mode>-<rep>-n<node>.log):\n\n%s", strings.Join(causes, ""))
	}

	each := func(f func(rtt, bench, target, mode string, rs runs)) {
		for _, bench := range benches {
			for _, rtt := range rtts {
				for _, target := range targets {
					for _, mode := range append([]string{"max", "rate"}, layouts...) {
						rs := all[key{rtt, bench, target, mode}]
						if len(rs) > 0 {
							f(rtt, bench, target, mode, rs)
						}
					}
				}
			}
		}
	}
	p("\n| Benchmark | RTT ms | Store | Mode | Ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/op | Generator CPU us/op | Runs |\n")
	p("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	each(func(rtt, bench, target, mode string, rs runs) {
		p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, rtt, target, mode,
			spread(rs, throughput),
			ms(rs, func(r result) float64 { return r.Latency.P50 }),
			ms(rs, func(r result) float64 { return r.Latency.P90 }),
			ms(rs, func(r result) float64 { return r.Latency.P99 }),
			ms(rs, func(r result) float64 { return r.Latency.P999 }),
			ms(rs, func(r result) float64 { return r.Latency.Mean }),
			ms(rs, func(r result) float64 { return r.Latency.Max }),
			num(rs, func(r result) float64 { return r.ServerCPUUsPerOp }, "%.1f"),
			num(rs, func(r result) float64 { return r.ClientCPUUsPerOp }, "%.1f"), len(rs))
	})
	p("\nStatistics are min / median / max over the repetitions that did not fail, not pooled percentiles. Runs is that count. At max every stream is busy. Open loop uses the shared rate. Latency starts at the scheduled arrival time.\n")

	hasWatches := false
	each(func(_, _, _, _ string, rs runs) { hasWatches = hasWatches || rs[0].Watch != nil })
	if hasWatches {
		p("\n| Watch benchmark | RTT ms | Store | Mode | Events/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/event | Runs |\n")
		p("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		each(func(rtt, bench, target, mode string, rs runs) {
			if rs[0].Watch == nil {
				return
			}
			p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, rtt, target, mode,
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

	p("\n| Benchmark | RTT ms | Store | Mode | Server cores per node | Offered ops/s | Unsent | Runs |\n")
	p("|---|---|---|---|---|---|---|---|\n")
	each(func(rtt, bench, target, mode string, rs runs) {
		p("| %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, rtt, target, mode, cores(rs),
			spread(rs, func(r result) float64 { return r.OfferedRate }),
			num(rs, func(r result) float64 { return float64(r.Unsent) }, "%.0f"), len(rs))
	})
	if err := ceilings(w, dir, rtts, benches, targets, func(rtt, bench, target, mode string) runs {
		return all[key{rtt, bench, target, mode}]
	}); err != nil {
		return err
	}
	return errors.Join(problems...)
}

// sortedKeys returns the keys of m in the order of the tables.
func sortedKeys[V any](m map[key]V, rtts, benches, targets []string) []key {
	var ks []key
	for _, bench := range benches {
		for _, rtt := range rtts {
			for _, target := range targets {
				for _, mode := range append([]string{"max", "rate"}, layouts...) {
					k := key{rtt, bench, target, mode}
					if _, ok := m[k]; ok {
						ks = append(ks, k)
					}
				}
			}
		}
	}
	return ks
}

// layouts are the max runs of bench/run.sh's LAYOUTS: tmpfs has all three
// nodes on tmpfs, alone only the followers.
var layouts = []string{"tmpfs", "alone"}

// ceilings prints, for each benchmark and RTT with a diskbench result, how
// close each tigerstore target's max runs come to the lower of its disk
// ceiling (the device's MiB/s over the device bytes per operation measured
// in the run) and its CPU reference (its median on tmpfs).
func ceilings(w io.Writer, dir string, rtts, benches, targets []string, all func(rtt, bench, target, mode string) runs) error {
	for _, bench := range benches {
		for _, rtt := range rtts {
			header := false
			b, err := os.ReadFile(filepath.Join(dir, "rtt-"+rtt, bench, "diskbench.jsonl"))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			var disk struct {
				Max float64 `json:"max_mib_per_s"`
			}
			for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
				if err := json.Unmarshal([]byte(line), &disk); err != nil {
					return fmt.Errorf("%s diskbench: %w", bench, err)
				}
			}
			if disk.Max <= 0 {
				return fmt.Errorf("%s diskbench: no max_mib_per_s", bench)
			}
			for _, target := range targets {
				tmpfs := all(rtt, bench, target, "tmpfs")
				if len(tmpfs) == 0 {
					continue
				}
				cpu, _, _ := median(tmpfs, func(r result) float64 { return r.Throughput })
				for _, mode := range []string{"max", "alone"} {
					rs := all(rtt, bench, target, mode)
					if len(rs) == 0 {
						continue
					}
					got, _, _ := median(rs, func(r result) float64 { return r.Throughput })
					perOp, _, _ := median(rs, func(r result) float64 { return r.DiskBytesPerOp })
					if perOp <= 0 {
						return fmt.Errorf("%s %s %s: no disk bytes per operation", bench, target, mode)
					}
					ceiling := disk.Max * (1 << 20) / perOp
					if !header {
						_, _ = fmt.Fprintf(w, "\n| Benchmark | Store | Layout | Ops/s | Disk bytes/op | Disk ceiling | CPU reference | Share of the lower |\n|---|---|---|---|---|---|---|---|\n")
						header = true
					}
					layout := "shared disk"
					if mode == "alone" {
						layout = "leader alone"
					}
					_, _ = fmt.Fprintf(w, "| %s | %s | %s | %s | %.0f | %s | %s | %.1f%% |\n", bench, target, layout,
						thousands(got), perOp, thousands(ceiling), thousands(cpu), 100*got/min(ceiling, cpu))
				}
			}
			_, _ = fmt.Fprintf(w, "\n%s at RTT %s ms: the disk takes %.1f MiB/s at most (diskbench: O_DIRECT and fdatasync on written space). Medians over the runs. The disk ceiling is that rate over the median device bytes per operation in the same runs; the CPU reference is the median with all three nodes on tmpfs.\n", bench, rtt, disk.Max)
		}
	}
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

func medianOf(rs runs, f func(result) float64) float64 {
	m, _, _ := median(rs, f)
	return m
}

// medianText formats the median of f over rs, or a dash without runs.
func medianText(rs runs, f func(result) float64, format func(float64) string) string {
	if len(rs) == 0 {
		return "-"
	}
	return format(medianOf(rs, f))
}

func twoDecimals(v float64) string { return fmt.Sprintf("%.2f", v) }

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
