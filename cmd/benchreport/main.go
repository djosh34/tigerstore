// Command benchreport turns the loadgen results written by bench/run.sh into
// Markdown tables with min, median and max of every per-run statistic,
// seed by seed, and each store's share of its ceiling.
//
//	benchreport <dir> [<recordings>]
//
// <dir> holds one directory rtt-<ms> per round-trip time between nodes, each
// with one directory per benchmark, with files named
// <target>-<mode>-<seed>.<rep>.json, mode max, rate, or the layouts tmpfs
// and alone that only tigerstore targets run, and the run's placement in
// <target>-<mode>-<seed>.<rep>.placement, which names its leader. A run that
// failed has <target>-<mode>-<seed>.<rep>.failed with its cause, and its
// result, if any, is left out. A ceiling.json in <dir> (cmd/ceiling) adds
// the runner, the ceilings and each store's share of them; a
// generator-<benchmark>.json, loadgen's run against cmd/nullstore, adds the
// generator's own ceiling.
//
// The ceiling of a benchmark, for each seed and RTT, is the most
// operations a second the runner allows for the least work an operation
// must do (ceiling.go): the lowest of the disk, loopback, the least leader
// CPU from first principles (floor.go), and for k8s the recording's
// dependent calls, whose recordings are in <recordings>, bench/recordings
// by default.
//
// After the tables it exits 1 if a tigerstore run failed, or if the
// generator used more than 0.8 of its core in a tigerstore run, which then
// does not count. A failed etcd run is reported and is no error: etcd runs
// for comparison only.
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
// of times, and errRunFailed a tigerstore run that failed.
var (
	errInvalidResult = errors.New("invalid result")
	errUnequalRuns   = errors.New("unequal runs")
	errRunFailed     = errors.New("tigerstore run failed")
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
	Unsent               int64     `json:"unsent"`
	Conns                int       `json:"conns"`
	Endpoints            []string  `json:"endpoints"`
	StreamsPerConn       int       `json:"streams_per_conn"`
	LeaderCPUUsPerOp     float64   `json:"leader_cpu_us_per_op"`
	ServerCores          []float64 `json:"server_cores_by_node"`
	PeerSentBytesPerOp   []float64 `json:"peer_sent_bytes_per_op_by_node"`
	ClientCPUUsPerOp     float64   `json:"client_cpu_us_per_op"`
	ClientCores          float64   `json:"client_cores"`
	DiskBytesPerOp       float64   `json:"disk_bytes_per_op"`
	MinWrittenBytesPerOp float64   `json:"min_written_bytes_per_op"`
	RampClimbing         bool      `json:"ramp_climbing"`
	PerOp                perOp     `json:"per_op"`
	Keys                 int       `json:"keys"`
	ValueSize            int       `json:"value_size"`
	K8s                  *struct {
		Shape     string `json:"shape"`
		Copies    int    `json:"copies"`
		ItemBytes int64  `json:"item_bytes"`
	} `json:"k8s"`
	// Seed and Leader come from the run's file names: the seed it ran,
	// and the node that led it, n1 to n3.
	Seed, Leader string `json:"-"`
	Watch        *struct {
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

// perOp is what an operation carried on average (cmd/loadgen).
type perOp struct {
	OutBytes float64 `json:"out_bytes"`
	InBytes  float64 `json:"in_bytes"`
	KVs      float64 `json:"kvs"`
	PrevKVs  float64 `json:"prev_kvs"`
	Events   float64 `json:"event_kvs"`
	Writes   float64 `json:"writes"`
	Creates  float64 `json:"creates"`
	Deletes  float64 `json:"deletes"`
	Entries  float64 `json:"entries"`
	Checks   float64 `json:"checks"`
	Leases   float64 `json:"leases"`
}

// runs holds the repetitions of one benchmark, target and mode.
type runs []result

// key names the runs of one target in one mode of one benchmark at one RTT.
type key struct{ rtt, bench, target, mode string }

// failure is a failed run: its repetition and the cause bench/run.sh wrote.
type failure struct{ rep, cause string }

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: benchreport <dir> [<recordings>]")
		os.Exit(2)
	}
	traces := "bench/recordings"
	if len(os.Args) == 3 {
		traces = os.Args[2]
	}
	if err := report(os.Stdout, os.Args[1], traces); err != nil {
		fmt.Fprintln(os.Stderr, "benchreport:", err)
		os.Exit(1)
	}
}

func report(w io.Writer, dir, traces string) error {
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
	var problems []error
	var rtts, benches, targets []string
	rttMs := map[string]float64{}
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
		ms, err := strconv.ParseFloat(k.rtt, 64)
		if err != nil {
			return key{}, "", fmt.Errorf("%s: want a directory rtt-<ms>: %w", f, err)
		}
		rttMs[k.rtt] = ms
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
		k, rep, err := parse(f, ".json")
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
		if k.target != "etcd" && r.ClientCores > generatorCores {
			// The run does not count: it is a failed run, with the cause.
			cause := fmt.Sprintf("the generator used %.2f of its core, more than %.1f", r.ClientCores, generatorCores)
			failed[k] = append(failed[k], failure{rep, cause})
			problems = append(problems, fmt.Errorf("%w: %s at RTT %s ms, %s %s %s: %s", errGeneratorLimit, k.bench, k.rtt, k.target, k.mode, rep, cause))
			continue
		}
		r.Seed, _, _ = strings.Cut(rep, ".")
		if r.Leader, err = leader(strings.TrimSuffix(f, ".json") + ".placement"); err != nil {
			return err
		}
		all[k] = append(all[k], r)
	}
	// Every target that ran a benchmark at an RTT ran the same runs there,
	// failed or not, or the comparison is unfair. etcd runs no layouts, and
	// may run at the first RTT only.
	count := func(k key) int { return len(all[k]) + len(failed[k]) }
	ran := func(rtt, bench, target string) bool {
		for _, mode := range []string{"max", "rate"} {
			if count(key{rtt, bench, target, mode}) > 0 {
				return true
			}
		}
		return false
	}
	for _, k := range append(slices.Collect(maps.Keys(all)), slices.Collect(maps.Keys(failed))...) {
		for _, t := range targets {
			if t == "etcd" && slices.Contains(layouts, k.mode) || !ran(k.rtt, k.bench, t) {
				continue
			}
			other := key{k.rtt, k.bench, t, k.mode}
			if count(other) != count(k) {
				return fmt.Errorf("%w: RTT %s ms %s %s: %s has %d runs, %s has %d", errUnequalRuns, k.rtt, k.bench, k.mode, k.target, count(k), t, count(other))
			}
		}
	}
	slices.SortFunc(rtts, func(a, b string) int { return cmp.Compare(rttMs[a], rttMs[b]) })
	slices.SortFunc(benches, benchOrder)
	// etcd first, as the baseline.
	slices.SortStableFunc(targets, func(a, b string) int {
		return boolCmp(b == "etcd", a == "etcd")
	})

	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }

	p("| Benchmark | Store | RTT ms | Max ops/s | Max over etcd | Rate ops/s | p50 ms | p99 ms | Failed runs |\n")
	p("|---|---|---|---|---|---|---|---|---|\n")
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
					switch {
					case len(etcdMax) > 0:
						over = fmt.Sprintf("%.1fx", medianOf(maxRuns, throughput)/medianOf(etcdMax, throughput))
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
	p("\nMedians over every seed's runs that did not fail; the shares of the ceiling, seed by seed, are below. RTT is the round trip between nodes; clients reach the nodes without delay. Max over etcd is the store's median max throughput over etcd's at the same RTT, for comparison only: etcd runs at the first RTT. The rate is half of etcd's median max, the same for every store and RTT. Failed runs counts every mode and layout. A failed etcd run is reported and fails nothing; a failed tigerstore run fails the bench.\n")

	var causes []string
	for _, bench := range benches {
		for _, rtt := range rtts {
			b, err := os.ReadFile(filepath.Join(dir, "rtt-"+rtt, bench, "norate"))
			if err == nil {
				causes = append(causes, fmt.Sprintf("- RTT %s ms, %s: no rate runs, no shared rate: %s\n", rtt, bench, strings.TrimSpace(string(b))))
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			for _, target := range targets {
				for _, mode := range append([]string{"max", "rate"}, layouts...) {
					for _, f := range failed[key{rtt, bench, target, mode}] {
						causes = append(causes, fmt.Sprintf("- RTT %s ms, %s, %s %s %s: %s\n", rtt, bench, target, mode, f.rep, f.cause))
					}
				}
			}
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
	p("\n| Benchmark | RTT ms | Store | Mode | In flight | Ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Leader CPU us/op | Generator CPU us/op | Runs |\n")
	p("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	each(func(rtt, bench, target, mode string, rs runs) {
		p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", bench, rtt, target, mode,
			num(rs, func(r result) float64 { return float64(r.Conns * r.StreamsPerConn) }, "%.0f"),
			spread(rs, throughput),
			ms(rs, func(r result) float64 { return r.Latency.P50 }),
			ms(rs, func(r result) float64 { return r.Latency.P90 }),
			ms(rs, func(r result) float64 { return r.Latency.P99 }),
			ms(rs, func(r result) float64 { return r.Latency.P999 }),
			ms(rs, func(r result) float64 { return r.Latency.Mean }),
			ms(rs, func(r result) float64 { return r.Latency.Max }),
			num(rs, func(r result) float64 { return r.LeaderCPUUsPerOp }, "%.1f"),
			num(rs, func(r result) float64 { return r.ClientCPUUsPerOp }, "%.1f"), len(rs))
	})
	p("\nStatistics are min / median / max over the repetitions that did not fail, not pooled percentiles. Runs is that count. At max every stream is busy, and the requests in flight doubled until throughput climbed less than 5%% at two steps in a row; In flight is the limit on requests in flight at the step with the most throughput. txn-1k never has more in flight than its 10,000 keys. Open loop uses the shared rate. Latency starts at the scheduled arrival time. Leader CPU is the leader's process alone; the followers' cores are in the next table.\n")

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
	p("\nServer cores per node are leader first, then the two followers.\n")
	var climbing []string
	each(func(rtt, bench, target, mode string, rs runs) {
		n := 0
		for _, r := range rs {
			if r.RampClimbing {
				n++
			}
		}
		if n > 0 {
			climbing = append(climbing, fmt.Sprintf("- RTT %s ms, %s, %s %s: %d of %d runs\n", rtt, bench, target, mode, n, len(rs)))
		}
	})
	if len(climbing) > 0 {
		p("\nMax runs whose throughput still climbed at the last step of the ramp, so the requests in flight may have limited them:\n\n%s", strings.Join(climbing, ""))
	}
	maxOf := func(rtt, bench, target string) runs { return all[key{rtt, bench, target, "max"}] }
	shapes(w, rtts, benches, targets, maxOf)
	nodeCosts(w, rtts, benches, targets, maxOf)
	if err := ceilings(w, dir, traces, rtts, benches, targets, func(rtt, bench, target string) runs {
		return all[key{rtt, bench, target, "max"}]
	}); err != nil {
		return err
	}
	if err := generator(w, dir, benches, targets, func(rtt, bench, target string) runs {
		return all[key{rtt, bench, target, "max"}]
	}, rtts); err != nil {
		return err
	}
	return errors.Join(problems...)
}

// leader returns the node that led a run, n1 to n3, from the placement
// bench/cluster.sh wrote: node i's client port ends in i0. A run without a
// placement, as one whose cluster did not start, has none.
func leader(path string) (string, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var pl struct {
		Leader string `json:"leader"`
	}
	if err := json.Unmarshal(b, &pl); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	_, port, _ := strings.Cut(pl.Leader, ":")
	if len(port) < 2 {
		return "", fmt.Errorf("%s: leader %q has no port", path, pl.Leader)
	}
	return "n" + port[len(port)-2:len(port)-1], nil
}

// layouts are the max runs of bench/run.sh's LAYOUTS: tmpfs has all three
// nodes on tmpfs, alone only the followers.
var layouts = []string{"tmpfs", "alone"}

// generator prints loadgen's own ceiling, from its run against
// cmd/nullstore, next to the most of its core it used at a tigerstore
// target's max.
func generator(w io.Writer, dir string, benches, targets []string, all maxRuns, rtts []string) error {
	header := false
	for _, bench := range benches {
		b, err := os.ReadFile(filepath.Join(dir, "generator-"+bench+".json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var g result
		if err := json.Unmarshal(b, &g); err != nil {
			return fmt.Errorf("generator-%s.json: %w", bench, err)
		}
		if !g.Valid || g.ClientCPUUsPerOp <= 0 {
			return fmt.Errorf("generator-%s.json: %w", bench, errInvalidResult)
		}
		most, at := 0.0, "-"
		for _, target := range targets {
			if target == "etcd" {
				continue
			}
			for _, rtt := range rtts {
				rs := all(rtt, bench, target)
				if len(rs) == 0 {
					continue
				}
				if c := medianOf(rs, func(r result) float64 { return r.ClientCores }); c > most {
					most, at = c, fmt.Sprintf("%s at %s ms", target, rtt)
				}
			}
		}
		if !header {
			_, _ = fmt.Fprintf(w, "\n| Benchmark | Ops/s against nullstore | Generator cores | Generator CPU us/op | Ops/s on its one core | Most generator cores at tigerstore's max |\n|---|---|---|---|---|---|\n")
			header = true
		}
		_, _ = fmt.Fprintf(w, "| %s | %s | %.2f | %.2f | %s | %.2f (%s) |\n", bench, thousands(g.Throughput), g.ClientCores,
			g.ClientCPUUsPerOp, thousands(1e6/g.ClientCPUUsPerOp), most, at)
	}
	if header {
		_, _ = fmt.Fprintf(w, "\nThe generator's ceiling: loadgen on its one core against cmd/nullstore, which answers every call at once from the leader's and followers' cores. Ops/s on its one core is what it would do with its whole core, 1 s over its CPU per op. The last column is the median share of its core it used at a tigerstore build's max, at the RTT where that was highest; a tigerstore run in which it used more than 0.8 does not count.\n")
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
	order := []string{"put", "txn", "delete", "range", "list", "watch", "k8s"}
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
