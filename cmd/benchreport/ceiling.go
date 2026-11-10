package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// runner is what cmd/ceiling measured on the bench's runner.
type runner struct {
	Kernel     string     `json:"kernel"`
	CPU        string     `json:"cpu"`
	Disk       string     `json:"disk"`
	DiskMiBps  float64    `json:"disk_mib_per_s"`
	Primitives primitives `json:"primitives"`
}

// nodes is the cluster's size: the nodes that write every durable byte to
// the one shared disk.
const nodes = 3

// maxRuns returns the max runs of a target at an RTT in a benchmark.
type maxRuns func(rtt, bench, target string) runs

// targetShare is the share of its ceiling a workload must reach (#498).
const targetShare = 0.8

// halfCore is the most of its one core the generator may use at a
// workload's target rate (#503).
const halfCore = 0.5

// errGeneratorLimit is a generator that would need more than half its core
// to drive a workload at its target rate.
var errGeneratorLimit = errors.New("the generator would be the limit")

// limits are the four limits of one benchmark, seed and RTT, in ops/s; a
// limit that does not apply is +Inf.
type limits struct {
	disk, network, cpu, chain float64
	cpuNs                     []part
}

func (l limits) ceiling() (float64, string) {
	c, by := l.disk, "disk"
	for _, x := range []struct {
		v    float64
		name string
	}{{l.network, "network"}, {l.cpu, "leader CPU"}, {l.chain, "dependent calls"}} {
		if x.v < c {
			c, by = x.v, x.name
		}
	}
	return c, by
}

// ceilings prints, for each benchmark, seed and RTT, the ceiling from
// first principles and each store's share of it, if dir has the runner's
// measurements (ceiling.json). The ceiling is the lowest of four limits:
// the shared disk's durable bandwidth over the bytes an operation must
// make durable on three nodes; loopback's bandwidth over the bytes it
// sends; one core over the least leader CPU it needs (floor); and, for
// k8s, the most calls a second the recording's dependent calls allow at
// the RTT. It returns a problem for a workload whose target rate would
// take the generator more than half its core.
func ceilings(w io.Writer, dir, traces string, rtts, benches, targets []string, all maxRuns) ([]error, error) {
	b, err := os.ReadFile(filepath.Join(dir, "ceiling.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rn runner
	if err := json.Unmarshal(b, &rn); err != nil {
		return nil, fmt.Errorf("ceiling.json: %w", err)
	}
	if rn.DiskMiBps <= 0 || rn.Primitives.LoopbackBps <= 0 {
		return nil, errors.New("ceiling.json: no disk or loopback bandwidth")
	}
	pr := rn.Primitives
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("\nThe runner: kernel %s, CPU %s. The three nodes keep their data on one shared disk, %s, which makes at most %.1f MiB/s durable. Loopback carries %.2f GB/s on three connections. A durable 4 KiB write takes %.0f µs.\n",
		rn.Kernel, rn.CPU, rn.Disk, rn.DiskMiBps, pr.LoopbackBps/1e9, pr.SyncLatencyNs/1e3)
	p("\nPrimitives, each timed alone on one core: TLS seal %.0f ns a record + %.3f ns/B, open %.0f + %.3f; TCP send %.0f ns a call + %.3f ns/B, receive %.0f + %.3f; CRC32C %.3f ns/B; copy %.3f ns/B; O_DIRECT write %.0f ns a call + %.3f ns/B; fdatasync %.0f ns; at a million keys a hash lookup %.0f ns, a B-tree insert %.0f ns and delete %.0f ns; a cold read %.0f ns; a request parsed %.0f ns a call + %.3f ns/B, a call's HTTP/2 frames %.0f ns, a key-value's metadata %.0f ns.\n",
		pr.Seal.CallNs, pr.Seal.ByteNs, pr.Open.CallNs, pr.Open.ByteNs, pr.Send.CallNs, pr.Send.ByteNs, pr.Recv.CallNs, pr.Recv.ByteNs,
		pr.CRC.ByteNs, pr.Copy.ByteNs, pr.Write.CallNs, pr.Write.ByteNs, pr.Sync.CallNs, pr.LookupNs, pr.InsertNs, pr.DeleteNs, pr.MissNs,
		pr.Parse.CallNs, pr.Parse.ByteNs, pr.FrameNs, pr.KVNs)
	p("\n| Benchmark | Seed | Shape | RTT ms | Durable B/op per node | Disk ops/s | Network ops/s | Leader CPU µs/op | CPU ops/s | Dependent calls ops/s | Ceiling ops/s | Limited by |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	type cell struct{ bench, seed, rtt string }
	ceil := map[cell]float64{}
	type floorRow struct {
		bench, seed string
		parts       []part
	}
	var floors []floorRow
	for _, bench := range benches {
		var everything runs
		for _, rtt := range rtts {
			for _, target := range targets {
				everything = append(everything, all(rtt, bench, target)...)
			}
		}
		for _, seed := range seedsOf(everything) {
			rs := seedRuns(everything, seed)
			l := loadOf(rs)
			floors = append(floors, floorRow{bench, seed, floor(pr, l)})
			var chains map[string]float64
			if rs[0].K8s != nil && traces != "" {
				if chains, err = chainLimits(traces, seed, rs[0].K8s.Copies, rtts, pr.SyncLatencyNs); err != nil {
					return nil, err
				}
			}
			for _, rtt := range rtts {
				lim := limits{disk: math.Inf(1), network: math.Inf(1), cpu: math.Inf(1), chain: math.Inf(1)}
				if l.Durable > 0 {
					lim.disk = rn.DiskMiBps * (1 << 20) / (nodes * l.Durable)
				}
				if lb := loopbackBytes(l); lb > 0 {
					lim.network = pr.LoopbackBps / lb
				}
				lim.cpuNs = floor(pr, l)
				lim.cpu = 1e9 / sum(lim.cpuNs)
				if c, ok := chains[rtt]; ok {
					lim.chain = c
				}
				c, by := lim.ceiling()
				k := cell{bench, seed, rtt}
				ceil[k] = c
				p("| %s | %s | %s | %s | %.0f | %s | %s | %.2f | %s | %s | %s | %s |\n", bench, seed, shapeOf(rs), rtt, l.Durable,
					limit(lim.disk), limit(lim.network), sum(lim.cpuNs)/1e3, limit(lim.cpu), limit(lim.chain), thousands(c), by)
			}
		}
	}
	p("\nThe ceiling is the lowest limit. Disk: the shared disk's bandwidth over three times the bytes an operation must make durable, its keys and values and each compare's key and revision. Network: loopback's bandwidth over what an operation sends, all nodes together. Leader CPU: one core over the least CPU the leader needs, the primitives above times what the operation carries (cmd/loadgen's per-operation counts, medians over the seed's max runs of every store); the parts are below. Dependent calls: each copy of the recording makes its calls from where it starts in no less than the longest chain of calls that wait on replies, a write taking the RTT and a durable write, a linearizable read the RTT (k8s only). The target is %.0f%% of the ceiling.\n", 100*targetShare)
	p("\n| Benchmark | Seed | Least leader CPU, µs/op by part |\n|---|---|---|\n")
	for _, f := range floors {
		var s []string
		for _, x := range f.parts {
			s = append(s, fmt.Sprintf("%s %.3f", x.Name, x.Ns/1e3))
		}
		p("| %s | %s | %s |\n", f.bench, f.seed, strings.Join(s, "; "))
	}
	var problems []error
	p("\n| Benchmark | Seed | RTT ms | Store | Median max ops/s | Share of the ceiling | Target ops/s | Generator cores at the target |\n|---|---|---|---|---|---|---|---|\n")
	for _, bench := range benches {
		for _, rtt := range rtts {
			for _, target := range targets {
				for _, seed := range seedsOf(all(rtt, bench, target)) {
					rs := seedRuns(all(rtt, bench, target), seed)
					c := ceil[cell{bench, seed, rtt}]
					if c == 0 || math.IsInf(c, 1) {
						continue
					}
					got := medianOf(rs, throughput)
					goal := targetShare * c
					cores := medianOf(rs, func(r result) float64 { return r.ClientCPUUsPerOp }) * goal / 1e6
					p("| %s | %s | %s | %s | %s | %.1f%% | %s | %.2f |\n", bench, seed, rtt, target, thousands(got), 100*got/c, thousands(goal), cores)
					if target != "etcd" && cores > halfCore {
						problems = append(problems, fmt.Errorf("%w: %s seed %s at RTT %s ms, %s: %.2f cores at %s ops/s", errGeneratorLimit, bench, seed, rtt, target, cores, thousands(goal)))
					}
				}
			}
		}
	}
	p("\nShare is the median of the seed's max runs over its ceiling at that RTT. Generator cores at the target is loadgen's CPU per operation in those runs times the target rate; above %.1f the generator would be the limit, and the bench fails.\n", halfCore)
	return problems, nil
}

// loadOf is what the operations of rs carried: medians over the runs.
func loadOf(rs runs) load {
	m := func(f func(r result) float64) float64 { return medianOf(rs, f) }
	l := load{
		PerOp: perOp{
			OutBytes: m(func(r result) float64 { return r.PerOp.OutBytes }),
			InBytes:  m(func(r result) float64 { return r.PerOp.InBytes }),
			KVs:      m(func(r result) float64 { return r.PerOp.KVs }),
			Writes:   m(func(r result) float64 { return r.PerOp.Writes }),
			Creates:  m(func(r result) float64 { return r.PerOp.Creates }),
			Deletes:  m(func(r result) float64 { return r.PerOp.Deletes }),
		},
		Durable:   m(func(r result) float64 { return r.MinWrittenBytesPerOp }),
		Endpoints: m(func(r result) float64 { return float64(min(max(len(r.Endpoints), 1), nodes)) }),
	}
	l.Conns = m(func(r result) float64 { return float64(max(r.Conns, 1)) }) / l.Endpoints
	return l
}

// chainLimits returns, by RTT, the most calls a second the dependent calls
// of a k8s seed's draw allow.
func chainLimits(traces, seed string, copies int, rtts []string, syncNs float64) (map[string]float64, error) {
	n, err := strconv.ParseUint(seed, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("k8s seed %q: %w", seed, err)
	}
	d, err := k8strace.DrawSeed(traces, n, copies)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, rtt := range rtts {
		ms, err := strconv.ParseFloat(rtt, 64)
		if err != nil {
			return nil, err
		}
		trip := time.Duration(ms * float64(time.Millisecond))
		out[rtt] = d.Bound(k8strace.Least{Write: trip + time.Duration(syncNs), Read: trip})
	}
	return out, nil
}

func limit(v float64) string {
	if math.IsInf(v, 1) {
		return "-"
	}
	return thousands(v)
}
