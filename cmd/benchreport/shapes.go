package main

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// seedRuns returns the runs of rs with seed, in the order they ran.
func seedRuns(rs runs, seed string) runs {
	var out runs
	for _, r := range rs {
		if r.Seed == seed {
			out = append(out, r)
		}
	}
	return out
}

// seedsOf returns the seeds of rs, in numeric order.
func seedsOf(rs runs) []string {
	var seeds []string
	for _, r := range rs {
		if !slices.Contains(seeds, r.Seed) {
			seeds = append(seeds, r.Seed)
		}
	}
	slices.SortFunc(seeds, func(a, b string) int {
		x, errA := strconv.ParseUint(a, 10, 64)
		y, errB := strconv.ParseUint(b, 10, 64)
		if errA != nil || errB != nil {
			return strings.Compare(a, b)
		}
		return cmp.Compare(x, y)
	})
	return seeds
}

// shapeOf names what a seed drew: the recorded cluster shape for k8s.
func shapeOf(rs runs) string {
	for _, r := range rs {
		if r.K8s != nil {
			return r.K8s.Shape
		}
	}
	return "-"
}

// spreadOf is (max - min) / median of the runs' throughput.
func spreadOf(rs runs) float64 {
	med, lo, hi := median(rs, func(r result) float64 { return r.Throughput })
	if med == 0 {
		return 0
	}
	return (hi - lo) / med
}

// shapes prints each seed's max runs, target by target: every run with
// the node that led it, their median, and their spread, which compares
// repeats of the same seed and shape only. The candidate is set against
// the baseline seed by seed: a difference is beyond the noise when it is
// larger than the spread of both.
func shapes(w io.Writer, rtts, benches, targets []string, all maxRuns) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("\n| Benchmark | Seed | Shape | RTT ms | Store | Max runs, ops/s (leader) | Median ops/s | Spread | Against baseline |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, bench := range benches {
		var everything runs
		for _, rtt := range rtts {
			for _, target := range targets {
				everything = append(everything, all(rtt, bench, target)...)
			}
		}
		for _, seed := range seedsOf(everything) {
			shape := shapeOf(seedRuns(everything, seed))
			for _, rtt := range rtts {
				for _, target := range targets {
					rs := seedRuns(all(rtt, bench, target), seed)
					if len(rs) == 0 {
						continue
					}
					var each []string
					for _, r := range rs {
						each = append(each, fmt.Sprintf("%s (%s)", thousands(r.Throughput), cmp.Or(r.Leader, "?")))
					}
					against := "-"
					if base := seedRuns(all(rtt, bench, "baseline"), seed); target == "candidate" && len(base) > 0 {
						got, was := medianOf(rs, throughput), medianOf(base, throughput)
						change := got/was - 1
						noise := max(spreadOf(rs), spreadOf(base))
						against = fmt.Sprintf("%+.1f%%", 100*change)
						if abs(change) <= noise {
							against += ", within the spread"
						}
					}
					p("| %s | %s | %s | %s | %s | %s | %s | %.1f%% | %s |\n", bench, seed, shape, rtt, target,
						strings.Join(each, ", "), thousands(medianOf(rs, throughput)), 100*spreadOf(rs), against)
				}
			}
		}
	}
	p("\nEach seed draws its workload, for k8s a recorded cluster shape and where each copy of it begins (bench/recordings), and runs it on every store. Spread is (max - min) / median over the repeats of that seed on one store, never across seeds or shapes. Against baseline is the candidate's median over the baseline's for the seed; a change no larger than the spread of either is noise.\n")
}

// nodeCosts prints what each seed's max runs cost the nodes, store by
// store: the leader's CPU and the bytes the leader and each follower sent
// to their peers, per operation of the window.
func nodeCosts(w io.Writer, rtts, benches, targets []string, all maxRuns) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("\n| Benchmark | Seed | RTT ms | Store | Leader CPU us/op | Leader peer B/op | Follower peer B/op | Server cores per node |\n|---|---|---|---|---|---|---|---|\n")
	for _, bench := range benches {
		var everything runs
		for _, rtt := range rtts {
			for _, target := range targets {
				everything = append(everything, all(rtt, bench, target)...)
			}
		}
		for _, seed := range seedsOf(everything) {
			for _, rtt := range rtts {
				for _, target := range targets {
					rs := seedRuns(all(rtt, bench, target), seed)
					if len(rs) == 0 {
						continue
					}
					p("| %s | %s | %s | %s | %s | %s | %s | %s |\n", bench, seed, rtt, target,
						num(rs, func(r result) float64 { return r.LeaderCPUUsPerOp }, "%.2f"),
						peerBytes(rs, leaderPeerBytes), peerBytes(rs, followerPeerBytes), cores(rs))
				}
			}
		}
	}
	p("\nValues are min / median / max over the seed's repeats. Follower peer B/op is the mean of the followers in each run. The leader is the node that led at the end of the window; a run without peer bytes (an older loadgen, or a leader change) shows -.\n")
}

// peerBytes is num over the runs that measured their peer bytes.
func peerBytes(rs runs, f func(result) float64) string {
	var measured runs
	for _, r := range rs {
		if len(r.PeerSentBytesPerOp) > 1 {
			measured = append(measured, r)
		}
	}
	if len(measured) == 0 {
		return "-"
	}
	return num(measured, f, "%.0f")
}

func leaderPeerBytes(r result) float64 { return r.PeerSentBytesPerOp[0] }

func followerPeerBytes(r result) float64 {
	var sum float64
	for _, b := range r.PeerSentBytesPerOp[1:] {
		sum += b
	}
	return sum / float64(len(r.PeerSentBytesPerOp)-1)
}

func throughput(r result) float64 { return r.Throughput }

func abs(x float64) float64 { return max(x, -x) }
