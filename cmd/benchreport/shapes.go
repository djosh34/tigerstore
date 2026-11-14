package main

import (
	"cmp"
	"fmt"
	"io"
	"math"
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
						switch {
						case len(rs) == 1 || len(base) == 1:
							// A store without repeats has no spread: the
							// paired table judges.
							against += ", one run on a store: judged by shape below"
						case abs(change) <= noise:
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
// store: the leader's CPU, and the bytes the leader and each follower sent
// to their peers, per operation of the window.
func nodeCosts(w io.Writer, rtts, benches, targets []string, all maxRuns) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("\n| Benchmark | Seed | RTT ms | Store | Leader CPU us/op | Peer B/op per node | Server cores per node |\n|---|---|---|---|---|---|---|\n")
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
					p("| %s | %s | %s | %s | %s | %s | %s |\n", bench, seed, rtt, target,
						num(rs, func(r result) float64 { return r.LeaderCPUUsPerOp }, "%.2f"), peerBytes(rs), cores(rs))
				}
			}
		}
	}
	p("\nValues are min / median / max over the seed's repeats. Peer B/op per node is the leader at the end of the window first, then the followers in the order of their endpoints, over the runs that measured them: their count follows in brackets when some did not (an older loadgen, a /metrics that could not be read, or no single leader at the end). Server cores per node are leader first, then the followers, over every run.\n")
}

// peerBytes is each node's peer bytes per operation, leader first, over
// the runs that measured them.
func peerBytes(rs runs) string {
	var measured runs
	for _, r := range rs {
		if len(r.PeerSentBytesPerOp) > 0 {
			measured = append(measured, r)
		}
	}
	if len(measured) == 0 {
		return "-"
	}
	var s []string
	for i := range measured[0].PeerSentBytesPerOp {
		s = append(s, num(measured, func(r result) float64 {
			if i < len(r.PeerSentBytesPerOp) {
				return r.PeerSentBytesPerOp[i]
			}
			return 0
		}, "%.0f"))
	}
	out := strings.Join(s, "; ")
	if len(measured) < len(rs) {
		out += fmt.Sprintf(" (%d of %d runs)", len(measured), len(rs))
	}
	return out
}

func throughput(r result) float64 { return r.Throughput }

func abs(x float64) float64 { return max(x, -x) }

// pairCell is one shape of a benchmark at one RTT.
type pairCell struct{ bench, shape, rtt string }

func (c pairCell) String() string { return c.bench + "/" + c.shape + "/" + c.rtt }

// pairs returns, per shape of each benchmark and RTT, the paired ratio of
// every seed that ran on both stores: the candidate's median max
// throughput over the baseline's. In bench.yml's quick grid a seed's two
// stores ran back to back on one runner, so the ratio leaves out how the
// seeds and the runners differ. The cells come in order.
func pairs(rtts, benches []string, all maxRuns) ([]pairCell, map[pairCell][]float64) {
	var cells []pairCell
	ratios := map[pairCell][]float64{}
	for _, bench := range benches {
		var everything runs
		for _, rtt := range rtts {
			everything = append(everything, all(rtt, bench, "baseline")...)
			everything = append(everything, all(rtt, bench, "candidate")...)
		}
		var shapeList []string
		seedsOfShape := map[string][]string{}
		for _, seed := range seedsOf(everything) {
			shape := shapeOf(seedRuns(everything, seed))
			if _, ok := seedsOfShape[shape]; !ok {
				shapeList = append(shapeList, shape)
			}
			seedsOfShape[shape] = append(seedsOfShape[shape], seed)
		}
		for _, shape := range shapeList {
			for _, rtt := range rtts {
				c := pairCell{bench, shape, rtt}
				for _, seed := range seedsOfShape[shape] {
					base, cand := seedRuns(all(rtt, bench, "baseline"), seed), seedRuns(all(rtt, bench, "candidate"), seed)
					if len(base) == 0 || len(cand) == 0 {
						continue
					}
					ratios[c] = append(ratios[c], medianOf(cand, throughput)/medianOf(base, throughput))
				}
				if len(ratios[c]) > 0 {
					cells = append(cells, c)
				}
			}
		}
	}
	return cells, ratios
}

// noise is the A/A noise of the paired ratio, per shape and RTT (pairCell's
// String): the largest |ratio - 1| one pair showed in a run of a ref
// against itself (bench/noise.json).
type noise map[string]float64

// pairedNoise is what a run's paired ratios show, written to
// paired.json: per cell its seeds and largest |ratio - 1|. Of an A/A run,
// that is its noise.
type pairedNoise struct {
	Seeds int     `json:"seeds"`
	Noise float64 `json:"noise"`
}

// byShape sets the candidate against the baseline shape by shape: the
// median of the seeds' paired ratios, judged against the A/A noise of
// that shape and RTT. It returns each cell's paired noise, for
// paired.json.
func byShape(w io.Writer, rtts, benches []string, all maxRuns, nz noise) map[string]pairedNoise {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	cells, ratios := pairs(rtts, benches, all)
	out := map[string]pairedNoise{}
	if len(cells) == 0 {
		return out
	}
	p("\n| Benchmark | Shape | RTT ms | Seeds | Paired ratios, min / median / max | A/A noise | Against baseline |\n|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		rs := ratios[c]
		lo, med, hi := slices.Min(rs), medianFloat(rs), slices.Max(rs)
		spread := 0.0
		for _, r := range rs {
			spread = max(spread, abs(r-1))
		}
		out[c.String()] = pairedNoise{Seeds: len(rs), Noise: math.Round(spread*1e4) / 1e4}
		change := med - 1
		against := fmt.Sprintf("%+.1f%%", 100*change)
		aa := "-"
		if n, ok := nz[c.String()]; ok {
			aa = fmt.Sprintf("±%.1f%%", 100*n)
			if abs(change) <= n {
				against += ", within the A/A noise"
			} else {
				against += ", beyond the A/A noise"
			}
		} else {
			against += ", no A/A noise for this shape and RTT"
		}
		p("| %s | %s | %s | %d | %.3f / %.3f / %.3f | %s | %s |\n", c.bench, c.shape, c.rtt, len(rs), lo, med, hi, aa, against)
	}
	p("\nShape by shape: each seed's paired ratio is the candidate's median max throughput over the baseline's, on the runner where the two ran back to back. Against baseline is the median ratio over the seeds that drew the shape. The A/A noise is the largest |ratio - 1| one pair showed for that shape and RTT in a run of one ref against itself (bench/noise.json); a median beyond it is a change. This run's own pairs are in paired.json.\n")
	return out
}

// medianFloat is the median of xs, which is not empty.
func medianFloat(xs []float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	if n := len(s); n%2 == 1 {
		return s[n/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}
