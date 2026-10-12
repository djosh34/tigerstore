package main

import (
	"math"
	"math/bits"
)

// hist counts durations in nanoseconds in log-linear buckets: exact below
// 256 ns, then 128 buckets per power of two, so a bucket is at most 0.8%
// wide.
const (
	histSubBits = 7
	histBuckets = (64 - histSubBits) << histSubBits
)

type hist struct {
	counts   [histBuckets]uint64
	n        uint64
	sum      float64
	min, max int64
}

func histIndex(v int64) int {
	if v < 1<<(histSubBits+1) {
		return int(max(v, 0))
	}
	shift := bits.Len64(uint64(v)) - histSubBits - 1
	return shift<<histSubBits + int(uint64(v)>>shift)
}

// histLow is the smallest value of bucket i.
func histLow(i int) int64 {
	if i < 1<<(histSubBits+1) {
		return int64(i)
	}
	shift := i>>histSubBits - 1
	return int64(i-shift<<histSubBits) << shift
}

func (h *hist) record(v int64) {
	if h.n == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.counts[histIndex(v)]++
	h.n++
	h.sum += float64(v)
}

func (h *hist) merge(o *hist) {
	if o.n == 0 {
		return
	}
	if h.n == 0 || o.min < h.min {
		h.min = o.min
	}
	h.max = max(h.max, o.max)
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	h.sum += o.sum
}

// quantile returns the value at quantile q in [0, 1]: the middle of the
// bucket that holds it, clamped to the observed minimum and maximum.
func (h *hist) quantile(q float64) int64 {
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	rank = max(rank, 1)
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			lo := histLow(i)
			hi := histLow(i+1) - 1
			return min(max(lo+(hi-lo)/2, h.min), h.max)
		}
	}
	return h.max
}

// latency is a latency summary in microseconds.
type latency struct {
	Count uint64  `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
}

func (h *hist) summary() latency {
	us := func(ns int64) float64 { return float64(ns) / 1e3 }
	s := latency{
		Count: h.n,
		P50:   us(h.quantile(0.5)),
		P90:   us(h.quantile(0.9)),
		P99:   us(h.quantile(0.99)),
		P999:  us(h.quantile(0.999)),
		Max:   us(h.max),
	}
	if h.n > 0 {
		s.Mean = h.sum / float64(h.n) / 1e3
	}
	return s
}
