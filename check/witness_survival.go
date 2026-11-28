package check

import (
	"sort"
)

// Image keys in a range nobody can put can only disappear. A count of the
// entire initial set therefore proves every member survives through that
// revision, even beyond reachable's horizon. This is only a negative proof
// for plain-delete gap trials, not a writer or a positive witness.
const (
	maxSurvivalWidth  = 128
	maxSurvivalRanges = 512
)

type survivalRange struct {
	lo, hi  int
	through int64
}

func (r *revisionOrder) survivalStep() bool {
	r.survivalItems++
	return r.indexStep()
}

func (r *revisionOrder) indexSurvival() {
	st := r.w.h.Start
	// A lease-only Start must retain exactly the old work and allocations.
	if r.testNoSurvival || st == nil || len(st.KVs) == 0 {
		return
	}
	// Reuse indexCounts, including both branches of every possible txn. Only
	// image keys and possible request put targets participate in the width;
	// log-only keys can cost filtering work but never change the proof/cutoff.
	image := make(map[int]bool, len(st.KVs))
	for _, kv := range st.KVs {
		if r.survivalStep() {
			return
		}
		image[r.w.live.index(kv.Key)] = true
	}
	var keys []int
	for k := range r.w.keys {
		if r.survivalStep() {
			return
		}
		if image[k] || len(r.putRevs[k]) != 0 || len(r.putters[k]) != 0 {
			keys = append(keys, k)
		}
	}
	// countRevs is sorted, and counts at a revision retain observation order.
	// Cache at most 512 full-width candidate spans, including failed putter
	// checks. Thus even an all-unknown input scans <=512*128 keys. Duplicates
	// of retained spans still update their deadline after the cutoff.
	ranges := map[[2]int]int{}
	var retained []survivalRange
	for _, rev := range r.countRevs {
		if r.survivalStep() {
			return
		}
		if rev <= r.first || rev > r.top {
			continue
		}
		for _, c := range r.counts[rev] {
			if r.survivalStep() {
				return
			}
			lo, hi := sort.SearchInts(keys, c.lo), sort.SearchInts(keys, c.hi)
			if c.n <= 0 || hi-lo > maxSurvivalWidth || int64(hi-lo) != c.n {
				continue
			}
			span := [2]int{lo, hi}
			if j, ok := ranges[span]; ok {
				if j >= 0 {
					retained[j].through = rev
				}
				continue
			}
			if len(ranges) == maxSurvivalRanges {
				continue
			}
			ranges[span] = -1
			full := true
			for _, k := range keys[lo:hi] {
				if r.survivalStep() {
					return
				}
				if !image[k] || len(r.putRevs[k]) != 0 || len(r.putters[k]) != 0 {
					full = false
					break
				}
			}
			if full {
				ranges[span] = len(retained)
				retained = append(retained, survivalRange{lo, hi, rev})
			}
		}
	}
	for _, span := range retained {
		if r.survivalStep() {
			return
		}
		for _, k := range keys[span.lo:span.hi] {
			if r.survivalStep() {
				return
			}
			if r.survives == nil {
				r.survives = map[string]int64{}
			}
			key := r.w.keys[k]
			r.survives[key] = max(r.survives[key], span.through)
		}
	}
}

func (r *revisionOrder) survivalBlocked(i int, rev int64, key string) bool {
	if r.survives[key] < rev || rev <= r.first || r.w.reqs[i].Delete == nil || len(r.stack) == 0 || r.stack[len(r.stack)-1].rev != rev {
		return false
	}
	if _, ok := r.resolved[rev]; ok {
		return false
	}
	if _, ok := r.writer(rev); ok {
		return false
	}
	if r.testSurvival != nil {
		r.testSurvival(i, rev, key)
	}
	return true
}
