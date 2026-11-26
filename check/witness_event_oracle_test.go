package check

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// oldEventCandidates is #894's enumeration, deliberately independent of the
// fused summary AND its candidate slice. In particular an omitted CAS in the
// new builder cannot make the oracle silently omit the same writer.
func oldEventCandidates(r *revisionOrder, rev int64) []int {
	h := r.w.h
	out := slices.Clone(r.byRev(r.revokes, rev))
	possible := r.ambiguous
	if !r.w.slow {
		possible = slices.Clone(r.writing)
		leases := map[int64]bool{}
		for _, kv := range r.w.live.kvs {
			if r.step() {
				return nil
			}
			if kv.Version != 0 && kv.Lease != 0 && !leases[kv.Lease] {
				leases[kv.Lease] = true
				possible = append(possible, r.revokesOfLease[kv.Lease]...)
			}
		}
	}
	var amb []int
	for _, i := range possible {
		if r.step() {
			return nil
		}
		if at, ok := r.reserved[i]; ok && at != rev {
			continue
		}
		if l := h.Ops[i].Req.LeaseRevoke; l != nil && r.grants[l.ID] <= 1 && r.alive[l.ID] >= rev {
			continue
		}
		if !r.used[i] && r.lb[i] < rev {
			amb = append(amb, i)
		}
	}
	revoke := func(i int) int {
		if h.Ops[i].Req.LeaseRevoke != nil {
			return 1
		}
		return 0
	}
	sortWithStep(amb, func(a, b int) int {
		return cmp.Or(cmp.Compare(revoke(a), revoke(b)), cmp.Compare(r.w.calls[a], r.w.calls[b]), cmp.Compare(a, b))
	}, r.step)
	return append(out, amb...)
}

type eventOracleSite struct {
	pool                []int
	probes, fits        int
	accepted, exhausted bool
	start, end          int
}

type eventOracleClaim struct {
	key  string
	cert eventCertificate
}

type eventOracleRun struct {
	err        string
	timeout    bool
	steps      int64
	indexSteps int64
	sites      map[string]*eventOracleSite
	claims     []eventOracleClaim
	trace      []eligibilitySearchStep
	r          *revisionOrder
}

// A semantic prefix is reconstructed by a separate complete fast-path replay,
// not save()/restore() on the checker making the claim. Include operation
// identities, placements, generations and parent continuations, but not the
// pure due/scope/readiness caches that the optimization intentionally skips.
func eventPrefix(r *revisionOrder, rev int64) string {
	x := struct {
		Rev, Compacted int64
		LastID         uint64
		Ops            []int
		KVs            []history.KeyValue
		Leases         []int64
		Used           []bool
		Places         [][2]uint64
		Parents        [][2]int64
	}{Rev: rev, Compacted: r.w.live.compacted, LastID: r.w.lastID, KVs: r.w.live.kvs, Used: r.used}
	for _, e := range r.w.events {
		x.Ops = append(x.Ops, e.op)
	}
	for _, p := range r.w.at {
		x.Places = append(x.Places, [2]uint64{p.bits, uint64(p.rank)})
	}
	for _, f := range r.stack {
		if f.rev < rev {
			x.Parents = append(x.Parents, [2]int64{f.rev, int64(f.next)})
		}
	}
	r.w.live.leases.each(func(id int64) { x.Leases = append(x.Leases, id) })
	b, _ := json.Marshal(x)
	return string(b)
}

func eventTraceStep(r *revisionOrder, phase string, op int) eligibilitySearchStep {
	w := r.w
	x := eligibilitySearchStep{phase: phase, op: op, searchRev: r.rev, rev: w.live.rev, compacted: w.live.compacted}
	if phase != "candidate" && phase != "fit" {
		x.keys, x.events, x.states = slices.Clone(w.live.kvs), slices.Clone(w.events), slices.Clone(w.states)
		x.at, x.placed, x.used, x.firsts = slices.Clone(w.at), slices.Clone(w.placed), slices.Clone(r.used), slices.Clone(w.firsts)
		w.live.leases.each(func(id int64) { x.leases = append(x.leases, id) })
	}
	for _, f := range r.stack {
		x.frames = append(x.frames, [2]int64{f.rev, int64(f.next)})
	}
	return x
}

func runEventOracle(h *history.History, old bool, budget int64, setup func(*revisionOrder), stopAtFit bool) *eventOracleRun {
	w := newWitness(h, time.Now().Add(time.Minute))
	w.maxSteps = budget
	r := newRevisionOrder(w)
	w.search = r
	o := &eventOracleRun{sites: map[string]*eventOracleSite{}, r: r}
	if old {
		r.testNoEventCertificate = true
		r.testCandidates = func(rev int64) []int { return oldEventCandidates(r, rev) }
	}
	active := map[*frame]*eventOracleSite{}
	r.testTrace = func(phase string, op int) {
		pos := len(o.trace)
		o.trace = append(o.trace, eventTraceStep(r, phase, op))
		var f *frame
		if len(r.stack) > 0 {
			f = r.stack[len(r.stack)-1]
		}
		switch phase {
		case "gap":
			site := &eventOracleSite{pool: slices.Clone(f.cands), start: pos, end: -1}
			o.sites[eventPrefix(r, f.rev)] = site
			active[f] = site
		case "candidate":
			if site := active[f]; site != nil {
				site.probes++
			}
		case "fit":
			if site := active[f]; site != nil {
				site.fits++
			}
			if stopAtFit {
				w.stopped = true
			}
		case "apply":
			if site := active[f]; site != nil && w.live.rev == f.rev {
				site.accepted = true
			}
		case "exhaust":
			if site := active[f]; site != nil && !w.stopped {
				site.exhausted, site.end = true, pos+1
			}
		}
	}
	r.testCertificate = func(rev int64, c eventCertificate, _ []int) {
		o.claims = append(o.claims, eventOracleClaim{eventPrefix(r, rev), c})
	}
	err := r.index()
	o.indexSteps = w.steps
	if err == nil {
		if setup != nil {
			setup(r)
		}
		err = r.search()
	}
	o.trace = append(o.trace, eventTraceStep(r, "done", -1))
	o.timeout = errors.Is(err, ErrWitnessTimeout) || w.stopped
	o.err = "pass"
	if err != nil {
		o.err = fmt.Sprintf("witness check (%s) of %d operations: %s", FromRevisions, len(h.Ops), err)
	}
	o.steps = w.steps
	return o
}

// A false fits result on cancellation is NOT evidence of exhaustion. The
// original write loop emits exhaust only after checking the stop latch.
func confirmedEventSite(site *eventOracleSite) bool {
	return site != nil && site.exhausted && !site.accepted && site.end >= site.start && site.probes == len(site.pool) && site.fits == len(site.pool)
}

func compareEventRuns(t *testing.T, old, got *eventOracleRun) [eventRejectReasons]int {
	t.Helper()
	var coverage [eventRejectReasons]int
	if old.timeout {
		t.Fatal("stopped independent replay is inconclusive, not a certificate confirmation")
	}
	if got.timeout || old.err != got.err {
		t.Fatalf("old %q (%d), new %q (%d)", old.err, old.steps, got.err, got.steps)
	}
	drop := make([]bool, len(old.trace))
	for _, claim := range got.claims {
		site := old.sites[claim.key]
		if !confirmedEventSite(site) {
			t.Fatalf("certificate not confirmed by independent old enumeration/fits: %+v", site)
		}
		if len(site.pool) > 0 {
			for k, n := range claim.cert.reasons {
				coverage[k] += int(n)
			}
		}
		for i := site.start; i < site.end; i++ {
			drop[i] = true
		}
	}
	var want, trace []eligibilitySearchStep
	for i, x := range old.trace {
		if !drop[i] {
			want = append(want, x)
		}
	}
	for _, x := range got.trace {
		if x.phase != "certified" {
			trace = append(trace, x)
		}
	}
	if !reflect.DeepEqual(trace, want) {
		for i := 0; i < min(len(trace), len(want)); i++ {
			if !reflect.DeepEqual(trace[i], want[i]) {
				t.Fatalf("retained search trace differs at %d: new %s/%d rev %d; old %s/%d rev %d", i, trace[i].phase, trace[i].op, trace[i].searchRev, want[i].phase, want[i].op, want[i].searchRev)
			}
		}
		t.Fatalf("retained search trace lengths: new %d old %d", len(trace), len(want))
	}
	// Native map iteration can change index-sort charges by one (#894's
	// measurements already use the maximum). The two isolated indexes are
	// otherwise unchanged; compare the search charges independently here.
	if got.steps-got.indexSteps > old.steps-old.indexSteps {
		t.Fatalf("search charges increased: old %d new %d", old.steps-old.indexSteps, got.steps-got.indexSteps)
	}
	if len(got.claims) == 0 && got.steps-got.indexSteps != old.steps-old.indexSteps {
		t.Fatalf("all-unknown search charges differ: old %d new %d", old.steps-old.indexSteps, got.steps-got.indexSteps)
	}
	return coverage
}

// Randomized impossible revision: observations pin four image keys, and a
// shuffled ambiguous pool mixes all six negative reasons. The independent
// oracle must actually enumerate every nonempty pool, not merely certify an
// empty candidate list. General randomized histories supply unknown fallbacks.
func eventDeadHistory(seed uint64, n int) *history.History {
	rng := rand.New(rand.NewPCG(seed, 876))
	st := &history.Start{Rev: 10}
	for _, key := range []string{"a", "b", "c", "d"} {
		st.KVs = append(st.KVs, history.KeyValue{Key: key, Value: key, CreateRevision: 2, ModRevision: 9, Version: 3})
	}
	h := &history.History{Start: st}
	for i := range n {
		key := st.KVs[rng.IntN(len(st.KVs))].Key
		var req history.Request
		switch rng.IntN(eventRejectReasons) {
		case eventEmptyDelete:
			req.Delete = &history.DeleteRequest{Key: "m"}
		case eventBlockedDelete:
			req.Delete = &history.DeleteRequest{Key: key, PrevKV: rng.IntN(2) == 0}
		case eventBlockedPut:
			req.Put = &history.PutRequest{Key: key, Value: strconv.Itoa(i)}
		case eventMissingLease:
			req.Put = &history.PutRequest{Key: "m", Value: "v", Lease: 99}
		case eventReadOnlyCAS:
			req.Txn = &history.TxnRequest{Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: 8}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: "m", Value: "v"}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key, Revision: 1}}}}
		case eventCompaction:
			req.Compact = &history.CompactRequest{Revision: 9}
		}
		h.Ops = append(h.Ops, history.Op{Client: i, Call: 1 + rng.Int64N(20), Return: 30, Req: req, Err: errUnavailable})
	}
	h.Ops = append(h.Ops, history.Op{Client: n, Call: 100, Return: 101, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}, Resp: &history.Response{Revision: 11, Range: &history.RangeResponse{KVs: slices.Clone(st.KVs), Count: int64(len(st.KVs))}}})
	return h
}

func TestEventCertificateRandomizedOracle(t *testing.T) {
	var coverage [eventRejectReasons]int
	hits, unknown, histories := 0, 0, 0
	for seed := range uint64(256) {
		for _, leased := range []bool{false, true} {
			for _, image := range []bool{false, true} {
				h := eligibilityHistory(seed, leased, image)
				for _, corrupt := range []bool{false, true} {
					if corrupt {
						data, _ := json.Marshal(h)
						h = &history.History{}
						if err := json.Unmarshal(data, h); err != nil {
							t.Fatal(err)
						}
						h.Ops[len(h.Ops)-1].Resp.Range.Count++
					}
					old := runEventOracle(h, true, 2_000_000, nil, false)
					got := runEventOracle(h, false, 2_000_000, nil, false)
					c := compareEventRuns(t, old, got)
					for k, n := range c {
						coverage[k] += n
					}
					hits += len(got.claims)
					unknown += len(got.sites)
					histories++
				}
			}
		}
	}
	for seed := range uint64(128) {
		h := eventDeadHistory(seed, 6+int(seed%19))
		old := runEventOracle(h, true, 2_000_000, nil, false)
		got := runEventOracle(h, false, 2_000_000, nil, false)
		c := compareEventRuns(t, old, got)
		for k, n := range c {
			coverage[k] += n
		}
		hits += len(got.claims)
		unknown += len(got.sites)
		histories++
	}
	if hits == 0 || unknown == 0 {
		t.Fatalf("oracle coverage hits=%d unknown=%d", hits, unknown)
	}
	for reason, n := range coverage {
		if n == 0 {
			t.Fatalf("nonempty independent pools never exercised reason %d", reason)
		}
	}
	t.Logf("%d isolated history pairs; %d certificates, %d unknown sites; nonempty old-pool rejection coverage %v", histories, hits, unknown, coverage)
}
