package check

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// Rebuild the proof from raw image/requests/responses, without the production
// put/count/deadline indexes, keysOf, model requests, Log or Effect.
func survivalReference(h *history.History) map[string]int64 {
	out := map[string]int64{}
	if h.Start == nil || len(h.Start.KVs) == 0 {
		return out
	}
	image, put := map[string]bool{}, map[string]bool{}
	for _, kv := range h.Start.KVs {
		image[kv.Key] = true
	}
	counts := map[int64][]struct {
		key, end string
		n        int64
	}{}
	var revs []int64
	top := h.Start.Rev
	for _, op := range h.Ops {
		if op.Resp != nil || op.Err != nil && op.Err.Ambiguous {
			q := op.Req
			if q.Put != nil {
				put[q.Put.Key] = true
			}
			if q.Txn != nil {
				for _, branch := range [][]history.TxnOp{q.Txn.Success, q.Txn.Failure} {
					for _, o := range branch {
						if o.Put != nil {
							put[o.Put.Key] = true
						}
					}
				}
			}
		}
		if op.Resp == nil {
			continue
		}
		top = max(top, op.Resp.Revision)
		at := op.Resp.Revision
		var key, end string
		var n int64
		switch {
		case op.Req.Range != nil && !op.Req.Range.Serializable:
			q := op.Req.Range
			key, end, n = q.Key, q.End, op.Resp.Range.Count
			if q.Revision > 0 {
				at = q.Revision
			}
		case op.Req.Delete != nil:
			q := op.Req.Delete
			key, end, n = q.Key, q.End, op.Resp.Delete.Deleted
			if n > 0 {
				at--
			}
		default:
			continue
		}
		if len(counts[at]) == 0 {
			revs = append(revs, at)
		}
		counts[at] = append(counts[at], struct {
			key, end string
			n        int64
		}{key, end, n})
	}
	keys := slices.Sorted(maps.Keys(image))
	for key := range put {
		if !image[key] {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	slices.Sort(revs)
	seen := map[[2]string]bool{}
	for _, at := range revs {
		if at <= h.Start.Rev || at > top {
			continue
		}
		for _, c := range counts[at] {
			var span []string
			for _, key := range keys {
				if history.InRange(key, c.key, c.end) {
					span = append(span, key)
				}
			}
			if c.n <= 0 || len(span) > 128 || int64(len(span)) != c.n {
				continue
			}
			// Canonicalize by included request/image keys, not request endpoints.
			id := [2]string{span[0], span[len(span)-1]}
			full, known := seen[id]
			if !known {
				if len(seen) == 512 {
					continue
				}
				full = true
				for _, key := range span {
					if !image[key] || put[key] {
						full = false
						break
					}
				}
				seen[id] = full
			}
			if full {
				for _, key := range span {
					out[key] = max(out[key], at)
				}
			}
		}
	}
	return out
}

type survivalRun struct {
	*soleRun
	items     int64
	soleHits  int
	pruned    map[int]bool
	deadlines map[string]int64
}

func runSurvival(h *history.History, off, trace bool, budget int64) *survivalRun {
	out := &survivalRun{soleRun: newSoleRun(h), pruned: map[int]bool{}}
	want := survivalReference(h)
	var r *revisionOrder
	active := -1
	setup := func(o *options) {
		o.testRevision = func(v *revisionOrder) {
			r = v
			r.testNoSurvival = off
			r.testSoleGrant = func(_, _ int) { out.soleHits++ }
			r.testRejection = func(rev int64, err error) {
				out.rejections[soleRejection{rev, err.Error()}] = true
				if trace {
					n := soleBoundary(r, "dead", -1)
					n.rejection = soleRejection{rev, err.Error()}
					out.nodes = append(out.nodes, n)
				}
			}
			r.testSurvival = func(i int, rev int64, key string) {
				out.hits++
				if want[key] < rev || r.w.reqs[i].Delete == nil {
					out.oracleErr = fmt.Errorf("unproved survival hit %s at %d", key, rev)
				}
				if active >= 0 {
					out.pruned[active] = true
				}
			}
			r.testTrace = func(phase string, op int) {
				if phase == "revision" && !off && !maps.Equal(r.survives, want) {
					out.oracleErr = fmt.Errorf("raw deadline rebuild differs: got %v want %v", r.survives, want)
				}
				if !trace {
					return
				}
				switch phase {
				case "fit", "exhaust":
					out.nodes = append(out.nodes, soleBoundary(r, phase, op))
					if phase == "fit" {
						active = len(out.nodes) - 1
					}
				case "fit-accepted":
					if active >= 0 {
						out.nodes[active].accepted = true
					}
				case "apply":
					if op >= 0 && h.Ops[op].Resp != nil {
						out.nodes = append(out.nodes, soleBoundary(r, phase, op))
					}
				}
			}
		}
	}
	err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(budget), StepsUsed(&out.steps), setup)
	out.err = "pass"
	if err != nil {
		out.err = err.Error()
	}
	out.publicErr = err
	out.timeout = errors.Is(err, ErrWitnessTimeout)
	if r != nil {
		out.at = r.at
		out.stopped = r.w.stopped
		out.items = r.survivalItems
		out.deadlines = r.survives
		if r.failure != nil {
			out.failure = r.failure.Error()
		}
		if trace {
			out.nodes = append(out.nodes, soleBoundary(r, "done", -1))
		}
	}
	// Inserting N index items changes the rounded shared index ledger by at
	// most ceil(N/256). No allowance for per-probe work or a second search.
	out.extraUnits = ((out.items + 255) / 256) * workUnitsPerStep
	return out
}

func survivalCheck(old, got *survivalRun) (int, error) {
	if err := soleVerdictCheck(old.soleRun, got.soleRun); err != nil {
		return 0, err
	}
	if got.at > old.at {
		return 0, fmt.Errorf("selected revision advanced: %d > %d", got.at, old.at)
	}
	i, prunes := 0, 0
	for j, n := range got.nodes {
		if i >= len(old.nodes) {
			return 0, fmt.Errorf("extra retained boundary %d", j)
		}
		a := old.nodes[i]
		if a.rejection != n.rejection || !reflect.DeepEqual(a.state, n.state) || !reflect.DeepEqual(a.leases, n.leases) {
			return 0, fmt.Errorf("retained boundary %d differs (%s op %d at %d / %s op %d at %d)", j, a.state.phase, a.state.op, a.state.searchRev, n.state.phase, n.state.op, n.state.searchRev)
		}
		i++
		if a.accepted != n.accepted {
			if !a.accepted || n.accepted || !got.pruned[j] {
				return 0, errors.New("unproved subtree removal")
			}
			prunes++
			for i < len(old.nodes) && !soleContinuation(a, old.nodes[i]) {
				i++
			}
		}
	}
	if i != len(old.nodes) {
		return 0, errors.New("unexplained reference suffix")
	}
	return prunes, nil
}

// Astra's 69 -> 3 counterexample, optionally with a valid sibling beyond the
// horizon. The only lost key has no put source in the entire history.
func survivalAstra(sibling bool, n int, leased bool) *history.History {
	st := &history.Start{Rev: 2, KVs: []history.KeyValue{{Key: "a", CreateRevision: 2, ModRevision: 2, Version: 1}}}
	for j := range n {
		st.KVs = append(st.KVs, history.KeyValue{Key: fmt.Sprintf("b%03d", j), CreateRevision: 2, ModRevision: 2, Version: 1})
	}
	if leased {
		st.Leases = []int64{9}
		for j := 1; j < len(st.KVs); j++ {
			st.KVs[j].Lease = 9
		}
	}
	h := &history.History{Start: st, Ops: []history.Op{{Client: 0, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "a", End: "c"}}, Err: errUnavailable}}}
	if sibling {
		h.Ops = append(h.Ops, history.Op{Client: 1, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "a"}}, Err: errUnavailable})
	}
	for j := range 65 {
		h.Ops = append(h.Ops, history.Op{Client: 2, Call: 10 + int64(j)*2, Return: 11 + int64(j)*2, Req: history.Request{Put: &history.PutRequest{Key: "z", Value: strconv.Itoa(j)}}, Resp: &history.Response{Revision: 4 + int64(j), Put: &history.PutResponse{}}})
	}
	h.Ops = append(h.Ops, history.Op{Client: 3, Call: 150, Return: 151, Req: history.Request{Delete: &history.DeleteRequest{Key: "b", End: "c"}}, Resp: &history.Response{Revision: 69, Delete: &history.DeleteResponse{Deleted: int64(n)}}})
	return h
}

func TestSurvivalAstraAndSibling(t *testing.T) {
	for _, valid := range []bool{false, true} {
		h := survivalAstra(valid, 1, false)
		old, got := runSurvival(h, true, true, 100000), runSurvival(h, false, true, 100000)
		n, err := survivalCheck(old, got)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 || got.hits == 0 {
			t.Fatal("missing actual pruning")
		}
		if valid {
			if got.err != "pass" {
				t.Fatal(got.err)
			}
		} else {
			if old.at != 69 || got.at != 3 || !old.rejections[soleRejection{3, got.failure}] {
				t.Fatalf("want 69 -> 3, got %d -> %d", old.at, got.at)
			}
		}
		t.Logf("sibling=%v: old %d new %d, construction %d items / <=%d steps, hits %d prunes %d", valid, old.steps, got.steps, got.items, (got.items+255)/256, got.hits, n)
	}
}

func TestSurvivalImageFamilies(t *testing.T) {
	hits, prunes := 0, 0
	for seed := range 64 {
		h := survivalAstra(seed%2 == 0, 1+seed%4, seed%3 == 0)
		if seed%4 == 1 {
			h.Ops[len(h.Ops)-1].Resp.Delete.Deleted--
		} // partial (or zero) is unknown
		if seed%5 == 0 {
			h.Ops = append(h.Ops, history.Op{Client: 100, Call: 1, Return: 2, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}}, Err: errUnavailable})
		}
		old, got := runSurvival(h, true, true, 200000), runSurvival(h, false, true, 200000)
		n, err := survivalCheck(old, got)
		if err != nil {
			t.Fatalf("family %d: %v", seed, err)
		}
		hits += got.hits
		prunes += n
	}
	if hits == 0 || prunes == 0 {
		t.Fatal("generated family did not prune")
	}
	t.Logf("64 Start families: hits %d, actual prunes %d", hits, prunes)
}

func TestSurvivalOracleRejectsMutations(t *testing.T) {
	h := survivalAstra(false, 1, false)
	old, got := runSurvival(h, true, true, 100000), runSurvival(h, false, true, 100000)
	if _, err := survivalCheck(old, got); err != nil {
		t.Fatal(err)
	}
	stopped := *old
	stopped.soleRun = &soleRun{stopped: true}
	if _, err := survivalCheck(&stopped, got); err == nil {
		t.Fatal("stopped reference confirmed proof")
	}
	bad := *got
	copyRun := *got.soleRun
	bad.soleRun = &copyRun
	bad.err = "invented explanation: " + got.err
	bad.publicErr = errors.New(bad.err)
	if _, err := survivalCheck(old, &bad); err == nil {
		t.Fatal("invented returned explanation accepted")
	}
	bad = *got
	copyRun = *got.soleRun
	bad.soleRun = &copyRun
	bad.at = old.at + 1
	if _, err := survivalCheck(old, &bad); err == nil {
		t.Fatal("later selected revision accepted")
	}
	bad = *got
	bad.pruned = map[int]bool{}
	if _, err := survivalCheck(old, &bad); err == nil {
		t.Fatal("unproved pruning accepted")
	}
	bad = *got
	copyRun = *got.soleRun
	bad.soleRun = &copyRun
	bad.nodes = slices.Clone(got.nodes)
	bad.nodes[0].state.op++
	if _, err := survivalCheck(old, &bad); err == nil {
		t.Fatal("changed retained trace accepted")
	}
}

// These mutate only the public return, leaving failure, dead evidence and
// every semantic boundary untouched. Internal membership alone approves
// them, which was the reviewer's counterexample on the 69 -> 3 history.
func TestSurvivalReturnedExplanation(t *testing.T) {
	h := survivalAstra(false, 1, false)
	old, got := runSurvival(h, true, true, 100000), runSurvival(h, false, true, 100000)
	if n, err := survivalCheck(old, got); err != nil || n != 1 {
		t.Fatalf("69 -> 3 baseline: prunes %d, error %v", n, err)
	}
	if old.at != 69 || got.at != 3 || !old.rejections[soleRejection{got.at, got.failure}] {
		t.Fatal("counterexample requires genuine internal membership")
	}
	for _, tc := range []struct{ name, returned string }{
		{"invented", "invented explanation: " + got.err},
		{"wrong-wrapper", strings.Replace(got.err, "(revisions)", "(log)", 1)},
		{"old-text-wrong-revision", old.err},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := *got
			copyRun := *got.soleRun
			bad.soleRun = &copyRun
			bad.err = tc.returned
			bad.publicErr = errors.New(tc.returned)
			if bad.failure != got.failure || !maps.Equal(bad.rejections, got.rejections) || !reflect.DeepEqual(bad.nodes, got.nodes) {
				t.Fatal("return-only mutation changed internal evidence")
			}
			if _, err := survivalCheck(old, &bad); err == nil {
				t.Fatal("oracle approved mutated public return")
			}
		})
	}
}

func TestSurvivalCapSweeps(t *testing.T) {
	h := survivalAstra(true, 1, false)
	full := runSurvival(h, false, false, 100000)
	for cap := int64(1); cap <= full.steps+5; cap++ {
		a, b := runSurvival(h, true, false, cap), runSurvival(h, false, false, cap+(full.items+255)/256)
		if a.timeout {
			continue
		}
		if b.timeout {
			t.Fatalf("new timed out beyond construction allowance at %d", cap)
		}
		if (a.err == "pass") != (b.err == "pass") {
			t.Fatal("cap verdict differs")
		}
		if err := soleStepCheck(a.soleRun, b.soleRun); err != nil {
			t.Fatal(err)
		}
	}
	for cap := int64(1); cap < 4; cap++ {
		r := runSurvival(h, false, false, cap)
		if r.timeout && (strings.Contains(r.err, "no acked op wrote") || strings.Contains(r.err, "gets ")) {
			t.Fatal("branch failure polluted timeout")
		}
	}
}
