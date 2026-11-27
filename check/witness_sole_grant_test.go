package check

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/djosh34/tigerstore/history"
)

type soleRejection struct {
	rev  int64
	text string
}
type soleCountProof struct{ rev, need, oldMaybe, newMaybe int64 }

type soleNode struct {
	state     eligibilitySearchStep
	leases    [][]int64
	accepted  bool
	proof     *soleCountProof
	rejection soleRejection
}
type soleRun struct {
	err        string
	publicErr  error
	timeout    bool
	stopped    bool
	steps, at  int64
	failure    string
	rejections map[soleRejection]bool
	nodes      []soleNode
	hits       int
	extraUnits int64
	oracleErr  error
}

type soleGrantSource struct {
	n, op  int
	usable bool
}

// Build grant provenance independently from history, not r.grants. A
// production metadata mutation must not contaminate its own reference.
func soleReferenceGrants(h *history.History) map[int64]soleGrantSource {
	out := map[int64]soleGrantSource{}
	add := func(id int64, op int, usable bool) { g := out[id]; g.n++; g.op = op; g.usable = usable; out[id] = g }
	if h.Start != nil {
		for _, id := range h.Start.Leases {
			add(id, imageGrant, true)
		}
	}
	for i, op := range h.Ops {
		if q := op.Req.LeaseGrant; q != nil {
			id := q.ID
			if op.Resp != nil {
				id = op.Resp.LeaseGrant.ID
			}
			add(id, i, op.Err == nil || op.Err.Ambiguous)
		}
	}
	return out
}

// This is the old creator enumeration with an independent, optional
// sole-grant predicate. It has no charged callbacks or mutable caches.
// Work includes each visited putter and its charged txn-predicate items.
func soleReferenceCreate(r *revisionOrder, s *state, k int, rev int64, skip int, rule bool, grants map[int64]soleGrantSource) (bool, int64) {
	ats := r.putRevs[k]
	j := sort.Search(len(ats), func(j int) bool { return ats[j] > s.rev })
	if j < len(ats) && ats[j] <= rev {
		return true, 0
	}
	gap := rule && len(r.stack) > 0 && r.stack[len(r.stack)-1].rev == s.rev
	var work int64
	for _, i := range r.putters[k] {
		work += ordinaryUnits
		op := &r.w.h.Ops[i]
		if op.Resp != nil {
			if op.Resp.Revision > s.rev && op.Resp.Revision <= rev {
				return true, work
			}
			continue
		}
		if i == skip || r.used[i] || r.lb[i] >= rev {
			continue
		}
		if p := op.Req.Put; gap && p != nil && p.Lease != 0 && !s.hasLease(p.Lease) {
			g := grants[p.Lease]
			if g.n == 1 && g.usable && (g.op == imageGrant || g.op >= 0 && r.used[g.op]) {
				continue
			}
		}
		if touchesWithStep(s, op.Req, k, true, func() bool { work += ordinaryUnits; return false }) {
			return true, work
		}
	}
	return false, work
}

func soleStepCheck(old, got *soleRun) error {
	allowance := (got.extraUnits + workUnitsPerStep - 1) / workUnitsPerStep
	if got.steps > old.steps+allowance {
		return fmt.Errorf("new %d steps exceed old %d plus newly charged putter allowance %d (%d units)", got.steps, old.steps, allowance, got.extraUnits)
	}
	return nil
}

// A trace boundary keeps the entire committed prefix, not only the next
// candidate's identity. Trial apply/undo, cache generations and accounting
// are not semantic tree nodes. Normalize generation IDs by whether a
// cached answer belongs to the current replay, and compare every historic
// lease set by value rather than persistent-tree allocation shape.
func soleBoundary(r *revisionOrder, phase string, op int) soleNode {
	x := eventTraceStep(r, "sole-boundary", op)
	x.phase = phase
	// Empty restored backing slices and never-allocated slices are the
	// same prefix, not different applications.
	if len(x.events) == 0 {
		x.events = nil
	}
	if len(x.placed) == 0 {
		x.placed = nil
	}
	for i, f := range x.firsts {
		if f.at < 0 || f.at >= len(r.w.states) || f.id != r.w.states[f.at].id {
			x.firsts[i] = firstAnswer{}
		} else {
			x.firsts[i].id = 1
		}
	}
	n := soleNode{state: x}
	for i := range n.state.states {
		var ids []int64
		n.state.states[i].leases.each(func(id int64) { ids = append(ids, id) })
		n.leases = append(n.leases, ids)
		n.state.states[i].id = 0
		n.state.states[i].leases = leaseSet{}
	}
	return n
}

func runSoleOracle(h *history.History, old, trace bool, budget int64) *soleRun {
	out := &soleRun{rejections: map[soleRejection]bool{}}
	var r *revisionOrder
	grants := soleReferenceGrants(h)
	activeFit := -1
	setup := func(o *options) {
		o.testRevision = func(v *revisionOrder) {
			r = v
			r.testNoSoleGrant = old
			r.testRejection = func(rev int64, err error) {
				out.rejections[soleRejection{rev, err.Error()}] = true
				if trace {
					n := soleBoundary(r, "dead", -1)
					n.rejection = soleRejection{rev, err.Error()}
					out.nodes = append(out.nodes, n)
				}
			}
			r.testSoleGrant = func(_, _ int) { out.hits++ }
			if !old || trace {
				r.testCreate = func(s *state, k int, rev int64, skip int) func(bool) {
					want, work := soleReferenceCreate(r, s, k, rev, skip, !old, grants)
					_, oldWork := soleReferenceCreate(r, s, k, rev, skip, false, grants)
					steps, left := r.w.steps, int64(r.budgetLeft)
					return func(got bool) {
						actual := (r.w.steps-steps)*workUnitsPerStep + left - int64(r.budgetLeft)
						if r.w.stopped && r.w.maxSteps > 0 && r.w.steps > r.w.maxSteps {
							actual -= workUnitsPerStep - ordinaryUnits
						}
						out.extraUnits += max(0, actual-oldWork)
						if !r.w.stopped && (got != want || actual != work) {
							out.oracleErr = fmt.Errorf("creator query key %s after %d, count %d: got %v/%d units, reference %v/%d", s.keys[k], s.rev, rev, got, actual, want, work)
						}
					}
				}
				r.testCountFailure = func(s *state, skip int, c count, sure, maybe int64) {
					var before, after int64
					for k := c.lo; k < c.hi; k++ {
						was, _ := soleReferenceCreate(r, s, k, c.rev, skip, false, grants)
						now, _ := soleReferenceCreate(r, s, k, c.rev, skip, !old, grants)
						if s.kvs[k].Version != 0 || was {
							before++
						}
						if s.kvs[k].Version != 0 || now {
							after++
						}
					}
					if after != maybe {
						out.oracleErr = fmt.Errorf("decisive count %d rejected with maybe=%d but independent creators give %d", c.rev, maybe, after)
					}
					if !old && trace && activeFit >= 0 && sure <= c.n && after < c.n && before >= c.n {
						out.nodes[activeFit].proof = &soleCountProof{c.rev, c.n, before, after}
					}
				}
			}
			if trace {
				r.testTrace = func(phase string, op int) {
					switch phase {
					case "fit", "exhaust":
						out.nodes = append(out.nodes, soleBoundary(r, phase, op))
						if phase == "fit" {
							activeFit = len(out.nodes) - 1
						}
					case "fit-accepted":
						if activeFit >= 0 {
							out.nodes[activeFit].accepted = true
						}
					case "apply":
						if op >= 0 && h.Ops[op].Resp != nil {
							out.nodes = append(out.nodes, soleBoundary(r, phase, op))
						}
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
	out.timeout = errors.Is(out.publicErr, ErrWitnessTimeout)
	if r != nil {
		out.at = r.at
		if r.failure != nil {
			out.failure = r.failure.Error()
		}
		out.stopped = r.w.stopped
		if trace {
			out.nodes = append(out.nodes, soleBoundary(r, "done", -1))
		}
	}
	return out
}

// A stopped reference cannot establish either rejection membership or
// subtree exhaustion. Callers must not treat a pair of timeouts as proof.
func soleVerdictCheck(old, got *soleRun) error {
	if old.oracleErr != nil {
		return old.oracleErr
	}
	if got.oracleErr != nil {
		return got.oracleErr
	}
	if old.timeout || got.timeout || old.stopped || got.stopped {
		return errors.New("stopped oracle is inconclusive")
	}
	if err := soleStepCheck(old, got); err != nil {
		return err
	}
	if (old.err == "pass") != (got.err == "pass") {
		return fmt.Errorf("verdict differs: old %s new %s", old.err, got.err)
	}
	if old.err != got.err && (got.at > old.at || !old.rejections[soleRejection{got.at, got.failure}]) {
		return fmt.Errorf("new explanation was not an old rejection at or below %d: new rev %d: %s", old.at, got.at, got.err)
	}
	return nil
}

func soleContinuation(root, next soleNode) bool {
	if next.state.phase != "fit" && next.state.phase != "exhaust" && next.state.phase != "done" {
		return false
	}
	f := root.state.frames
	if next.state.phase == "done" && len(f) == 0 {
		return true
	}
	if len(f) == 0 || len(next.state.frames) != len(f) {
		return false
	}
	want := f[len(f)-1][0]
	if next.state.searchRev != want {
		return false
	}
	for i := range f {
		if f[i][0] != next.state.frames[i][0] || i < len(f)-1 && f[i][1] != next.state.frames[i][1] {
			return false
		}
	}
	return next.state.frames[len(f)-1][1] >= f[len(f)-1][1]
}

func soleTraceCheck(old, got *soleRun) (int, error) {
	if err := soleVerdictCheck(old, got); err != nil {
		return 0, err
	}
	pruned, i := 0, 0
	for j, n := range got.nodes {
		if i >= len(old.nodes) {
			return 0, fmt.Errorf("new trace has extra boundary %d", j)
		}
		a := old.nodes[i]
		if a.rejection != n.rejection {
			return 0, fmt.Errorf("retained dead node %d differs: old (%d, %q), new (%d, %q)", j, a.rejection.rev, a.rejection.text, n.rejection.rev, n.rejection.text)
		}
		if !reflect.DeepEqual(a.state, n.state) || !reflect.DeepEqual(a.leases, n.leases) {
			return 0, fmt.Errorf("retained boundary %d differs: old %s op %d rev %d, new %s op %d rev %d", j, a.state.phase, a.state.op, a.state.searchRev, n.state.phase, n.state.op, n.state.searchRev)
		}
		i++
		if a.accepted != n.accepted {
			if !a.accepted || n.accepted || n.proof == nil || n.proof.oldMaybe < n.proof.need || n.proof.newMaybe >= n.proof.need {
				return 0, errors.New("trace difference is not a sole-grant subtree rejection")
			}
			pruned++
			for i < len(old.nodes) && !soleContinuation(a, old.nodes[i]) {
				i++
			}
		}
	}
	if i != len(old.nodes) {
		return 0, fmt.Errorf("unexplained old trace suffix: %d boundaries", len(old.nodes)-i)
	}
	return pruned, nil
}

func compareSoleTrace(t *testing.T, old, got *soleRun) int {
	t.Helper()
	n, err := soleTraceCheck(old, got)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func soleAstraHistory() *history.History {
	return &history.History{Start: &history.Start{Rev: 2, Leases: []int64{9}, KVs: []history.KeyValue{{Key: "a", CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: "b", CreateRevision: 2, ModRevision: 2, Version: 1}}}, Ops: []history.Op{
		{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}}, Resp: &history.Response{Revision: 2, LeaseRevoke: &history.LeaseRevokeResponse{}}},
		{Client: 1, Call: 3, Return: 4, Req: history.Request{Delete: &history.DeleteRequest{Key: "a", End: "c"}}, Err: errUnavailable},
		{Client: 2, Call: 3, Return: 4, Req: history.Request{Put: &history.PutRequest{Key: "b", Lease: 9}}, Err: errUnavailable},
		{Client: 3, Call: 5, Return: 6, Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}}, Resp: &history.Response{Revision: 4, Delete: &history.DeleteResponse{Deleted: 1}}},
	}}
}

func soleOpusHistory(t *testing.T) *history.History {
	t.Helper()
	b := newBuilder(t, "k0", "k5", "k6")
	b.put(1, "k6", "a")
	b.put(1, "k5", "b")
	b.do(1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 7, TTL: 60}})
	b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}})
	p := b.do(2, history.Request{Put: &history.PutRequest{Key: "k6", Value: "c", Lease: 7}})
	d := b.del(2, "k5")
	b.put(1, "k0", "z")
	b.del(1, "k6")
	h := b.done()
	h.Log = nil
	amb := &history.Error{Code: "Unavailable", Message: "no answer", Ambiguous: true}
	h.Ops[p].Resp, h.Ops[p].Err = nil, amb
	h.Ops[d].Resp, h.Ops[d].Err = nil, amb
	h.Ops[d].Req = history.Request{Delete: &history.DeleteRequest{Key: "k5", End: "k7"}}
	return h
}

func TestSoleGrantReviewerHistories(t *testing.T) {
	want := []string{
		`witness check (revisions) of 4 operations: no acked op wrote revision 3, and no ambiguous mutation or acked revoke can have written it in the order so far; the order before it: event 1 (op 0, to revision 2) lease revoke {"id":9}`,
		`witness check (revisions) of 8 operations: no acked op wrote revision 4, and no ambiguous mutation or acked revoke can have written it in the order so far; the order before it: event 2 (op 1, to revision 3) put {"key":"k5","value":"b"}; event 3 (op 2, to revision 3) lease grant {"id":7,"ttl":60}; event 4 (op 3, to revision 3) lease revoke {"id":7}`,
	}
	for j, h := range []*history.History{soleAstraHistory(), soleOpusHistory(t)} {
		old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
		if got.err != want[j] {
			t.Fatalf("pinned reviewer explanation changed: %s", got.err)
		}
		if got.hits == 0 || got.err == old.err || compareSoleTrace(t, old, got) == 0 {
			t.Fatal("counterexample did not exercise pruning and explanation membership")
		}
		if got.steps > old.steps {
			t.Fatalf("pruned counterexample costs more: %d > %d", got.steps, old.steps)
		}
	}
}

func TestSoleGrantWholeSearchOracle(t *testing.T) {
	histories, hits, prunes := 0, 0, 0
	for seed := range uint64(256) {
		for _, leased := range []bool{false, true} {
			for _, image := range []bool{false, true} {
				for _, corrupt := range []bool{false, true} {
					h := eligibilityHistory(seed, leased, image)
					if corrupt {
						rng := rand.New(rand.NewPCG(seed, 894))
						if !staleRead(rng, h) {
							h.Ops[len(h.Ops)-1].Resp.Range.Count++
						}
					}
					old, got := runSoleOracle(h, true, true, 2000000), runSoleOracle(h, false, true, 2000000)
					if old.err != got.err {
						t.Fatalf("seed %d leased=%v image=%v corrupt=%v: full explanation changed: old %s new %s", seed, leased, image, corrupt, old.err, got.err)
					}
					prunes += compareSoleTrace(t, old, got)
					hits += got.hits
					histories++
				}
			}
		}
	}
	if histories != 2048 {
		t.Fatal(histories)
	}
	moreHits, morePrunes := soleReviewCorpus(t)
	hits += moreHits
	prunes += morePrunes
	if hits == 0 || prunes == 0 {
		t.Fatal("large oracle must see genuine pruning")
	}
	t.Logf("%d baseline histories plus 64 lease families: exact full verdicts/explanations and retained traces; %d capability hits, %d pruned subtrees", histories, hits, prunes)
}

func TestSoleGrantOracleRejectsTimeout(t *testing.T) {
	old := runSoleOracle(soleAstraHistory(), true, true, 1)
	got := runSoleOracle(soleAstraHistory(), false, true, 100000)
	if err := soleVerdictCheck(old, got); err == nil || !strings.Contains(err.Error(), "inconclusive") {
		t.Fatal("stopped reference was accepted")
	}
	if !old.timeout || strings.Contains(old.err, "no acked op wrote") || strings.Contains(old.err, "gets ") {
		t.Fatalf("timeout fabricated a history failure: %s", old.err)
	}
}

func TestSoleGrantMetadata(t *testing.T) {
	if unsafe.Sizeof(grantCount{}) != 8 {
		t.Fatal("grant map payload grew")
	}
	for _, tc := range []struct {
		name                                string
		image                               bool
		grants                              int
		used, live, failed, ambiguous, zero bool
		want                                bool
	}{
		{name: "operation zero", grants: 1, used: true, want: true},
		{name: "unused", grants: 1},
		{name: "live", grants: 1, used: true, live: true},
		{name: "definite failure", grants: 1, used: true, failed: true},
		{name: "ambiguous grant", grants: 1, used: true, ambiguous: true, want: true},
		{name: "unknown ID", grants: 1, used: true, ambiguous: true, zero: true},
		{name: "response ID", grants: 1, used: true, zero: true, want: true},
		{name: "regrant", grants: 2, used: true},
		{name: "keyless image", image: true, want: true},
		{name: "image and regrant", image: true, grants: 1, used: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &history.History{}
			if tc.image {
				h.Start = &history.Start{Rev: 1, Leases: []int64{9}}
			}
			for j := range tc.grants {
				id := int64(9)
				if tc.zero {
					id = 0
				}
				op := history.Op{Client: 0, Call: int64(10*j + 1), Return: int64(10*j + 2), Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: id, TTL: 10}}, Resp: &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 9, TTL: 10}}}
				if tc.failed {
					op.Resp = nil
					op.Err = &history.Error{Code: "Unavailable", Message: "definite"}
				}
				if tc.ambiguous {
					op.Resp = nil
					op.Err = errUnavailable
				}
				h.Ops = append(h.Ops, op)
			}
			p := len(h.Ops)
			h.Ops = append(h.Ops, history.Op{Client: 1, Call: 100, Return: 101, Req: history.Request{Put: &history.PutRequest{Key: "k", Lease: 9}}, Err: errUnavailable})
			w := newWitness(h, time.Now().Add(time.Minute))
			r := newRevisionOrder(w)
			w.search = r
			if err := r.index(); err != nil {
				t.Fatal(err)
			}
			if tc.used {
				for i := range tc.grants {
					r.use(i)
				}
			}
			post := w.live.clone()
			if tc.live {
				post.leases = post.leases.with(9)
			} else {
				post.leases = post.leases.without(9)
			}
			// Invert the parent: feasibility must use the supplied post-trial state.
			if tc.live {
				w.live.leases = w.live.leases.without(9)
			} else {
				w.live.leases = w.live.leases.with(9)
			}
			if got := r.soleLeaseGone(post, p); got != tc.want {
				t.Fatalf("got %v want %v; grant %+v", got, tc.want, r.grants[9])
			}
			r.testNoSoleGrant = true
			if r.soleLeaseGone(post, p) {
				t.Fatal("same-binary old-rule hook ignored")
			}
			r.testNoSoleGrant = false
			w.reqs[p] = history.Request{Put: &history.PutRequest{Key: "k"}}
			if r.soleLeaseGone(post, p) {
				t.Fatal("lease-zero put changed")
			}
			w.reqs[p] = history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "k", Lease: 9}}}}}
			if r.soleLeaseGone(post, p) {
				t.Fatal("transaction changed")
			}
		})
	}
}

func TestSoleGrantLongScanAndStop(t *testing.T) {
	const n = 150000
	h := &history.History{Ops: make([]history.Op, n+1)}
	h.Ops[0] = history.Op{Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9}}, Err: errUnavailable}
	for i := 1; i <= n; i++ {
		h.Ops[i] = history.Op{Req: history.Request{Put: &history.PutRequest{Key: "k", Lease: 9}}, Err: errUnavailable}
	}
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	r.countGrant(9, 0)
	r.use(0)
	r.stack = []*frame{{rev: w.live.rev}}
	k := w.live.index("k")
	r.putRevs = make([][]int64, len(w.keys))
	r.putters = make([][]int, len(w.keys))
	for i := 1; i <= n; i++ {
		r.putters[k] = append(r.putters[k], i)
	}
	if r.may(w.live, k, 10, -1, true) {
		t.Fatal("exhaustive dead-lease scan found a creator")
	}
	if w.steps < int64(n*ordinaryUnits/workUnitsPerStep) || w.workTicks < int64(n/64) {
		t.Fatalf("scan not charged/polled: %d steps, %d ticks", w.steps, w.workTicks)
	}
	w.reqs[n].Put.Lease = 0
	if !r.may(w.live, k, 10, -1, true) {
		t.Fatal("long scan missed its last conservative creator")
	}
	w.reqs[n].Put.Lease = 9
	calls := 0
	w.steps, w.workTicks = 0, 0
	r.workLeft, r.budgetLeft = 0, 0
	w.stop = func() bool { calls++; return calls == 2 }
	_ = r.may(w.live, k, 10, -1, true)
	if !w.stopped || calls != 2 || w.workTicks != 2048 {
		t.Fatalf("long scan did not stop at bounded poll: calls=%d ticks=%d", calls, w.workTicks)
	}
}

func TestSoleGrantRestoreIdentity(t *testing.T) {
	h := soleAstraHistory()
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	// Install a real ambiguous grant identity in place of the image sentinel.
	r.grants[9] = grantCount{n: 1, op: 0}
	m := r.save()
	post := w.live.clone()
	post.leases = post.leases.without(9)
	if r.soleLeaseGone(post, 2) {
		t.Fatal("unused sibling grant excluded")
	}
	r.use(0)
	if !r.soleLeaseGone(post, 2) {
		t.Fatal("consumed exact grant not excluded")
	}
	r.restore(m)
	if w.stopped || r.soleLeaseGone(post, 2) || r.used[0] {
		t.Fatal("sibling restore retained consumed grant")
	}
}

// Keep a direct scope assertion for may(delete), which computes sure.
func soleValidBranches(n int) *history.History {
	h := soleAstraHistory()
	h.Start.KVs = append(h.Start.KVs, history.KeyValue{Key: "c", CreateRevision: 2, ModRevision: 2, Version: 1})
	revoke, bad, repair, last := h.Ops[0], h.Ops[1], h.Ops[2], h.Ops[3]
	h.Ops = []history.Op{revoke}
	for j := range n {
		p := bad
		p.Client = 10 + j
		h.Ops = append(h.Ops, p)
	}
	h.Ops = append(h.Ops, repair,
		history.Op{Client: 4, Call: 3, Return: 4, Req: history.Request{Delete: &history.DeleteRequest{Key: "a"}}, Err: errUnavailable},
		history.Op{Client: 5, Call: 3, Return: 4, Req: history.Request{Delete: &history.DeleteRequest{Key: "c"}}, Err: errUnavailable})
	last.Resp = &history.Response{Revision: 5, Delete: &history.DeleteResponse{Deleted: 1}}
	h.Ops = append(h.Ops, last)
	return h
}

func TestSoleGrantPrunedBranchesAndCaps(t *testing.T) {
	for _, n := range []int{1, 4, 12} {
		h := soleValidBranches(n)
		old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
		if got.err != "pass" || compareSoleTrace(t, old, got) == 0 || got.hits == 0 {
			t.Fatal("valid branching history did not prune")
		}
		if got.steps > old.steps {
			t.Fatalf("steps increased: old %d new %d", old.steps, got.steps)
		}
		if n != 1 {
			continue
		}
		for cap := int64(1); cap <= old.steps+2; cap++ {
			a, b := runSoleOracle(h, true, true, cap), runSoleOracle(h, false, true, cap)
			if err := soleStepCheck(a, b); err != nil {
				t.Fatalf("cap %d: %v", cap, err)
			}
			if !a.timeout && !b.timeout {
				compareSoleTrace(t, a, b)
			}
			if b.timeout && (strings.Contains(b.err, "no acked op wrote") || strings.Contains(b.err, "gets ")) {
				t.Fatal("budget stop carried a history rejection")
			}
		}
	}
}

func TestSoleGrantMandatoryWriter(t *testing.T) {
	h := soleAstraHistory()
	h.Start.KVs = h.Start.KVs[1:]
	h.Ops[1].Req = history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a", Value: "token"}}, {Delete: &history.DeleteRequest{Key: "b"}}}}}
	h.Ops[3] = history.Op{Client: 3, Call: 5, Return: 6, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 3, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "token", CreateRevision: 3, ModRevision: 3, Version: 1}}}}}
	h.Ops = append(h.Ops, history.Op{Client: 3, Call: 7, Return: 8, Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}}, Resp: &history.Response{Revision: 4, Delete: &history.DeleteResponse{Deleted: 1}}})
	old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
	if err := soleVerdictCheck(old, got); err != nil {
		t.Fatal(err)
	}
	compareSoleTrace(t, old, got)
	if old.err != got.err || got.hits != 0 {
		t.Fatal("mandatory writer must retain its old rejection path")
	}
}

func TestSoleGrantTraceRejectsMissingRetainedWork(t *testing.T) {
	h := soleValidBranches(3)
	old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
	if compareSoleTrace(t, old, got) == 0 {
		t.Fatal("test needs real pruning")
	}
	bad := *got
	bad.nodes = slices.Clone(got.nodes)
	marked := false
	for i := range bad.nodes {
		if bad.nodes[i].proof != nil {
			bad.nodes[i].proof = nil
			marked = true
			break
		}
	}
	if !marked {
		t.Fatal("test needs a decisive count proof")
	}
	if _, err := soleTraceCheck(old, &bad); err == nil {
		t.Fatal("trace accepted an unmarked subtree omission")
	}
	for j, n := range got.nodes {
		if n.accepted {
			bad.nodes = append(slices.Clone(got.nodes[:j]), got.nodes[j+1:]...)
			if _, err := soleTraceCheck(old, &bad); err == nil {
				t.Fatal("trace accepted missing retained work")
			}
			return
		}
	}
	t.Fatal("test needs a retained accepted fit")
}

func TestSoleGrantRegrantAfterRevoke(t *testing.T) {
	for _, image := range []bool{false, true} {
		b := newBuilder(t, "k")
		if image {
			b = newBuilderFrom(t, &history.Start{Rev: 1, Leases: []int64{9}}, "k")
		} else {
			b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}})
		}
		b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}})
		b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}})
		b.put(1, "k", "v")
		h := b.done()
		h.Log = nil
		p := len(h.Ops)
		h.Ops = append(h.Ops, history.Op{Client: 2, Call: b.now + 1, Return: b.now + 2, Req: history.Request{Put: &history.PutRequest{Key: "k", Lease: 9}}, Err: errUnavailable})
		w := newWitness(h, time.Now().Add(time.Minute))
		r := newRevisionOrder(w)
		w.search = r
		if err := r.index(); err != nil {
			t.Fatal(err)
		}
		if !r.finish(r.first) || r.grants[9].n != 2 || !w.live.hasLease(9) {
			t.Fatal("regrant setup failed")
		}
		post := w.live.clone()
		post.leases = post.leases.without(9)
		if r.soleLeaseGone(post, p) {
			t.Fatal("regrant was treated as a consumed sole grant")
		}
		old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
		compareSoleTrace(t, old, got)
		if got.err != "pass" || got.hits != 0 {
			t.Fatalf("regrant path changed: %s", got.err)
		}
	}
}

func TestSoleGrantUnknownIDModel(t *testing.T) {
	revoke := history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}}
	unknown := history.Request{LeaseGrant: &history.LeaseGrantRequest{TTL: 10}}
	assigned := history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}}
	put := history.Request{Put: &history.PutRequest{Key: "b", Lease: 9}}
	h := &history.History{Start: &history.Start{Rev: 2, Leases: []int64{9}}, Log: []history.Request{revoke, assigned, put}, Ops: []history.Op{
		{Client: 0, Call: 1, Return: 2, Req: revoke, Resp: &history.Response{Revision: 2, LeaseRevoke: &history.LeaseRevokeResponse{}}},
		{Client: 1, Call: 3, Return: 4, Req: unknown, Err: errUnavailable},
		{Client: 2, Call: 5, Return: 6, Req: put, Resp: &history.Response{Revision: 3, Put: &history.PutResponse{}}},
	}}
	if err := Witnessed(h, FromLog, time.Minute); err != nil {
		t.Fatal(err)
	}
	old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
	compareSoleTrace(t, old, got)
	if got.err == "pass" || got.err != old.err {
		t.Fatal("ambiguous zero-ID grant changed the existing model")
	}
}

func TestSoleGrantLeasedOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(876, 3))
	hits := 0
	for range 256 {
		h := leasedHistory(rng)
		old, got := runSoleOracle(h, true, true, 2000000), runSoleOracle(h, false, true, 2000000)
		compareSoleTrace(t, old, got)
		hits += got.hits
	}
	moreHits, prunes := soleReviewCorpus(t)
	hits += moreHits
	if hits == 0 || prunes == 0 {
		t.Fatal("leased-history oracle must see genuine pruning")
	}
	t.Logf("256 lease histories plus 64 lease families, %d capability hits, %d pruned subtrees", hits, prunes)
}

func TestSoleGrantNestedExpiry(t *testing.T) {
	h := review898NestedLeaseHistory()
	old, got := runSoleOracle(h, true, true, 2000000), runSoleOracle(h, false, true, 2000000)
	compareSoleTrace(t, old, got)
}

func TestSoleGrantTrialGrantRollback(t *testing.T) {
	h := soleAstraHistory()
	h.Start.Leases = nil
	h.Ops[0] = history.Op{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}}, Err: errUnavailable}
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	// A required deletion makes this leased put fail after its trial grant.
	r.deleted = r.add(r.deleted, 3, w.live.index("a"))
	m := r.save()
	applied, rolled := false, false
	r.testTrace = func(phase string, op int) {
		if phase == "apply" && op == 0 {
			applied = true
		}
		if phase == "restore" {
			rolled = true
		}
	}
	if r.fitsGranted(2, 3) || w.stopped || !applied || !rolled {
		t.Fatal("missing real failed trial grant")
	}
	if r.used[0] || w.live.hasLease(9) {
		t.Fatal("failed trial retained its grant")
	}
	if r.soleLeaseGone(w.live, 2) {
		t.Fatal("rolled-back grant treated as spent")
	}
	// A sibling sees the grant unused; after success it is live, not dead.
	r.deleted = nil
	if !r.fitsGranted(2, 3) || !r.used[0] || !w.live.hasLease(9) || r.soleLeaseGone(w.live, 2) {
		t.Fatal("unused sibling grant cannot support a put")
	}
	r.restore(m)
	if w.stopped || r.used[0] || r.soleLeaseGone(w.live, 2) {
		t.Fatal("sibling restore lost exact grant identity")
	}
}

func TestSoleGrantSureUnchanged(t *testing.T) {
	h := soleAstraHistory()
	w := newWitness(h, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	if err := r.index(); err != nil {
		t.Fatal(err)
	}
	for k := range w.keys {
		got := r.may(w.live, k, 4, -1, false)
		r.testNoSoleGrant = true
		old := r.may(w.live, k, 4, -1, false)
		r.testNoSoleGrant = false
		if got != old {
			t.Fatal("deletion feasibility changed")
		}
	}
}
