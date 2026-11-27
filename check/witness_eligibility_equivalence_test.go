package check

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// preEligibilityQuery is d90be96b's retained bitmap query, before #894:
// no from threshold/intersection. Keep this path in tests, not production.
// The candidate/time/revision indexes and snapshots are otherwise identical.
func preEligibilityQuery(r *revisionOrder, call, rev int64, yield func(optional) bool) {
	if r.dueWork == nil {
		r.dueWork = r.newDueWork()
	}
	d := r.dueWork
	if r.step() {
		return
	}
	if !d.valid {
		d.reset(r)
	}
	if d.remaining == 0 || r.w.stopped {
		return
	}
	timed, revision := d.zeroTime, d.zeroRev
	timedLeft, revisionLeft := d.zeroTimeLeft, d.zeroRevLeft
	if call != 0 {
		d.move(r, d.gone, &d.timeAt, d.timed, &d.timedLeft, func(o optional) bool { return o.gone < call })
		timed, timedLeft = d.timed, d.timedLeft
	}
	if rev != 0 {
		d.move(r, d.by, &d.revAt, d.revision, &d.revisionLeft, func(o optional) bool { return o.by <= rev })
		revision, revisionLeft = d.revision, d.revisionLeft
	}
	if timedLeft == 0 && revisionLeft == 0 {
		return
	}
	compacted := r.w.live.compacted
	var snapshot []uint64
	if n := len(d.buffers); n > 0 {
		snapshot, d.buffers = d.buffers[n-1], d.buffers[:n-1]
	} else {
		snapshot = make([]uint64, len(d.available))
	}
	defer func() { d.buffers = append(d.buffers, snapshot) }()
	for j := range snapshot {
		if r.step() {
			return
		}
		snapshot[j] = (timed[j] | revision[j]) & d.available[j]
	}
	for j, word := range snapshot {
		if r.step() {
			return
		}
		for word != 0 {
			if r.step() {
				return
			}
			k := j*64 + bits.TrailingZeros64(word)
			word &= word - 1
			o := d.candidates[k]
			c := r.w.reqs[o.op].Compact
			if r.used[o.op] || c != nil && c.Revision <= compacted {
				d.remove(k)
				continue
			}
			if !yield(o) {
				return
			}
		}
	}
}

// Search steps are semantic turns, not weighted accounting ticks (the
// optimization deliberately removes bookkeeping/trials that cannot apply).
// Capture every revision, candidate considered, fit, apply and restore, with
// the complete order, replay summaries, key/lease state and op placements.
// Comparing only the final witness would miss reordered failed branches.
type eligibilitySearchStep struct {
	phase     string
	op        int
	searchRev int64
	rev       int64
	compacted int64
	keys      []history.KeyValue
	leases    []int64
	events    []event
	states    []summary
	at        []place
	placed    []int
	used      []bool
	firsts    []firstAnswer
	frames    [][2]int64
}

func eligibilitySearch(t *testing.T, h *history.History, old bool) (string, []eligibilitySearchStep, int, int) {
	t.Helper()
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	w := newWitness(h, time.Now().Add(time.Minute))
	w.maxSteps = 2_000_000 // A runaway generated case fails, never compares timeouts.
	r := newRevisionOrder(w)
	// This gate isolates #894's eligibility change. The event certificate
	// has its own independent old-enumeration oracle and trace gate.
	r.testNoEventCertificate = true
	w.search = r
	future := 0
	if old {
		r.testDueQuery = func(call, rev, from int64, yield func(optional) bool) {
			preEligibilityQuery(r, call, rev, func(o optional) bool {
				if o.from > from {
					future++
				}
				return yield(o)
			})
		}
	}
	var trace []eligibilitySearchStep
	r.testTrace = func(phase string, op int) {
		x := eligibilitySearchStep{phase: phase, op: op, searchRev: r.rev, rev: w.live.rev, compacted: w.live.compacted}
		// Candidate/fit probes record their exact order and frame cursors.
		// Snapshot state at every turn/apply/restore/done instead of copying
		// the unchanged order for each rejected probe in a hard gap.
		if phase != "candidate" && phase != "fit" {
			x.keys, x.events, x.states = slices.Clone(w.live.kvs), slices.Clone(w.events), slices.Clone(w.states)
			x.at, x.placed, x.used, x.firsts = slices.Clone(w.at), slices.Clone(w.placed), slices.Clone(r.used), slices.Clone(w.firsts)
			w.live.leases.each(func(id int64) { x.leases = append(x.leases, id) })
		}
		for _, f := range r.stack {
			x.frames = append(x.frames, [2]int64{f.rev, int64(f.next)})
		}
		trace = append(trace, x)
	}
	err := r.index()
	if err == nil {
		err = r.search()
	}
	r.testTrace("done", -1)
	if errors.Is(err, ErrWitnessTimeout) || w.stopped {
		t.Fatalf("generated history did not complete (old=%v, steps=%d): %v", old, w.steps, err)
	}
	verdict := "pass"
	if err != nil {
		verdict = fmt.Sprintf("witness check (%s) of %d operations: %s", FromRevisions, len(h.Ops), err)
	}
	return verdict, trace, future, r.backs
}

// legacyUsedSnapshots is the pre-#878 representation, independent of
// packedUsed/restoreUsed. The surrounding charged loops are unchanged.
func legacyUsedSnapshots(r *revisionOrder) {
	r.testKeepPopped = true // The old pop kept backing slots, too.
	saved := map[*state][]bool{}
	r.testSaveUsed = func(m *mark) { saved[m.live] = slices.Clone(r.used) }
	r.testRestoreUsed = func(m mark) { r.used = slices.Clone(saved[m.live]) }
}

// snapshotOracle checks the old boolean representation at every boundary
// of the existing search, instead of replaying that search twice more.
// Equal saved/restored flags imply identical subsequent choices; unchanged
// accounting/poll state preserves the weighted trace by induction. All
// recorded/generated cases use this, including in the coverage job's
// non-race -short pass (the 2,048-history gate does not shrink under -short).
func snapshotOracle(t *testing.T) Option {
	t.Helper()
	return func(o *options) {
		o.testRevision = func(r *revisionOrder) {
			saved := map[*state][]bool{}
			uncharged := func(run func()) {
				steps, ticks := r.w.steps, r.w.workTicks
				work, budget, stopped := r.workLeft, r.budgetLeft, r.w.stopped
				run()
				if r.w.steps != steps || r.w.workTicks != ticks || r.workLeft != work || r.budgetLeft != budget || r.w.stopped != stopped {
					t.Fatal("snapshot representation changed accounting or stop/poll state")
				}
			}
			r.testSaveUsed = func(m *mark) {
				uncharged(func() {
					old := slices.Clone(r.used)
					m.used = packedUsed(r.used)
					if len(m.used) != (len(old)+63)/64 {
						t.Fatal("packed snapshot dropped a partial word")
					}
					for i, want := range old {
						if got := m.used[i/64]&(uint64(1)<<uint(i%64)) != 0; got != want {
							t.Fatalf("saved operation %d differs from the old snapshot", i)
						}
					}
					saved[m.live] = old
				})
			}
			r.testRestoreUsed = func(m mark) {
				uncharged(func() {
					want, ok := saved[m.live]
					if !ok {
						t.Fatal("restore has no independent old snapshot")
					}
					restoreUsed(r.used, m.used)
					if !slices.Equal(r.used, want) {
						t.Fatal("restored flags differ from the old snapshot")
					}
				})
			}
		}
	}
}

// Keep a small explicit two-search weighted/semantic-trace comparison as
// well as the full corpus's per-boundary oracle, without duplicating the
// entire checker race workload.
func TestPackedUsedLegacySearchTrace(t *testing.T) {
	compareUsedSnapshots(t, budgetHistory(t, "seed-59113215.json"))
}

func compareUsedSnapshots(t *testing.T, h *history.History) {
	t.Helper()
	type result struct {
		verdict string
		steps   int64
		trace   [32]byte
	}
	run := func(old bool) result {
		digest := sha256.New()
		var out result
		setup := func(r *revisionOrder) {
			if old {
				legacyUsedSnapshots(r)
			}
			r.testTrace = func(phase string, op int) {
				_, _ = fmt.Fprintf(digest, "%s %d %d %d %d %d %d\n", phase, op, r.w.steps, r.rev, r.w.live.rev, len(r.stack), len(r.w.events))
				if phase != "candidate" && phase != "fit" {
					_, _ = fmt.Fprintf(digest, "%#v %#v\n", r.w.live.kvs, r.w.placed)
					r.w.live.leases.each(func(id int64) { _, _ = fmt.Fprintf(digest, "lease %d\n", id) })
				}
			}
		}
		err := Witnessed(h, FromRevisions, time.Minute, StepsUsed(&out.steps), func(o *options) { o.testRevision = setup })
		out.verdict = "pass"
		if err != nil {
			out.verdict = err.Error()
		}
		if errors.Is(err, ErrWitnessTimeout) {
			t.Fatalf("snapshot equivalence must complete: %v", err)
		}
		copy(out.trace[:], digest.Sum(nil))
		return out
	}
	old, packed := run(true), run(false)
	if old != packed {
		t.Fatalf("snapshot representation changed verdict, weighted steps or trace: old %+v, packed %+v", old, packed)
	}
}

func TestPackedUsedRetentionAndRestore(t *testing.T) {
	for _, n := range []int{64, 0, 1, 63, 65, 4097, 1 << 20} {
		w := &witness{live: newState(nil, nil), states: make([]summary, 1), at: make([]place, n), deadline: time.Now().Add(time.Hour)}
		r := &revisionOrder{w: w, used: make([]bool, n)}
		for i := range r.used {
			r.used[i] = i%13 == 0 || i == n-1
		}
		want := slices.Clone(r.used)
		m := r.save()
		bytes := int64(cap(m.used)) * int64(reflect.TypeOf(m.used).Elem().Size())
		if bytes > int64((n+63)/64)*8 || w.steps != 0 {
			t.Fatalf("n=%d snapshot retains %d bytes or charges %d steps", n, bytes, w.steps)
		}
		var address *bool
		if n > 0 {
			address = &r.used[0]
		}
		for i := range r.used {
			r.used[i] = true
		}
		r.restore(m)
		wantSteps := int64((n*ordinaryUnits + workUnitsPerStep - 1) / workUnitsPerStep)
		if w.stopped || !slices.Equal(r.used, want) || w.steps != wantSteps {
			t.Fatalf("n=%d restore differs or uses %d steps, want %d", n, w.steps, wantSteps)
		}
		if n > 0 && &r.used[0] != address {
			t.Fatal("restore allocated a full working bitmap instead of reusing it")
		}
		if n > 4096 {
			clear(r.used)
			w.steps, w.maxSteps = 0, 1
			r.workLeft, r.budgetLeft = 0, 0
			r.restore(m)
			if !w.stopped || slices.Contains(r.used, true) {
				t.Fatal("a stopped charged scan crossed the used-restore boundary")
			}
		}
	}
}

func TestExhaustedFrameDropsBackingSlot(t *testing.T) {
	w := newWitness(&history.History{}, time.Now().Add(time.Minute))
	r := newRevisionOrder(w)
	w.search = r
	f := &frame{rev: 2, cands: make([]int, 4096), next: 4096, mark: r.save()}
	r.stack = append(r.stack, f)
	backing := r.stack[:cap(r.stack)]
	if r.write(2) || len(r.stack) != 0 || w.stopped {
		t.Fatal("test must exhaust and pop its gap")
	}
	if backing[0] != nil {
		t.Fatal("popped frame still pins candidates, snapshot and live state through stack capacity")
	}
}

// eligibilityHistory draws either a lease/compaction-heavy workload or a
// mutation-heavy one (2/3 of mutations ambiguous, half of those applied).
// Half start from an image of a random prefix, including live/empty leases
// and aged key versions. Calls overlap, grants reuse IDs, and final reads
// force the hidden writes to have observable effects.
func eligibilityHistory(seed uint64, leased, image bool) *history.History {
	rng := rand.New(rand.NewPCG(seed, 876))
	keys := []string{"a", "b", "c", "d"}
	st := newState(keys, nil)
	h := &history.History{}
	request := func(i int) history.Request {
		key := keys[rng.IntN(len(keys))]
		id := 1 + rng.Int64N(4)
		kind := rng.IntN(8)
		if !leased {
			kind %= 4
		}
		switch kind {
		case 0:
			p := &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i), PrevKV: true}
			if leased && rng.IntN(2) == 0 {
				p.Lease = id
			}
			return history.Request{Put: p}
		case 1:
			d := &history.DeleteRequest{Key: key, PrevKV: rng.IntN(2) == 0}
			if rng.IntN(3) == 0 {
				d.End = history.AllKeys
			}
			return history.Request{Delete: d}
		case 2:
			return history.Request{Txn: &history.TxnRequest{
				Compare: []history.Compare{{Key: key, Target: history.TargetVersion, Result: history.Equal, Number: rng.Int64N(3)}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: fmt.Sprintf("tx%d", i), Lease: id}}},
				Failure: []history.TxnOp{{Delete: &history.DeleteRequest{Key: key}}},
			}}
		case 3:
			return history.Request{Range: &history.RangeRequest{Key: key}}
		case 4:
			return history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: id, TTL: 10}}
		case 5:
			return history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: id}}
		case 6:
			return history.Request{Compact: &history.CompactRequest{Revision: 1 + rng.Int64N(st.rev)}}
		default:
			return history.Request{Range: &history.RangeRequest{Key: key, Revision: st.rev}}
		}
	}
	if image {
		for i := range 20 {
			st.step(request(-i-1), false)
		}
		start := &history.Start{Rev: st.rev}
		for _, kv := range st.kvs {
			if kv.Version != 0 {
				start.KVs = append(start.KVs, kv)
			}
		}
		st.leases.each(func(id int64) { start.Leases = append(start.Leases, id) })
		h.Start = start
		st = newState(keys, start) // Image starts are compacted at the image revision.
	}
	for i := range 32 {
		req := request(i)
		op := history.Op{Client: i % 4, Call: int64(i*100+100) - rng.Int64N(120), Return: int64(i*100+200) + rng.Int64N(120), Req: req}
		if req.Mutates() && rng.IntN(3) != 0 {
			op.Err = errUnavailable
			if rng.IntN(2) == 0 {
				st.step(req, false)
			}
		} else {
			o := st.step(req, false)
			op.Resp, op.Err = o.resp, o.err
		}
		h.Ops = append(h.Ops, op)
		// Bound the unobserved stretches, not the number of search turns:
		// a corrupt final count must not build a huge permutation corpus
		// in CI (especially with the race detector's allocation overhead).
		if i%8 == 7 {
			read := history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}
			out := st.step(read, false)
			at := int64(i*100 + 201)
			h.Ops = append(h.Ops, history.Op{Client: 9, Call: at, Return: at + 1, Req: read, Resp: out.resp, Err: out.err})
		}
	}
	req := history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys, CountOnly: true}}
	o := st.step(req, false)
	h.Ops = append(h.Ops, history.Op{Client: 4, Call: 10_000, Return: 10_001, Req: req, Resp: o.resp, Err: o.err})
	return h
}

func TestEligibilityWholeSearchEquivalence(t *testing.T) {
	const seeds = 256
	histories, failures, future, backs := 0, 0, 0, 0
	for seed := range uint64(seeds) {
		for _, leased := range []bool{false, true} {
			for _, image := range []bool{false, true} {
				h := eligibilityHistory(seed, leased, image)
				for _, corrupt := range []bool{false, true} {
					if corrupt {
						// Generate a fresh deep copy; neither checker may modify
						// the other's input or the uncorrupted case.
						data, err := json.Marshal(h)
						if err != nil {
							t.Fatal(err)
						}
						h = &history.History{}
						if err := json.Unmarshal(data, h); err != nil {
							t.Fatal(err)
						}
						rng := rand.New(rand.NewPCG(seed, 894))
						if !staleRead(rng, h) {
							h.Ops[len(h.Ops)-1].Resp.Range.Count++
						}
					}
					want, oldTrace, omitted, backtracks := eligibilitySearch(t, h, true)
					got, trace, _, _ := eligibilitySearch(t, h, false)
					label := fmt.Sprintf("seed=%d leased=%v image=%v corrupt=%v", seed, leased, image, corrupt)
					if got != want {
						t.Fatalf("%s: got %q, pre-intersection %q", label, got, want)
					}
					if !reflect.DeepEqual(trace, oldTrace) {
						for i := 0; i < min(len(trace), len(oldTrace)); i++ {
							if !reflect.DeepEqual(trace[i], oldTrace[i]) {
								t.Fatalf("%s: search step %d differs (new %s/%d, old %s/%d)", label, i, trace[i].phase, trace[i].op, oldTrace[i].phase, oldTrace[i].op)
							}
						}
						t.Fatalf("%s: search lengths differ: new %d, old %d", label, len(trace), len(oldTrace))
					}
					public := "pass"
					if err := Witnessed(h, FromRevisions, time.Minute, snapshotOracle(t)); err != nil {
						public = err.Error()
					}
					if got != public {
						t.Fatalf("%s: instrumented %q, public %q", label, got, public)
					}
					histories++
					if got != "pass" {
						failures++
					}
					future += omitted
					backs += backtracks
				}
			}
		}
	}
	if histories != seeds*8 || failures == 0 || failures == histories || future == 0 || backs == 0 {
		t.Fatalf("insufficient coverage: histories=%d failures=%d future=%d backtracks=%d", histories, failures, future, backs)
	}
	t.Logf("%d histories: exact verdicts/explanations and step-for-step semantic search traces; %d failures, %d omitted future visits, %d backtracks", histories, failures, future, backs)
}
