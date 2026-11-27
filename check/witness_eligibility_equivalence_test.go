package check

import (
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
					if err := Witnessed(h, FromRevisions, time.Minute); err != nil {
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
