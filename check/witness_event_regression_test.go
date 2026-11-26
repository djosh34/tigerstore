package check

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// The two review counterexample families, including the historical-zero
// header-5/revision-4 variant. A zero count may still be credited an unused
// deleter by may(), so an earlier exhaustion error is not interchangeable.
func eventZeroCountGap(t *testing.T, fillers int, deleter bool, first int64) *history.History {
	t.Helper()
	keys := []string{"a", "b"}
	for i := range fillers {
		keys = append(keys, fmt.Sprintf("f%02d", i))
	}
	var b *builder
	if first == 1 {
		b = newBuilder(t, keys...)
	} else {
		b = newBuilderFrom(t, &history.Start{Rev: first}, keys...)
	}
	for i := range fillers {
		b.put(0, fmt.Sprintf("f%02d", i), "v")
	}
	b.put(0, "a", "1")
	h := b.done()
	h.Log = nil
	c, now := b.s.rev+1, b.now
	h.Ops = append(h.Ops, history.Op{Client: 5, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "b", Value: "x"}}, Err: errUnavailable})
	if deleter {
		h.Ops = append(h.Ops, history.Op{Client: 6, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}}, Err: errUnavailable})
	}
	h.Ops = append(h.Ops,
		history.Op{Client: 1, Call: now + 10, Return: now + 20, Req: history.Request{Put: &history.PutRequest{Key: "a", Value: "2"}}, Resp: &history.Response{Revision: c + 1, Put: &history.PutResponse{}}},
		history.Op{Client: 2, Call: now + 30, Return: now + 40, Req: history.Request{Range: &history.RangeRequest{Key: "a", Revision: c}}, Resp: &history.Response{Revision: c + 1, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "1", CreateRevision: c - 1, ModRevision: c - 1, Version: 1}}}}},
		history.Op{Client: 3, Call: now + 50, Return: now + 60, Req: history.Request{Range: &history.RangeRequest{Key: "b", Revision: c}}, Resp: &history.Response{Revision: c + 1, Range: &history.RangeResponse{Count: 0}}},
	)
	return h
}

func eventCreatedLaterGap(t *testing.T, fillers int) *history.History {
	t.Helper()
	keys := []string{"a", "b"}
	for i := range fillers {
		keys = append(keys, fmt.Sprintf("f%02d", i))
	}
	b := newBuilder(t, keys...)
	for i := range fillers {
		b.put(0, fmt.Sprintf("f%02d", i), "v")
	}
	b.put(0, "a", "1")
	h := b.done()
	h.Log = nil
	c, now := b.s.rev+1, b.now
	h.Ops = append(h.Ops,
		history.Op{Client: 5, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "b", Value: "x"}}, Err: errUnavailable},
		history.Op{Client: 6, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}}, Err: errUnavailable},
		history.Op{Client: 1, Call: now + 10, Return: now + 20, Req: history.Request{Put: &history.PutRequest{Key: "a", Value: "2"}}, Resp: &history.Response{Revision: c + 1, Put: &history.PutResponse{}}},
		history.Op{Client: 1, Call: now + 30, Return: now + 40, Req: history.Request{Put: &history.PutRequest{Key: "b", Value: "y"}}, Resp: &history.Response{Revision: c + 2, Put: &history.PutResponse{}}},
		history.Op{Client: 2, Call: now + 50, Return: now + 60, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: "c"}}, Resp: &history.Response{Revision: c + 2, Range: &history.RangeResponse{Count: 2, KVs: []history.KeyValue{{Key: "a", Value: "2", CreateRevision: c - 1, ModRevision: c + 1, Version: 2}, {Key: "b", Value: "y", CreateRevision: c + 2, ModRevision: c + 2, Version: 1}}}}},
		history.Op{Client: 3, Call: now + 50, Return: now + 60, Req: history.Request{Range: &history.RangeRequest{Key: "a", Revision: c}}, Resp: &history.Response{Revision: c + 2, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "1", CreateRevision: c - 1, ModRevision: c - 1, Version: 1}}}}},
	)
	return h
}

func eventWriteUndoHistory() *history.History {
	return &history.History{Start: &history.Start{Rev: 2, KVs: []history.KeyValue{{Key: "a", Value: "v", CreateRevision: 2, ModRevision: 2, Version: 1}}}, Ops: []history.Op{
		{Client: 0, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "a", End: history.AllKeys}}, Err: errUnavailable},
		{Client: 1, Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}, Resp: &history.Response{Revision: 3, Range: &history.RangeResponse{}}},
		{Client: 0, Call: 5, Return: 6, Req: history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "v"}}, {Delete: &history.DeleteRequest{Key: "a", End: history.AllKeys}}}}}, Err: errUnavailable},
		{Client: 1, Call: 7, Return: 8, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}, Resp: &history.Response{Revision: 4, Range: &history.RangeResponse{}}},
	}}
}

func TestEventCertificateReviewHistories(t *testing.T) {
	undo := eventWriteUndoHistory()
	old, got := runEventOracle(undo, true, 2_000_000, nil, false), runEventOracle(undo, false, 2_000_000, nil, false)
	compareEventRuns(t, old, got)
	if got.err != "pass" || len(got.claims) != 0 {
		t.Fatal("write-then-undo is the only writer and must pass via fallback")
	}
	for _, fillers := range []int{0, 10, 11, 12} {
		for _, deleter := range []bool{false, true} {
			h := eventZeroCountGap(t, fillers, deleter, 1)
			old, got = runEventOracle(h, true, 2_000_000, nil, false), runEventOracle(h, false, 2_000_000, nil, false)
			compareEventRuns(t, old, got)
			if got.err == "pass" || !strings.Contains(got.err, "b") {
				t.Fatal("zero-count counterexample must retain its failure")
			}
		}
		h := eventCreatedLaterGap(t, fillers)
		old, got = runEventOracle(h, true, 2_000_000, nil, false), runEventOracle(h, false, 2_000_000, nil, false)
		compareEventRuns(t, old, got)
	}
	h := eventZeroCountGap(t, 0, true, 2)
	old, got = runEventOracle(h, true, 2_000_000, nil, false), runEventOracle(h, false, 2_000_000, nil, false)
	compareEventRuns(t, old, got)
	if !strings.Contains(got.err, `"revision":4`) {
		t.Fatalf("header-5 historical-zero explanation changed: %s", got.err)
	}
	// roomNeed's 16/17 boundary is unrelated to certification; leave both
	// searches' room policy and full explanations unchanged.
	for _, need := range []int{16, 17} {
		h := &history.History{Start: &history.Start{Rev: 2, KVs: []history.KeyValue{{Key: "a", Value: "v", CreateRevision: 2, ModRevision: 2, Version: 1}}}}
		for i := range need {
			h.Ops = append(h.Ops, history.Op{Client: i, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "a", Value: "v"}}, Err: errUnavailable})
		}
		h.Ops = append(h.Ops, history.Op{Client: need, Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: int64(need + 2), Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "v", CreateRevision: 2, ModRevision: int64(need + 2), Version: int64(need + 1)}}}}})
		setup := func(r *revisionOrder) {
			total := 0
			for _, n := range r.roomNeed(r.w.live, int64(need+2)) {
				total += n
			}
			if total != need {
				t.Fatalf("room need %d, want %d", total, need)
			}
		}
		old, got = runEventOracle(h, true, 2_000_000, setup, false), runEventOracle(h, false, 2_000_000, setup, false)
		compareEventRuns(t, old, got)
	}
}

func eventDeadPair() *history.History {
	st := &history.Start{Rev: 2, KVs: []history.KeyValue{{Key: "a", Value: "a", CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: "z", Value: "z", CreateRevision: 2, ModRevision: 2, Version: 1}}}
	return &history.History{Start: st, Ops: []history.Op{
		{Client: 0, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "a"}}, Err: errUnavailable},
		{Client: 1, Call: 2, Return: 3, Req: history.Request{Delete: &history.DeleteRequest{Key: "a"}}, Err: errUnavailable},
		{Client: 2, Call: 3, Return: 4, Req: eventCAS("a", 999, "a"), Err: errUnavailable},
		{Client: 3, Call: 2000, Return: 2001, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}}, Resp: &history.Response{Revision: 4, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{st.KVs[1]}}}},
	}}
}

func TestEventCertificateFailureOrdering(t *testing.T) {
	for _, deeper := range []bool{false, true} {
		setup := func(r *revisionOrder) {
			if deeper {
				r.failure = errors.New("prior deeper rejection")
				r.at = 100
			}
		}
		old, got := runEventOracle(eventDeadPair(), true, 2_000_000, setup, false), runEventOracle(eventDeadPair(), false, 2_000_000, setup, false)
		compareEventRuns(t, old, got)
		if len(got.claims) != 2 {
			t.Fatalf("expected two dead child prefixes, got %d", len(got.claims))
		}
		if deeper {
			if !strings.Contains(got.err, "prior deeper rejection") {
				t.Fatal(got.err)
			}
		} else if !strings.Contains(got.err, "op 1,") || !strings.Contains(got.err, "revision 4") {
			t.Fatalf("equal-revision tie must name the later parent op: %s", got.err)
		}
	}
	h := eventDeadHistory(1, 6)
	// Fail finish(first) before any child can be constructed.
	h.Ops[len(h.Ops)-1].Resp.Revision = 10
	h.Ops[len(h.Ops)-1].Resp.Range.KVs[0].Value = "wrong"
	old, got := runEventOracle(h, true, 2_000_000, nil, false), runEventOracle(h, false, 2_000_000, nil, false)
	compareEventRuns(t, old, got)
	if len(got.claims) != 0 || got.err == "pass" || strings.Contains(got.err, "no acked op wrote") {
		t.Fatalf("finish(parent) failure was moved: %s", got.err)
	}
}

func TestEventCertificateLateChildEarlierParent(t *testing.T) {
	h := eventDeadPair()
	h.Ops[1].Req = eventCAS("a", 2, "a")
	h.Ops[1].Call = 10
	h.Ops[1].Return = 11
	h.Ops[2].Req = history.Request{Delete: &history.DeleteRequest{Key: "a"}}
	h.Ops[2].Call = 1000
	h.Ops[2].Return = 1001
	for i := range 130 {
		id := int64(i + 1)
		h.Start.Leases = append(h.Start.Leases, id)
		h.Ops = append(h.Ops, history.Op{Client: i + 4, Call: 0, Return: 1, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: id}}, Err: errUnavailable})
	}
	late, rewound := false, false
	setup := func(r *revisionOrder) {
		for i := range 130 {
			r.optional = append(r.optional, optional{op: i + 4, late: true, from: 1000, gone: int64(i + 1), by: 1000})
		}
		prev := r.testTrace
		r.testTrace = func(phase string, op int) {
			if phase == "exhaust" && r.rev == 4 && r.dueWork.timeAt == 130 {
				late = true
			}
			if phase == "apply" && op == 1 && r.dueWork.timeAt < 64 {
				rewound = true
			}
			prev(phase, op)
		}
	}
	old, got := runEventOracle(h, true, 2_000_000, setup, false), runEventOracle(h, false, 2_000_000, setup, false)
	compareEventRuns(t, old, got)
	if got.err != "pass" || len(got.claims) == 0 || !late || !rewound {
		t.Fatalf("missing late-child/earlier-parent coverage: %s hits=%d late=%v rewind=%v", got.err, len(got.claims), late, rewound)
	}
}

func TestEventCertificateMaxStepsSweep(t *testing.T) {
	for _, h := range []*history.History{eventDeadHistory(1, 12), eventDeadPair(), eventWriteUndoHistory()} {
		base := runEventOracle(h, true, 2_000_000, nil, false)
		if base.timeout {
			t.Fatal("sweep reference did not complete")
		}
		for k := int64(1); k <= base.steps+2; k++ {
			old, got := runEventOracle(h, true, k, nil, false), runEventOracle(h, false, k, nil, false)
			if !old.timeout && (got.timeout || got.steps > k || got.err != old.err) {
				t.Fatalf("cap %d: old %s (%d), new %s (%d)", k, old.err, old.steps, got.err, got.steps)
			}
			if !old.timeout && len(got.claims) == 0 && got.steps != old.steps {
				t.Fatalf("all-unknown charges differ at cap %d: old %d new %d", k, old.steps, got.steps)
			}
		}
	}
}

func TestEventCertificatePoolScale(t *testing.T) {
	for _, n := range []int{128, 43726} {
		h := eventDeadHistory(1, 1)
		q := h.Ops[0]
		q.Req = history.Request{Delete: &history.DeleteRequest{Key: "a"}}
		q.Err = errUnavailable
		read := h.Ops[len(h.Ops)-1]
		h.Ops = nil
		for i := range n {
			q.Client = i
			h.Ops = append(h.Ops, q)
		}
		read.Client = n
		h.Ops = append(h.Ops, read)
		// Independent construction-only baseline, with exactly the same
		// mark and original enumeration/sort, but no fits probes. Merely
		// staying below the old whole-search cost admits a second scan.
		buildW := newWitness(h, time.Now().Add(time.Minute))
		buildR := newRevisionOrder(buildW)
		buildW.search = buildR
		if err := buildR.index(); err != nil {
			t.Fatal(err)
		}
		if !buildR.finish(buildR.first) {
			t.Fatal(buildR.failure)
		}
		beforeBuild := buildW.steps
		_ = buildR.save()
		_ = oldEventCandidates(buildR, 11)
		constructionSteps := buildW.steps - beforeBuild
		var steps [2]int64
		for j, old := range []bool{true, false} {
			w := newWitness(h, time.Now().Add(time.Minute))
			r := newRevisionOrder(w)
			w.search = r
			r.testNoEventCertificate = old
			if old {
				r.testCandidates = func(rev int64) []int { return oldEventCandidates(r, rev) }
			}
			if err := r.index(); err != nil {
				t.Fatal(err)
			}
			if !r.finish(r.first) {
				t.Fatal(r.failure)
			}
			probes := 0
			r.testTrace = func(phase string, _ int) {
				if phase == "candidate" {
					probes++
				}
			}
			before := w.steps
			if r.write(11) || w.stopped {
				t.Fatal("dead pool unexpectedly wrote/stopped")
			}
			steps[j] = w.steps - before
			if old && probes != n || !old && probes != 0 {
				t.Fatalf("old=%v pool=%d probes=%d", old, n, probes)
			}
		}
		if steps[1] != constructionSteps {
			t.Fatalf("pool %d: certificate hit costs %d steps, construction-only baseline %d; extra pool work is forbidden", n, steps[1], constructionSteps)
		}
		if steps[1] >= steps[0] {
			t.Fatalf("pool %d: old %d new %d", n, steps[0], steps[1])
		}
		t.Logf("pool %d: old %d new %d weighted steps, no extra pool scan", n, steps[0], steps[1])
	}
}

// No production stop hook or accounting callback is inherited by the oracle.
// The two witnesses share only immutable history inputs, not caches/generations.
func TestEventOracleIsolation(t *testing.T) {
	h := eventDeadHistory(4, 12)
	old := runEventOracle(h, true, 2_000_000, nil, false)
	got := runEventOracle(h, false, 2_000_000, nil, false)
	compareEventRuns(t, old, got)
	old.r.w.stopped = true
	old.r.w.lastID = 999
	clear(old.r.used)
	old.r.w.at = nil
	if got.r.w.stopped || got.r.w.lastID == 999 || len(got.r.w.at) == 0 || !slices.Equal(got.r.used, got.trace[len(got.trace)-1].used) {
		t.Fatal("oracle and claimant share mutable state")
	}
}
