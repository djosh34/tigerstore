package check

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// From Opus's /tmp/rev918-probe_test.go.txt. Extend #905's review families
// with an independent image range and its full-count obligation beyond the
// horizon. The early ambiguous delete gives survival a real subtree to prune.
func survivalJointHistory(seed uint64, keys int) *history.History {
	h := soleReviewGenerated(seed)
	if h.Start == nil {
		h.Start = &history.Start{Rev: 2}
	}
	top, last := h.Start.Rev, int64(0)
	for _, op := range h.Ops {
		if op.Resp != nil {
			top = max(top, op.Resp.Revision)
		}
		last = max(last, op.Return)
	}
	for j := range keys {
		h.Start.KVs = append(h.Start.KVs, history.KeyValue{Key: fmt.Sprintf("s/%d", j), CreateRevision: h.Start.Rev, ModRevision: h.Start.Rev, Version: 1})
	}
	slices.SortFunc(h.Start.KVs, func(a, b history.KeyValue) int { return strings.Compare(a.Key, b.Key) })
	h.Ops = append(h.Ops, history.Op{Client: 50, Call: 1, Return: 2, Req: history.Request{Delete: &history.DeleteRequest{Key: "s/", End: "s0"}}, Err: errUnavailable})
	for j := range 66 {
		c := last + 10 + int64(j)*2
		h.Ops = append(h.Ops, history.Op{Client: 51, Call: c, Return: c + 1, Req: history.Request{Put: &history.PutRequest{Key: "zz", Value: strconv.Itoa(j)}}, Resp: &history.Response{Revision: top + 1 + int64(j), Put: &history.PutResponse{}}})
	}
	c := last + 500
	h.Ops = append(h.Ops, history.Op{Client: 52, Call: c, Return: c + 1, Req: history.Request{Delete: &history.DeleteRequest{Key: "s/", End: "s0"}}, Resp: &history.Response{Revision: top + 67, Delete: &history.DeleteResponse{Deleted: int64(keys)}}})
	return h
}

// Fresh both-off run without either feature's oracle callbacks. Its public
// return and all dead calls independently check the end-to-end composition.
func runSurvivalBothOff(h *history.History) *soleRun {
	out := newSoleRun(h)
	var r *revisionOrder
	setup := func(o *options) {
		o.testRevision = func(v *revisionOrder) {
			r = v
			r.testNoSoleGrant, r.testNoSurvival = true, true
			r.testRejection = func(rev int64, err error) { out.rejections[soleRejection{rev, err.Error()}] = true }
		}
	}
	err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(2000000), StepsUsed(&out.steps), setup)
	out.err = "pass"
	if err != nil {
		out.err = err.Error()
	}
	out.publicErr = err
	out.timeout = errors.Is(err, ErrWitnessTimeout)
	if r != nil {
		out.at, out.stopped = r.at, r.w.stopped
		if r.failure != nil {
			out.failure = r.failure.Error()
		}
	}
	return out
}

func TestSurvivalJointSoleGrant(t *testing.T) {
	histories, both, solePrunes, survivalPrunes := 0, 0, 0, 0
	for seed := range uint64(64) {
		for _, keys := range []int{1, 3} {
			h := survivalJointHistory(seed, keys)
			off := runSurvivalBothOff(h)
			soleOff, soleOnly := runSoleOracle(h, true, true, 2000000), runSoleOracle(h, false, true, 2000000)
			survivalOff, prod := runSurvival(h, true, true, 2000000), runSurvival(h, false, true, 2000000)
			n, err := soleTraceCheck(soleOff, soleOnly)
			if err != nil {
				t.Fatalf("seed %d keys %d: #905 leg: %v", seed, keys, err)
			}
			solePrunes += n
			n, err = survivalCheck(survivalOff, prod)
			if err != nil {
				t.Fatalf("seed %d keys %d: survival leg: %v", seed, keys, err)
			}
			survivalPrunes += n
			if off.err != soleOff.err || off.steps != soleOff.steps || soleOnly.err != survivalOff.err || soleOnly.steps != survivalOff.steps {
				t.Fatalf("seed %d keys %d: oracle legs do not join", seed, keys)
			}
			// Compose the two separately enforced work allowances; do not give the
			// direct comparison uncharged reference work or accept stopped runs.
			direct := *prod.soleRun
			direct.extraUnits += soleOnly.extraUnits
			if err := soleVerdictCheck(off, &direct); err != nil {
				t.Fatalf("seed %d keys %d: direct both-off comparison: %v", seed, keys, err)
			}
			if prod.at > off.at {
				t.Fatal("production selected a later rejection")
			}
			if prod.soleHits > 0 && prod.hits > 0 {
				both++
			}
			histories++
		}
	}
	if histories != 128 || both != 76 || solePrunes == 0 || survivalPrunes == 0 {
		t.Fatalf("joint corpus lacks actual co-hits/pruning: both %d, #905 prunes %d, survival prunes %d", both, solePrunes, survivalPrunes)
	}
	t.Logf("%d joint histories: %d with both rules firing in production; #905 prunes %d, survival prunes %d", histories, both, solePrunes, survivalPrunes)
}
