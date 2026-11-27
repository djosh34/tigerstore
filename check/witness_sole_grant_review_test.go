package check

import (
	"errors"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func soleMixedHistory(copies int) *history.History {
	h := soleAstraHistory()
	repair := h.Ops[2]
	for j := range copies {
		p := repair
		p.Client = 5 + j
		h.Ops = append(h.Ops, p)
	}
	h.Ops = append(h.Ops, history.Op{Client: 4, Call: 3, Return: 4, Req: history.Request{Put: &history.PutRequest{Key: "b"}}, Err: errUnavailable})
	return h
}

func TestSoleGrantMixedPutterTrace(t *testing.T) {
	h := soleMixedHistory(0)
	old, got := runSoleOracle(h, true, true, 100000), runSoleOracle(h, false, true, 100000)
	if got.err != "pass" || got.hits == 0 || compareSoleTrace(t, old, got) != 0 {
		t.Fatal("mixed putters should retain the losing range-delete branch")
	}
	// A visit is not a count proof: the unleased survivor keeps b creatable.
	for _, n := range got.nodes {
		if n.state.phase == "fit" && n.state.op == 1 && (!n.accepted || n.proof != nil) {
			t.Fatal("feasible range delete marked as prunable")
		}
	}
	losingApply, losingDead := -1, -1
	for i, n := range got.nodes {
		if n.state.phase == "apply" && n.state.op == 3 && n.state.rev == 3 {
			losingApply = i
		}
		if n.state.phase == "dead" && n.state.searchRev == 4 {
			losingDead = i
		}
	}
	if losingApply < 0 || losingDead <= losingApply {
		t.Fatal("retained losing branch lacks mandatory application/rejection")
	}
	bad := *got
	bad.nodes = nil
	for i, n := range got.nodes {
		if i != losingApply && i != losingDead {
			bad.nodes = append(bad.nodes, n)
		}
	}
	if _, err := soleTraceCheck(old, &bad); err == nil {
		t.Fatal("oracle accepted the missing mandatory application/dead mutation")
	}
	// The rejection's callback revision is semantic, even if its text and
	// surrounding search state remain identical.
	bad = *got
	bad.nodes = append([]soleNode(nil), got.nodes...)
	if bad.nodes[losingDead].rejection.rev != 4 {
		t.Fatal("missing revision-4 rejection")
	}
	bad.nodes[losingDead].rejection.rev = 3
	if _, err := soleTraceCheck(old, &bad); err == nil {
		t.Fatal("oracle accepted a changed retained rejection revision")
	}
	// Mimic the former oracle's false confirmation after a spent visit.
	bad = *got
	bad.nodes = nil
	for j, n := range got.nodes {
		bad.nodes = append(bad.nodes, n)
		if n.state.phase == "fit" && n.state.op == 1 {
			bad.nodes[len(bad.nodes)-1].accepted = false
			k := j + 1
			for k < len(got.nodes) && !soleContinuation(n, got.nodes[k]) {
				k++
			}
			bad.nodes = append(bad.nodes, got.nodes[k:]...)
			break
		}
	}
	if _, err := soleTraceCheck(old, &bad); err == nil {
		t.Fatal("oracle accepted pruning after a nondecisive visit")
	}
}

func TestSoleGrantExtraVisitStepPins(t *testing.T) {
	opus := soleValidBranches(1)
	repair := opus.Ops[2]
	for j := range 2000 {
		p := repair
		p.Client = 100 + j
		opus.Ops = append(opus.Ops, p)
	}
	opus.Ops = append(opus.Ops, history.Op{Client: 10000, Call: 3, Return: 4, Req: history.Request{Put: &history.PutRequest{Key: "b"}}, Err: errUnavailable})
	for _, tc := range []struct {
		name     string
		h        *history.History
		old, new int64
	}{{"Astra", soleMixedHistory(57), 13, 14}, {"Opus", opus, 166, 170}} {
		t.Run(tc.name, func(t *testing.T) {
			old, got := runSoleOracle(tc.h, true, false, 2000000), runSoleOracle(tc.h, false, false, 2000000)
			if err := soleVerdictCheck(old, got); err != nil {
				t.Fatal(err)
			}
			allowance := (got.extraUnits + workUnitsPerStep - 1) / workUnitsPerStep
			if old.err != "pass" || got.err != "pass" || old.steps != tc.old || got.steps != tc.new || allowance != tc.new-tc.old {
				t.Fatalf("old/new %d/%d, allowance %d (%d units), want %d/%d", old.steps, got.steps, allowance, got.extraUnits, tc.old, tc.new)
			}
			t.Logf("old %d new %d; %d newly charged ordinary putter items, exact step allowance %d", old.steps, got.steps, got.extraUnits/ordinaryUnits, allowance)
			if tc.name == "Astra" {
				a, b := runSoleOracle(tc.h, true, false, 13), runSoleOracle(tc.h, false, false, 13)
				if a.publicErr != nil || !errors.Is(b.publicErr, ErrWitnessTimeout) || a.steps != 13 || b.steps != 14 {
					t.Fatalf("cap 13 public result: old %v new %v, steps %d/%d", a.publicErr, b.publicErr, a.steps, b.steps)
				}
				if !b.stopped {
					t.Fatal("public timeout did not stop the internal search")
				}
				if !strings.Contains(b.publicErr.Error(), "the step budget (13 of 13 weighted steps)") || strings.Contains(b.publicErr.Error(), "no acked op wrote") || strings.Contains(b.publicErr.Error(), "gets ") {
					t.Fatalf("cap 13 budget diagnostic: %v", b.publicErr)
				}
				if err := soleStepCheck(a, b); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Families deliberately exercise true subtree pruning and conservative
// creators whose leases may still become live. Every instance is run
// through the whole search, not a hand-assembled revisionOrder.
func soleReviewGenerated(seed uint64) *history.History {
	var h *history.History
	switch seed % 5 {
	case 0:
		h = soleValidBranches(1 + int(seed%4))
	case 1:
		h = soleAstraHistory()
		h.Start.Leases = nil
		h.Ops[0] = history.Op{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}}, Err: errUnavailable}
		h.Ops[3].Resp = &history.Response{Revision: 5, Delete: &history.DeleteResponse{Deleted: 1}}
	case 2:
		h = soleAstraHistory()
		for i := range h.Start.KVs {
			h.Start.KVs[i].Lease = 9
		}
		h.Ops = []history.Op{
			{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 9}}, Err: errUnavailable},
			{Client: 1, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "b", Lease: 9}}, Err: errUnavailable},
			{Client: 2, Call: 3, Return: 4, Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}}, Resp: &history.Response{Revision: 4, Delete: &history.DeleteResponse{Deleted: 1}}},
		}
		h.Watches = []history.Watch{{Client: 3, Req: history.WatchRequest{Key: "a", StartRevision: 3}, Responses: []history.WatchResponse{{Time: 3, Revision: 3, Events: []history.Event{{Type: history.EventDelete, KV: history.KeyValue{Key: "a", ModRevision: 3}}}}}}}
	case 3:
		h = soleAstraHistory()
		h.Start.Leases = nil
		h.Ops[0] = history.Op{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 9, TTL: 10}}, Err: &history.Error{Code: "Unavailable", Message: "definite"}}
	case 4:
		h = soleMixedHistory(1 + int(seed%4))
	}
	id := int64(100 + seed)
	for i := range h.Start.Leases {
		h.Start.Leases[i] = id
	}
	for i := range h.Start.KVs {
		if h.Start.KVs[i].Lease != 0 {
			h.Start.KVs[i].Lease = id
		}
	}
	for i := range h.Ops {
		q := &h.Ops[i].Req
		if q.LeaseGrant != nil {
			q.LeaseGrant.ID = id
		}
		if q.LeaseRevoke != nil {
			q.LeaseRevoke.ID = id
		}
		if q.Put != nil && q.Put.Lease != 0 {
			q.Put.Lease = id
		}
	}
	return h
}

func soleReviewCorpus(t *testing.T) (hits, prunes int) {
	t.Helper()
	for seed := range uint64(64) {
		h := soleReviewGenerated(seed)
		old, got := runSoleOracle(h, true, true, 2000000), runSoleOracle(h, false, true, 2000000)
		if err := soleVerdictCheck(old, got); err != nil {
			t.Fatalf("review family %d seed %d: %v", seed%5, seed, err)
		}
		n, err := soleTraceCheck(old, got)
		if err != nil {
			t.Fatalf("review family %d seed %d: %v", seed%5, seed, err)
		}
		if seed%5 == 0 && (got.hits == 0 || n == 0) {
			t.Fatal("pruning family did not prune")
		}
		if seed%5 == 1 && got.err != "pass" {
			t.Fatal("unused grant lost a valid completion")
		}
		if seed%5 == 2 && (got.hits == 0 || n == 0) {
			t.Fatal("post-trial family did not prune")
		}
		if seed%5 == 3 && (got.hits != 0 || n != 0 || old.err != got.err) {
			t.Fatal("definite-failure grant left the conservative path")
		}
		if seed%5 == 4 && (got.hits == 0 || n != 0 || got.err != "pass") {
			t.Fatal("mixed creator fallback did not retain its branches")
		}
		hits += got.hits
		prunes += n
	}
	if hits == 0 || prunes == 0 {
		t.Fatal("whole-search review corpus needs actual hits and pruning")
	}
	return hits, prunes
}

// Recorded rule-off replays stay in other. The shard guard assigns the
// separate saved-row replay and whole-search oracle to histories for balance.
// Each side remains pinned to the same full-string/step reference, with
// no second search added inside the original production-history tests.
func TestSoleGrantRecordedRuleOff(t *testing.T) {
	got := map[string]string{}
	visitDueRecordedHistories(t, func(name string, h *history.History) {
		old := runSoleOracle(h, true, false, 2000000)
		if old.timeout || old.oracleErr != nil {
			t.Fatalf("%s: inconclusive old rule: %s, %v", name, old.err, old.oracleErr)
		}
		got[name] = old.err
	})
	checkDueRecordedVerdicts(t, got)
}

func TestSoleGrantSavedRuleOff(t *testing.T) {
	for _, tc := range weightedSavedRows {
		t.Run(tc.name, func(t *testing.T) {
			old := runSoleOracle(budgetHistory(t, tc.name), true, false, 2000000)
			if (tc.name == "grants-81000" || tc.name == "gaps-256-expiries-4096") && (old.publicErr != nil || old.err != "pass") {
				t.Fatalf("synthetic old-rule row %s must pass at %d steps: %s", tc.name, old.steps, old.err)
			}
			if old.timeout || old.oracleErr != nil || old.steps != tc.weighted {
				t.Fatalf("old saved row %s: %d steps want %d; %v", tc.name, old.steps, tc.weighted, old.oracleErr)
			}
		})
	}
}

func TestSoleGrantGeneratedLeaseFamilies(t *testing.T) {
	hits, prunes := soleReviewCorpus(t)
	t.Logf("64 generated lease families: %d hits, %d genuine pruned subtrees", hits, prunes)
}
