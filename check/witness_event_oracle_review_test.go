package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// From Astra's read-only /tmp/rev898-pi-reproducer_test.go. The leased
// put fits revision 4, but nested expiry removes its lease before apply;
// applying it then fails at revision 3. Acceptance must be recorded at the
// fitsGranted return, not inferred from the later application outcome.
func review898NestedLeaseHistory() *history.History {
	return &history.History{Start: &history.Start{Rev: 2, Leases: []int64{7}}, Ops: []history.Op{
		{Client: 1, Call: 1, Return: 2, Req: history.Request{Put: &history.PutRequest{Key: "z", Value: "v"}}, Resp: &history.Response{Revision: 3, Put: &history.PutResponse{}}},
		{Client: 2, Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "z", Revision: 2}}, Err: errCompacted},
		{Client: 3, Call: 5, Return: 6, Req: history.Request{Put: &history.PutRequest{Key: "x", Lease: 7}}, Err: errUnavailable},
		{Client: 4, Call: 7, Return: 8, Req: history.Request{Put: &history.PutRequest{Key: "x", Lease: 7}}, Err: errLeaseNotFound},
		{Client: 5, Call: 9, Return: 10, Req: history.Request{Compact: &history.CompactRequest{Revision: 3}}, Err: errUnavailable},
		{Client: 6, Call: 0, Return: 1, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}}, Err: errUnavailable},
		{Client: 7, Call: 11, Return: 12, Req: history.Request{Range: &history.RangeRequest{Key: "z", CountOnly: true}}, Resp: &history.Response{Revision: 4, Range: &history.RangeResponse{Count: 1}}},
	}}
}

func TestReview898OracleMustRejectSuccessfulFit(t *testing.T) {
	old := runEventOracle(review898NestedLeaseHistory(), true, 2_000_000, nil, false)
	if old.inputErr != nil || old.timeout || len(old.sites) != 1 {
		t.Fatalf("old search did not exercise the intended pool: %s", old.err)
	}
	appliedWithoutWrite := false
	for _, x := range old.trace {
		if x.phase == "apply" && x.op == 2 && x.searchRev == 4 && x.rev == 3 {
			appliedWithoutWrite = true
		}
	}
	if !appliedWithoutWrite {
		t.Fatal("old search no longer accepts the leased put before nested expiry")
	}
	for _, site := range old.sites {
		if !site.accepted || confirmedEventSite(site) {
			t.Fatalf("oracle confirmed exhaustion despite a successful old fitsGranted return: %+v", site)
		}
	}
}

// Under the wrong missing-lease predicate (ignoring live.hasLease), this
// comparison used to pass while dropping the accepted probe's entire trace.
// The independent oracle must now reject that fabricated certificate.
func TestReview898OracleComparesNestedLeaseHistory(t *testing.T) {
	h := review898NestedLeaseHistory()
	old := runEventOracle(h, true, 2_000_000, nil, false)
	got := runEventOracle(h, false, 2_000_000, nil, false)
	compareEventRuns(t, old, got)
}

func TestEventDeadHistoryGeneratorCoverage(t *testing.T) {
	var successCAS, missingCompare, grant, currentRange, imageLease bool
	for seed := range uint64(64) {
		h := eventDeadHistory(seed, 24)
		imageLease = imageLease || len(h.Start.Leases) != 0
		for _, op := range h.Ops {
			grant = grant || op.Req.LeaseGrant != nil
			q := op.Req.Txn
			if q == nil {
				continue
			}
			c := q.Compare[0]
			present := false
			var mod int64
			for _, kv := range h.Start.KVs {
				if kv.Key == c.Key {
					present, mod = true, kv.ModRevision
				}
			}
			if present && c.Number == mod {
				successCAS = true
			}
			if !present && c.Number == 0 {
				missingCompare = true
			}
			if c.Number != mod && q.Failure[0].Range.Revision == 0 {
				currentRange = true
			}
		}
	}
	if !successCAS || !missingCompare || !grant || !currentRange || !imageLease {
		t.Fatalf("generator coverage: successful CAS=%v missing mod-zero=%v grant=%v no-revision range=%v image lease=%v", successCAS, missingCompare, grant, currentRange, imageLease)
	}
}
