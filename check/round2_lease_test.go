package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestRound2UnusedTxnLease shrinks the revision-witness disagreement of
// the five-voter bridge, seed 1482101. An unchosen Txn branch does not
// prove that its lease still exists. The log and Porcupine both have the
// valid ordering: grant, expire without keys, reject a leased put, write
// another key, then take the Txn's read-only failure branch.
func TestRound2UnusedTxnLease(t *testing.T) {
	b := newBuilder(t, "a", "b")
	grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
	expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
	b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "rejected", Lease: 42}})
	b.put(0, "a", "x")
	b.do(0, history.Request{Txn: &history.TxnRequest{
		Compare: []history.Compare{{Key: "a", Target: history.TargetVersion, Result: history.Equal, Number: 0}},
		Success: []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "unused", Lease: 42}}},
		Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: "a"}}},
	}})
	h := b.done()
	h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
	h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
	t.Run("Porcupine", func(t *testing.T) { mustPass(t, "Linearizable", Linearizable(h, timeout)) })
	for _, mode := range bothModes {
		t.Run(mode.String(), func(t *testing.T) { mustPass(t, "Witnessed", Witnessed(h, mode, timeout)) })
	}
}
