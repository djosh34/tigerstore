package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessedUnusedTxnLease reduces #290: the expiry must precede the
// rejected put, although a later Txn mentions that lease in its unchosen
// branch. Neither a success nor a failure answer proves that lease alive.
func TestWitnessedUnusedTxnLease(t *testing.T) {
	for _, succeeded := range []bool{false, true} {
		name := "failure"
		if succeeded {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			b := newBuilder(t, "a", "b")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "rejected", Lease: 42}})
			b.put(0, "a", "x")
			read := []history.TxnOp{{Range: &history.RangeRequest{Key: "a"}}}
			unused := []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "unused", Lease: 42}}}
			txn := &history.TxnRequest{
				Compare: []history.Compare{{Key: "a", Target: history.TargetVersion, Result: history.Equal, Number: 0}},
				Success: unused, Failure: read,
			}
			if succeeded {
				txn.Compare[0].Number = 1
				txn.Success, txn.Failure = read, unused
			}
			b.do(0, history.Request{Txn: txn})
			h := b.done()
			// An expiry is an ambiguous revoke called when its lease was
			// granted; its original, valid position remains in the log.
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			t.Run("Porcupine", func(t *testing.T) { mustPass(t, "Linearizable", Linearizable(h, timeout)) })
			for _, mode := range bothModes {
				t.Run(mode.String(), func(t *testing.T) { mustPass(t, "Witnessed", Witnessed(h, mode, timeout)) })
			}
		})
	}
}
