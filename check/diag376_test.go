package check

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// The late compaction can explain an old compacted rejection, but cannot
// run before its call. Trying it must not expire leases at earlier revisions.
func TestDiag376LateCompactionDoesNotExpireLease(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "overwritten"
		if deleting {
			name = "deleted"
		}
		t.Run(name, func(t *testing.T) {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "initial")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 60}})
			put := b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "leased", Lease: 42}})
			if deleting {
				b.del(0, "a")
			} else {
				b.put(0, "a", "unleased")
				b.get(0, "a")
			}
			expiry := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Lease: 42}})
			b.put(0, "b", "later")
			compact := b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			for _, i := range []int{put, expiry, compact} {
				h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
			}
			mustPass(t, "Linearizable", Linearizable(h, time.Second))
			for _, mode := range bothModes {
				mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, time.Second))
			}
		})
	}
}
