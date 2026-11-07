package check

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessLeasedHistories checks true histories that the witness from
// the revisions refused (#514): an acked revoke whose lease only an
// ambiguous grant made (PCG(124, 77) 546, PCG(154, 77) 675), a rejected
// compaction that needs an acked revoke before an ambiguous compaction
// (PCG(131, 77) 590), and an ambiguous compaction that needs a revision
// no answer saw (PCG(175, 77) 295). Indexes count from 0.
func TestWitnessLeasedHistories(t *testing.T) {
	for _, c := range []struct{ seed, index uint64 }{{124, 546}, {154, 675}, {131, 590}, {175, 295}} {
		t.Run(fmt.Sprintf("PCG(%d,77)-%d", c.seed, c.index), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(c.seed, 77))
			var h *history.History
			for range c.index + 1 {
				h = leasedHistory(rng)
			}
			for _, mode := range []WitnessMode{FromLog, FromRevisions} {
				for _, slow := range []bool{false, true} {
					if err := Witnessed(h, mode, time.Minute, func(o *options) { o.slow = slow }); err != nil {
						t.Errorf("%s, slow %v: %v", mode, slow, err)
					}
				}
			}
		})
	}
}

// TestWitnessAmbiguousGrantForRevoke has an acked revoke of lease 2 that
// only an ambiguous grant made, called after two overlapping calls on
// lease 1 that must go against their call order. The true history passes;
// the grant called after the revoke returned, a grant of another lease,
// and a second acked revoke of the one grant are refused, as Porcupine
// refuses them (#514).
func TestWitnessAmbiguousGrantForRevoke(t *testing.T) {
	for _, name := range []string{"true", "grant after the revoke", "another lease", "two revokes"} {
		t.Run(name, func(t *testing.T) {
			b := newBuilder(t)
			b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 1, TTL: 10}})
			b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}})
			g := b.do(2, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 2, TTL: 10}})
			b.do(3, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 2}})
			h := b.done()
			h.Ops[0].Call, h.Ops[0].Return = 6, 19
			h.Ops[1].Call, h.Ops[1].Return = 5, 24
			h.Ops[g].Call, h.Ops[g].Return = 45, 46
			h.Ops[g].Resp, h.Ops[g].Err = nil, errUnavailable
			h.Ops[3].Call, h.Ops[3].Return = 70, 93
			switch name {
			case "grant after the revoke":
				h.Ops[g].Call, h.Ops[g].Return = 100, 101
			case "another lease":
				h.Ops[g].Req.LeaseGrant.ID = 3
			case "two revokes":
				op := h.Ops[3]
				op.Client, op.Call, op.Return = 4, 110, 120
				h.Ops = append(h.Ops, op)
			}
			want := name == "true"
			if err := Linearizable(h, time.Minute); (err == nil) != want {
				t.Fatalf("Porcupine: want pass %v, got %v", want, err)
			}
			for _, slow := range []bool{false, true} {
				if err := Witnessed(h, FromRevisions, time.Minute, func(o *options) { o.slow = slow }); (err == nil) != want {
					t.Errorf("revisions, slow %v: want pass %v, got %v", slow, want, err)
				}
			}
		})
	}
}
