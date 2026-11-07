package check

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// These are the two remaining examples from #514. Indexes are zero-based;
// drawing a stale answer would consume more random numbers and change them.
func TestDiag514Examples(t *testing.T) {
	for _, c := range []struct{ seed, index uint64 }{{124, 546}, {154, 675}} {
		t.Run(fmt.Sprintf("PCG-%d-77-history-%d", c.seed, c.index), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(c.seed, 77))
			var h *history.History
			for i := uint64(0); i <= c.index; i++ {
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

// A four-op version of the failure. The first two calls overlap and must
// be ordered against call order. The ambiguous grant must follow both.
// Its finite error-return time is not a linearization deadline.
func diag514Small(t *testing.T) *history.History {
	t.Helper()
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
	return h
}

func TestDiag514SmallAndFalseHistories(t *testing.T) {
	for _, name := range []string{"true", "late call", "wrong lease", "revoke twice"} {
		t.Run(name, func(t *testing.T) {
			h := diag514Small(t)
			switch name {
			case "late call":
				h.Ops[2].Call, h.Ops[2].Return = 100, 101
			case "wrong lease":
				h.Ops[2].Req.LeaseGrant.ID = 3
			case "revoke twice":
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

// Opt in so normal package tests stay short. This scans 60,000 true
// histories, including the two examples, without drawing stale answers.
func TestDiag514ScanTrueLeasedHistories(t *testing.T) {
	if os.Getenv("TIGERSTORE_DIAG514_SCAN") != "1" {
		t.Skip("set TIGERSTORE_DIAG514_SCAN=1 for the 60,000-history scan")
	}
	rejected := 0
	for seed := uint64(120); seed < 180; seed++ {
		rng := rand.New(rand.NewPCG(seed, 77))
		for i := range 1000 {
			h := leasedHistory(rng)
			if err := Witnessed(h, FromLog, time.Minute); err != nil {
				t.Fatalf("PCG(%d,77) history %d log: %v", seed, i, err)
			}
			if err := Witnessed(h, FromRevisions, time.Minute); err != nil {
				rejected++
				t.Errorf("PCG(%d,77) history %d revisions: %v", seed, i, err)
			}
		}
		if (seed-119)%10 == 0 {
			t.Logf("checked %d histories; %d refused", (seed-119)*1000, rejected)
		}
	}
	t.Logf("60,000 true histories: %d refused by revisions; all passed from the log", rejected)
}
