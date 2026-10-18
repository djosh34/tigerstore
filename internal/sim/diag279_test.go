package sim

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// Same fault and workload as TestHalfOpenLink seed 1. A client timeout
// longer than the fault makes its supply problem easy to reproduce:
// random endpoint selection eventually parks every client on the bad node.
func TestDiag279HalfOpenClientSupply(t *testing.T) {
	cfg := DefaultConfig(4001)
	cfg.Faults = Faults{}
	cfg.Clients = 12
	cfg.PinClients = true
	cfg.ClientTimeout = 7 * Second
	cfg.HalfOpenAt = 5 * Second
	cfg.LiveFrom = 12 * Second
	cfg.Duration = 12 * Second
	cfg.Heal = 3 * Second
	res := run(t, cfg)
	h := res.halfOpens[0]
	type tally struct {
		Calls, OK, Rejected, Stopped, Timeout, NoLeader int
		MaxLatency                                      int64
	}
	var counts [3]tally
	healthy := 0
	for _, r := range res.History {
		if r.Call < h.start || r.Call >= h.end {
			continue
		}
		c := &counts[r.Node]
		c.Calls++
		c.MaxLatency = max(c.MaxLatency, r.Return-r.Call)
		switch {
		case r.Err == nil:
			c.OK++
		case errors.Is(r.Err, rpctypes.ErrGRPCStopped):
			c.Stopped++
		case errors.Is(r.Err, errClientTimeout):
			c.Timeout++
		case errors.Is(r.Err, rpctypes.ErrGRPCNoLeader):
			c.NoLeader++
		default:
			c.Rejected++
		}
		if r.Node == h.to || errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
			continue
		}
		if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
			t.Errorf("healthy node %d: %v", r.Node+1, r.Err)
		}
		if r.Return-r.Call >= Second {
			t.Errorf("healthy node %d took %d ms", r.Node+1, (r.Return-r.Call)/Millisecond)
		}
		healthy++
	}
	t.Logf("seed 4001: half-open %d->%d; per-node counts %+v; healthy=%d", h.from+1, h.to+1, counts, healthy)
	if healthy < 20 {
		t.Fatalf("only %d calls to healthy pair: random clients can all wait on the cut-off follower", healthy)
	}
}
