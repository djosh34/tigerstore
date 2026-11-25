package sim

import (
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestLivenessTakesABriefBusyRefusal checks the liveness rule's bounds on
// a refusal as too many requests (busyAnswered, #818's ruling): one is an
// answer when it came within 1 s, its client had a call taken within 5 s
// after it or made none, it is in no run of more than 3 back to back, and
// its node refused so at most 5% of its calls; past any bound the rule
// fails, as on a call that never got through.
func TestLivenessTakesABriefBusyRefusal(t *testing.T) {
	s, err := newSim(DefaultConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.LiveFrom = 1
	busy, ok := rpctypes.ErrGRPCRequestTooManyRequests, error(nil)
	type call struct {
		at, took float64 // seconds
		err      error
	}
	run := func(calls []call) error {
		res := &Result{}
		for i := range 100 {
			at := int64(10+i) * Second
			res.History = append(res.History, &Record{Client: 0, Node: 0, Call: at, Return: at + Millisecond})
		}
		for _, c := range calls {
			at := int64(c.at * float64(Second))
			// As the client records them: an error it does not know to be
			// a rejection is ambiguous (ambiguous).
			res.History = append(res.History, &Record{Client: 1, Node: 0, Call: at, Return: at + int64(c.took*float64(Second)), Err: c.err, Ambiguous: c.err != nil && ambiguous(c.err)})
		}
		return s.checkLive(res)
	}
	pairs := func(n int) []call {
		var cs []call
		for i := range n {
			at := 200 + 2*float64(i)
			cs = append(cs, call{at, 0.01, busy}, call{at + 1, 0.01, ok})
		}
		return cs
	}
	for _, c := range []struct {
		name   string
		calls  []call
		answer bool
	}{
		{"one, then a call taken", []call{{200, 0.01, busy}, {201, 0.01, ok}}, true},
		{"one as the client's last call", []call{{200, 0.01, busy}}, true},
		{"three back to back, then a call taken", []call{{200, 0.01, busy}, {200.1, 0.01, busy}, {200.2, 0.01, busy}, {201, 0.01, ok}}, true},
		{"four back to back, then a call taken", []call{{200, 0.01, busy}, {200.1, 0.01, busy}, {200.2, 0.01, busy}, {200.3, 0.01, busy}, {201, 0.01, ok}}, false},
		{"answered after 1.5 s", []call{{200, 1.5, busy}, {202, 0.01, ok}}, false},
		{"the next call taken 6 s after it", []call{{200, 0.01, busy}, {206.1, 0.01, ok}}, false},
		{"the next call a rejection, then none taken within 5 s", []call{{200, 0.01, busy}, {201, 0.01, rpctypes.ErrGRPCCompacted}, {206, 0.01, ok}}, false},
		{"4 of the node's 108 calls", pairs(4), true},
		{"10 of the node's 120 calls, past 5%", pairs(10), false},
	} {
		err := run(c.calls)
		if got := err == nil; got != c.answer {
			t.Errorf("%s: the rule gave %v, want answered %v", c.name, err, c.answer)
		}
		if err != nil && !strings.Contains(err.Error(), "client 1 call at 200000 ms") {
			t.Errorf("%s: the rule failed on another call than the refusal at 200 s: %v", c.name, err)
		}
	}
}
