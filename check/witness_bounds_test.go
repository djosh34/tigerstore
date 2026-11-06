package check

import (
	"math/rand/v2"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestEventTimesMatchScan pushes random events, acked or not, and takes
// some back, and compares after and before at random times with a scan of
// the events, from trees made empty and made with room for some events
// (newEventTimes), so that pushes fill them and go past them.
func TestEventTimesMatchScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 515))
	h := &history.History{}
	for range 200 {
		op := history.Op{Call: rng.Int64N(1000), Req: history.Request{Put: &history.PutRequest{Key: "a"}}}
		op.Return = op.Call + rng.Int64N(100)
		if rng.IntN(3) > 0 {
			op.Resp = &history.Response{}
		}
		h.Ops = append(h.Ops, op)
	}
	for _, size := range []int{0, 1, 2, 3, 50, 4096} {
		times := newEventTimes(size)
		var events []int
		for step := range 2000 {
			switch {
			case rng.IntN(5) == 0 && len(events) > 0:
				n := rng.IntN(len(events))
				events = events[:n]
				times.truncate(n)
			case rng.IntN(10) == 0:
				events = append(events, -1)
				times.push(h, -1)
			default:
				op := rng.IntN(len(h.Ops))
				events = append(events, op)
				times.push(h, op)
			}
			at := rng.Int64N(1100)
			after, before := 0, len(events)
			for k, op := range events {
				if op < 0 {
					continue
				}
				if o := &h.Ops[op]; o.Resp != nil && o.Return < at {
					after = k + 1
				}
			}
			for k, op := range events {
				if op >= 0 && h.Ops[op].Call > at {
					before = k
					break
				}
			}
			if got := times.after(at); got != after {
				t.Fatalf("room %d, step %d: after(%d) = %d, the scan says %d", size, step, at, got, after)
			}
			if got := times.before(at); got != before {
				t.Fatalf("room %d, step %d: before(%d) = %d, the scan says %d", size, step, at, got, before)
			}
		}
	}
}
