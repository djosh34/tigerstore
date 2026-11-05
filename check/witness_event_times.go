package check

import (
	"math"

	"github.com/djosh34/tigerstore/history"
)

// Prototype append/pop index of event times. The minimum acked return
// finds the last event before a call; the maximum call finds the first
// event after a return. No full-order scan or sort per compaction trial.
type (
	eventTime  struct{ minReturn, maxCall int64 }
	eventTimes struct {
		nodes       []eventTime
		capacity, n int
	}
)

var noEventTime = eventTime{math.MaxInt64, math.MinInt64}

func newEventTimes() *eventTimes {
	return &eventTimes{nodes: []eventTime{noEventTime, noEventTime}, capacity: 1}
}

func mergeEventTimes(a, b eventTime) eventTime {
	return eventTime{min(a.minReturn, b.minReturn), max(a.maxCall, b.maxCall)}
}

func (t *eventTimes) set(i int, e eventTime) {
	p := t.capacity + i
	t.nodes[p] = e
	for p /= 2; p > 0; p /= 2 {
		t.nodes[p] = mergeEventTimes(t.nodes[p*2], t.nodes[p*2+1])
	}
}

func (t *eventTimes) append(h *history.History, op int) {
	if t.n == t.capacity {
		nodes := make([]eventTime, t.capacity*4)
		for i := range nodes {
			nodes[i] = noEventTime
		}
		copy(nodes[t.capacity*2:], t.nodes[t.capacity:])
		t.capacity *= 2
		t.nodes = nodes
		for p := t.capacity - 1; p > 0; p-- {
			t.nodes[p] = mergeEventTimes(t.nodes[p*2], t.nodes[p*2+1])
		}
	}
	e := noEventTime
	if op >= 0 {
		o := h.Ops[op]
		e.maxCall = o.Call
		if o.Resp != nil {
			e.minReturn = o.Return
		}
	}
	t.set(t.n, e)
	t.n++
}

func (t *eventTimes) truncate(n int) {
	for t.n > n {
		t.n--
		t.set(t.n, noEventTime)
	}
}

func (t *eventTimes) after(call int64) int {
	if t.nodes[1].minReturn >= call {
		return 0
	}
	p := 1
	for p < t.capacity {
		p *= 2
		if t.nodes[p+1].minReturn < call {
			p++
		}
	}
	return p - t.capacity + 1
}

func (t *eventTimes) before(ret int64) int {
	if t.nodes[1].maxCall <= ret {
		return t.n
	}
	p := 1
	for p < t.capacity {
		p *= 2
		if t.nodes[p].maxCall <= ret {
			p++
		}
	}
	return p - t.capacity
}
