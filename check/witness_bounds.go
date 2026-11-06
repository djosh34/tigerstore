package check

import (
	"math"

	"github.com/djosh34/tigerstore/history"
)

// eventTimes keeps the call and return time of each event of the order,
// as the events are applied and undone, so that the real-time bounds of an
// op that changed nothing come without a look at every op (eventBounds):
// a compaction's trial asks for them again and again, and a sort of every
// placed op at each ask was most of what was left of a busy history's
// check (#499). It is a tree over the events, in their order, whose nodes
// keep their subtree's earliest return of an acked op and latest call.
//
// It holds the events alone, where eventBounds' scan also counts the ops
// that observe placed. Those are only placed by the last observe of an
// order, and restore removes them before the search asks again; the
// witness with slow set scans, and the tests compare the two.
type eventTimes struct {
	nodes []eventTime
	// leaves is the size of the tree's bottom row, n how many of them
	// hold an event.
	leaves, n int
}

type eventTime struct{ firstReturn, lastCall int64 }

var noTime = eventTime{firstReturn: math.MaxInt64, lastCall: math.MinInt64}

func (t *eventTimes) set(i int, e eventTime) {
	p := t.leaves + i
	t.nodes[p] = e
	for p /= 2; p > 0; p /= 2 {
		a, b := t.nodes[2*p], t.nodes[2*p+1]
		t.nodes[p] = eventTime{firstReturn: min(a.firstReturn, b.firstReturn), lastCall: max(a.lastCall, b.lastCall)}
	}
}

// push adds the times of the next event, of op, or none when op is not
// an op of the history.
func (t *eventTimes) push(h *history.History, op int) {
	if t.n == t.leaves {
		old := t.nodes
		t.leaves = max(1, 2*t.leaves)
		t.nodes = make([]eventTime, 2*t.leaves)
		for i := range t.nodes {
			t.nodes[i] = noTime
		}
		for i := range t.n {
			t.set(i, old[len(old)/2+i])
		}
	}
	e := noTime
	if op >= 0 {
		o := &h.Ops[op]
		e.lastCall = o.Call
		if o.Resp != nil {
			e.firstReturn = o.Return
		}
	}
	t.set(t.n, e)
	t.n++
}

// truncate keeps the first n events.
func (t *eventTimes) truncate(n int) {
	for t.n > n {
		t.n--
		t.set(t.n, noTime)
	}
}

// after returns how many events from the first an op called at call must
// follow: up to the last acked one that returned before call.
func (t *eventTimes) after(call int64) int {
	if t.n == 0 || t.nodes[1].firstReturn >= call {
		return 0
	}
	p := 1
	for p < t.leaves {
		p *= 2
		if t.nodes[p+1].firstReturn < call {
			p++
		}
	}
	return p - t.leaves + 1
}

// before returns how many events from the first an op that returned at
// ret may follow: those before the first one called after ret.
func (t *eventTimes) before(ret int64) int {
	if t.n == 0 || t.nodes[1].lastCall <= ret {
		return t.n
	}
	p := 1
	for p < t.leaves {
		p *= 2
		if t.nodes[p].lastCall <= ret {
			p++
		}
	}
	return p - t.leaves
}
