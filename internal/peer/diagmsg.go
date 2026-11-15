package peer

import (
	"slices"
	"sync"
	"time"
)

// Diagnosis only: per message type, the time from queued on the lane to
// framed for the socket, and from framed to read by the receiver.
var diagM struct {
	sync.Mutex
	q, w map[string][]time.Duration
	last time.Time
}

func diagMsg(t *TCP, typ string, qAt, fAt int64) {
	now := time.Now()
	diagM.Lock()
	defer diagM.Unlock()
	if diagM.q == nil {
		diagM.q, diagM.w, diagM.last = map[string][]time.Duration{}, map[string][]time.Duration{}, now
	}
	diagM.q[typ] = append(diagM.q[typ], time.Duration(fAt-qAt))
	diagM.w[typ] = append(diagM.w[typ], time.Duration(now.UnixNano()-fAt))
	if now.Sub(diagM.last) < 5*time.Second {
		return
	}
	diagM.last = now
	for _, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range diagM.q {
			if !yield(k) {
				return
			}
		}
	}) {
		t.logf("diag msg %s n=%d queued->framed %s framed->read %s", k, len(diagM.q[k]), diagPct(diagM.q[k]), diagPct(diagM.w[k]))
	}
	clear(diagM.q)
	clear(diagM.w)
}

func diagPct(v []time.Duration) string {
	if len(v) == 0 {
		return "-"
	}
	slices.Sort(v)
	var s time.Duration
	for _, x := range v {
		s += x
	}
	return (s / time.Duration(len(v))).String() + "/" + v[len(v)/2].String() + "/" + v[len(v)*9/10].String() + "/" + v[len(v)*99/100].String()
}
