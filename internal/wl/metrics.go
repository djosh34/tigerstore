package wl

import "sync/atomic"

// nodeMetrics is the node's state that goroutines off the loop read: the
// HTTP endpoints and the metrics (prom.go).
type nodeMetrics struct {
	isLeader  atomic.Int64
	hasLeader atomic.Int64
	peerSent  atomic.Uint64
	// alarms are the raised alarms' texts, as the apply last left them;
	// learner is whether the node is one.
	alarms  atomic.Pointer[[]string]
	learner atomic.Bool
}
