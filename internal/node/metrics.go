package node

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/djosh34/tigerstore/internal/metrics"
)

// warningApply is etcd's --warning-apply-duration: applies that take
// longer count in etcd_server_slow_apply_total.
const warningApply = 100 * time.Millisecond

// timer is the core's Timer. The core calls it from the committer
// goroutine only.
type timer struct {
	m     *metrics.Set
	epoch time.Time
	// apply holds the observers of each op, failed and succeeded.
	apply  map[string]*[2]prometheus.Observer
	ranges [2]prometheus.Observer
}

func newTimer(m *metrics.Set) *timer {
	return &timer{
		m:      m,
		epoch:  time.Now(),
		apply:  map[string]*[2]prometheus.Observer{},
		ranges: [2]prometheus.Observer{m.Range.WithLabelValues("false"), m.Range.WithLabelValues("true")},
	}
}

func (t *timer) Now() int64 { return int64(time.Since(t.epoch)) }

// seconds is a duration in seconds, in whole microseconds, as etcd
// observes its apply and range times.
func seconds(nanos int64) float64 { return float64(nanos/1e3) / 1e6 }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (t *timer) Applied(op string, ok bool, nanos int64) {
	o := t.apply[op]
	if o == nil {
		o = &[2]prometheus.Observer{t.m.Apply.WithLabelValues("v3", op, "false"), t.m.Apply.WithLabelValues("v3", op, "true")}
		t.apply[op] = o
	}
	o[b2i(ok)].Observe(seconds(nanos))
	if time.Duration(nanos) > warningApply {
		t.m.SlowApplies.Inc()
	}
	if op == "Compaction" && ok {
		// The index compacts as the compaction applies; there is no
		// database to compact after it.
		t.m.IndexCompactionPause.Observe(float64(nanos / 1e6))
		t.m.DBCompactionLast.Set(float64(time.Now().Unix()))
	}
}

func (t *timer) Read(op string, ok bool, nanos int64) {
	switch op {
	case "Range":
		t.ranges[b2i(ok)].Observe(seconds(nanos))
	case "Hash":
		t.m.Hash.Observe(time.Duration(nanos).Seconds())
	case "HashKV":
		t.m.HashRev.Observe(time.Duration(nanos).Seconds())
	}
}

// MetricSet returns the node's metrics, which the server reports and the
// peer transport counts into.
func (n *Node) MetricSet() *metrics.Set { return n.cfg.Metrics }

// peerLabel is the label etcd gives a peer: its ID in hexadecimal.
func peerLabel(id uint64) string { return strconv.FormatUint(id, 16) }

// syncTimed syncs f and observes how long it took.
func syncTimed(f interface{ Sync() error }, h prometheus.Observer) error {
	start := time.Now()
	err := f.Sync()
	h.Observe(time.Since(start).Seconds())
	return err
}
