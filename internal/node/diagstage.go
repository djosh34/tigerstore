package node

import (
	"slices"
	"time"
)

// diagStages logs, every 5 s, how long entries took from being queued for
// the local disk to durable, to committed and to applied, and the turns'
// periods and work. Diagnosis only.
type diagStages struct {
	q                    []diagEnt
	durable, comm, appl  []time.Duration
	turns                int
	turnWork, turnPeriod time.Duration
	lastTurn, lastLog    time.Time
	calls                int
}

type diagEnt struct {
	idx                uint64
	queued, durableAt time.Time
	committed         bool
	commitAt          time.Time
}

func (d *diagStages) queued(idx uint64, at time.Time) {
	d.q = append(d.q, diagEnt{idx: idx, queued: at})
}

func (d *diagStages) persisted(idx uint64, at time.Time) {
	for i := range d.q {
		if d.q[i].idx <= idx && d.q[i].durableAt.IsZero() {
			d.q[i].durableAt = at
			d.durable = append(d.durable, at.Sub(d.q[i].queued))
		}
	}
}

func (d *diagStages) progress(commit, applied uint64, now time.Time) {
	for i := range d.q {
		e := &d.q[i]
		if !e.committed && e.idx <= commit {
			e.committed, e.commitAt = true, now
			d.comm = append(d.comm, now.Sub(e.queued))
		}
	}
	k := 0
	for _, e := range d.q {
		if e.committed && e.idx <= applied && !e.durableAt.IsZero() {
			d.appl = append(d.appl, now.Sub(e.commitAt))
			continue
		}
		d.q[k] = e
		k++
	}
	d.q = d.q[:k]
	if len(d.q) > 100000 {
		d.q = d.q[len(d.q)-100000:]
	}
}

func (d *diagStages) turn(start, end time.Time, calls int, logf func(string, ...any)) {
	if !d.lastTurn.IsZero() {
		d.turnPeriod += start.Sub(d.lastTurn)
	}
	d.lastTurn = start
	d.turnWork += end.Sub(start)
	d.turns++
	d.calls += calls
	if d.lastLog.IsZero() {
		d.lastLog = end
	}
	if end.Sub(d.lastLog) < 5*time.Second || d.turns == 0 {
		return
	}
	pct := func(v []time.Duration) string {
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
	logf("diag stages: turns %d period %v work %v calls/turn %d; durable mean/p50/p90/p99 %s; commit %s; apply-after-commit %s",
		d.turns, d.turnPeriod/time.Duration(d.turns), d.turnWork/time.Duration(d.turns), d.calls/d.turns, pct(d.durable), pct(d.comm), pct(d.appl))
	d.durable, d.comm, d.appl = d.durable[:0], d.comm[:0], d.appl[:0]
	d.turns, d.turnWork, d.turnPeriod, d.calls = 0, 0, 0, 0
	d.lastLog = end
}
