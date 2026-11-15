package wl

import "time"

// Auto compaction, as etcd v3.7.2's compactors (v3compactor) run it: on
// the leader only, through the log as a client's Compact. Here they run
// on the loop's ticks.
//
// periodic, with a retention R: it notes the revision every R/10 (at
// most every 6 minutes), and once R has passed since the start, or since
// the last compaction with R up to an hour and else an hour, it compacts
// at the revision it noted R ago.
//
// revision, with a retention of N revisions: every 5 minutes it compacts
// at the current revision less N.

// compactor is the node's auto compaction, zero for none.
type compactor struct {
	period    time.Duration
	revisions int64

	next        time.Time // when it looks next
	revs        []int64   // periodic: the revisions noted, oldest first
	base        time.Duration
	lastSuccess time.Time
	last        int64 // the revision it compacted at last
}

// revisionInterval is how often the revision compactor runs.
const revisionInterval = 5 * time.Minute

func newCompactor(period time.Duration, revisions int64, now time.Time) compactor {
	c := compactor{period: period, revisions: revisions, base: period, lastSuccess: now}
	switch {
	case period > 0:
		c.next = now
	case revisions > 0:
		c.next = now.Add(revisionInterval)
	}
	return c
}

// autoCompact runs the compactor if its time came.
func (n *node) autoCompact() {
	c := &n.compactor
	if c.next.IsZero() || n.now.Before(c.next) {
		return
	}
	if c.period > 0 {
		n.compactPeriodically()
		return
	}
	c.next = n.now.Add(revisionInterval)
	rev := n.st.rev - c.revisions
	if !n.leading() || rev <= 0 || rev == c.last {
		return
	}
	n.proposeCompact(rev)
}

// compactPeriodically is etcd's Periodic compactor, one step.
func (n *node) compactPeriodically() {
	c := &n.compactor
	interval := min(c.period, time.Hour)
	retry := interval / 10
	retentions := int(c.period/retry) + 1
	c.next = n.now.Add(retry)
	c.revs = append(c.revs, n.st.rev)
	if len(c.revs) > retentions {
		c.revs = c.revs[1:]
	}
	if !n.leading() {
		return
	}
	rev := c.revs[0]
	if n.now.Sub(c.lastSuccess) < c.base || rev == c.last {
		return
	}
	c.base = interval
	n.proposeCompact(rev)
}

// proposeCompact compacts at rev through the log, as a client's Compact
// would; one at a revision compacted already fails in the apply, which
// etcd counts as done.
func (n *node) proposeCompact(rev int64) {
	c := &n.compactor
	c.last, c.lastSuccess = rev, n.now
	if rev <= n.st.compactRev {
		return
	}
	n.logf("auto compaction at revision %d", rev)
	n.addOp(noHandle, kCompact, appendVarintField(nil, 1, uint64(rev)))
}
