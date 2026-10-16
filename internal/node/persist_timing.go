package node

import "time"

// persistTiming partitions one group's wall time. A rotation includes its
// final sync, truncate, close and new segment creation. A normal sync may
// cover several groups; their sync spans overlap. Reporting includes
// afterSync, earlier reports in the same sync batch, the persisted channel
// and the committer's Persisted calls. These are wall times, not CPU times.
type persistTiming struct {
	queued, reporting                             time.Time
	queue, append, flush, syncQueue, sync, rotation time.Duration
}

func (n *Node) logPersistTiming(done persistDone) {
	p := done.timing
	if p.queued.IsZero() {
		return
	}
	end := time.Now()
	if end.Sub(p.queued) <= n.cfg.Tick {
		return
	}
	var first, last uint64
	for _, j := range done.jobs {
		for _, e := range j.Entries {
			if first == 0 {
				first = e.GetIndex()
			}
			last = e.GetIndex()
		}
	}
	n.logf("persist timing: entries=%d..%d queued=%s end=%s total=%v queue=%v append=%v flush=%v sync_queue=%v sync=%v rotation=%v report=%v",
		first, last, p.queued.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano),
		end.Sub(p.queued), p.queue, p.append, p.flush, p.syncQueue, p.sync, p.rotation, end.Sub(p.reporting))
}
