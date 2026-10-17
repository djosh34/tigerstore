package node

import "time"

// persistTimes are the wall times of one group on its way to the disk:
// queued for the writer, appended from start, flushed from flush to
// flushed, and synced from sync to synced, in one sync that may cover
// several groups. The committer logs those of a slow group (#88).
type persistTimes struct {
	queued, start, flush, flushed, sync, synced time.Time
}

func (n *Node) logPersistTiming(done persistDone) {
	p := done.times
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
	n.logf("persist timing: entries=%d..%d queued=%s end=%s total=%v queue=%v append=%v flush=%v sync_queue=%v sync=%v report=%v",
		first, last, p.queued.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano), end.Sub(p.queued),
		p.start.Sub(p.queued), p.flush.Sub(p.start), p.flushed.Sub(p.flush), p.sync.Sub(p.flushed), p.synced.Sub(p.sync), end.Sub(p.synced))
}
