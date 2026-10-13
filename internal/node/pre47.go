package node

import "sync/atomic"

// ClockCounters is an observer with no clock reads, mutexes, logging or per-event
// allocation. The append worker measures the change in arrivals while WAL.Sync
// runs, so its span includes the write, fsync and scheduling before resumption.
// Only the diagnostic variants allocate it. A paired control measures its cost.
type ClockCounters struct {
	arrivals    atomic.Uint64
	groups      atomic.Uint64
	during      atomic.Uint64
	entryGroups atomic.Uint64
	entries     atomic.Uint64
	entryBytes  atomic.Uint64
	hist        [513]atomic.Uint64
	inbox       [513]atomic.Uint64
	deferred    [513]atomic.Uint64
}

func (n *Node) ClockStats() map[string]any {
	if n.clocks == nil {
		return nil
	}
	c := n.clocks
	result := map[string]any{"arrivals": c.arrivals.Load(), "groups": c.groups.Load(), "arrivals_during_append": c.during.Load(), "entry_groups": c.entryGroups.Load(), "entries": c.entries.Load(), "entry_bytes": c.entryBytes.Load()}
	for name, src := range map[string]*[513]atomic.Uint64{"arrivals_per_append": &c.hist, "completion_inbox": &c.inbox, "completion_deferred": &c.deferred} {
		values := make([]uint64, len(src))
		for i := range src {
			values[i] = src[i].Load()
		}
		result[name] = values
	}
	return result
}
