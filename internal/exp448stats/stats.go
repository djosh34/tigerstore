// Package exp448stats is throwaway work accounting for research #448.
package exp448stats

import "sync/atomic"

var Sent, Delivered, Applied, Ticks, Writes, Bytes, Syncs, DirSyncs, WatchEvents, PutApplies, TxnApplies, SyncedBytes atomic.Int64

func Reset() {
	for _, p := range []*atomic.Int64{&Sent, &Delivered, &Applied, &Ticks, &Writes, &Bytes, &Syncs, &DirSyncs, &WatchEvents, &PutApplies, &TxnApplies, &SyncedBytes} {
		p.Store(0)
	}
}
func Snapshot() map[string]int64 {
	return map[string]int64{"sent": Sent.Load(), "delivered": Delivered.Load(), "applied": Applied.Load(), "ticks": Ticks.Load(), "writes": Writes.Load(), "bytes": Bytes.Load(), "fsyncs": Syncs.Load(), "dirsyncs": DirSyncs.Load(), "watch_events": WatchEvents.Load(), "put_applies": PutApplies.Load(), "txn_applies": TxnApplies.Load(), "synced_bytes": SyncedBytes.Load()}
}
