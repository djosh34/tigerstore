package node

// Experiment-only timing aggregates for #484. Disabled unless GAP_TIMING=1.
// Group means are not critical-path request latency (groups overlap).
import (
	"encoding/json"
	"expvar"
	"os"
	"sync"
	"time"
)

var gapTiming = os.Getenv("GAP_TIMING") == "1"
var gapMu sync.Mutex
var gapStages = map[string]int64{}

func init() {
	if gapTiming {
		expvar.Publish("ceiling_gap", expvar.Func(func() any {
			gapMu.Lock()
			defer gapMu.Unlock()
			b, _ := json.Marshal(gapStages)
			return json.RawMessage(b)
		}))
	}
}

func gapPersist(done persistDone, end time.Time) {
	if !gapTiming || done.times.queued.IsZero() {
		return
	}
	p := done.times
	gapMu.Lock()
	defer gapMu.Unlock()
	gapStages["groups"]++
	for _, j := range done.jobs {
		for _, e := range j.Entries {
			gapStages["entries"]++
			gapStages["entry_data_bytes"] += int64(len(e.Data))
		}
	}
	for k, d := range map[string]time.Duration{
		"queue_ns": p.start.Sub(p.queued), "append_ns": p.flush.Sub(p.start),
		"flush_ns": p.flushed.Sub(p.flush), "sync_queue_ns": p.sync.Sub(p.flushed),
		"sync_ns": p.synced.Sub(p.sync), "report_ns": end.Sub(p.synced),
		"total_ns": end.Sub(p.queued),
	} {
		gapStages[k] += int64(d)
	}
}
