package peer

import (
	"fmt"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/metrics"
)

// Prototype 602-5: counts of the peer traffic, logged every countEvery as
// one "peercount" line of cumulative values, with this process's client
// calls, so that a log gives them per operation.

// countEvery is how often the transport logs its counts.
const countEvery = time.Second

// pcounts are a transport's counts.
type pcounts struct {
	// framesSent and framesRecv are the bytes of Raft frames, length
	// prefix included.
	framesSent, framesRecv atomic.Int64
	// apps are the MsgApps sent, appEntries their entries, appData the
	// entries' data bytes as sent.
	apps, appEntries, appData atomic.Int64
	// props are the MsgProps sent, propData their entries' data bytes.
	props, propData atomic.Int64
	// The dedup of prototype 602-5. hashed and full are the entries of
	// sent MsgApps that went out as a hash and in full. side and sideData
	// are the side copies sent and their bytes. miss counts entries of
	// received MsgApps whose hash this node did not hold on arrival,
	// missWait their total wait and missMax the longest, in µs; unresolved
	// those it still did not hold after the wait, which it asked the
	// leader for (nacks).
	hashed, full, side, sideData, miss, missWait, missMax, unresolved atomic.Int64
	// resolved counts the entries of received MsgApps that were a hash
	// held on arrival.
	resolved atomic.Int64
}

// count counts a frame of n bytes that sent m.
func (c *pcounts) count(m *raftpb.Message, n int) {
	c.framesSent.Add(int64(n))
	switch m.GetType() {
	case raftpb.MsgApp:
		c.apps.Add(1)
		c.appEntries.Add(int64(len(m.Entries)))
		var d int64
		for _, e := range m.Entries {
			d += int64(len(e.Data))
		}
		c.appData.Add(d)
	case raftpb.MsgProp:
		c.props.Add(1)
		var d int64
		for _, e := range m.Entries {
			d += int64(len(e.Data))
		}
		c.propData.Add(d)
	}
}

// logCounts logs the counts every countEvery until the transport closes.
func (t *TCP) logCounts() {
	tick := time.NewTicker(countEvery)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-t.ctx.Done():
			return
		}
		c := &t.pc
		t.logf("%s", fmt.Sprintf("peercount id=%d unixms=%d calls=%d sock_w=%d sock_wb=%d sock_r=%d sock_rb=%d frames_sent=%d frames_recv=%d apps=%d app_entries=%d app_data=%d props=%d prop_data=%d hashed=%d full=%d side=%d side_data=%d resolved=%d miss=%d miss_wait_us=%d miss_max_us=%d unresolved=%d",
			t.cfg.ID, time.Now().UnixMilli(), metrics.ClientCalls.Load(),
			t.io.Writes.Load(), t.io.WriteBytes.Load(), t.io.Reads.Load(), t.io.ReadBytes.Load(),
			c.framesSent.Load(), c.framesRecv.Load(), c.apps.Load(), c.appEntries.Load(), c.appData.Load(),
			c.props.Load(), c.propData.Load(), c.hashed.Load(), c.full.Load(), c.side.Load(), c.sideData.Load(),
			c.resolved.Load(), c.miss.Load(), c.missWait.Load(), c.missMax.Load(), c.unresolved.Load()))
	}
}
