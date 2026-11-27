package wl

import (
	"fmt"
	"testing"
)

// One piece can end after selecting, but before copying, a later version
// of its last key. Key-ID equality is not byte/field cursor identity.
func TestWatchFillChecksumDoesNotCrossVersionAtDescriptorBoundary(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	const count = 2050 // 2048 descriptors plus the next same-key version
	for i := range count {
		putValue(t, st, log, "/same", fmt.Sprintf("v-%04d", i))
	}
	dropRecords(st)
	s.kind = kWatch
	wi := n.w.add(watcher{sock: c.idx, gen: c.gen, sid: s.id, key: []byte("/same"), syncRev: 2})
	// Drive the serializer with a descriptor-sized cohort directly; the
	// ordinary sync-turn event budget is intentionally much smaller.
	x := &n.w.ws[wi]
	wire, err := n.watchFillWire(st, x, 2, st.rev+1, st.rev, n.term, false)
	if err != nil || n.newWatchFill(wi, x, c, s, wire, st.rev+1) == nil {
		t.Fatalf("could not admit the complete cohort: %v", err)
	}
	jobs := runFillWork(t, n, l)
	if st.bad != nil || jobs < 2 {
		t.Fatalf("checksum failed or descriptor boundary was not exercised: jobs=%d bad=%v", jobs, st.bad)
	}
	got := 0
	for _, response := range decodeWatchMessages(t, fillQueuedBytes(t, c)) {
		for _, event := range response.Events {
			if event.Kv.ModRevision != int64(got+2) || string(event.Kv.Value) != fmt.Sprintf("v-%04d", got) {
				t.Fatalf("event %d lost its own version/value: %v", got, event)
			}
			got++
		}
	}
	if got != count || n.w.ws[wi].syncRev != 0 {
		t.Fatalf("watch lost progress: events=%d cursor=%d revision=%d", got, n.w.ws[wi].syncRev, st.rev)
	}
	if n.fillBook.used > n.fillBook.limit || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool {
		t.Fatal("descriptor-boundary progress overspent W/P")
	}
}
