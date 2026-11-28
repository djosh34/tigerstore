package wl

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestWatchFillScalarSizeMatchesMainFragmentBoundaries(t *testing.T) {
	c, _, _, _, _ := fillRangeFixture(t)
	n := c.node
	for _, adds := range [][]int{
		{1},
		{maxRequestBytes + 100},
		{20, 30, 40},
		{maxRequestBytes / 2, maxRequestBytes / 2},
		{maxRequestBytes - 64, 32, 32, maxRequestBytes - 32, 16},
	} {
		for _, fragmented := range []bool{false, true} {
			x := watcher{id: 128, fragment: fragmented}
			hdr := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
			s := watchFillSize{header: hdr, part: hdr + 2}
			body := hdr
			for _, add := range adds {
				s.add(add)
				body += add
			}
			want := n.watchWire(&x, body, func(f func(int)) {
				for _, add := range adds {
					f(add)
				}
			})
			got := 5 + body
			if fragmented && body >= maxRequestBytes && len(adds) > 1 {
				got = s.wire()
			}
			if got != want {
				t.Fatalf("fragment=%v adds=%v: scalar=%d main=%d", fragmented, adds, got, want)
			}
		}
	}
	if unsafe.Sizeof(bigEv{}) != 32 {
		t.Fatalf("metadata hint changed main's event descriptor: %d bytes", unsafe.Sizeof(bigEv{}))
	}
}

func TestWatchFillReusesSelectedUnfragmentedMessageSize(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	n := c.node
	const count = 200
	for i := range count {
		putValue(t, st, log, "/same", fmt.Sprintf("version-%04d", i))
	}
	dropRecords(st)
	s, wi := establishFillWatch(t, n, c, "/same", 2)
	x := &n.w.ws[wi]
	budget := syncBudget{changes: count + 1, events: count + 1, bytes: 1 << 20, bigAt: -1}
	if _, err := n.startWatchFill(wi, x, c, s, &budget); err != nil {
		t.Fatal(err)
	}
	if len(n.fills) != 1 {
		t.Fatal("selection did not admit one complete fill")
	}
	q := n.fills[0]
	if q.watch.fragmented || q.watch.messageEvents != count || q.watch.payloadBytes != q.stage.size-5 || q.watch.messages != 0 {
		t.Fatalf("selection did not cache its exact first message: %+v", q.watch)
	}
	runFillWork(t, n, l)
	responses := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if len(responses) != 1 || len(responses[0].Events) != count {
		t.Fatalf("cached selection changed response boundaries: %v", responses)
	}
	for i, event := range responses[0].Events {
		if event.Kv.ModRevision != int64(i+2) || string(event.Kv.Value) != fmt.Sprintf("version-%04d", i) {
			t.Fatalf("event %d lost its selected history state: %v", i, event)
		}
	}
}

func TestStagedResidentWatchDoesNotRepeatInitialHeaders(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	n := c.node
	putValue(t, st, log, "/headers", "cold")
	dropRecords(st)
	s, wi := establishFillWatch(t, n, c, "/headers", 2)
	x := &n.w.ws[wi]
	x.prevKV = false // This case exercises a wholly resident second batch.
	if !s.watch || !s.streaming {
		t.Fatal("the watch wrote initial headers without marking its response open")
	}
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	runFillWork(t, n, l)
	if n.stageCredits.used == 0 {
		t.Fatal("the cold response did not remain P-funded behind the zero DATA window")
	}
	putValue(t, st, log, "/headers", "resident")
	// Build the subsequent resident batch while the earlier cold batch owns P.
	x.syncRev = st.rev
	n.w.unsynced = append(n.w.unsynced, s.wids[0])
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	stages := 0
	for _, h := range c.flow.held {
		if stage := c.heldStage(&h); stage != nil {
			stages++
			if stage.responsePending {
				t.Fatal("a later watch batch would inject HEADERS into the existing response")
			}
		}
	}
	got := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if stages != 2 || len(got) != 2 || len(got[0].Events) != 1 || len(got[1].Events) != 1 || string(got[0].Events[0].Kv.Value) != "cold" || string(got[1].Events[0].Kv.Value) != "resident" {
		t.Fatalf("cold/resident batches were replayed or merged: stages=%d responses=%v rev=%d next=%d wait=%v slow=%v short=%v P=%d", stages, got, st.rev, x.syncRev, x.fetchWait, c.slow(), n.roomShort, n.stageCredits.used)
	}
}

func TestEmptyFillCreditWakeDoesNotSpinOrWakeUnrelatedReaders(t *testing.T) {
	n := &node{cursorFetch: true, fillCreditWake: true}
	if n.fillsReady() {
		t.Fatal("a returned loan with no waiter creates an empty epoll turn")
	}
	n.updateFillCredits()
	if n.fillCreditWake || n.roomFreed {
		t.Fatal("an empty credit notification woke unrelated work")
	}
	n.fillCreditWake, n.legacyWaitP = true, true
	if !n.fillsReady() {
		t.Fatal("a real P-blocked legacy fill lost its credit wake")
	}
	n.updateFillCredits()
	if n.legacyWaitP || !n.roomFreed {
		t.Fatal("a real credit waiter was not released")
	}
	n.fillCreditWake, n.roomFreed = true, false
	n.roomWait = []roomRead{{}}
	if !n.fillsReady() {
		t.Fatal("an unsent range waiting for P lost its credit wake")
	}
	n.updateFillCredits()
	if !n.roomFreed {
		t.Fatal("the waiting unsent range was not allowed to retry")
	}
}
