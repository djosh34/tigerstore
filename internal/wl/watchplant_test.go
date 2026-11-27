//go:build planted

package wl

import (
	"bytes"
	"slices"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
)

func plantedDeliveryBytes(t *testing.T, cursor, paused, historical, cold bool, groups [][]string) []byte {
	t.Helper()
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	n.cursorFetch = cursor
	s.kind = kWatch
	wi := int32(-1)
	watch := watcher{sock: c.idx, gen: c.gen, sid: s.id, id: 1, key: []byte("/"), end: []byte{0}}
	if !historical {
		wi = n.w.add(watch)
	}
	for _, keys := range groups {
		st.rev++
		for _, key := range keys {
			id, err := st.intern([]byte(key))
			if err != nil {
				t.Fatal(err)
			}
			value := "value:" + key
			if _, err := st.put(id, []byte(value), 0, st.rev, log.add(value), false); err != nil {
				t.Fatal(err)
			}
			n.w.record(st, id, false, st.rev, st.keyAt(id, st.rev), nil)
		}
		st.endApply()
	}
	c.paused = paused
	n.flushEvents()
	c.paused = false
	if historical {
		watch.syncRev = 2
		wi = n.w.add(watch)
		n.w.unsynced = append(n.w.unsynced, wi)
	}
	if cursor && cold {
		dropRecords(st)
	}
	for step := 0; n.w.ws[wi].syncRev != 0; step++ {
		if step > 8 {
			t.Fatal("mutated delivery did not complete")
		}
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
		runFillWork(t, n, l)
	}
	if cursor && len(n.w.ws[wi].deliveryPlant.events) != 0 {
		t.Fatal("queue commit retained the mutation plan")
	}
	return fillQueuedBytes(t, c)
}

func TestCursorPlantedLiveDeliveryExactlyMatchesMainMutation(t *testing.T) {
	old := planted.Active
	t.Cleanup(func() { planted.Active = old })
	for _, bug := range []string{"B06a", "B06b"} {
		for _, cold := range []bool{false, true} {
			for _, tc := range []struct {
				name   string
				groups [][]string
				want   []string
			}{
				{"single-event", [][]string{{"/a"}}, []string{"/a"}},
				{"one-write", [][]string{{"/a", "/b", "/c"}}, []string{"/a", "/b"}},
				// B06a drops only the final write's final event, as main's
				// linked-list mutation does, not one from every revision.
				{"two-writes", [][]string{{"/a", "/b"}, {"/c", "/d"}}, []string{"/a", "/b", "/c"}},
			} {
				t.Run(bug+"/"+tc.name+"/cold="+strconv.FormatBool(cold), func(t *testing.T) {
					planted.Active = bug
					main := plantedDeliveryBytes(t, false, false, false, false, tc.groups)
					cursor := plantedDeliveryBytes(t, true, false, false, cold, tc.groups)
					if !bytes.Equal(main, cursor) {
						t.Fatal("cursor differs from main's actual mutated protobuf delivery")
					}
					want := tc.want
					if bug == "B06b" {
						want = nil
						for _, keys := range tc.groups {
							want = append(want, keys[0])
							want = append(want, keys...)
						}
					}
					var got []string
					for _, response := range decodeWatchMessages(t, cursor) {
						for _, event := range response.Events {
							got = append(got, string(event.Kv.Key))
							if string(event.Kv.Value) != "value:"+string(event.Kv.Key) {
								t.Fatal("mutation corrupted a value instead of dropping/duplicating an event")
							}
						}
					}
					if !slices.Equal(got, want) {
						t.Fatalf("wrong mutation: got %v, want %v", got, want)
					}
				})
			}
		}
	}
}

func TestCursorPlantsDoNotBroadenToHistoricalOrBlockedMainDelivery(t *testing.T) {
	old := planted.Active
	t.Cleanup(func() { planted.Active = old })
	groups := [][]string{{"/a", "/b"}, {"/c", "/d"}}
	for _, bug := range []string{"B06a", "B06b"} {
		for _, historical := range []bool{false, true} {
			planted.Active = ""
			want := plantedDeliveryBytes(t, false, !historical, historical, false, groups)
			planted.Active = bug
			main := plantedDeliveryBytes(t, false, !historical, historical, false, groups)
			cursor := plantedDeliveryBytes(t, true, !historical, historical, true, groups)
			if !bytes.Equal(main, want) || !bytes.Equal(cursor, want) {
				t.Fatal("plant was broadened beyond main's admitted live-delivery path")
			}
		}
	}
}

func TestCursorPlantResumeAndRetirement(t *testing.T) {
	c, _, st, log, _ := fillRangeFixture(t)
	n := c.node
	putValue(t, st, log, "/a", "value")
	ch := st.changes[revStart(st, 2)]
	x := watcher{key: []byte("/a"), deliveryPlant: watchDeliveryPlant{st: st, events: []change{ch, ch}}}
	var offsets []int
	if err := n.eachRevEvent(st, &x, revStart(st, 2), 1, func(off int, ev bigEv) bool {
		offsets = append(offsets, off)
		if ev.ch != ch {
			t.Fatal("resume changed logical event identity")
		}
		return true
	}); err != nil || !slices.Equal(offsets, []int{1}) {
		t.Fatalf("duplicate did not resume at its own ordinal: offsets=%v err=%v", offsets, err)
	}
	x.deliveryPlant.retire(2)
	if len(x.deliveryPlant.events) != 2 {
		t.Fatal("uncommitted revision lost its mutation")
	}
	x.deliveryPlant.retire(3)
	if x.deliveryPlant.st != nil || x.deliveryPlant.events != nil {
		t.Fatal("committed revision retained mutation state")
	}
	x.deliveryPlant = watchDeliveryPlant{st: st, events: []change{ch}}
	wi := n.w.add(x)
	n.w.remove(wi)
	if n.w.ws[wi].deliveryPlant.events != nil {
		t.Fatal("cancel retained a prior watch incarnation's mutation")
	}
	x.deliveryPlant = watchDeliveryPlant{st: st, events: []change{ch}}
	replacement, _ := newTestStore(t)
	handled, err := x.deliveryPlant.each(n, replacement, &x, 0, 0, func(int, bigEv) bool {
		t.Fatal("old-store mutation reached a replacement")
		return false
	})
	if handled || err != nil || x.deliveryPlant.events != nil || x.deliveryPlant.st != nil {
		t.Fatal("store replacement retained old mutation ownership")
	}
}

func TestCursorPlantCaptureDoesNotReserveOrSpendDeniedMainRoom(t *testing.T) {
	old := planted.Active
	planted.Active = "B06b"
	t.Cleanup(func() { planted.Active = old })
	c, _, _, _, _ := fillRangeFixture(t)
	n := c.node
	n.connOut, n.reserved = 1024, n.sizes.reserve()
	w := watches{events: []event{{next: -1, key: 0, rev: 2}}}
	var p watchDeliveryPlant
	p.capture(n, c, &w, 0, 4096)
	if len(p.events) != 0 || c.roomHeld != 0 || n.reserved != n.sizes.reserve() {
		t.Fatal("plant reserved credit or mutated main's refused live delivery")
	}
}
