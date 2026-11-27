package wl

import (
	"fmt"
	"testing"
)

func TestResidentSmallWatchWithoutMatchesStillHonorsTurnBudget(t *testing.T) {
	c, _, st, log, _ := fillRangeFixture(t)
	n := c.node
	for i := range 10 {
		putValue(t, st, log, fmt.Sprintf("/not-watched/%d", i), "small")
	}
	s, wi := establishFillWatch(t, n, c, "/never", 2)
	x := &n.w.ws[wi]
	b := syncBudget{changes: 3, events: syncEvents, bytes: n.syncBytes(), bigAt: -1}
	handled, done, err := n.tryResidentWatch(x, c, s, &b)
	if !handled || done || err != nil || b.changes != 0 || x.syncRev != 5 || len(fillQueuedBytes(t, c)) != 0 || len(n.fills) != 0 {
		t.Fatalf("non-matching small revisions ignored shared turn: handled=%v done=%v err=%v budget=%+v next=%d", handled, done, err, b, x.syncRev)
	}
}

func TestResidentSmallWatchSelectionAndWholeColdFallback(t *testing.T) {
	for _, cold := range []bool{false, true} {
		name := "resident"
		if cold {
			name = "cold-during-selection"
		}
		t.Run(name, func(t *testing.T) {
			c, _, st, log, l := fillRangeFixture(t)
			n := c.node
			putValue(t, st, log, "/resident/a", "first")
			putValue(t, st, log, "/resident/b", "second")
			s, wi := establishFillWatch(t, n, c, "/resident/", 2)
			x := &n.w.ws[wi]
			x.end = []byte("/resident0")
			view, from := st.rev, x.syncRev
			if cold {
				idB, _ := st.find([]byte("/resident/b"))
				reads := 0
				put := func(id uint32, rev int64) ([]byte, error) {
					reads++
					if reads == 2 {
						st.slab.free(st.keys[idB].rec, int(st.keys[idB].rlen))
						st.keys[idB].rec = 0
					}
					return st.residentPutAt(id, rev)
				}
				b := syncBudget{changes: syncChanges, events: syncEvents, bytes: n.syncBytes(), bigAt: -1}
				before := b
				handled, done, err := n.residentWatchRead(x, c, s, &b, st.residentAt, put)
				if handled || done || err != nil || b != before || x.syncRev != from || st.rev != view {
					t.Fatalf("cold probe advanced logical snapshot/budget: handled=%v done=%v err=%v", handled, done, err)
				}
				if len(fillQueuedBytes(t, c)) != 0 || len(n.fills) != 0 || n.fillBook != nil || len(n.w.syncBuf) != 0 {
					t.Fatal("cold probe leaked partial output or source ownership")
				}
			}
			if err := n.syncWatchers(); err != nil {
				t.Fatal(err)
			}
			if cold {
				runFillWork(t, n, l)
			} else if len(n.fills) != 0 || n.fillBook != nil || st.loads != 0 {
				t.Fatal("resident small watch created a source cursor")
			}
			got := decodeWatchMessages(t, fillQueuedBytes(t, c))
			if len(got) != 1 || got[0].Header.Revision != view || len(got[0].Events) != 2 || string(got[0].Events[0].Kv.Value) != "first" || string(got[0].Events[1].Kv.Value) != "second" || x.syncRev != 0 {
				t.Fatalf("not one complete same-snapshot watch response: %v", got)
			}
		})
	}
}
