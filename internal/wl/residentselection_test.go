package wl

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestResidentMultiRangeUsesMainSelectionAndFundedOutput(t *testing.T) {
	for _, mode := range []string{"plain", "keys", "count", "filter", "sort", "loan"} {
		t.Run(mode, func(t *testing.T) {
			var want []byte
			for _, cursor := range []bool{false, true} {
				c, s, st, log := replyRoomClient(t)
				t.Cleanup(c.dropAll)
				st.setBudget(83<<20, 0)
				n := c.node
				n.cursorFetch = cursor
				for i := range 8 {
					putValue(t, st, log, fmt.Sprintf("/resident/%02d", i), string(bytes.Repeat([]byte{byte('a' + i)}, 1024)))
				}
				msg := appendBytesField(appendBytesField(nil, 1, []byte("/resident/")), 2, []byte("/resident0"))
				switch mode {
				case "keys":
					msg = appendVarintField(msg, 8, 1)
				case "count":
					msg = appendVarintField(msg, 9, 1)
				case "filter":
					msg = appendVarintField(msg, 10, uint64(st.rev-3))
				case "sort":
					msg = appendVarintField(msg, 5, sortDescend)
				case "loan":
					n.connOut = 1024
				}
				serveRoomRange(t, c, s, msg)
				got := roomRangeReply(t, c)
				encoded, err := proto.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if !cursor {
					want = encoded
					continue
				}
				if !bytes.Equal(want, encoded) {
					t.Fatalf("resident %s differs from main: %v", mode, got)
				}
				if n.stats.rangeFillScans != 0 || len(n.fills) != 0 || st.loads != 0 {
					t.Fatal("resident batch entered source cursor")
				}
				if mode == "loan" && (n.stageCredits.used == 0 || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool) {
					t.Fatal("resident direct output loan was not completely P funded")
				}
			}
		})
	}
}

func TestResidentRangeColdDuringSelectionFallsBackWhole(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/boundary/a", "first")
	putValue(t, st, log, "/boundary/b", "second")
	n := c.node
	idB, ok := st.find([]byte("/boundary/b"))
	if !ok {
		t.Fatal("missing key")
	}
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/boundary/")), 2, []byte("/boundary0"))
	var r rangeReq
	if err := parseRange(msg, &r); err != nil {
		t.Fatal(err)
	}
	before := st.rev
	reads := 0
	accessor := func(id uint32, rev int64) ([]byte, error) {
		reads++
		if reads == 2 {
			st.slab.free(st.keys[idB].rec, int(st.keys[idB].rlen))
			st.keys[idB].rec = 0
		}
		return st.residentAt(id, rev)
	}
	if n.tryResidentRange(c, s, &r, msg, accessor) {
		t.Fatal("cold boundary published a resident reply")
	}
	if st.rev != before || len(c.flow.held) != 0 || len(n.fills) != 0 || n.fillBook != nil {
		t.Fatal("partial output/fetch state escaped failed resident selection")
	}
	n.serveRangeSlow(c, s, &r, msg)
	for len(n.fills) > 0 {
		n.pumpFills()
		if n.fillActive != nil {
			awaitFetchResult(t, l)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := roomRangeReply(t, c)
	if got.Header.Revision != before || got.Count != 2 || len(got.Kvs) != 2 || string(got.Kvs[0].Value) != "first" || string(got.Kvs[1].Value) != "second" {
		t.Fatalf("whole cursor fallback lost snapshot/value/count: %v", got)
	}
}
