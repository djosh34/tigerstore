package wl

import (
	"bytes"
	"fmt"
	"testing"
)

// These are small one-shot selections, not the paused large-response path.
// RangeStream's protocol chunks must not become separate snapshot decisions.
func TestResidentOneShotStreamKeepsMainWireAndChunkBoundaries(t *testing.T) {
	for _, mode := range []string{"plain", "limit", "keys", "count", "filter", "reverse", "value-sort", "loan"} {
		t.Run(mode, func(t *testing.T) {
			var want []byte
			for _, cursor := range []bool{false, true} {
				c, s, st, log, _ := fillRangeFixture(t)
				n := c.node
				n.cursorFetch = cursor
				s.kind = kRangeStream
				for i := range 40 {
					putValue(t, st, log, fmt.Sprintf("/stream/%02d", i), fmt.Sprintf("value-%02d", 39-i))
				}
				msg := appendBytesField(appendBytesField(nil, 1, []byte("/stream/")), 2, []byte("/stream0"))
				switch mode {
				case "limit":
					msg = appendVarintField(msg, 3, 35)
				case "keys":
					msg = appendVarintField(msg, 8, 1)
				case "count":
					msg = appendVarintField(msg, 9, 1)
				case "filter":
					msg = appendVarintField(msg, 10, uint64(st.rev-12))
				case "reverse":
					msg = appendVarintField(msg, 5, sortDescend)
				case "value-sort":
					msg = appendVarintField(appendVarintField(msg, 5, sortAscend), 6, sortByValue)
				case "loan":
					n.connOut = 64
				}
				serveRoomRange(t, c, s, msg)
				data := fillQueuedBytes(t, c)
				if !cursor {
					want = bytes.Clone(data)
					continue
				}
				if !bytes.Equal(data, want) {
					t.Fatalf("%s changed main RangeStream wire: want=%x got=%x", mode, want, data)
				}
				if len(n.fills) != 0 || n.fillBook != nil || n.stats.rangeFillScans != 0 || len(n.bigs) != 0 || st.loads != 0 {
					t.Fatal("hot one-shot stream entered a source cursor or large path")
				}
				parts := decodeFillStream(t, data)
				last := parts[len(parts)-1]
				count := int64(40)
				if mode == "filter" {
					count = 13
				}
				if last.Header.Revision != st.rev || last.Count != count {
					t.Fatal("final stream snapshot/count changed")
				}
				if mode == "limit" && (len(parts) != 3 || len(parts[0].Kvs) != 10 || len(parts[1].Kvs) != 20 || len(last.Kvs) != 5 || !last.More) {
					t.Fatal("main adaptive chunking or limit changed")
				}
				if mode == "loan" && (n.stageCredits.used == 0 || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool) {
					t.Fatal("stream loan not wholly P funded")
				}
			}
		})
	}
}

func TestResidentOneShotStreamColdSelectionFallsBackBeforeAnyChunk(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	s.kind = kRangeStream
	for i := range 40 {
		putValue(t, st, log, fmt.Sprintf("/stream/%02d", i), "original")
	}
	view := st.rev
	msg := appendVarintField(appendBytesField(appendBytesField(nil, 1, []byte("/stream/")), 2, []byte("/stream0")), 3, 35)
	var r rangeReq
	if err := parseRange(msg, &r); err != nil {
		t.Fatal(err)
	}
	reads := 0
	at := func(id uint32, rev int64) ([]byte, error) {
		reads++
		if reads == 2 {
			st.slab.free(st.keys[id].rec, int(st.keys[id].rlen))
			st.keys[id].rec = 0
		}
		return st.residentAt(id, rev)
	}
	if n.tryResidentRange(c, s, &r, msg, at) {
		t.Fatal("cold selection published a stream")
	}
	if len(fillQueuedBytes(t, c)) != 0 || len(n.fills) != 0 || n.fillBook != nil {
		t.Fatal("an early RangeStream chunk escaped before complete resident selection")
	}
	n.serveRangeSlow(c, s, &r, msg)
	n.pumpFills()
	if n.fillActive == nil {
		t.Fatal("cold fallback did not exercise off-loop fetching")
	}
	awaitFetchResult(t, l)
	// The original request's snapshot is held, even when a new version is
	// applied before the cold result's ACK/continuation is processed.
	putValue(t, st, log, "/stream/00", "replacement-a")
	putValue(t, st, log, "/stream/01", "replacement-b")
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	runFillWork(t, n, l)
	parts := decodeFillStream(t, fillQueuedBytes(t, c))
	if len(parts) != 3 || len(parts[0].Kvs) != 10 || len(parts[1].Kvs) != 20 || len(parts[2].Kvs) != 5 {
		t.Fatal("cursor fallback split/replayed an early chunk")
	}
	for i, part := range parts {
		if i < 2 && (part.Header != nil || part.Count != 0 || part.More) {
			t.Fatal("non-final chunk leaked final metadata")
		}
		for _, kv := range part.Kvs {
			if string(kv.Value) != "original" || kv.ModRevision > view {
				t.Fatalf("fallback mixed snapshots: %v", kv)
			}
		}
	}
	last := parts[2]
	if last.Header.Revision != view || last.Count != 40 || !last.More {
		t.Fatal("fallback changed original final header/count/more")
	}
}
