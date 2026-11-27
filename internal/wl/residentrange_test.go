package wl

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestResidentPointUsesMainSelectionWithoutCursorScan(t *testing.T) {
	for _, mode := range []string{"current", "negative-revision", "at-current-revision", "count", "keys", "filtered", "missing", "logical-loan"} {
		t.Run(mode, func(t *testing.T) {
			var baseline []byte
			for _, cursor := range []bool{false, true} {
				c, s, st, log := replyRoomClient(t)
				t.Cleanup(c.dropAll)
				st.setBudget(83<<20, 0)
				putValue(t, st, log, "/resident", string(bytes.Repeat([]byte("v"), 1024)))
				n := c.node
				n.cursorFetch = cursor
				key := "/resident"
				if mode == "missing" {
					key = "/missing"
				}
				msg := appendBytesField(nil, 1, []byte(key))
				switch mode {
				case "negative-revision":
					msg = appendVarintField(msg, 4, ^uint64(0))
				case "at-current-revision":
					msg = appendVarintField(msg, 4, uint64(st.rev))
				case "count":
					msg = appendVarintField(msg, 9, 1)
				case "keys":
					msg = appendVarintField(msg, 8, 1)
				case "filtered":
					msg = appendVarintField(msg, 10, uint64(st.rev+1))
				case "logical-loan":
					n.connOut = 128
				}
				serveRoomRange(t, c, s, msg)
				got := roomRangeReply(t, c)
				encoded, err := proto.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if !cursor {
					baseline = encoded
					continue
				}
				if !bytes.Equal(baseline, encoded) {
					t.Fatalf("main/cursor resident output differs: %v", got)
				}
				if n.stats.rangeFillScans != 0 || n.fillBook != nil || len(n.fills) != 0 || st.loads != 0 || n.legacyReading.c != nil {
					t.Fatalf("resident lookup performed a cursor/legacy fetch preflight: scans %d book %v fills %d loads %d", n.stats.rangeFillScans, n.fillBook, len(n.fills), st.loads)
				}
				if mode == "logical-loan" && (n.stageCredits.used == 0 || c.heldStage(&c.flow.held[0]) == nil || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool) {
					t.Fatal("resident shortcut bypassed complete P funding of its output loan")
				}
			}
		})
	}
}

func TestResidentPointDoesNotBypassHistoricalColdCursor(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/version", "before")
	old := st.rev
	putValue(t, st, log, "/version", "after")
	msg := appendVarintField(appendBytesField(nil, 1, []byte("/version")), 4, uint64(old))
	serveRoomRange(t, c, s, msg)
	n := c.node
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
	if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "before" || got.Kvs[0].ModRevision != old || got.Header.Revision != st.rev || n.stats.rangeFillScans == 0 {
		t.Fatalf("historical version lost cursor routing/identity: %+v scans=%d", got, n.stats.rangeFillScans)
	}
}
