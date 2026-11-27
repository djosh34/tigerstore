package wl

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// A final record may overshoot main's pre-next-record input bound. That
// outcome is unchanged; #908 does not exempt our canonical transport index.
func TestLegacyTinyInputsWithOversizedLastRecordComplete(t *testing.T) {
	var baseline int64
	const count = 100001
	for _, cursor := range []bool{false, true} {
		c, s, st, log, l := fillRangeFixture(t)
		n := c.node
		n.cursorFetch = cursor
		if !cursor {
			st.log = log
		}
		var scratch int64
		n.measureSort = func(a, b int) { scratch = max(scratch, int64(a)*24+int64(b)*96) }
		for i := range count - 1 {
			putValue(t, st, log, string([]byte{byte(i >> 16), byte(i >> 8), byte(i)}), "v")
		}
		putValue(t, st, log, string([]byte{1, 255, 255}), string(bytes.Repeat([]byte{'z'}, 1280<<10)))
		if cursor {
			dropRecords(st)
		}
		msg := appendBytesField(nil, 1, []byte{0})
		msg = appendBytesField(msg, 2, []byte{2})
		msg = appendVarintField(msg, 3, 1)
		msg = appendVarintField(msg, 5, sortAscend)
		msg = appendVarintField(msg, 6, sortByValue)
		msg = appendVarintField(msg, 8, 1)
		serveRoomRange(t, c, s, msg)
		for step := 0; n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
			if step > 1200 {
				t.Fatalf("main-admitted last-record overshoot did not finish; scratch excluded, P=%d", n.sizes.clientPool)
			}
			n.pumpFills()
			if n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool || n.fillBook.used > n.fillBook.limit {
				t.Fatal("new storage exceeded P/W")
			}
			if n.legacyRunning != nil {
				awaitFetchResult(t, l)
				if err := l.runPosts(); err != nil {
					t.Fatal(err)
				}
			}
		}
		var response *pb.RangeResponse
		if !cursor {
			response = roomRangeReply(t, c)
			baseline = scratch
		} else {
			response = &pb.RangeResponse{}
			data := fillQueuedBytes(t, c)
			if len(data) < 5 || proto.Unmarshal(data[5:], response) != nil {
				t.Fatal("invalid reply")
			}
		}
		if response.Count != count || len(response.Kvs) != 1 || len(response.Kvs[0].Value) != 0 || !response.More {
			t.Fatal("main's selected result changed")
		}
		if cursor && (scratch == 0 || scratch > baseline) {
			t.Fatal("main sort representation/capacity changed")
		}
	}
}
