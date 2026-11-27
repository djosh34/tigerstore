package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestStreamCursorChunkAdaptationTracksTheTarget(t *testing.T) {
	for _, tt := range []struct {
		target int
		want   int64
	}{{32, 5}, {128, 10}, {256, 20}} {
		if got := nextChunk(10, 100, tt.target); got != tt.want {
			t.Fatalf("target %d: got %d want %d", tt.target, got, tt.want)
		}
	}
}

func decodeFillStream(t *testing.T, data []byte) []*pb.RangeResponse {
	t.Helper()
	var out []*pb.RangeResponse
	for len(data) > 0 {
		if len(data) < 5 {
			t.Fatal("partial stream prefix")
		}
		size := int(binary.BigEndian.Uint32(data[1:5]))
		if size > len(data)-5 {
			t.Fatal("partial stream message")
		}
		var response pb.RangeStreamResponse
		if err := proto.Unmarshal(data[5:5+size], &response); err != nil {
			t.Fatal(err)
		}
		out = append(out, response.RangeResponse)
		data = data[5+size:]
	}
	return out
}

func TestCursorRangeStreamKeepsChunkBoundariesAndFinalView(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	s.kind = kRangeStream
	for i := range 100 {
		putValue(t, st, log, fmt.Sprintf("/k/%03d", i), "old")
	}
	view := st.rev
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	msg = appendVarintField(msg, 3, 35)
	serveRoomRange(t, c, s, msg)
	n := c.node
	n.pumpFills()
	awaitFetchResult(t, l)
	putValue(t, st, log, "/k/000", "new")
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	parts := decodeFillStream(t, fillQueuedBytes(t, c))
	if len(parts) != 3 || len(parts[0].Kvs) != 10 || len(parts[1].Kvs) != 20 || len(parts[2].Kvs) != 5 {
		t.Fatal("etcd's adaptive chunk boundaries changed")
	}
	for i, p := range parts {
		if i < 2 && (p.Header != nil || p.More || p.Count != 0) {
			t.Fatal("non-final chunk acquired final fields")
		}
		for _, kv := range p.Kvs {
			if string(kv.Value) != "old" || kv.ModRevision > view {
				t.Fatal("stream mixed admitted views")
			}
		}
	}
	last := parts[len(parts)-1]
	if last.Header.Revision != view || !last.More || last.Count != 100 || len(n.bigs) != 0 {
		t.Fatal("whole small stream became an abortable large reply or changed its final header/count")
	}
}

func TestLargeCursorRangeStreamUsesTheSameChunkProtocol(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	s.kind = kRangeStream
	n := c.node
	n.connOut = 64 << 10
	c.plain = true
	c.flow.conn, s.win = 1<<30, 1<<30
	for i := range 40 {
		putValue(t, st, log, fmt.Sprintf("/k/%03d", i), string(bytes.Repeat([]byte{byte(i)}, 64<<10)))
	}
	view := st.rev
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	fd := roomSocket(t, c)
	l.epfd = -1
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	serveRoomRange(t, c, s, msg)
	parts := decodeFillStream(t, fillData(t, driveLargeFills(t, n, l, c, fd, nil)))
	count := 0
	for _, p := range parts {
		for _, kv := range p.Kvs {
			if !bytes.Equal(kv.Value, bytes.Repeat([]byte{byte(count)}, 64<<10)) {
				t.Fatal("stream value changed")
			}
			count++
		}
	}
	last := parts[len(parts)-1]
	if count != 40 || last.Header.Revision != view || last.Count != 40 || last.More || st.heldReads() != 0 || n.stageCredits.used != 0 {
		t.Fatal("large stream did not finish its exact cursor")
	}
}
