package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func readFillSocket(t *testing.T, fd int, wire *[]byte) {
	t.Helper()
	var p [64 << 10]byte
	for {
		n, err := unix.Read(fd, p[:])
		if n > 0 {
			*wire = append(*wire, p[:n]...)
		}
		if errors.Is(err, unix.EAGAIN) {
			return
		}
		if err != nil || n == 0 {
			t.Fatalf("read fill socket: %d, %v", n, err)
		}
	}
}

func fillData(t *testing.T, wire []byte) []byte {
	t.Helper()
	var data []byte
	for len(wire) > 0 {
		if len(wire) < 9 {
			t.Fatal("partial frame")
		}
		n := int(wire[0])<<16 | int(wire[1])<<8 | int(wire[2])
		if n > len(wire)-9 {
			t.Fatal("partial payload")
		}
		if wire[3] == frRST {
			t.Fatal("large cursor reset the stream")
		}
		if wire[3] == frData {
			data = append(data, wire[9:9+n]...)
		}
		wire = wire[9+n:]
	}
	return data
}

func driveLargeFills(t *testing.T, n *node, l *loop, c *client, fd int, afterPiece func()) []byte {
	t.Helper()
	var wire []byte
	for steps := 0; len(n.bigs) > 0 || len(n.bigWatches) > 0 || len(n.fills) > 0 || c.waiting() > 0; steps++ {
		if steps > 4096 {
			t.Fatal("large cursor stopped making finite progress")
		}
		n.pumpFills()
		n.pumpBig()
		n.pumpBigWatches()
		if n.fillActive != nil {
			awaitFetchResult(t, l)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
			n.pumpFills()
			if afterPiece != nil {
				afterPiece()
				afterPiece = nil
			}
		}
		n.markDirty(&c.sock)
		l.flush()
		readFillSocket(t, fd, &wire)
		if cap(c.out) > 64<<10 {
			t.Fatal("large cursor exceeded its funded socket staging")
		}
	}
	return wire
}

func TestLargeWatchTakesHoldsOnlyWithFirstData(t *testing.T) {
	c, _, st, log, _ := fillRangeFixture(t)
	putValue(t, st, log, "/k", string(bytes.Repeat([]byte("a"), 1100<<10)))
	putValue(t, st, log, "/k", string(bytes.Repeat([]byte("b"), 1100<<10)))
	n := c.node
	s, wi := establishFillWatch(t, n, c, "/k", 3)
	s.win = 0
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if len(n.bigWatches) != 1 || !n.w.ws[wi].big {
		t.Fatal("revision was not eligible for main's large path")
	}
	for range 3 {
		n.pumpBigWatches()
		if st.heldReads() != 0 || c.waiting() != 0 || n.bigWatchReady() {
			t.Fatal("initial zero windows published data or pinned history")
		}
	}
	c.flow.conn = 1
	s.win = 1
	n.pumpBigWatches()
	b := n.bigWatches[0]
	if !b.started || !b.held || !b.heldPrev || st.heldReads() != 2 || c.waiting() == 0 {
		t.Fatal("first DATA and both required holds did not commit together")
	}
	n.endBigWatch(b)
	n.bigWatches = nil
}

func TestLargeWatchCursorSurvivesOverwriteAndCheckpointBetweenPieces(t *testing.T) {
	n, log := ckptNode(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.sizes, n.connOut, n.cursorFetch = sizeFor(83<<20), 64<<10, true
	n.st.setBudget(83<<20, 0)
	old := bytes.Repeat([]byte("a"), 1100<<10)
	current := bytes.Repeat([]byte("b"), 1100<<10)
	putValue(t, n.st, log, "/k", string(old))
	putValue(t, n.st, log, "/k", string(current))
	n.st.log = &fillColdLog{memLog: log}
	id, _ := n.st.find([]byte("/k"))
	n.st.keys[id].rec = 0
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
	l := &loop{n: n, hoFD: -1, epfd: -1}
	n.shell = l
	t.Cleanup(func() {
		if l.fetchWorker != nil {
			l.fetchWorker.stop()
			_ = l.runFetchCompletion()
		}
		c.dropAll()
		n.pumpFills()
	})
	s, wi := establishFillWatch(t, n, c, "/k", 3)
	fd := roomSocket(t, c)
	c.flow.conn, s.win = 1<<30, 1<<30
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	wire := driveLargeFills(t, n, l, c, fd, func() {
		putValue(t, n.st, log, "/k", "overwritten")
		n.shell = nil
		writeCkptAt(t, n, 9)
		n.shell = l
	})
	messages := decodeWatchMessages(t, fillData(t, wire))
	if len(messages) != 1 || len(messages[0].Events) != 1 {
		t.Fatal("large revision was fragmented or replayed")
	}
	ev := messages[0].Events[0]
	if messages[0].Header.Revision != 3 || !bytes.Equal(ev.Kv.Value, current) || ev.PrevKv == nil || !bytes.Equal(ev.PrevKv.Value, old) {
		t.Fatal("checkpoint or overwrite changed current/previous bytes")
	}
	if n.w.ws[wi].syncRev != 4 || n.st.heldReads() != 0 || n.stageCredits.used != 0 {
		t.Fatal("large revision did not advance/release its cursor")
	}
}

func TestLargeRangeCursorUsesBoundedDestinationPieces(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	n.connOut = 64 << 10
	c.plain = true
	c.flow.conn, s.win = 1<<30, 1<<30
	for _, k := range []string{"/k/a", "/k/b", "/k/c"} {
		putValue(t, st, log, k, string(bytes.Repeat([]byte(k[len(k)-1:]), 900<<10)))
	}
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	fd := roomSocket(t, c)
	l.epfd = -1
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	serveRoomRange(t, c, s, msg)
	wire := driveLargeFills(t, n, l, c, fd, nil)
	data := fillData(t, wire)
	if len(data) < 5 || int(binary.BigEndian.Uint32(data[1:5])) != len(data)-5 {
		t.Fatal("large range message boundary changed")
	}
	var r pb.RangeResponse
	if err := proto.Unmarshal(data[5:], &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 3 || r.Count != 3 {
		t.Fatal("large range cursor skipped records")
	}
	for i, kv := range r.Kvs {
		if !bytes.Equal(kv.Value, bytes.Repeat([]byte{byte('a' + i)}, 900<<10)) {
			t.Fatal("large range value changed")
		}
	}
	if n.stageCredits.used != 0 || st.heldReads() != 0 || len(st.away) != 0 || len(st.fetched) != 0 {
		t.Fatal("large range leaked or used the legacy cache")
	}
}
