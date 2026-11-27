package wl

import (
	"bytes"
	"fmt"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

type fillColdLog struct{ *memLog }

func (*fillColdLog) readNoWait([]byte, int64) error { return errAway }

func fillRangeFixture(t *testing.T) (*client, *stream, *store, *memLog, *loop) {
	t.Helper()
	c, s, st, log := replyRoomClient(t)
	st.setBudget(83<<20, 0)
	n := c.node
	n.sizes = sizeFor(83 << 20)
	n.cursorFetch = true
	st.log = &fillColdLog{memLog: log}
	l := &loop{n: n, hoFD: -1}
	n.shell = l
	t.Cleanup(func() {
		if l.fetchWorker != nil {
			l.fetchWorker.stop()
			_ = l.runFetchCompletion()
		}
		c.dropAll()
		n.pumpFills()
	})
	return c, s, st, log, l
}

func stagedRangeResponse(t *testing.T, c *client) *pb.RangeResponse {
	t.Helper()
	for _, h := range c.flow.held {
		if h.stage == nil {
			continue
		}
		s := h.stage
		wire := make([]byte, s.size)
		for at := 0; at < len(wire); {
			p := s.page(at)
			n := copy(wire[at:], p[stageHead+at%stagePayload:])
			at += n
		}
		var r pb.RangeResponse
		if err := proto.Unmarshal(wire[5:], &r); err != nil {
			t.Fatal(err)
		}
		return &r
	}
	t.Fatal("the range did not commit a whole staged response")
	return nil
}

func TestCursorFulfillmentPromotesForAnotherRequest(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/reuse", "original")
	id, _ := st.find([]byte("/reuse"))
	st.keys[id].rec = 0
	msg := appendBytesField(nil, 1, []byte("/reuse"))
	serveRoomRange(t, c, s, msg)
	runFillWork(t, c.node, l)
	if st.keys[id].rec == 0 {
		t.Fatal("validated current record was not promoted")
	}
	for _, sid := range []uint32{5, 7} {
		next := c.newStream(sid, kRange)
		serveRoomRange(t, c, next, msg)
		if len(c.node.fills) > 0 || c.node.legacyPending != nil {
			t.Fatal("a distinct request reread the same cold record")
		}
	}
}

func TestCursorRangeKeepsValuesAndHeaderAtItsAdmittedView(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	old := bytes.Repeat([]byte("a"), 16<<10)
	putValue(t, st, log, "/k", string(old))
	id, _ := st.find([]byte("/k"))
	st.keys[id].rec = 0
	n := c.node
	view := st.rev
	serveRoomRange(t, c, c.get(1), appendBytesField(nil, 1, []byte("/k")))
	if len(n.fills) != 1 || len(st.away) != 0 {
		t.Fatal("the cursor retained an offset-list plan")
	}
	n.pumpFills()
	if n.fillActive == nil || n.fillBook.used != fetchCursorWork {
		t.Fatal("dispatch did not own one complete work reservation")
	}
	awaitFetchResult(t, l)
	putValue(t, st, log, "/k", "newer")
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	r := stagedRangeResponse(t, c)
	if r.Header.Revision != view || len(r.Kvs) != 1 || r.Kvs[0].ModRevision != view || !bytes.Equal(r.Kvs[0].Value, old) {
		t.Fatalf("mixed read views: %v", r)
	}
	if len(n.fills) != 0 || n.fillBook.used != fetchCoreWork || c.fillOwners != 0 {
		t.Fatal("committed output retained cursor work or metadata")
	}
}

func TestCursorHistoricalRangeKeepsQValuesAndHHeader(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/k", "historical")
	q := st.rev
	putValue(t, st, log, "/k", "at admission")
	h := st.rev
	msg := appendBytesField(nil, 1, []byte("/k"))
	msg = appendVarintField(msg, 4, uint64(q))
	serveRoomRange(t, c, s, msg)
	n := c.node
	n.pumpFills()
	awaitFetchResult(t, l)
	putValue(t, st, log, "/k", "after admission")
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	r := stagedRangeResponse(t, c)
	if r.Header.Revision != h || len(r.Kvs) != 1 || r.Kvs[0].ModRevision != q || string(r.Kvs[0].Value) != "historical" {
		t.Fatalf("historical view/header changed: %v", r)
	}
}

func TestCursorRangeProgressIsIndependentOfCacheExpiry(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	const count = 3000
	for i := range count {
		putValue(t, st, log, fmt.Sprintf("/k/%04d", i), "v")
	}
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	serveRoomRange(t, c, s, msg)
	n := c.node
	handoffs := 0
	for len(n.fills) > 0 {
		n.pumpFills()
		if n.fillActive != nil {
			handoffs++
			if handoffs > 2 {
				t.Fatal("a strict cursor refetched previously consumed dependencies")
			}
			awaitFetchResult(t, l)
			n.now = n.now.Add(2 * time.Second)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
	}
	r := stagedRangeResponse(t, c)
	if len(r.Kvs) != count || r.Count != count || handoffs != 2 {
		t.Fatalf("cursor progress: %d values, count %d, handoffs %d", len(r.Kvs), r.Count, handoffs)
	}
	for i, kv := range r.Kvs {
		if string(kv.Key) != fmt.Sprintf("/k/%04d", i) || string(kv.Value) != "v" {
			t.Fatal("cursor skipped or changed a value")
		}
	}
	if len(st.fetched) != 0 || len(st.away) != 0 {
		t.Fatal("cursor data escaped into the legacy shared cache")
	}
}

func TestCursorCancellationWaitsForItsOwnWorkerAck(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/k", "old")
	id, _ := st.find([]byte("/k"))
	st.keys[id].rec = 0
	n := c.node
	serveRoomRange(t, c, s, appendBytesField(nil, 1, []byte("/k")))
	n.pumpFills()
	awaitFetchResult(t, l)
	before := n.stageCredits.used
	n.dropWaits(c, 1)
	if n.stageCredits.used != before || n.fillBook.used != fetchCursorWork {
		t.Fatal("cancellation reused an unacknowledged worker's loans")
	}
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	if len(n.fills) != 0 || n.stageCredits.used != 0 || n.fillBook.used != fetchCoreWork || len(c.flow.held) != 0 {
		t.Fatal("a cancelled operation published or retained resources")
	}
}

func TestStaleCursorPieceCannotClearTheNextPiece(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	for i := range 2050 {
		putValue(t, st, log, fmt.Sprintf("/k/%04d", i), "v")
	}
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	serveRoomRange(t, c, s, msg)
	n := c.node
	n.pumpFills()
	q := n.fillActive
	old := fillToken{op: q.op, piece: q.piece}
	awaitFetchResult(t, l)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	current := q.work
	if current == nil || q.piece == old.piece {
		t.Fatal("no distinct next piece")
	}
	if err := n.finishFillPiece(q, old, nil); err != nil {
		t.Fatal(err)
	}
	if n.fillActive != q || q.work != current || q.stage.loans != 1 || n.fillBook.used != fetchCursorWork {
		t.Fatal("a stale completion changed the next piece's wait or ownership")
	}
	awaitFetchResult(t, l)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	if len(stagedRangeResponse(t, c).Kvs) != 2050 {
		t.Fatal("the next piece was stranded")
	}
}

func TestUncommittedCursorRestartsAfterSameRevisionStoreReplacement(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/k", "old")
	id, _ := st.find([]byte("/k"))
	st.keys[id].rec = 0
	serveRoomRange(t, c, s, appendBytesField(nil, 1, []byte("/k")))
	n := c.node
	n.pumpFills()
	awaitFetchResult(t, l)
	next, nextLog := newTestStore(t)
	t.Cleanup(func() { _ = next.close() })
	next.setBudget(83<<20, 0)
	putValue(t, next, nextLog, "/k", "installed")
	if next.rev != st.rev {
		t.Fatal("fixture changed the revision")
	}
	n.invalidateFills(st)
	n.st = next
	if err := st.close(); err != nil {
		t.Fatal(err)
	}
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	if r := roomRangeReply(t, c); len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "installed" || r.Header.Revision != next.rev {
		t.Fatalf("replacement stranded or mixed the old view: %v", r)
	}
	if n.stageCredits.used != 0 || n.fillActive != nil || len(n.fills) != 0 {
		t.Fatal("old fill retained ownership")
	}
}

func TestCursorArenaAndDescriptorsFitTheOneWorkReservation(t *testing.T) {
	if unsafe.Sizeof(fillSpan{}) != 64 || unsafe.Sizeof(fillArena{})+stageUnit > fetchCursorWork-fetchCoreWork {
		t.Fatal("descriptor/control/scratch capacity outgrew the reserved work arena")
	}
}
