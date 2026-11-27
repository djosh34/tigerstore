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

func TestPhysicalFillWaitKeepsItsHintAndLetsAFundedReaderPass(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	other := partClient(t, n, 1)
	other.flow.conn = 0
	n.socks, n.gens = []any{c, other}, []uint16{1, 1}
	t.Cleanup(other.dropAll)
	for i := range 100 {
		putValue(t, st, log, fmt.Sprintf("/k/%03d", i), string(bytes.Repeat([]byte("v"), 16<<10)))
	}
	putValue(t, st, log, "/small", string(bytes.Repeat([]byte("s"), 8<<10)))
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	extra := n.sizes.clientPool - n.stageOther(0) - 128<<10
	c.growOut(int(extra))
	msg := appendBytesField(nil, 1, []byte("/k/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/k/")))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 || n.roomWait[0].body >= 0 || n.stats.rangeFillScans != 1 {
		t.Fatal("large request did not retain a physical admission hint")
	}
	for range 32 {
		n.roomFreed = true
		n.serveRoomWait()
	}
	if n.stats.rangeFillScans != 1 {
		t.Fatal("insufficient physical credits rebuilt the metadata plan")
	}
	before := n.now
	little := other.newStream(1, kRange)
	serveRoomRange(t, other, little, appendBytesField(nil, 1, []byte("/small")))
	for len(n.fills) > 0 {
		n.pumpFills()
		if n.fillActive != nil {
			awaitFetchResult(t, l)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n.now != before || len(n.roomWait) != 1 || len(fillQueuedBytes(t, other)) == 0 || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool {
		t.Fatal("independently funded small reader stalled or overspent P")
	}
	c.growOut(-int(extra))
	n.roomFreed = true
	n.serveRoomWait()
	if len(n.roomWait) != 0 || len(n.fills) != 1 || n.stats.rangeFillScans != 3 {
		t.Fatal("funded large retry did not rebuild exactly once")
	}
}

func TestCompletedCursorWaitsForSocketWithoutReserializing(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	c.plain = true
	putValue(t, st, log, "/ready", "value")
	id, _ := st.find([]byte("/ready"))
	st.keys[id].rec = 0
	msg := appendBytesField(nil, 1, []byte("/ready"))
	serveRoomRange(t, c, s, msg)
	n := c.node
	n.pumpFills()
	awaitFetchResult(t, l)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	q := n.fills[0]
	if !q.ready {
		t.Fatal("private message is incomplete")
	}
	other := c.newStream(5, kRange)
	other.win = 1 << 30
	c.flow.conn = 1 << 30
	wire := stageRangeBytes(t, 80<<10)
	stage := n.admitStage(c, len(wire))
	if stage == nil {
		t.Fatal("other stream lacks funding")
	}
	stage.append(wire)
	if !c.holdStage(other, stage) {
		t.Fatal("other stream did not commit")
	}
	for range 2 {
		c.writeFrame(frPing, flAck, make([]byte, 8))
	}
	if cap(c.out)-len(c.out) >= maxPlain+recOverhead+64 {
		t.Fatal("other stream did not occupy the bounded socket")
	}
	before := append([]byte(nil), c.out...)
	fetches := n.stats.fetches
	for range 3 {
		n.pumpFills()
		if q.cancelled || !q.ready || q.stage == nil || q.stage.flags&stageBroken != 0 || q.stage.written != q.stage.size || n.fillActive != nil || n.fillBook.used != fetchCoreWork || n.stats.fetches != fetches || !bytes.Equal(c.out, before) {
			t.Fatal("complete message was fetched/serialized again while its socket was busy")
		}
	}
	c.out = c.out[:0]
	c.ow = 0
	c.rec = -1
	c.flow.conn = 0
	n.pumpFills()
	var data []byte
	for _, h := range c.flow.held {
		if h.sid != s.id || h.stage == nil {
			continue
		}
		stage := h.stage
		for at := stage.read; at < stage.size; {
			p := stage.page(at)
			take := min(stage.size-at, stagePayload-at%stagePayload)
			data = append(data, p[stageHead+at%stagePayload:stageHead+at%stagePayload+take]...)
			at += take
		}
	}
	var response pb.RangeResponse
	if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "value" {
		t.Fatal("complete message did not publish intact after socket progress")
	}
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
	queued := int64(0)
	for _, h := range c.flow.held {
		if h.stage != nil {
			queued += h.stage.charged
		}
	}
	if n.stageCredits.used != queued || n.fillActive != nil || len(n.fills) != 0 {
		t.Fatal("old fill retained ownership beyond the new completed destination")
	}
}

func TestCursorArenaAndDescriptorsFitTheOneWorkReservation(t *testing.T) {
	if unsafe.Sizeof(fillSpan{}) != 64 || unsafe.Sizeof(fillArena{})+stageUnit > fetchCursorWork-fetchCoreWork {
		t.Fatal("descriptor/control/scratch capacity outgrew the reserved work arena")
	}
}
