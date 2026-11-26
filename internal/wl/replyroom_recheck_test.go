package wl

import (
	"fmt"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// keyAt can return a value-bearing cached record instead of a short
// key-only one. Its wire size is unchanged, but rangeKVs' pieces
// threshold uses the record size, not that projected wire size.
func TestAWarmedKeysOnlyRangeRechecksThePiecesPath(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	st.setBudget(96<<20, 0) // ordinary reads can keep whole records
	n.connOut, n.reserved = 1<<20, n.sizes.reserve()
	sendMsg(c, c.newStream(3, kRange), 900<<10)
	keys := make([]string, 4)
	for i := range keys {
		keys[i] = fmt.Sprintf("/k/%d/", i) + strings.Repeat("k", 70<<10)
		putValue(t, st, log, keys[i], strings.Repeat("v", 800<<10))
	}
	dropRecords(st)
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/k/")), 2, []byte("/k0"))
	msg = appendVarintField(msg, 8, 1) // keys_only
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 {
		t.Fatal("the cold keys-only range did not wait")
	}
	body, rev, share := n.roomWait[0].body, st.rev, n.connOut
	// Warm through ordinary value reads, not by editing cache metadata.
	for _, key := range keys {
		if _, ok := valueAt(t, st, key, 0); !ok {
			t.Fatal("the warming read found no value")
		}
	}
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	kvs, count, _, err := n.rangeKVs(&req, nil, 0)
	if err != nil || len(kvs) != 4 {
		t.Fatalf("warm range: %d records, error %v", len(kvs), err)
	}
	warmBody := sizeVarintField(4, uint64(count))
	for _, kv := range kvs {
		warmBody += sizeBytesField(2, len(keysOnly(kv, &req)))
	}
	if st.rev != rev || n.connOut != share || warmBody != body {
		t.Fatal("warming must not change the revision, share or reply body")
	}
	n.reserved -= 1024
	n.roomFreed = true
	n.serveRoomWait()
	if len(n.roomWait) != 0 || len(n.bigs) != 1 || c.heldCounted != 0 || c.roomWaiters != 0 {
		t.Fatal("the warmed keys-only range still waits instead of entering pieces")
	}
}

// A local wake must serve held writes too, not only ranges. Repeated
// insufficient drains must still check room before reloading the record.
func TestAPartialDrainWakesOnlyItsOwnHeldWrite(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut, n.reserved = 1<<20, n.sizes.reserve()
	peer := roomSocket(t, c)
	out := c.newStream(3, kRange)
	c.flow.conn, out.win = 1<<30, 1<<30
	sendMsg(c, out, 800<<10)
	c.flow.conn = 0
	value := strings.Repeat("p", 320<<10)
	putValue(t, st, log, "/held", value)
	id, _ := st.find([]byte("/held"))
	rec, err := st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	s.kind = kPut
	n.holdPut(c, s, rec)
	before := st.loads
	l := &loop{n: n, epfd: -1}
	for len(n.heldReplies) > 0 {
		queued := c.waiting()
		n.markDirty(&c.sock)
		l.flush()
		if c.dead || c.waiting() >= queued || c.waiting() <= n.connOut/2 || !c.wantOut {
			t.Fatal("the held write's socket did not partially drain above half")
		}
		if len(n.heldReplies) > 0 && st.loads != before {
			t.Fatal("an insufficient local wake reloaded the held write")
		}
		drainRoomSocket(t, peer)
	}
	if c.heldCounted != 0 || c.roomWaiters != 0 || c.roomHeld != 0 || n.reserved != n.sizes.reserve() || st.loads != before+1 {
		t.Fatal("the held write did not complete with one reload and no shared room")
	}
	if len(c.flow.held) == 0 {
		t.Fatal("the held write sent no reply")
	}
	var reply pb.PutResponse
	if err := proto.Unmarshal(c.flow.held[0].data[5:], &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Header.GetRevision() != st.rev || string(reply.PrevKv.GetValue()) != value {
		t.Fatal("the held write returned the wrong outcome")
	}
}

// A's own socket progress returns no shared reserve room. Unrelated
// writes invalidate B's size, but they do not justify retrying B on A's
// local wake: that reloaded B's 100 values on every partial flush.
func TestAClientsPartialDrainsDoNotRebuildAnotherClientsRange(t *testing.T) {
	b, bs, st, log := replyRoomClient(t)
	n := b.node
	n.connOut, n.reserved = 1<<20, n.sizes.reserve()
	a := partClient(t, n, 1)
	n.socks, n.gens = append(n.socks, a), append(n.gens, a.gen)
	peer := roomSocket(t, a)
	out := a.newStream(3, kRange)
	putValue(t, st, log, "/held", strings.Repeat("p", 1<<20))
	id, _ := st.find([]byte("/held"))
	rec, err := st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	n.holdPut(a, a.newStream(1, kPut), rec)
	for i := range 100 {
		putValue(t, st, log, fmt.Sprintf("/b/%03d", i), strings.Repeat("v", 16<<10))
	}
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/b/")), 2, []byte("/b0"))
	serveRoomRange(t, b, bs, msg)
	if len(n.roomWait) != 1 || len(n.heldReplies) != 1 {
		t.Fatal("the two clients did not leave their replies waiting")
	}
	before := st.loads
	l := &loop{n: n, epfd: -1}
	for i := range 32 {
		// Refill below A's share, then perform a real partial flush.
		// The peer is drained after each attempt, never in a goroutine.
		a.flow.conn, out.win = 1<<30, 1<<30
		sendMsg(a, out, (800<<10)-int(a.waiting()))
		a.flow.conn = 0
		putValue(t, st, log, fmt.Sprintf("/unrelated/%02d", i), "v")
		queued := a.waiting()
		n.markDirty(&a.sock)
		l.flush()
		if a.dead || a.waiting() >= queued || a.waiting() <= n.connOut/2 || !a.wantOut {
			t.Fatal("the socket did not partially drain above half the share")
		}
		if a.roomHeld != 0 || n.reserved != n.sizes.reserve() {
			t.Fatal("the local drain returned shared reserve room")
		}
		drainRoomSocket(t, peer)
	}
	if got := st.loads - before; got != 0 {
		t.Fatalf("32 client-local drains rebuilt %d values of the unrelated range, want 0", got)
	}
	if len(n.roomWait) != 1 || len(n.heldReplies) != 1 {
		t.Fatal("the replies no longer wait for the shared reserve")
	}
	// A genuine shared return must still retry B, despite no local
	// progress on B's socket, and release B's request charge exactly once.
	n.reserved = 0
	n.roomFreed = true
	n.serveRoomWait()
	if len(n.roomWait) != 0 || b.heldCounted != 0 || b.roomWaiters != 0 {
		t.Fatal("the shared reserve return did not release B's range")
	}
}
