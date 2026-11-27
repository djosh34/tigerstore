package wl

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// replyRoomClient has no send window and keeps no store record: rebuilding
// a waiting range counts in loads, and its reply stays whole for inspection.
func replyRoomClient(t *testing.T) (*client, *stream, *store, *memLog) {
	t.Helper()
	c, s := flowClient(t)
	st, log := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	c.node.st = st
	st.setBudget(1, 0)
	c.node.sizes = sizeFor(96 << 20)
	c.node.connOut = 512 << 10
	c.idx, c.gen = 0, 1
	c.node.socks, c.node.gens = []any{c}, []uint16{1}
	c.flow.conn, s.win = 0, 0
	return c, s, st, log
}

func serveRoomRange(t *testing.T, c *client, s *stream, msg []byte) {
	t.Helper()
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	c.node.serveRange(c, s, &req, msg)
}

func roomRangeReply(t *testing.T, c *client) *pb.RangeResponse {
	t.Helper()
	if len(c.flow.held) == 0 {
		t.Fatal("the range sent no whole reply")
	}
	data := c.flow.held[0].data
	if stage := c.flow.held[0].stage; stage != nil {
		if stage.written != stage.size || stage.read != 0 {
			t.Fatal("the range queued an incomplete reply")
		}
		for at := 0; at < stage.size; {
			page := stage.page(at)
			take := min(stage.size-at, stagePayload-at%stagePayload)
			data = append(data, page[stageHead+at%stagePayload:stageHead+at%stagePayload+take]...)
			at += take
		}
	}
	if len(data) < 5 {
		t.Fatal("the range sent no whole reply")
	}
	var reply pb.RangeResponse
	if err := proto.Unmarshal(data[5:], &reply); err != nil {
		t.Fatal(err)
	}
	return &reply
}

// A waiting range does not reread its values on every small return of
// reserve room. Main rebuilt 3,200 values for these 32 insufficient
// returns (#833); the reply still returns every value once it has room.
func TestARangeWaitingForRoomDoesNotReadItsValuesAgain(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	const keys = 100
	value := strings.Repeat("v", 16<<10)
	for i := range keys {
		putValue(t, st, log, fmt.Sprintf("/f/%04d", i), value)
	}
	n.reserved = n.sizes.reserve()
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/f/")), 2, []byte("/f0"))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 || st.loads != keys {
		t.Fatalf("%d waiting ranges and %d loads, want one and %d", len(n.roomWait), st.loads, keys)
	}
	before, counted := st.loads, c.heldCounted
	for range 32 {
		n.reserved -= 1024
		n.roomFreed = true
		n.serveRoomWait()
	}
	if st.loads != before {
		t.Fatalf("the range read %d values again without room for its reply", st.loads-before)
	}
	if len(n.roomWait) != 1 || c.roomWaiters != 1 || c.heldCounted != counted {
		t.Fatal("the range no longer waits with just its request counted")
	}
	n.reserved, n.roomFreed = 0, true
	n.serveRoomWait()
	if len(n.roomWait) != 0 || c.roomWaiters != 0 || c.heldCounted != 0 || st.loads != before+keys {
		t.Fatal("the waiting range did not rebuild exactly once after room returned")
	}
	reply := roomRangeReply(t, c)
	if len(reply.Kvs) != keys {
		t.Fatalf("the range returned %d keys, want %d", len(reply.Kvs), keys)
	}
	for i, kv := range reply.Kvs {
		if string(kv.Key) != fmt.Sprintf("/f/%04d", i) || string(kv.Value) != value {
			t.Fatalf("record %d is not its key and whole value", i)
		}
	}
}

// The old size must not hide a smaller current reply, an error for a
// compacted revision, or a reply that must go in pieces after a share
// shrinks. A new store can have the same revision and different values.
func TestAWaitingRangeChecksItsSizeAgainWhenTheStoreChanges(t *testing.T) {
	for _, change := range []string{"write", "compact", "install", "share"} {
		t.Run(change, func(t *testing.T) {
			c, s, st, log := replyRoomClient(t)
			n := c.node
			keys, size := 100, 16<<10
			if change == "share" {
				keys, size, n.connOut = 2, 1400<<10, 1<<20
			}
			for i := range keys {
				putValue(t, st, log, fmt.Sprintf("/f/%04d", i), strings.Repeat("v", size))
			}
			n.reserved = n.sizes.reserve()
			msg := appendBytesField(appendBytesField(nil, 1, []byte("/f/")), 2, []byte("/f0"))
			if change == "compact" {
				msg = appendVarintField(msg, 4, uint64(st.rev-1))
			}
			serveRoomRange(t, c, s, msg)
			if len(n.roomWait) != 1 {
				t.Fatal("the range did not wait for room")
			}
			switch change {
			case "write":
				for i := range keys {
					putValue(t, st, log, fmt.Sprintf("/f/%04d", i), "small")
				}
			case "compact":
				st.compact(st.rev) // no change to st.rev
			case "install":
				next, nextLog := newTestStore(t)
				t.Cleanup(func() { _ = next.close() })
				for i := range keys {
					putValue(t, next, nextLog, fmt.Sprintf("/f/%04d", i), "small")
				}
				if next.rev != st.rev {
					t.Fatal("the replacement must have the same revision")
				}
				n.st = next
			case "share":
				n.connOut = 1024
			}
			n.roomFreed = true
			n.serveRoomWait()
			if len(n.roomWait) != 0 || c.roomWaiters != 0 || c.heldCounted != 0 {
				t.Fatal("the range still waits for its old size")
			}
			switch change {
			case "share":
				if len(n.bigs) != 1 {
					t.Fatal("the reply past its new share and reserve did not go in pieces")
				}
			case "compact":
				if !bytes.Contains(c.out, []byte(msgCompacted)) || c.get(s.id) != nil {
					t.Fatal("the compacted range did not return its error and end the stream")
				}
			default:
				reply := roomRangeReply(t, c)
				if len(reply.Kvs) != keys || reply.Header.GetRevision() != n.st.rev {
					t.Fatal("the current reply's keys and header do not match the current store")
				}
				for _, kv := range reply.Kvs {
					if string(kv.Value) != "small" {
						t.Fatal("the current read returned a replaced value")
					}
				}
			}
		})
	}
}

// A changed current value can now fit without any reserve room. Do not
// leave it waiting on the larger size measured at the old revision.
func TestANewerSmallerReplyDoesNotWaitForItsOldSize(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut = 64 << 10
	n.reserved = n.sizes.reserve()
	putValue(t, st, log, "k", strings.Repeat("v", 128<<10))
	msg := appendBytesField(nil, 1, []byte("k"))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 {
		t.Fatal("the large reply did not wait")
	}
	putValue(t, st, log, "k", "small")
	n.roomFreed = true // still no room for its old reply
	n.serveRoomWait()
	if len(n.roomWait) != 0 || c.heldCounted != 0 {
		t.Fatal("the smaller current reply still waits on its old size")
	}
	reply := roomRangeReply(t, c)
	if len(reply.Kvs) != 1 || string(reply.Kvs[0].Value) != "small" || reply.Header.GetRevision() != st.rev {
		t.Fatal("the current reply's header and value do not identify the same state")
	}
}

// The header and client share are current, not part of the cached body.
// One more term byte leaves the old amount of room insufficient, so it
// must not even reload the value until that extra byte comes back.
func TestARoomWaitChecksTheCurrentHeaderBeforeLoading(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut, n.term, n.reserved = 64<<10, 127, n.sizes.reserve()
	value := strings.Repeat("v", 128<<10)
	putValue(t, st, log, "k", value)
	msg := appendBytesField(nil, 1, []byte("k"))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 {
		t.Fatal("the range did not wait")
	}
	id, _ := st.find([]byte("k"))
	ks, _ := st.stateAt(id, 0)
	record := kvSize(1, len(value), ks.create, ks.mod, ks.ver, ks.lease)
	old := int64(5 + sizeBytesField(1, n.hdrSize()) + sizeVarintField(4, 1) + sizeBytesField(2, record))
	n.reserved = n.sizes.reserve() - (old - n.connOut)
	n.term = 128
	before := st.loads
	n.roomFreed = true
	n.serveRoomWait()
	if len(n.roomWait) != 1 || st.loads != before {
		t.Fatal("the range rebuilt with room for only the old header")
	}
	n.reserved--
	n.roomFreed = true
	n.serveRoomWait()
	reply := roomRangeReply(t, c)
	if len(n.roomWait) != 0 || st.loads != before+1 || reply.Header.GetRaftTerm() != 128 || len(reply.Kvs) != 1 || string(reply.Kvs[0].Value) != value {
		t.Fatal("the extra header byte did not release the right reply")
	}
}

func TestARoomWaitDoesNotKeepAStorePointer(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut, n.reserved = 64<<10, n.sizes.reserve()
	putValue(t, st, log, "k", strings.Repeat("v", 128<<10))
	serveRoomRange(t, c, s, appendBytesField(nil, 1, []byte("k")))
	if len(n.roomWait) != 1 {
		t.Fatal("the range did not wait")
	}
	for field := range reflect.TypeOf(n.roomWait[0]).Fields() {
		if field.Type == reflect.TypeFor[*store]() {
			t.Fatal("a size identity check keeps the replaced store alive")
		}
	}
}

func TestAStreamErrorReleasesItsReplyRoomRequestCharge(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut, n.reserved = 64<<10, n.sizes.reserve()
	putValue(t, st, log, "k", strings.Repeat("v", 128<<10))
	msg := appendBytesField(nil, 1, []byte("k"))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 || c.heldCounted != int64(len(msg)) {
		t.Fatal("the fixture did not leave a counted request waiting for room")
	}
	n.replyError(c, s.id, codeInternal, "request error")
	n.roomFreed = true
	n.serveRoomWait()
	c.dropAll()
	if len(n.roomWait) != 0 || c.heldCounted != 0 || c.roomWaiters != 0 {
		t.Fatal("the ended stream left its request charged")
	}
}

// An install must wake room waiters even if no socket frees room: the
// old-store held reply must release its charge and its replaced store.
func TestAnInstallWakesAHeldReplyWaitingForRoom(t *testing.T) {
	n, sn := installFixture(t)
	defer func() { _ = n.st.close() }()
	log := n.st.log.(*memLog)
	putValue(t, n.st, log, "/follower", strings.Repeat("v", 128<<10))
	n.sizes = sizeFor(96 << 20)
	n.connOut, n.reserved = 64<<10, n.sizes.reserve()
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(1, kPut)
	id, _ := n.st.find([]byte("/follower"))
	rec, err := n.st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	n.holdPut(c, s, rec)
	n.serveHeld()
	if len(n.heldReplies) != 1 || n.heldReplies[0].fetching {
		t.Fatal("the fixture did not hold a reply waiting only for room")
	}
	n.roomFreed = false
	if err := n.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	n.serveRoomWait()
	if len(n.heldReplies) != 0 || c.heldCounted != 0 || c.roomWaiters != 0 {
		t.Fatal("the install left a room-waiting held reply and its old store alive")
	}
}
