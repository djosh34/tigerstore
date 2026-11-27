package wl

import (
	"testing"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"golang.org/x/sys/unix"
)

// Normal read-half admission is still legal while output is blocked. Its
// already-existing entitlement must not be sold to a private destination.
func TestOutputFundingProtectsSubsequentNormalReadAdmission(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	n.cursorFetch = true
	n.sizes = sizeFor(83 << 20)
	n.connOut = n.sizes.connOut(1)
	n.clients = 2
	c.flow.conn = 0
	other := newClient(&handoff{fd: -1, wr: c.wr, rd: c.rd}, n)
	defer c.dropAll()
	defer other.dropAll()
	wire := stageRangeBytes(t, 512)
	admitted := 0
	for i := range 1024 {
		stage := n.admitStage(c, len(wire))
		if stage == nil {
			break
		}
		s := c.newStream(uint32(3+2*i), kRange)
		stage.append(wire)
		if !c.holdStage(s, stage) {
			t.Fatal("funded complete response failed to commit")
		}
		admitted++
	}
	if admitted == 0 || n.cpool+other.bufNeed() > n.sizes.clientInBufs() {
		t.Fatal("fixture did not leave a legal normal read admission")
	}
	other.takeBufs()
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool {
		t.Fatal("normal read admission consumed irrevocably funded output P")
	}
}

func TestOutputFundingProtectsContinuationHeaderBacking(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	n.cursorFetch = true
	n.sizes = sizeFor(83 << 20)
	n.connOut = n.sizes.connOut(1)
	c.flow.conn = 0
	defer c.dropAll()
	n.cpool = n.sizes.clientInBufs()
	wire := stageRangeBytes(t, 512)
	for i := range 1024 {
		stage := n.admitStage(c, len(wire))
		if stage == nil {
			break
		}
		s := c.newStream(uint32(3+2*i), kRange)
		stage.append(wire)
		if !c.holdStage(s, stage) {
			t.Fatal("commit")
		}
	}
	if err := c.addBlock(make([]byte, maxHeaderBlock), true); err != nil {
		t.Fatal(err)
	}
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool {
		t.Fatal("CONTINUATION backing consumed irrevocably funded output P")
	}
}

func TestHandshakeLeftoversCannotStealFundedOutput(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	n.cursorFetch = true
	n.sizes = sizeFor(83 << 20)
	n.connOut = n.sizes.connOut(1)
	n.clients = 1
	c.flow.conn = 0
	n.cpool = n.sizes.clientInBufs()
	n.socks, n.gens = []any{c}, []uint16{1}
	wire := stageRangeBytes(t, 512)
	for i := range 1024 {
		stage := n.admitStage(c, len(wire))
		if stage == nil {
			break
		}
		s := c.newStream(uint32(3+2*i), kRange)
		stage.append(wire)
		if !c.holdStage(s, stage) {
			t.Fatal("commit")
		}
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	ep, err := unix.EpollCreate1(0)
	if err != nil {
		t.Fatal(err)
	}
	l := &loop{n: n, epfd: ep, handoffs: []*handoff{{kind: kindClient, fd: fds[0], left: []byte("p"), wr: c.wr, rd: c.rd}}}
	defer func() {
		c.dropAll()
		if len(n.socks) > 1 && n.socks[1] != nil {
			l.closeSock(1)
		} else {
			_ = unix.Close(fds[0])
		}
		_ = unix.Close(fds[1])
		_ = unix.Close(ep)
	}()
	l.takeHandoffs()
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool || len(l.handoffs) != 1 || n.clients != 1 {
		t.Fatal("handshake leftovers allocated input beyond the funded normal entitlement")
	}
	c.dropAll()
	l.takeHandoffs()
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool || len(l.handoffs) != 0 || n.clients != 2 || len(n.socks) < 2 || n.socks[1] == nil {
		t.Fatal("funded handoff did not resume without a timeout")
	}
}

// #908 excludes only main's unchanged candidate/kvRecs arrays. Observe
// actual capacities on both paths; no representation or ordering change.
func TestLegacySortScratchParity908(t *testing.T) {
	const count = 65536
	var mainBytes int64
	for _, cursor := range []bool{false, true} {
		c, s, st, log, l := fillRangeFixture(t)
		n := c.node
		n.cursorFetch = cursor
		var scratch int64
		n.measureSort = func(a, b int) {
			scratch = max(scratch, int64(a)*int64(unsafe.Sizeof([]byte{}))+int64(b)*int64(unsafe.Sizeof(kvRec{})))
		}
		for i := range count {
			putValue(t, st, log, string([]byte{0, byte(i >> 16), byte(i >> 8), byte(i)}), "v")
		}
		if cursor {
			dropRecords(st)
		}
		msg := appendBytesField(nil, 1, []byte{0})
		msg = appendBytesField(msg, 2, []byte{1})
		msg = appendVarintField(msg, 3, 1)
		msg = appendVarintField(msg, 5, sortAscend)
		msg = appendVarintField(msg, 6, sortByValue)
		msg = appendVarintField(msg, 8, 1)
		serveRoomRange(t, c, s, msg)
		if cursor {
			for step := 0; n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
				if step > count/128+4 {
					t.Fatal("cold sort did not finish")
				}
				n.pumpFills()
				if n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool || n.fillBook.used > n.fillBook.limit {
					t.Fatal("#908 exempted new storage")
				}
				if n.legacyRunning != nil {
					awaitFetchResult(t, l)
					if err := l.runPosts(); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		var response *pb.RangeResponse
		if !cursor {
			response = roomRangeReply(t, c)
		} else {
			response = &pb.RangeResponse{}
			data := fillQueuedBytes(t, c)
			if len(data) < 5 || proto.Unmarshal(data[5:], response) != nil {
				t.Fatal("invalid reply")
			}
		}
		minimum := int64(count) * int64(unsafe.Sizeof(kvRec{})+unsafe.Sizeof([]byte{}))
		if minimum != 7864320 || len(response.Kvs) != 1 || response.Count != count || !response.More || n.mainCaptureCount() != 0 {
			t.Fatal("main reference or selection changed")
		}
		if !cursor {
			mainBytes = scratch
			if mainBytes != 8028144 {
				t.Fatalf("main scratch reference changed: %d", mainBytes)
			}
		} else if scratch == 0 || scratch > mainBytes {
			t.Fatalf("#897 enlarged main scratch: %d > %d", scratch, mainBytes)
		}
		t.Logf("cursor=%v scratch=%d main=%d minimum=%d", cursor, scratch, mainBytes, minimum)
	}
}

func TestOrdinaryRepliesCannotSpendProtectedFillCapacity(t *testing.T) {
	for _, kind := range []string{"range", "keys-only", "count-only", "put-prev"} {
		t.Run(kind, func(t *testing.T) {
			c, _, st, log, l := fillRangeFixture(t)
			n := c.node
			putValue(t, st, log, "/hot", string(make([]byte, 16<<10)))
			other := partClient(t, n, 1)
			t.Cleanup(other.dropAll)
			other.flow.conn = 0
			n.socks, n.gens = []any{c, other}, []uint16{1, 1}
			n.clients = 2
			wire := stageRangeBytes(t, 512)
			for i := range 1024 {
				stage := n.admitStage(c, len(wire))
				if stage == nil {
					break
				}
				s := c.newStream(uint32(3+2*i), kRange)
				stage.append(wire)
				if !c.holdStage(s, stage) {
					t.Fatal("commit")
				}
			}
			s := other.newStream(1, kRange)
			msg := appendBytesField(nil, 1, []byte("/hot"))
			if kind != "put-prev" {
				if kind == "keys-only" {
					msg = appendVarintField(msg, 8, 1)
				}
				if kind == "count-only" {
					msg = appendVarintField(msg, 9, 1)
				}
				serveRoomRange(t, other, s, msg)
			} else {
				s.kind = kPut
				msg = appendBytesField(msg, 2, []byte("new"))
				msg = appendVarintField(msg, 4, 1)
				if err := n.applyPut(msg, 0, other, s); err != nil {
					t.Fatal(err)
				}
			}
			if used := n.stageOther(0) + n.stageCredits.used; used > n.sizes.clientPool {
				t.Fatalf("ordinary %s stole protected P: %d > %d", kind, used, n.sizes.clientPool)
			}
			if kind != "put-prev" {
				if len(n.roomWait) != 1 || len(other.flow.held) != 0 {
					t.Fatal("unfunded read did not remain unstarted")
				}
				c.dropAll()
				n.roomFreed = true
				n.serveRoomWait()
				r := roomRangeReply(t, other)
				if len(n.roomWait) != 0 || r.Count != 1 {
					t.Fatal("read did not resume after credit return")
				}
				if kind == "count-only" {
					if len(r.Kvs) != 0 {
						t.Fatal("count-only returned records")
					}
				} else if len(r.Kvs) != 1 || kind == "keys-only" && len(r.Kvs[0].Value) != 0 || kind == "range" && len(r.Kvs[0].Value) != 16<<10 {
					t.Fatal("funding changed the read's projection")
				}
			} else {
				if len(n.heldReplies) != 1 || st.rev != 3 {
					t.Fatal("write was not applied then held")
				}
				c.dropAll()
				n.roomFreed = true
				for step := 0; len(n.heldReplies) > 0; step++ {
					if step > 16 {
						t.Fatal("applied write did not resume with returned P")
					}
					n.serveHeld()
					n.pumpFills()
					if n.legacyRunning != nil {
						awaitFetchResult(t, l)
						if err := l.runPosts(); err != nil {
							t.Fatal(err)
						}
					}
				}
				var response pb.PutResponse
				data := fillQueuedBytes(t, other)
				if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil || response.PrevKv == nil || len(response.PrevKv.Value) != 16<<10 || st.rev != 3 {
					t.Fatal("funding changed applied write/previous value")
				}
			}
		})
	}
}

// A physical sizing hint reuses the existing body word, not a larger
// uncharged waiter/high-water container for every parked main request.
func TestPhysicalRoomHintDoesNotGrowMainWaiterMetadata(t *testing.T) {
	if unsafe.Sizeof(roomRead{}) > 80 {
		t.Fatalf("room waiter grew beyond main's 80 bytes: %d", unsafe.Sizeof(roomRead{}))
	}
}
