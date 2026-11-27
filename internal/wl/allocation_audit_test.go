package wl

import (
	"testing"
	"unsafe"

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

// This is a boundary proof, not a successful-fetch test. The round-4
// scope freezes main's sort handling, while option A exempts only #904's
// held capture. These scratch arrays are neither capture nor free P.
func TestMainAdmittedSortScratchExceedsTheUnchangedClientCeiling(t *testing.T) {
	c, s, st, log, _ := fillRangeFixture(t)
	n := c.node
	n.cursorFetch = false
	const count = 65536
	for i := range count {
		putValue(t, st, log, string([]byte{0, byte(i >> 16), byte(i >> 8), byte(i)}), "v")
	}
	msg := appendBytesField(nil, 1, []byte{0})
	msg = appendBytesField(msg, 2, []byte{1})
	msg = appendVarintField(msg, 3, 1)
	msg = appendVarintField(msg, 5, sortAscend)
	msg = appendVarintField(msg, 6, sortByValue)
	msg = appendVarintField(msg, 8, 1)
	serveRoomRange(t, c, s, msg)
	r := roomRangeReply(t, c)
	minimum := int64(count) * int64(unsafe.Sizeof(kvRec{})+unsafe.Sizeof([]byte{}))
	scratch := int64(count)*int64(unsafe.Sizeof(kvRec{})) + int64(cap(n.kvs))*int64(unsafe.Sizeof([]byte{}))
	if len(r.Kvs) != 1 || r.Count != count || !r.More || minimum <= n.sizes.clientPool+n.sizes.reserve() || n.mainCaptureCount() != 0 {
		t.Fatal("main sort/capture boundary changed")
	}
	t.Logf("UNRESOLVED sort scratch: main admitted; minimum=%d actual capacities=%d P=%d P+R=%d; not #904 capture", minimum, scratch, n.sizes.clientPool, n.sizes.clientPool+n.sizes.reserve())
}

func TestLegacySortCannotObtainImpossibleScratchCredit(t *testing.T) {
	c, s, st, log, _ := fillRangeFixture(t)
	n := c.node
	const count = 65536
	for i := range count {
		putValue(t, st, log, string([]byte{0, byte(i >> 16), byte(i >> 8), byte(i)}), "v")
	}
	dropRecords(st)
	msg := appendBytesField(nil, 1, []byte{0})
	msg = appendBytesField(msg, 2, []byte{1})
	msg = appendVarintField(msg, 3, 1)
	msg = appendVarintField(msg, 5, sortAscend)
	msg = appendVarintField(msg, 6, sortByValue)
	msg = appendVarintField(msg, 8, 1)
	serveRoomRange(t, c, s, msg)
	n.pumpFills()
	b := n.legacyPending
	if b == nil || b.prepared || !n.legacyWaitP || n.legacyRunning != nil || n.legacyFinishBound(b) <= n.sizes.clientPool+n.sizes.reserve() {
		t.Fatal("impossible sort scratch was allocated or the boundary changed")
	}
	t.Logf("UNRESOLVED liveness: main-admitted cold sort needs %d finish bytes before dispatch; P=%d", n.legacyFinishBound(b), n.sizes.clientPool)
}

// A physical sizing hint reuses the existing body word, not a larger
// uncharged waiter/high-water container for every parked main request.
func TestPhysicalRoomHintDoesNotGrowMainWaiterMetadata(t *testing.T) {
	if unsafe.Sizeof(roomRead{}) > 80 {
		t.Fatalf("room waiter grew beyond main's 80 bytes: %d", unsafe.Sizeof(roomRead{}))
	}
}
