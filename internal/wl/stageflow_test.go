package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func stageRangeBytes(t *testing.T, size int) []byte {
	t.Helper()
	r := &pb.RangeResponse{
		Header: &pb.ResponseHeader{Revision: 2}, Count: 1,
		Kvs: []*mvccpb.KeyValue{{Key: []byte("/stage"), Value: bytes.Repeat([]byte("v"), size-64)}},
	}
	for range 4 {
		n := 5 + proto.Size(r)
		if n == size {
			b, err := proto.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			return append(binary.BigEndian.AppendUint32([]byte{0}, uint32(len(b))), b...)
		}
		r.Kvs[0].Value = bytes.Repeat([]byte("v"), len(r.Kvs[0].Value)+size-n)
	}
	t.Fatal("could not size the fixture's RangeResponse")
	return nil
}

func stageWindow(inc uint32) []byte {
	p := make([]byte, 13)
	putFrameHeader(p, 4, frWindowUpdate, 0, 0)
	binary.BigEndian.PutUint32(p[9:], inc)
	return p
}

// A physical input buffer alone cannot solve this cycle. takeTurn needs
// its whole two MiB even though the DATA frame ahead of WINDOW_UPDATE is
// only 16 KiB. Output quota remains live, backed independently in P.
func TestAStagedReplyCannotSpendTheLogicalInputTurn(t *testing.T) {
	n, a, b, l, feed, _ := partNode(t)
	n.sizes, n.connOut = sizeFor(83<<20), 64<<10
	n.cursorFetch = true
	n.connIn = n.sizes.connIn(n.active)
	n.cpool = n.sizes.clientInBufs()
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	putValue(t, st, log, "/large", string(bytes.Repeat([]byte("l"), 1536<<10)))

	out := a.newStream(1, kRange)
	a.lastSid, a.flow.conn, out.win = 1, 0, 1<<30
	wire := stageRangeBytes(t, (64<<10)+(1<<20))
	stage := n.admitStage(a, len(wire))
	if stage == nil {
		t.Fatal("the completed range got no independent P backing")
	}
	stage.append(wire)
	if !a.holdStage(out, stage) || a.coveredRoom != 1<<20 || n.coveredRoom != 1<<20 || n.reserved != 0 {
		t.Fatal("output consumed the physical input guarantee or lost its logical quota")
	}
	// This waiter is live throughout, and needs more output reserve
	// than is free. It must not prevent an independent funded reader.
	large := b.newStream(1, kRange)
	msg := appendBytesField(nil, 1, []byte("/large"))
	n.waitRoom(b, large, msg, 1536<<10)
	reader := b.newStream(5, kRange)
	b.flow.conn, reader.win = 0, 1<<30
	little := n.admitStage(b, 8192)
	if little == nil {
		t.Fatal("the within-share 8 KiB reader was not independently funded")
	}
	little.append(stageRangeBytes(t, 8192))
	if !b.holdStage(reader, little) {
		t.Fatal("the reader did not commit its complete response")
	}
	at := n.now
	p := putPart(t, 3, true)
	p = append(p[:len(p)-17], stageWindow(2<<20)...)
	feed(a, p)
	if a.roomNeed == 0 || a.partial != 0 {
		t.Fatal("the DATA frame did not wait ahead of WINDOW_UPDATE")
	}
	l.serveWaiting()
	if a.roomNeed != 0 || a.pr != a.pw || n.inTurn != a || n.turnRoom != 2<<20 || n.now != at {
		t.Fatalf("input did not take its full turn without timeout: need=%d unread=%d turn=%d", a.roomNeed, a.pw-a.pr, n.turnRoom)
	}
	if a.flow.conn == 0 || a.waiting() == 0 {
		t.Fatal("the frame reader did not reach the blocked reply's WINDOW_UPDATE")
	}
	feed(b, stageWindow(64<<10))
	n.serveRoomWait()
	if little.read != little.size || len(n.roomWait) != 1 || b.roomWaiters != 1 || n.now != at {
		t.Fatal("the independent reader stalled or the live large waiter was discarded")
	}
	a.dropAll()
	b.dropAll()
}

func TestPrivateStageIsNotParkedRequestBackpressure(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	n.sizes, n.connOut, c.flow.conn = sizeFor(83<<20), 64<<10, 0
	stage := n.admitStage(c, (64<<10)+(1<<20))
	if stage == nil || c.holdsTooMuch() || c.heldCounted != 0 || n.cout < stage.charged {
		t.Fatal("private destination credit was mistaken for parked request bytes")
	}
	stage.append(stageRangeBytes(t, stage.size))
	if !c.holdStage(s, stage) || !c.holdsTooMuch() {
		t.Fatal("completed held output did not acquire main's ordinary backpressure")
	}
	c.dropAll()
}

func TestAnUnbackedOutputCannotBorrowTheInputGuarantee(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	n.sizes, n.connOut = sizeFor(83<<20), 64<<10
	n.cursorFetch = true
	n.cpool = n.sizes.clientPool
	if n.admitStage(c, (64<<10)+(1<<20)) != nil {
		t.Fatal("output was admitted without independent client-pool backing")
	}
	if n.reserved != 0 || n.coveredRoom != 0 || c.roomHeld != 0 || n.stageCredits.used != 0 || n.stagePromises != 0 {
		t.Fatal("failed output admission consumed the input guarantee")
	}
}

func TestStagedOutputDrainsThroughPartialSocketFlushes(t *testing.T) {
	testStageSocket(t, true)
}

func TestStagedTLSOutputDrainsThroughPartialSocketFlushes(t *testing.T) {
	testStageSocket(t, false)
}

func testStageSocket(t *testing.T, plain bool) {
	t.Helper()
	c, s := flowClient(t)
	n := c.node
	n.sizes, n.connOut = sizeFor(83<<20), 64<<10
	n.socks, n.gens = []any{c}, []uint16{c.gen}
	c.plain, c.flow.conn, s.win = plain, 8<<20, 8<<20
	fd := roomSocket(t, c)
	wire := stageRangeBytes(t, (64<<10)+(1<<20))
	stage := n.admitStage(c, len(wire))
	if stage == nil {
		t.Fatal("no stage credit")
	}
	stage.append(wire)
	if !c.holdStage(s, stage) {
		t.Fatal("the message did not commit")
	}
	l := &loop{n: n, epfd: -1}
	var got []byte
	var buf [64 << 10]byte
	for step := 0; n.stageCredits.used > 0 || c.waiting() > 0; step++ {
		if step > 2*len(wire) {
			t.Fatal("staged output stopped making bounded socket progress")
		}
		n.markDirty(&c.sock) // the writable event's work
		l.flush()
		if c.dirty || cap(c.out) > 64<<10 {
			t.Fatal("stage refill left a lost dirty entry or grew past its funded socket")
		}
		before := len(got)
		for {
			read, err := unix.Read(fd, buf[:])
			got = append(got, buf[:max(0, read)]...)
			if errors.Is(err, unix.EAGAIN) {
				break
			}
			if err != nil || read == 0 {
				t.Fatalf("read staged output: %d bytes, %v", read, err)
			}
		}
		if len(got) == before {
			t.Fatal("a ready staged response made no progress after its socket drained")
		}
	}
	if !plain {
		r := sock{rd: c.rd, in: got, iw: len(got), pt: make([]byte, 64<<10)}
		if err := r.open(); err != nil {
			t.Fatal(err)
		}
		got = r.pt[:r.pw]
	}
	var message []byte
	for len(got) > 0 {
		if len(got) < 9 {
			t.Fatal("partial frame header")
		}
		size := int(got[0])<<16 | int(got[1])<<8 | int(got[2])
		if size > len(got)-9 || got[3] != frData || binary.BigEndian.Uint32(got[5:9]) != s.id {
			t.Fatal("staged output changed frame boundaries or stream ownership")
		}
		message = append(message, got[9:9+size]...)
		got = got[9+size:]
	}
	if !bytes.Equal(message, wire) || n.coveredRoom != 0 || n.reserved != 0 || n.stagePromises != 0 || c.stages != 0 {
		t.Fatal("draining changed response bytes or retained staging/reserve credit")
	}
	c.dropAll()
	if n.cout != 0 {
		t.Fatal("closing the drained client retained output capacity")
	}
}

func TestCancelledStagedReplyKeepsCreditAcrossClientReplacement(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	n.sizes, n.connOut = sizeFor(83<<20), 64<<10
	stage := n.admitStage(c, (64<<10)+(1<<20))
	if stage == nil || !stage.borrow() {
		t.Fatal("the worker got no funded loan")
	}
	loan := stage.span(0, 32)
	before := n.stageCredits.used
	stage.discard()
	c.dropAll()
	if n.stageCredits.used != before || n.coveredRoom != 1<<20 || n.reserved != 0 || n.stageOrphans != 1 {
		t.Fatal("closing a client recycled its in-flight destination or output quota")
	}
	newer := newClient(&handoff{fd: -1}, n)
	newer.gen = c.gen + 1
	newer.takeBufs()
	newer.newStream(1, kRange)
	next := n.admitStage(newer, 8192)
	if next == nil {
		t.Fatal("the quarantined client blocked an independently funded reader")
	}
	if n.admitStage(newer, (64<<10)+(1536<<10)) != nil {
		t.Fatal("a replacement client reused the cancelled job's logical output quota")
	}
	copy(loan, "late completion")
	if !stage.acknowledge() || stage.acknowledge() || n.stageCredits.used != next.charged || n.coveredRoom != 0 || next.flags != 0 || n.stageOrphans != 0 {
		t.Fatal("the stale completion released the replacement client's allocation")
	}
	next.discard()
	newer.dropAll()
	if n.stageCredits.used != 0 || n.stagePromises != 0 || n.cout != 0 || n.reserved != 0 {
		t.Fatal("the last acknowledgement left an allocation or promise behind")
	}
}
