package wl

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// flowClient is a client whose records seal with a fixed key, for tests
// of what it writes.
func flowClient(t *testing.T) (*client, *stream) {
	t.Helper()
	b, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t)
	c := newClient(&handoff{fd: -1, wr: aeadDir{aead: g}, rd: aeadDir{aead: g}}, n)
	c.takeBufs()
	return c, c.newStream(1, kRange)
}

func sendMsg(c *client, s *stream, n int) {
	var w msgW
	w.begin(c, s, n)
	w.write(make([]byte, n))
	w.end()
}

func TestAMessageWithinTheWindowsGoesOutAtOnce(t *testing.T) {
	c, s := flowClient(t)
	sendMsg(c, s, 1000)
	if s.holds || len(c.flow.held) != 0 {
		t.Fatal("a message within the windows was held")
	}
	if s.win != defaultWindow-1005 || c.flow.conn != defaultWindow-1005 {
		t.Fatalf("windows %d and %d after 1005 bytes", s.win, c.flow.conn)
	}
}

func TestAMessagePastTheWindowWaitsForUpdates(t *testing.T) {
	c, s := flowClient(t)
	sendMsg(c, s, 100000)
	if !s.holds || len(c.flow.held) != 1 {
		t.Fatal("a message past the window was not held")
	}
	// What the windows took went out: the first 65535 bytes.
	if got := len(c.flow.held[0].data); got != 100005-defaultWindow {
		t.Fatalf("%d bytes held after the window, want %d", got, 100005-defaultWindow)
	}
	// Trailers wait behind the message.
	c.node.endStreamOK(c, s)
	if len(c.flow.held) != 2 || c.flow.held[1].trailer == nil {
		t.Fatal("the trailers did not wait behind the held message")
	}
	// The stream's window opens, but the connection's is still shut.
	c.windowUpdate(1, 1<<20)
	if len(c.flow.held[0].data) == 0 {
		t.Fatal("bytes went out past the connection's window")
	}
	c.windowUpdate(0, 1<<20)
	if len(c.flow.held) != 0 {
		t.Fatalf("%d entries still held after both windows opened", len(c.flow.held))
	}
	if len(c.flow.streams) != 0 {
		t.Fatal("the stream's window stayed in the flow after it held nothing")
	}
}

func TestANewInitialWindowMovesEveryStream(t *testing.T) {
	c, s := flowClient(t)
	sendMsg(c, s, 70000)
	if !s.holds {
		t.Fatal("not held")
	}
	c.flow.conn = 1 << 30
	c.setInitial(1 << 20)
	if s.holds || len(c.flow.held) != 0 {
		t.Fatal("a larger initial window did not let the held message out")
	}
	s2 := c.newStream(3, kRange)
	if s2.win != 1<<20 {
		t.Fatalf("a new stream's window is %d, want the new initial", s2.win)
	}
}

func TestAResetStreamDropsWhatItHolds(t *testing.T) {
	c, s := flowClient(t)
	sendMsg(c, s, 100000)
	c.dropHeld(1)
	if s.holds || len(c.flow.held) != 0 || len(c.flow.streams) != 0 {
		t.Fatal("a reset stream still holds data")
	}
}

func TestAWriteThatNeverAppliesTimesOut(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	s.kind = kPut
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.now = time.Now()
	for _, seq := range []uint64{1, 2} {
		n.fwd[seq] = fwdBatch{seq: seq, handles: []handle{mkHandle(c, s.id)}}
		n.proposed = append(n.proposed, proposal{seq: seq, at: n.now})
	}
	n.fwd[2].handles = n.fwd[2].handles[:0] // batch 2 applied and answered
	n.now = n.now.Add(requestTimeout - time.Millisecond)
	n.expireProposals()
	if len(n.proposed) != 2 {
		t.Fatal("a write timed out early")
	}
	n.now = n.now.Add(time.Millisecond)
	n.expireProposals()
	if len(n.proposed) != 0 || len(n.fwd[1].handles) != 0 {
		t.Fatal("a write past its timeout still waits")
	}
	if got := n.gs.calls[kPut].handled[codeUnavailable]; got != 1 {
		t.Fatalf("%d timed-out answers, want 1", got)
	}
}

// A writer that writes past the length it began with breaks the
// connection and stops, instead of opening empty frames for good.
func TestAMessageWrittenPastItsLengthBreaksTheConnection(t *testing.T) {
	c, s := flowClient(t)
	var w msgW
	w.begin(c, s, 3)
	w.write(make([]byte, 10))
	w.end()
	if !c.broken {
		t.Fatal("a message written past its length left the connection whole")
	}
}

// A socket whose reader never quite catches up keeps its send buffer
// bounded by what waits, not by everything sent since it last emptied.
// The buffer was emptied only when a flush wrote all of it, so a leader's
// buffer to a follower that always lagged a little grew by every byte it
// sent: the leader of #696 reached 6.9 GB on a 3 GiB budget.
func TestASendBufferStaysWithinWhatWaits(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fds[1]) }()
	if err := unix.SetNonblock(fds[0], true); err != nil {
		t.Fatal(err)
	}
	c, _ := flowClient(t)
	s := newSock(&handoff{fd: fds[0], wr: c.wr}, &counters{})
	defer func() { _ = unix.Close(fds[0]) }()
	read := func(n int) {
		b := make([]byte, n)
		for got := 0; got < n; {
			k, err := unix.Read(fds[1], b[got:])
			if err != nil {
				t.Fatal(err)
			}
			got += k
		}
	}
	const backlog, step, rounds = 1 << 20, 64 << 10, 256
	chunk := make([]byte, 4<<10)
	put := func(n int) {
		for ; n > 0; n -= len(chunk) {
			s.put(chunk)
		}
	}
	put(backlog)
	if _, err := s.flush(); err != nil {
		t.Fatal(err)
	}
	for range rounds {
		put(step)
		ok, err := s.flush()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("the reader caught up; the test needs it to lag")
		}
		read(step)
	}
	if limit := 8 * backlog; cap(s.out) > limit {
		t.Fatalf("the send buffer grew to %d bytes after %d were sent with about %d waiting; want at most %d",
			cap(s.out), backlog+rounds*step, backlog, limit)
	}
}

// What a client's streams hold for want of window is counted as it is
// held, sent and dropped; past the client's share, its new requests are
// refused as etcd refuses too many, and its watches send nothing now.
func TestAClientThatHoldsTooMuchIsRefused(t *testing.T) {
	c, s := flowClient(t)
	c.node.connOut = 128 << 10
	sendMsg(c, s, 100000)
	held := int64(100005 - defaultWindow)
	if c.flow.bytes != held {
		t.Fatalf("%d bytes counted held, want %d", c.flow.bytes, held)
	}
	if c.holdsTooMuch() || c.slow() {
		t.Fatal("a client within its share counts as holding too much")
	}
	s2 := c.newStream(3, kRange)
	sendMsg(c, s2, 200000)
	if !c.holdsTooMuch() || !c.slow() {
		t.Fatalf("%d bytes held over a share of %d do not count", c.flow.bytes, c.node.connOut)
	}
	s3 := c.newStream(5, kRange)
	before := len(c.out)
	if err := c.node.request(c, s3, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(c.out[before:], []byte("etcdserver: too many requests")) {
		t.Fatal("a request of a client that holds too much was not refused")
	}
	c.dropHeld(s2.id)
	c.windowUpdate(0, 1<<20)
	c.windowUpdate(s.id, 1<<20)
	if c.flow.bytes != 0 || len(c.flow.held) != 0 {
		t.Fatalf("%d bytes still counted held, %d entries, after the drop and the windows", c.flow.bytes, len(c.flow.held))
	}
}

// A header block sent in CONTINUATION frames past maxHeaderBlock breaks
// the connection, and what it held is counted in the client buffers until
// then: a client cannot hold the node's memory with a block it never ends.
func TestAHeaderBlockPastItsLimitBreaksTheConnection(t *testing.T) {
	c, _ := flowClient(t)
	frame := func(typ, flags byte, sid uint32, n int) []byte {
		b := make([]byte, 9+n)
		putFrameHeader(b, n, typ, flags, sid)
		return b
	}
	in := append([]byte(nil), clientPreface...)
	in = append(in, frame(frSettings, 0, 0, 0)...)
	in = append(in, frame(frHeaders, 0, 3, 16<<10)...)
	for range 3 {
		in = append(in, frame(frContinuation, 0, 3, 16<<10)...)
	}
	c.node.connOut = 1 << 20 // its replies, the settings' ack, do not pause it
	before := c.node.cpool
	c.pt, c.pr, c.pw = in, 0, len(in)
	if err := c.frames(c.node); err != nil {
		t.Fatalf("a block of 64 KiB: %v", err)
	}
	if held := c.node.cpool - before; held < 64<<10 {
		t.Fatalf("a block of 64 KiB in part counts %d bytes in the client buffers", held)
	}
	c.pt, c.pr, c.pw = frame(frContinuation, 0, 3, 1), 0, 10
	if err := c.frames(c.node); !errors.Is(err, errHeaderBlock) {
		t.Fatalf("a block past %d bytes: %v", maxHeaderBlock, err)
	}
}

// A stream's receive window is given back as its DATA comes in, as the
// connection's is: a keepalive or watch stream that sends past the 1 GiB
// it starts with goes on. Given back only on the connection, such a
// stream's client waited for good.
func TestAStreamsWindowIsGivenBackAsItsDataComesIn(t *testing.T) {
	ack := windowAck
	windowAck = 1500
	t.Cleanup(func() { windowAck = ack })
	n, _ := testNode(t)
	n.connOut, n.sizes = 1<<20, sizeFor(0)
	c := newClient(&handoff{fd: -1, plain: true}, n)
	c.takeBufs()
	c.preface = true
	c.newStream(1, kWatch)
	// 3000 bytes of a message of 100000, which stays unfinished.
	var in []byte
	for i := range 3 {
		p := make([]byte, 1000)
		if i == 0 {
			binary.BigEndian.PutUint32(p[1:], 100000)
		}
		h := make([]byte, 9)
		putFrameHeader(h, len(p), frData, 0, 1)
		in = append(append(in, h...), p...)
	}
	c.pt, c.pr, c.pw = in, 0, len(in)
	if err := c.frames(n); err != nil {
		t.Fatalf("%v; out %q", err, c.out)
	}
	given := 0
	for out := c.out; len(out) >= 9; {
		l := int(out[0])<<16 | int(out[1])<<8 | int(out[2])
		if out[3] == frWindowUpdate && binary.BigEndian.Uint32(out[5:]) == 1 {
			given += int(binary.BigEndian.Uint32(out[9:]))
		}
		out = out[9+l:]
	}
	if given != 2000 {
		t.Fatalf("stream 1 was given back %d bytes of the 3000 it sent, want 2000 at the 1500 mark", given)
	}
}

// The requests of side calls (Lock, Election, health) whose client has
// not ended its side count in its requests in part: kept uncounted, one
// connection that never ended its calls held any memory it sent. They
// came within the client's window, which the read half held room for,
// so none is refused for room, even with the read half full (#737).
// Ended, a call's request goes out of the count.
func TestSideCallRequestsCountInTheClientsShare(t *testing.T) {
	n, _ := testNode(t)
	n.connOut, n.sizes = 1<<20, sizeFor(128<<20)
	n.sideReqs = map[handle][]byte{}
	c := newClient(&handoff{fd: -1, plain: true}, n)
	c.takeBufs()
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.cpool = n.sizes.clientInBufs()
	msg := make([]byte, 1<<20)
	refused := 0
	for i := range 8 {
		s := c.newStream(uint32(2*i+1), kSide)
		n.sideRequest(c, s, msg)
		if !s.open {
			refused++
		}
	}
	if c.partial != 8*int64(len(msg)) || refused != 0 {
		t.Fatalf("8 side calls of 1 MiB kept %d MiB, %d refused; want 8 and none", c.partial>>20, refused)
	}
	held := c.partial
	n.takeSideReq(mkHandle(c, 1))
	if c.partial != held-int64(len(msg)) {
		t.Fatalf("a call's request taken left %d bytes counted, want %d", c.partial, held-int64(len(msg)))
	}
}
