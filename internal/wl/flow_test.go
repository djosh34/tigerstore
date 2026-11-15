package wl

import (
	"crypto/aes"
	"crypto/cipher"
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
	c := newClient(&handoff{fd: -1, wr: aeadDir{aead: g}, rd: aeadDir{aead: g}}, &counters{})
	c.node = n
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
