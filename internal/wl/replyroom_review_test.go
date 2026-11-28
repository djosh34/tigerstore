package wl

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// A shrunken share can require pieces before the reply reaches cannotFit:
// rangeKVs uses oneShot, not share + reserve, to choose the pieces path.
func TestAShrunkenShareRechecksThePiecesThreshold(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		budget, before, after int64
		keys, value           int
	}{
		{"Astra", 192 << 20, 3 << 20, 1536 << 10, 5, 640 << 10},
		{"Opus", 2 << 30, 4 << 20, 64 << 10, 3, 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s, st, log := replyRoomClient(t)
			n := c.node
			n.sizes, n.connOut = sizeFor(tc.budget), tc.before
			n.reserved = n.sizes.reserve()
			if tc.name == "Opus" {
				// Existing output makes the 3 MiB reply exceed its
				// original 4 MiB share before that share shrinks.
				sendMsg(c, c.newStream(3, kRange), 2<<20)
			}
			for i := range tc.keys {
				putValue(t, st, log, fmt.Sprintf("/p/%d", i), strings.Repeat("v", tc.value))
			}
			msg := appendBytesField(appendBytesField(nil, 1, []byte("/p/")), 2, []byte("/p0"))
			serveRoomRange(t, c, s, msg)
			if len(n.roomWait) != 1 {
				t.Fatal("the whole reply did not wait")
			}
			n.connOut = tc.after
			if body := n.roomWait[0].body; int64(body) <= n.oneShot() || n.cannotFit(int64(5+sizeBytesField(1, n.hdrSize())+body)) {
				t.Fatal("the reproduction must exceed oneShot but still fit share plus reserve")
			}
			n.reserved -= 1024 // still insufficient for the old whole reply
			n.roomFreed = true
			n.serveRoomWait()
			if len(n.roomWait) != 0 || len(n.bigs) != 1 || c.roomWaiters != 0 || c.heldCounted != 0 {
				t.Fatal("the range waits for a reserve it no longer needs instead of starting pieces")
			}
		})
	}
}

// More active clients, not a test-set roomFreed flag, must wake the
// waiting range when they shrink its share past the whole-reply limit:
// at once as they take their buffers (node.reshare), and the tick keeps
// the share.
func TestAClientTickWakesARangeWhenItsShareShrinks(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut = n.sizes.connOut(n.active)
	if n.connOut != 1536<<10 {
		t.Fatalf("initial share %d, want 1.5 MiB", n.connOut)
	}
	n.reserved = n.sizes.reserve()
	for i := range 2 {
		putValue(t, st, log, fmt.Sprintf("/p/%d", i), strings.Repeat("v", 1400<<10))
	}
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/p/")), 2, []byte("/p0"))
	serveRoomRange(t, c, s, msg)
	if len(n.roomWait) != 1 {
		t.Fatal("the whole reply did not wait")
	}
	n.roomFreed = false
	for i := 1; i < 3; i++ {
		other := partClient(t, n, i)
		n.socks, n.gens = append(n.socks, other), append(n.gens, other.gen)
	}
	l := &loop{n: n}
	l.clientTick()
	if n.connOut != 512<<10 {
		t.Fatalf("new share %d, want 512 KiB", n.connOut)
	}
	n.serveRoomWait()
	if len(n.roomWait) != 0 || len(n.bigs) != 1 || c.heldCounted != 0 {
		t.Fatal("the tick did not wake the now-oversized range for pieces")
	}
}

// The real socket flush drains only part of the client's output and hits
// EAGAIN. Its remaining bytes are above half the share, but a waiting
// reply now fits. Neither a reserve release nor a half-share crossing
// may be required to wake it.
func TestAPartialSocketDrainWakesAReplyAboveHalfTheShare(t *testing.T) {
	c, s, st, log := replyRoomClient(t)
	n := c.node
	n.connOut, n.reserved = 1<<20, n.sizes.reserve()
	peer := roomSocket(t, c)
	other := c.newStream(3, kRange)
	c.flow.conn, other.win = 1<<30, 1<<30
	sendMsg(c, other, 800<<10)
	c.flow.conn = 0 // the waiting range's eventual reply stays inspectable
	before := c.waiting()
	putValue(t, st, log, "k", strings.Repeat("v", 320<<10))
	serveRoomRange(t, c, s, appendBytesField(nil, 1, []byte("k")))
	if len(n.roomWait) != 1 || c.roomHeld != 0 {
		t.Fatal("the reply did not wait behind output without owning reserve room")
	}
	n.roomFreed = false
	l := &loop{n: n, epfd: -1}
	// A clamped send buffer may take several drains, not one. Never
	// assume how many bytes Linux lets a nonblocking flush send.
	for len(n.roomWait) != 0 {
		n.markDirty(&c.sock)
		l.flush()
		if c.dead || c.waiting() >= before || c.waiting() <= n.connOut/2 || !c.wantOut {
			t.Fatalf("not a partial drain above half: before=%d after=%d dead=%v wantOut=%v", before, c.waiting(), c.dead, c.wantOut)
		}
		before = c.waiting()
		drainRoomSocket(t, peer)
	}
	if len(n.roomWait) != 0 || c.heldCounted != 0 || c.roomHeld != 0 || n.reserved != n.sizes.reserve() {
		t.Fatal("a reply fitting the partial drain still waits for reserve room")
	}
	reply := roomRangeReply(t, c)
	if len(reply.Kvs) != 1 || string(reply.Kvs[0].Value) != strings.Repeat("v", 320<<10) {
		t.Fatal("the partial drain released the wrong reply")
	}
}

// roomSocket deliberately asks for a small send buffer. Tests drive as
// many actual drains as needed, including when Linux clamps it lower.
func roomSocket(t *testing.T, c *client) int {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	if err := unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 32<<10); err != nil {
		t.Fatal(err)
	}
	c.fd = fds[0]
	return fds[1]
}

func drainRoomSocket(t *testing.T, fd int) {
	t.Helper()
	var buf [64 << 10]byte
	total := 0
	for {
		n, err := unix.Read(fd, buf[:])
		if n > 0 {
			total += n
		}
		if errors.Is(err, unix.EAGAIN) {
			break
		}
		if err != nil || n == 0 {
			t.Fatalf("drain: %d bytes, error %v", n, err)
		}
	}
	if total == 0 {
		t.Fatal("the socket flush sent no bytes")
	}
}
