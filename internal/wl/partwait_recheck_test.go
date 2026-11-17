package wl

import (
	"testing"

	"golang.org/x/sys/unix"
)

// The N re-check of #689's part wait (f3cd34f7), its findings as tests.

// A client that waits for its buffers and whose other end closed is
// closed: its close stays reported until it reads, and the loop turned
// again at once, the simulator's clock stopped (N1).
func TestAStarvedClientWhoseOtherEndClosedIsClosed(t *testing.T) {
	n, a, _, l, feed, _ := partNode(t)
	feed(a, putPart(t, 1, true)) // a's part waits: n.waiting holds it
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := partClient(t, n, 2)
	c.fd = fds[0]
	c.giveBufs()
	n.socks, n.gens = append(n.socks, c), append(n.gens, 1)
	n.clients++
	if _, err := unix.Write(fds[1], []byte("x")); err != nil {
		t.Fatal(err)
	}
	l.input(2, false) // it reads again, and waits for buffers
	if !c.starved || c.dead {
		t.Fatalf("a client that reads while a part waits: starved %v, closed %v", c.starved, c.dead)
	}
	_ = unix.Close(fds[1])
	l.input(2, true)
	if !c.dead {
		t.Fatal("a starved client whose other end closed stays open")
	}
}

// A client served that waits again at its next part goes behind those
// that wait still, once: it is not served again before them (N2).
func TestAClientServedThatWaitsAgainGoesBehind(t *testing.T) {
	n, a, b, l, _, tooMany := partNode(t)
	n.reserved = n.sizes.reserve() // replies hold the reserve: no turn
	p := append(putPart(t, 1, true), putPart(t, 1, false)...)
	p = append(p, putPart(t, 1, false)...)
	a.pt, a.pr, a.pw = append(a.pt[:0], p...), 0, len(p)
	if err := a.frames(n); err != nil {
		t.Fatal(err)
	}
	q := putPart(t, 3, true)
	b.pt, b.pr, b.pw = append(b.pt[:0], q...), 0, len(q)
	if err := b.frames(n); err != nil {
		t.Fatal(err)
	}
	n.cpool -= part
	l.serveWaiting()
	if len(n.waiting) != 2 || n.waiting[0] != b || n.waiting[1] != a {
		t.Fatalf("a served and waiting again: %d in line, b first %v", len(n.waiting), len(n.waiting) > 0 && n.waiting[0] == b)
	}
	n.cpool -= part
	l.serveWaiting()
	if b.roomNeed != 0 || a.roomNeed != part || tooMany() != 0 {
		t.Fatalf("room for one part again: b needs %d, a needs %d, %d refused; want b served", b.roomNeed, a.roomNeed, tooMany())
	}
}

// The room a turn gives back goes to the ranges that wait for the reserve
// before another turn takes it (N4).
func TestTheReserveGoesToWaitingRepliesBeforeTheNextTurn(t *testing.T) {
	n, a, b, l, feed, _ := partNode(t)
	feed(a, putPart(t, 1, true))
	feed(b, putPart(t, 3, true))
	l.serveWaiting()
	if n.inTurn != a {
		t.Fatal("setup: a has no turn")
	}
	n.roomWait = append(n.roomWait, awayRead{h: mkHandle(b, 99)}) // a range waits for room
	a.endStream(a.get(1))
	l.serveWaiting()
	if len(n.roomWait) != 0 {
		t.Fatalf("the turn's room went on with %d ranges waiting for it", len(n.roomWait))
	}
}

// A part's refusal for its wait is for the stream that waited: when that
// stream ended while it waited, another stream's part behind it waits as
// any, and is not refused at once (N5).
func TestALateRefusalIsForTheStreamThatWaited(t *testing.T) {
	n, a, _, l, _, tooMany := partNode(t)
	n.reserved = n.sizes.reserve() // replies hold the reserve: no turn
	p := append(putPart(t, 1, true), putPart(t, 3, true)...)
	a.pt, a.pr, a.pw = append(a.pt[:0], p...), 0, len(p)
	if err := a.frames(n); err != nil {
		t.Fatal(err)
	}
	a.endStream(a.get(1))
	n.now = n.now.Add(requestTimeout)
	l.serveWaiting()
	if tooMany() != 0 || a.roomNeed != part || a.roomSid != 3 {
		t.Fatalf("stream 3's part behind an ended stream: %d refused, needs %d on stream %d; want it waiting", tooMany(), a.roomNeed, a.roomSid)
	}
}

// What waits to be sent to the clients stays within the send half, the
// reserve and each one's send buffer at twice its base: a buffer a large
// reply grew does not keep four times its base once it is sent. Kept so,
// 33 clients at 96 MiB held 9.96 MB of 8.59 MB (#689's O re-check, Astra,
// whose probe this is).
func TestTheSendBuffersKeepWithinTheSendHalfAndTheReserve(t *testing.T) {
	n, _, _, _, _, _ := partNode(t)
	n.sizes = sizeFor(96 << 20)
	n.connOut = minConnOut
	n.cout = 0
	const clients = 33
	cs := make([]*client, clients)
	readers := make(map[*client]int)
	flush := func(c *client) {
		if ok, err := c.flush(); !ok || err != nil {
			t.Fatalf("flush: %v %v", ok, err)
		}
		var buf [256 << 10]byte
		for {
			_, err := unix.Read(readers[c], buf[:])
			if err == unix.EAGAIN {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		n.giveRoom(c, false)
	}
	for i := range cs {
		c := partClient(t, n, i)
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
		c.fd, readers[c] = fds[0], fds[1]
		cs[i] = c
		c.flow.conn = 128 << 10
		c.setInitial(128 << 10)
		s := c.newStream(1, kRange)
		if !n.msgRoom(c, (1<<20)+5) {
			t.Fatal("reply cannot take reserve")
		}
		sendMsg(c, s, 1<<20)
		for c.flow.bytes > 0 {
			flush(c)
			c.windowUpdate(0, 128<<10)
			c.windowUpdate(1, 128<<10)
		}
		flush(c)
	}
	if n.reserved != 0 || n.inTurn != nil {
		t.Fatal("setup has outstanding reserve")
	}
	for _, c := range cs[:2] {
		s := c.newStream(3, kRange)
		if !n.msgRoom(c, (1<<20)+5) {
			t.Fatal("two replies should fit reserve")
		}
		sendMsg(c, s, 1<<20)
	}
	z := n.sizes
	limit := z.reserve() + max(z.clientBufs()-z.clientInBufs(), clients*minConnOut) + clients*2*clientOutBuf
	if n.cout > limit {
		t.Fatalf("send pool %d > test limit %d by %d; reserved %d <= %d, no input turn/debt; retained cap/client=%d", n.cout, limit, n.cout-limit, n.reserved, z.reserve(), cap(cs[2].out))
	}
}

// The turn's client that waits again in its turn, the buffers filled by
// what took no part of the read half's line (a handshake's leftover, as
// newClient takes it), goes first: room within its turn serves it though
// a client ahead of it in line finds none in the read half (#689's O
// re-check, Opus O4).
func TestTheTurnsClientGoesFirstInLine(t *testing.T) {
	n, a, b, l, feed, tooMany := partNode(t)
	feed(a, putPart(t, 1, true))
	feed(b, putPart(t, 3, true))
	l.serveWaiting()
	if n.inTurn != a || a.roomNeed != 0 || b.roomNeed != part {
		t.Fatalf("setup: turn a %v, a needs %d, b needs %d", n.inTurn == a, a.roomNeed, b.roomNeed)
	}
	n.cpool = n.turnLimit // other buffers fill the turn's limit
	feed(a, putPart(t, 1, false))
	if a.roomNeed != part || len(n.waiting) != 2 || n.waiting[0] != b {
		t.Fatalf("a in its turn, the limit full: needs %d, %d in line, b first %v", a.roomNeed, len(n.waiting), len(n.waiting) > 0 && n.waiting[0] == b)
	}
	n.cpool -= part
	l.serveWaiting()
	if a.roomNeed != 0 || b.roomNeed != part || tooMany() != 0 {
		t.Fatalf("room within a's turn: a needs %d, b needs %d, %d refused; want a served", a.roomNeed, b.roomNeed, tooMany())
	}
}

// What the buffers take with no check of their room while a turn runs, a
// new connection's handshake leftovers among them, does not take the
// turn's room: its client goes on within its turn. Taken from it, the
// turn's client waited at a part its turn could not serve, first in line,
// and every client behind it waited requestTimeout (#735's loop on
// 38cf23e4, as clients connected).
func TestATurnKeepsItsRoomPastWhatIsTakenUnchecked(t *testing.T) {
	n, a, b, l, feed, tooMany := partNode(t)
	feed(a, putPart(t, 1, true))
	l.serveWaiting()
	if n.inTurn != a || a.roomNeed != 0 {
		t.Fatalf("setup: turn a %v, a needs %d", n.inTurn == a, a.roomNeed)
	}
	b.growLoose(int(n.turnLimit - n.cpool)) // as a connection's handshake leftovers
	feed(a, putPart(t, 1, false))
	if a.roomNeed != 0 || a.partial != 2*part || tooMany() != 0 {
		t.Fatalf("the turn's client after others took its room unchecked: needs %d, holds %d in part, %d refused", a.roomNeed, a.partial, tooMany())
	}
}
