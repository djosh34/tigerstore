package wl

import (
	"testing"
	"time"

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

// A client that keeps starting requests in its turn gives the turn up
// once the requests it took it for are whole, and none is refused (N3).
func TestATurnEndsWithTheRequestsItWasFor(t *testing.T) {
	n, a, b, l, feed, tooMany := partNode(t)
	feed(a, putPart(t, 1, true))
	feed(b, putPart(t, 3, true))
	l.serveWaiting()
	if n.inTurn != a || b.roomNeed != part {
		t.Fatalf("setup: turn %v, b needs %d", n.inTurn == a, b.roomNeed)
	}
	feed(a, putPart(t, 5, true)) // a starts another in its turn
	if a.roomNeed != 0 {
		t.Fatalf("a's next request waits for %d", a.roomNeed)
	}
	a.endStream(a.get(1)) // the one it took the turn for is whole
	n.now = n.now.Add(time.Second)
	l.serveWaiting()
	if n.inTurn == a || b.roomNeed != 0 || tooMany() != 0 {
		t.Fatalf("a's turn's request whole: a keeps the turn %v, b needs %d, %d refused", n.inTurn == a, b.roomNeed, tooMany())
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
