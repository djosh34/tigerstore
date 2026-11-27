package wl

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestDeferredHandshakeAlsoRequestsBufferRotation(t *testing.T) {
	c, s, _, _, l := fillRangeFixture(t)
	n := c.node
	s.open = false
	c.streams = 0
	c.busy = 0
	n.clients = 1
	ep, err := unix.EpollCreate1(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(ep) }()
	l.epfd = ep
	l.tick = 1
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(pair[0]) }()
	defer func() { _ = unix.Close(pair[1]) }()
	// Reserve the rest of the normal half; freeing this idle client is the
	// only way the handoff can acquire its ordinary buffers. No clock/sleep.
	n.cpool = n.sizes.clientInBufs()
	l.handoffs = []*handoff{{fd: pair[0], kind: kindClient, plain: true, left: []byte("PRI")}}
	l.takeHandoffs()
	if !l.handoffWaiting || len(l.handoffs) != 1 || len(n.waiting) != 0 {
		t.Fatal("handoff was not a bufferless waiter")
	}
	l.serveWaiting()
	if l.handoffWaiting || len(l.handoffs) != 0 || c.in != nil || len(n.socks) != 2 {
		t.Fatal("registered clients starved the deferred handshake")
	}
	next, ok := n.socks[1].(*client)
	if !ok || next.in == nil || next.iw != 3 {
		t.Fatal("handshake leftovers were lost or not activated")
	}
	next.dropAll()
}
