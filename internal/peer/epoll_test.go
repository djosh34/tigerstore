//go:build linux

package peer

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/epoll"
	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestRemovedPeerLeavesTheLoop runs node 1's transport on an epoll loop,
// as the tigerstore command does, and then drops node 2 from its peers.
// The lanes' goroutines end, and their connections must leave the loop
// and the transport: a socket that another goroutine closes sends epoll
// no event, so the close has to tell the loop itself.
func TestRemovedPeerLeavesTheLoop(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	c1, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	c2, _, _, err := ca.Issue("node", 3)
	if err != nil {
		t.Fatal(err)
	}
	p, err := epoll.New()
	if err != nil {
		t.Fatal(err)
	}
	l := loop.New(p)
	go func() { _ = l.Run(t.Context()) }()
	addrs, lns := listeners(t, 1, 2)
	b := start(t, 2, lns[2], addrs, c2, ca.Pool)
	defer b.t.Close() //nolint:errcheck // closing a test transport
	a := startWith(t, 1, lns[1], addrs, c1, ca.Pool, func(c *Config) { c.Loop = l })
	defer a.t.Close() //nolint:errcheck // closing a test transport
	l.Post(func() { a.t.Send(msg(1, "x")) })
	receive(t, b, 1)
	outbound := func() int {
		a.t.mu.Lock()
		defer a.t.mu.Unlock()
		n := 0
		for c := range a.t.conns {
			if _, in := a.t.inbound[c]; !in {
				n++
			}
		}
		return n
	}
	if outbound() == 0 {
		t.Fatal("node 1 has no connection to node 2")
	}
	l.Post(func() { a.t.SetPeers(map[uint64]string{}, nil) })
	for wait := time.Now().Add(10 * time.Second); outbound() > 0; {
		if time.Now().After(wait) {
			t.Fatalf("%d connections to the dropped peer are still tracked", outbound())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
