package wl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The reserve gives the room a message passes its client's share by, to
// one client at a time while it has it, and takes it back as the client's
// bytes leave; a message within the share, or a small one, takes none
// (#727).
func TestTheReserveTakesWhatPassesAShare(t *testing.T) {
	n := &node{sizes: sizeFor(256 << 20)}
	n.connOut = n.sizes.connOut(16)
	reserve := n.sizes.reserve()
	const msg = maxRequestBytes
	a, b := &client{}, &client{}
	a.node, b.node = n, n
	a.out = make([]byte, 100<<10)
	if !n.msgRoom(a, n.connOut-100<<10) || n.reserved != 0 {
		t.Fatalf("a message within its share took %d of the reserve", n.reserved)
	}
	if !n.msgRoom(a, msg) {
		t.Fatal("the reserve, empty, gave no room")
	}
	if past := int64(len(a.out)) + msg - n.connOut; a.roomHeld != past || n.reserved != past {
		t.Fatalf("a holds %d of the reserve, %d taken; want %d", a.roomHeld, n.reserved, past)
	}
	a.out = append(a.out, make([]byte, msg)...)
	if n.msgRoom(b, msg) || !n.roomShort || n.stats.roomWaits != 1 {
		t.Fatalf("b got room the reserve does not have: %d of %d taken", n.reserved, reserve)
	}
	if !n.msgRoom(b, smallMsg) {
		t.Fatal("a small message waited for the reserve")
	}
	// a's bytes leave, all but what passes its share by a little.
	a.out = a.out[:n.connOut+1000]
	n.giveRoom(a, false)
	if a.roomHeld != 1000 || n.reserved != 1000 || n.roomShort {
		t.Fatalf("after a's flush it holds %d, %d taken, short %v; want 1000, 1000, false", a.roomHeld, n.reserved, n.roomShort)
	}
	if !n.msgRoom(b, msg) {
		t.Fatal("b got no room once a gave it back")
	}
	n.giveRoom(a, true)
	n.giveRoom(b, true)
	if n.reserved != 0 || a.roomHeld != 0 || b.roomHeld != 0 {
		t.Fatalf("closed clients hold %d and %d, %d taken", a.roomHeld, b.roomHeld, n.reserved)
	}
	if msg > reserve {
		t.Fatalf("one message of %d bytes passes the whole reserve of %d", msg, reserve)
	}
}

// Many clients at once each read a key of a 1 MiB value, near the largest
// request the read half takes at that budget, on a small budget, where each reply passes its client's share
// (64 KiB, the least, as the reserve takes the whole send half at 96 MiB).
// Every reply is right, what waits to be sent to the clients stays within
// the reserve and their shares, and replies waited for the reserve. A
// read that finds the reserve spent waits, unbuilt, until room comes
// back (#727).
func TestManyLargeRepliesStayWithinTheReserve(t *testing.T) {
	const budget = 96 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := tc.clientOf(t, tc.endpoints[0])
	// Near the limit of a request the read half takes at this budget.
	value := bytes.Repeat([]byte("r"), 1<<20)
	if _, err := c.Put(ctx, "/r/v", string(value)); err != nil {
		t.Fatal(err)
	}
	l := loopOf(tc.ids[0])
	z := sizeFor(budget)
	// The reserve, the clients' shares, and each one's send buffer at
	// twice its empty size, as a record opens in it.
	limit := z.reserve() + 33*(minConnOut+2*clientOutBuf)
	// The reserve spent: a read waits for it, and is served once its
	// room comes back.
	setReserved := func(v int64) {
		done := make(chan struct{})
		l.post(func() error {
			l.n.reserved = v
			l.n.roomFreed, l.n.roomShort = v == 0 && len(l.n.roomWait) > 0, v > 0
			close(done)
			return nil
		})
		<-done
	}
	setReserved(z.reserve())
	got := make(chan error, 1)
	go func() {
		r, err := c.Get(ctx, "/r/v", clientv3.WithSerializable())
		if err == nil && (len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, value)) {
			err = errors.New("a wrong reply")
		}
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("a read past its share was served with the reserve spent: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	setReserved(0)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the read that waited for the reserve was not served once it had room")
	}
	var most int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			most = max(most, seeNode(l).cout)
			time.Sleep(time.Millisecond)
		}
	}()
	const clients, reads = 32, 4
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		cli := tc.clientOf(t, tc.endpoints[0])
		wg.Go(func() {
			for range reads {
				r, err := cli.Get(ctx, "/r/v", clientv3.WithSerializable())
				switch {
				case err != nil:
					errs <- fmt.Errorf("client %d: %w", i, err)
					return
				case len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, value):
					errs <- fmt.Errorf("client %d: a wrong reply", i)
					return
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	<-sampled
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	waits := make(chan [2]int64, 1)
	l.post(func() error {
		waits <- [2]int64{int64(l.n.stats.roomWaits), l.n.reserved}
		return nil
	})
	w := <-waits
	t.Logf("the clients' send buffers held at most %d KiB of %d KiB; %d messages waited for the reserve; %d bytes of it still taken", most>>10, limit>>10, w[0], w[1])
	if most > limit {
		t.Fatalf("what waits to be sent held %d KiB, past the send half and the reserve, %d KiB", most>>10, limit>>10)
	}
	if w[0] == 0 {
		t.Fatal("no reply waited for the reserve: the test's replies did not meet")
	}
}
