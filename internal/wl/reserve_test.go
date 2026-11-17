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

// A write whose reply finds the reserve spent is applied, and its reply
// waits unbuilt until room comes back, its records named by key and
// revision and their states held: it is never refused, so that a
// compare-and-put's client never sees a failure for a write that
// applied, and retries it into a failed compare. Here a Txn that puts
// with prev_kv and ranges, a Put with prev_kv and a DeleteRange with
// prev_kv wait while their records are overwritten and compacted away,
// and each gets its right reply once the reserve has room (#727).
func TestAWritesReplyWaitsForTheReserve(t *testing.T) {
	const budget = 96 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := tc.clientOf(t, tc.endpoints[0])
	// Each reply passes its client's share, the least at this budget.
	big := func(b byte) string { return string(bytes.Repeat([]byte{b}, 300<<10)) }
	put := func(k, v string) int64 {
		r, err := c.Put(ctx, k, v)
		if err != nil {
			t.Fatal(err)
		}
		return r.Header.Revision
	}
	modA := put("/h/a", big('a'))
	modB := put("/h/b", big('b'))
	modP := put("/h/p", big('p'))
	l := loopOf(tc.ids[0])
	z := sizeFor(budget)
	setReserved := func(v int64) {
		done := make(chan struct{})
		l.post(func() error {
			l.n.reserved = v
			l.n.roomFreed, l.n.roomShort = v == 0, v > 0
			close(done)
			return nil
		})
		<-done
	}
	setReserved(z.reserve())

	txn := make(chan *clientv3.TxnResponse, 1)
	del := make(chan *clientv3.DeleteResponse, 1)
	pp := make(chan *clientv3.PutResponse, 1)
	errs := make(chan error, 3)
	// Each on a client of its own: a reply that waits holds up nothing
	// else of its connection, but these tests read on other clients.
	ct, cd, cp := tc.clientOf(t, tc.endpoints[0]), tc.clientOf(t, tc.endpoints[0]), tc.clientOf(t, tc.endpoints[0])
	go func() {
		r, err := ct.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/h/a"), "=", modA)).
			Then(clientv3.OpPut("/h/a", "A", clientv3.WithPrevKV()), clientv3.OpGet("/h/b")).Commit()
		if err != nil {
			errs <- fmt.Errorf("the Txn: %w", err)
			return
		}
		txn <- r
	}()
	// All three apply, and none is answered.
	versions := func() map[string]int64 {
		r, err := c.Get(ctx, "/h/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			t.Fatal(err)
		}
		ver := map[string]int64{}
		for _, kv := range r.Kvs {
			ver[string(kv.Key)] = kv.Version
		}
		return ver
	}
	applied := func() bool {
		ver := versions()
		_, bLive := ver["/h/b"]
		return ver["/h/a"] == 2 && ver["/h/p"] == 2 && !bLive
	}
	// The delete and the put go after the Txn, which reads /h/b.
	deadline := time.Now().Add(10 * time.Second)
	for versions()["/h/a"] != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the Txn did not apply with the reserve spent")
		}
		time.Sleep(20 * time.Millisecond)
	}
	go func() {
		r, err := cd.Delete(ctx, "/h/b", clientv3.WithPrevKV())
		if err != nil {
			errs <- fmt.Errorf("the delete: %w", err)
			return
		}
		del <- r
	}()
	go func() {
		r, err := cp.Put(ctx, "/h/p", "P", clientv3.WithPrevKV())
		if err != nil {
			errs <- fmt.Errorf("the put: %w", err)
			return
		}
		pp <- r
	}()
	for !applied() {
		if time.Now().After(deadline) {
			t.Fatal("the three writes did not apply with the reserve spent")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-errs:
		t.Fatalf("a write that applied failed: %v", err)
	case <-txn:
		t.Fatal("the Txn's reply was sent with the reserve spent")
	case <-del:
		t.Fatal("the delete's reply was sent with the reserve spent")
	case <-pp:
		t.Fatal("the put's reply was sent with the reserve spent")
	case <-time.After(300 * time.Millisecond):
	}
	// Their records are overwritten and compacted meanwhile.
	put("/h/a", "A2")
	rev := put("/h/p", "P2")
	if _, err := c.Compact(ctx, rev, clientv3.WithCompactPhysical()); err != nil {
		t.Fatal(err)
	}
	held := make(chan [2]int, 1)
	l.post(func() error {
		held <- [2]int{len(l.n.heldReplies), len(l.n.st.readHolds)}
		return nil
	})
	if h := <-held; h[0] != 3 || h[1] != 3 {
		t.Fatalf("%d replies held, %d revisions held; want 3 and 3", h[0], h[1])
	}
	setReserved(0)
	for range 3 {
		select {
		case r := <-txn:
			if !r.Succeeded || len(r.Responses) != 2 {
				t.Fatalf("the Txn: succeeded %v, %d responses", r.Succeeded, len(r.Responses))
			}
			prev := r.Responses[0].GetResponsePut().PrevKv
			if prev == nil || string(prev.Value) != big('a') || prev.ModRevision != modA {
				t.Fatal("the Txn's put came back without its value before")
			}
			got := r.Responses[1].GetResponseRange().Kvs
			if len(got) != 1 || string(got[0].Value) != big('b') || got[0].ModRevision != modB {
				t.Fatal("the Txn's range came back wrong")
			}
		case r := <-del:
			if r.Deleted != 1 || len(r.PrevKvs) != 1 || string(r.PrevKvs[0].Value) != big('b') || r.PrevKvs[0].ModRevision != modB {
				t.Fatal("the delete came back without its value before")
			}
		case r := <-pp:
			if r.PrevKv == nil || string(r.PrevKv.Value) != big('p') || r.PrevKv.ModRevision != modP {
				t.Fatal("the put came back without its value before")
			}
		case err := <-errs:
			t.Fatalf("a write that applied failed: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("a held reply was not sent once the reserve had room")
		}
	}
	l.post(func() error {
		held <- [2]int{len(l.n.heldReplies), len(l.n.st.readHolds)}
		return nil
	})
	if h := <-held; h[0] != 0 || h[1] != 0 {
		t.Fatalf("after the replies, %d still held, %d revisions held", h[0], h[1])
	}
}
