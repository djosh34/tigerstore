package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// call is a blocking call that runs in a goroutine.
type call[T any] struct {
	done chan struct{}
	resp T
	err  error
}

func start[T any](fn func() (T, error)) *call[T] {
	c := &call[T]{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		c.resp, c.err = fn()
	}()
	return c
}

// waiting fails the test if the call returns within 200 ms.
func (c *call[T]) waiting(t *testing.T, what string) {
	t.Helper()
	select {
	case <-c.done:
		t.Fatalf("%s returned without waiting: %v, %v", what, c.resp, c.err)
	case <-time.After(200 * time.Millisecond):
	}
}

// wait returns the call's answer, failing the test after 10 s.
func (c *call[T]) wait(t *testing.T, what string) (T, error) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s still waits after 10 s", what)
	}
	return c.resp, c.err
}

// node2 starts a cluster and returns a connection to node 2 once the
// cluster has a leader.
func node2(t *testing.T) *grpc.ClientConn {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(2, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return pb.NewKVClient(conn).Put(ctx, &pb.PutRequest{Key: []byte("ready"), Value: []byte("1")})
	})
	return conn
}

// grant grants a lease of 60 s.
func grant(t *testing.T, l pb.LeaseClient) int64 {
	t.Helper()
	resp, err := l.LeaseGrant(context.Background(), &pb.LeaseGrantRequest{TTL: 60})
	if err != nil {
		t.Fatal(err)
	}
	return resp.ID
}

// keys returns the keys under name/, oldest first.
func keys(t *testing.T, kv pb.KVClient, name string) []string {
	t.Helper()
	resp, err := kv.Range(context.Background(), &pb.RangeRequest{Key: []byte(name + "/"), RangeEnd: []byte(name + "0"), SortTarget: pb.RangeRequest_CREATE, SortOrder: pb.RangeRequest_ASCEND})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, kv := range resp.Kvs {
		out = append(out, string(kv.Key))
	}
	return out
}

// TestLock checks Lock and Unlock as etcd v3.7.2 serves them: the holder
// key is the name, a slash and the lease in hex; waiters get the lock in
// the order they asked; a lost lease hands the lock on; a waiter that
// gives up deletes its key; and a waiter whose lease ends while it waits
// gets etcd's "session is expired".
func TestLock(t *testing.T) {
	conn := node2(t)
	lk, kv, leases := v3lockpb.NewLockClient(conn), pb.NewKVClient(conn), pb.NewLeaseClient(conn)
	ctx := context.Background()
	a, b, c := grant(t, leases), grant(t, leases), grant(t, leases)

	held, err := lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("m"), Lease: a})
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("m/%x", a); string(held.Key) != want || held.Header.MemberId != 2 {
		t.Fatalf("lock a = %v, want key %s", held, want)
	}
	// The same lease takes the lock it holds again at once.
	again, err := lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("m"), Lease: a})
	if err != nil || string(again.Key) != string(held.Key) || again.Header.Revision != held.Header.Revision {
		t.Fatalf("lock a again = %v, %v; first %v", again, err, held)
	}

	lockB := start(func() (*v3lockpb.LockResponse, error) {
		return lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("m"), Lease: b})
	})
	lockB.waiting(t, "lock b")
	lockC := start(func() (*v3lockpb.LockResponse, error) {
		return lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("m"), Lease: c})
	})
	lockC.waiting(t, "lock c")

	un, err := lk.Unlock(ctx, &v3lockpb.UnlockRequest{Key: held.Key})
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := lockB.wait(t, "lock b")
	if err != nil || string(gotB.Key) != fmt.Sprintf("m/%x", b) || gotB.Header.Revision != un.Header.Revision {
		t.Fatalf("lock b = %v, %v after unlock at %d", gotB, err, un.Header.Revision)
	}
	lockC.waiting(t, "lock c behind b")

	// Revoking the holder's lease deletes its key and hands the lock on.
	if _, err := leases.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: b}); err != nil {
		t.Fatal(err)
	}
	gotC, err := lockC.wait(t, "lock c")
	if err != nil || string(gotC.Key) != fmt.Sprintf("m/%x", c) {
		t.Fatalf("lock c = %v, %v", gotC, err)
	}

	// A waiter that gives up deletes its key.
	d := grant(t, leases)
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := lk.Lock(short, &v3lockpb.LockRequest{Name: []byte("m"), Lease: d}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("lock d with a deadline: %v", err)
	}
	want := fmt.Sprintf("m/%x", c)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got := keys(t, kv, "m")
		if len(got) == 1 && got[0] == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keys after lock d gave up: %v, want [%s]", got, want)
		}
	}

	// A waiter whose own key goes while it waits fails once it is its
	// turn, and does not hold the lock.
	e := grant(t, leases)
	lockE := start(func() (*v3lockpb.LockResponse, error) {
		return lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("m"), Lease: e})
	})
	lockE.waiting(t, "lock e")
	if _, err := leases.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: e}); err != nil {
		t.Fatal(err)
	}
	if _, err := lk.Unlock(ctx, &v3lockpb.UnlockRequest{Key: gotC.Key}); err != nil {
		t.Fatal(err)
	}
	if _, err := lockE.wait(t, "lock e"); status.Code(err) != codes.Unknown || status.Convert(err).Message() != "mutex: session is expired" {
		t.Fatalf("lock e after its lease was revoked: %v", err)
	}
}

// TestLockErrors checks etcd v3.7.2's answers to a Lock without a lease,
// with a lease that does not exist, and an Unlock without a key: etcd's
// own errors come with code Unknown, as its in-process client returns
// them.
func TestLockErrors(t *testing.T) {
	conn := node2(t)
	lk, leases := v3lockpb.NewLockClient(conn), pb.NewLeaseClient(conn)
	ctx := context.Background()

	// Lease 0 gets a new lease of 60 s, which nothing keeps alive.
	held, err := lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("n")})
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if _, err := fmt.Sscanf(string(held.Key), "n/%x", &id); err != nil {
		t.Fatalf("lock key %q: %v", held.Key, err)
	}
	ttl, err := leases.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	if err != nil || ttl.GrantedTTL != 60 || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != string(held.Key) {
		t.Fatalf("lease of the lock = %v, %v", ttl, err)
	}

	_, err = lk.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("n"), Lease: 12345})
	if status.Code(err) != codes.Unknown || status.Convert(err).Message() != "etcdserver: requested lease not found" {
		t.Fatalf("lock with a missing lease: %v", err)
	}
	_, err = lk.Unlock(ctx, &v3lockpb.UnlockRequest{})
	if status.Code(err) != codes.Unknown || status.Convert(err).Message() != "etcdserver: key is not provided" {
		t.Fatalf("unlock without a key: %v", err)
	}
	// Unlock deletes any key, held or not.
	un, err := lk.Unlock(ctx, &v3lockpb.UnlockRequest{Key: []byte("not-a-lock")})
	if err != nil || un.Header.Revision == 0 {
		t.Fatalf("unlock of another key = %v, %v", un, err)
	}
}
