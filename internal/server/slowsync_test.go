package server_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/node"
)

// TestSlowSyncsKeepTheLeader is #180's case. Under -race on a shared
// runner one sync of the log took up to 600 ms, past the 200 ms that the
// disk watchdog allowed at the test cluster's 10 ms ticks, and a leader
// stopped in the middle of a test. With every sync taking 400 ms, writes
// must still succeed, and no node may see a change of leader.
func TestSlowSyncsKeepTheLeader(t *testing.T) {
	var slow atomic.Bool
	c := newClusterWith(t, func(c *cluster) {
		c.nodeConfig = func(nc *node.Config) {
			nc.BeforeSync = func() {
				if slow.Load() {
					time.Sleep(400 * time.Millisecond)
				}
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 93)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	changes := func() int64 {
		var sum int64
		for id := uint64(1); id <= 3; id++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			m, err := c.nodes[id].Metrics(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			sum += m.LeaderChanges
		}
		return sum
	}
	before := changes()
	slow.Store(true)
	for i := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v")})
		cancel()
		if err != nil {
			t.Fatalf("put %d with slow syncs: %v", i, err)
		}
	}
	slow.Store(false)
	if after := changes(); after != before {
		t.Fatalf("the nodes saw %d changes of leader during slow syncs", after-before)
	}
}

// requestTimeout is etcd's request timeout at its default election
// timeout of 1 s: 5 s plus two election timeouts.
const requestTimeout = 7 * time.Second

// TestHeldFollowerSyncsKeepTheLeader is #88's case at a real node's
// 100 ms ticks. Both followers' syncs are held for 4 s, so the leader's
// entry waits past three election timeouts to commit, which the commit
// watchdog took for a dead cluster and stopped the leader. As in etcd
// (#233), the leader only warns: the put returns within etcd's 7 s
// request timeout, no node stops, and every node then reads the value.
func TestHeldFollowerSyncsKeepTheLeader(t *testing.T) {
	var held [4]atomic.Int64 // unix nanoseconds to hold syncs until, by ID
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		c.nodeConfig = func(nc *node.Config) {
			id := nc.Core.ID
			nc.BeforeSync = func() {
				if d := time.Until(time.Unix(0, held[id].Load())); d > 0 {
					time.Sleep(d)
				}
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	lead := uint64(0)
	for id := uint64(1); id <= 3; id++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := c.nodes[id].Metrics(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if m.IsLeader {
			lead = id
		}
	}
	if lead == 0 {
		t.Fatal("no leader")
	}
	until := time.Now().Add(4 * time.Second).UnixNano()
	for id := uint64(1); id <= 3; id++ {
		if id != lead {
			held[id].Store(until)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	start := time.Now()
	_, err = c.client(lead, cert).Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("held")})
	cancel()
	if err != nil {
		t.Fatalf("put with the followers' syncs held, after %v: %v", time.Since(start), err)
	}
	t.Logf("the put took %v", time.Since(start))
	for id := uint64(1); id <= 3; id++ {
		got := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
			return c.client(id, cert).Range(ctx, &pb.RangeRequest{Key: []byte("k")})
		})
		if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "held" {
			t.Fatalf("node %d reads %v, want the acknowledged value", id, got.Kvs)
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	m, err := c.nodes[lead].Metrics(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if m.SlowCommits == 0 {
		t.Fatalf("the leader never warned of a slow commit; its longest commit wait was %d ticks", m.MaxCommitWait)
	}
}
