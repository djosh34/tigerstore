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
