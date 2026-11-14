package server_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/node"
)

// TestTurnTakesBoundedCalls keeps 1,500 puts in flight on one connection
// to the leader for a second: no turn of a node's loop takes more calls
// than fit in it (Node.TurnRoom), however many whole frames one read of
// the socket brought, and every turn has a limit that bounds it: the
// calls that fit before the node's next tick, or in 15 ms if the tick is
// sooner, or a fixed few before the node measured what a call costs
// (#647).
func TestTurnTakesBoundedCalls(t *testing.T) {
	var over, unbounded, turns, cut atomic.Int64
	var peak atomic.Int64
	c := newClusterWith(t, func(c *cluster) {
		c.nodeConfig = func(nc *node.Config) {
			nc.Took = func(calls, limit int, _ bool) {
				turns.Add(1)
				if calls > limit {
					over.Add(1)
				}
				if calls == limit && limit < 1500 {
					cut.Add(1)
				}
				if limit > 1<<20 {
					unbounded.Add(1)
				}
				for {
					p := peak.Load()
					if int64(calls) <= p || peak.CompareAndSwap(p, int64(calls)) {
						return
					}
				}
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	kv := c.client(c.nodes[1].Leader(), cert)
	stop := time.Now().Add(time.Second)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := range 1500 {
		key := fmt.Appendf(nil, "turn/%d", i)
		wg.Go(func() {
			for time.Now().Before(stop) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: []byte("v")})
				cancel()
				if err != nil {
					failed.Add(1)
					return
				}
			}
		})
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d puts failed", failed.Load())
	}
	t.Logf("%d turns took calls, the most %d; %d were cut at their limit", turns.Load(), peak.Load(), cut.Load())
	if n := over.Load(); n > 0 {
		t.Fatalf("%d turns took more calls than fit in them", n)
	}
	if n := unbounded.Load(); n > 0 {
		t.Fatalf("%d turns had no limit to their calls", n)
	}
	if turns.Load() == 0 {
		t.Fatal("no turn took a call")
	}
}
