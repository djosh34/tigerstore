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
// to the leader for a second: no turn of a node's loop takes more than
// node.MaxTurnCalls calls, however many whole frames one read of the
// socket brought, and the load fills turns to near that (#647).
func TestTurnTakesBoundedCalls(t *testing.T) {
	var peak atomic.Int64
	c := newClusterWith(t, func(c *cluster) {
		c.nodeConfig = func(nc *node.Config) {
			nc.Took = func(calls int) {
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
	p := peak.Load()
	t.Logf("the most calls a turn took: %d", p)
	if p > node.MaxTurnCalls {
		t.Fatalf("a turn took %d calls, more than %d", p, node.MaxTurnCalls)
	}
	if p < node.MaxTurnCalls/2 {
		t.Fatalf("the load filled no turn: at most %d calls, against a bound of %d", p, node.MaxTurnCalls)
	}
}
