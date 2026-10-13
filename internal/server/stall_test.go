package server_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestLargeValuesAtRealTicks writes values near etcd's request limit from
// clients on every node, with a real node's 100 ms ticks, for as long as
// TIGERSTORE_STRESS says (for example 10s). A leader whose entry waits
// one election timeout (1 s) to commit stops itself, and the test fails
// with core.ErrCommitStalled when the cluster stops. The nodes log slow
// log writes, peer messages that waited in their queue, and leader
// changes, so a failure's log shows where the time went (#88).
//
// It is skipped unless TIGERSTORE_STRESS is set: there are no snapshots
// yet, so every value stays in each node's memory, and CI runs it as its
// own job.
func TestLargeValuesAtRealTicks(t *testing.T) {
	env := os.Getenv("TIGERSTORE_STRESS")
	if env == "" {
		t.Skip("set TIGERSTORE_STRESS to a duration to run")
	}
	duration, err := time.ParseDuration(env)
	if err != nil {
		t.Fatal(err)
	}
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		c.stallElections = 1
	})
	cert, _, _, err := c.ca.Issue("client", 92)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("0123456789abcdef"), (core.MaxRequestBytes-1024)/16)
	// Wait for a leader before the clock starts.
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	stop := time.Now().Add(duration)
	var wg sync.WaitGroup
	var mu sync.Mutex
	puts, failed := 0, 0
	for id := uint64(1); id <= 3; id++ {
		kv := c.client(id, cert)
		wg.Go(func() {
			key := fmt.Appendf(nil, "big/%d", id)
			for time.Now().Before(stop) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
				cancel()
				mu.Lock()
				if err != nil {
					failed++
					if failed <= 5 {
						t.Logf("put on node %d: %v", id, err)
					}
				} else {
					puts++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	t.Logf("%d puts of %d bytes in %v, %d failed", puts, len(value), duration, failed)
	if failed > 0 {
		t.Errorf("%d puts failed", failed)
	}
}
