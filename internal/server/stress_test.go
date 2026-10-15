package server_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// maxPut bounds one put in TestLargeValuesAtRealTicks: one election
// timeout at 100 ms ticks.
const maxPut = time.Second

// TestLargeValuesAtRealTicks writes values near etcd's request limit from
// two clients on every node, with a real node's 100 ms ticks, for as long
// as TIGERSTORE_STRESS says (for example 10s). It fails if a put takes
// longer than one election timeout (1 s), and a leader whose entry waits
// that long to commit stops itself, so the test also fails with
// core.ErrCommitStalled when the cluster stops. The tick watchdog alone
// would miss a committer that stalls, since it then counts no ticks; the
// put deadline is on the wall clock. The nodes log slow
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
		// Nothing compacts the overwritten values, which pass etcd's
		// 2 GiB quota within the run on a fast machine.
		c.coreConfig = func(cfg *core.Config) { cfg.QuotaBytes = -1 }
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
	var puts atomic.Int64
	for id := uint64(1); id <= 3; id++ {
		kv := c.client(id, cert)
		for n := range 2 {
			wg.Go(func() {
				key := fmt.Appendf(nil, "big/%d/%d", id, n)
				for time.Now().Before(stop) {
					// A put waits for its entry to commit and apply; a
					// commit within one election timeout leaves it well
					// under a second.
					ctx, cancel := context.WithTimeout(context.Background(), maxPut)
					start := time.Now()
					_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
					cancel()
					if err != nil {
						t.Errorf("put on node %d after %v: %v", id, time.Since(start), err)
						return
					}
					puts.Add(1)
				}
			})
		}
	}
	wg.Wait()
	t.Logf("%d puts of %d bytes in %v", puts.Load(), len(value), duration)
}
