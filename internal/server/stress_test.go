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

// TestLargeValuesAtRealTicks writes values near etcd's request limit from
// two clients on every node, with a real node's 100 ms ticks, for as long
// as TIGERSTORE_STRESS says (for example 10s). Each put has etcd's 7 s
// request timeout (#233). It fails if a put fails, if a node stops, or if
// a node then reads another value than the last one each client put: no
// stop and no lost write. The latencies are measurements, not limits: the
// test logs the longest put, the longest commit wait and the longest log
// write per node, and the nodes log slow log writes, peer messages that
// waited in their queue, and leader changes (#88).
//
// It is skipped unless TIGERSTORE_STRESS is set; CI runs it as its own
// job.
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
	// longest[id-1] is the longest put on node id, in nanoseconds, and
	// last the value each client put last.
	var longest [3]atomic.Int64
	last := map[string][]byte{}
	var mu sync.Mutex
	for id := uint64(1); id <= 3; id++ {
		kv := c.client(id, cert)
		for n := range 2 {
			key := fmt.Sprintf("big/%d/%d", id, n)
			wg.Go(func() {
				for seq := 0; time.Now().Before(stop); seq++ {
					v := append(fmt.Appendf(nil, "%d/", seq), value...)
					ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
					start := time.Now()
					_, err := kv.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: v})
					cancel()
					if err != nil {
						end := time.Now()
						t.Errorf("put on node %d after %v: %v; key=%q start=%s end=%s", id, end.Sub(start), err,
							key, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
						return
					}
					puts.Add(1)
					mu.Lock()
					last[key] = v
					mu.Unlock()
					l, d := &longest[id-1], int64(time.Since(start))
					for cur := l.Load(); d > cur && !l.CompareAndSwap(cur, d); cur = l.Load() {
					}
				}
			})
		}
	}
	wg.Wait()
	t.Logf("%d puts of %d bytes in %v", puts.Load(), len(value), duration)
	for id := uint64(1); id <= 3; id++ {
		for key, v := range last {
			got := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
				return c.client(id, cert).Range(ctx, &pb.RangeRequest{Key: []byte(key)})
			})
			if len(got.Kvs) != 1 || !bytes.Equal(got.Kvs[0].Value, v) {
				t.Errorf("node %d does not read the last value put to %q", id, key)
			}
		}
	}
	// The margins under the limits (#88): the longest put, the longest a
	// leader's append waited to commit, and the longest write and sync of
	// the log, per node. The commit wait counts ticks from the tick of the
	// append to the tick that sees its commit, so N ticks is a wait of
	// more than N-2 and at most N ticks.
	for id := uint64(1); id <= 3; id++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := c.nodes[id].Metrics(ctx)
		cancel()
		if err != nil {
			t.Errorf("node %d: longest put %v, no metrics: %v", id, time.Duration(longest[id-1].Load()), err)
			continue
		}
		t.Logf("node %d: longest put %v, longest commit wait %d ticks of %v, longest log write %v",
			id, time.Duration(longest[id-1].Load()), m.MaxCommitWait, c.tick, time.Duration(m.MaxLogWriteNanos))
	}
}
