package server_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

// TestDiag88SlowDisk holds the three sync goroutines, not the committer,
// through the stress test's ten-tick watchdog. No durable quorum exists
// until release. The client uses etcd's seven-second request allowance.
func TestDiag88SlowDisk(t *testing.T) {
	var armed atomic.Bool
	release := make(chan struct{})
	var once sync.Once
	unhold := func() { once.Do(func() { close(release) }) }
	var faults atomic.Int64
	var failed atomic.Int64
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		c.stallElections = 1
		c.nodeConfig = func(cfg *node.Config) {
			id := cfg.Core.ID
			cfg.BeforeSync = func() {
				if armed.Load() {
					faults.Add(1)
					<-release
				}
			}
			cfg.Turn = func(n *core.Node, eff core.Effects, err error) {
				if err != nil {
					failed.Add(1)
					t.Logf("node %d failed while sync held: %v; status=%+v metrics=%+v", id, err, n.RaftStatus(), n.Metrics())
				}
			}
		}
	})
	// Release before cluster cleanup even when an assertion fails.
	defer unhold()
	cert, _, _, err := c.ca.Issue("client", 988)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("ready"), Value: []byte("v")})
	})
	armed.Store(true)
	timer := time.AfterFunc(1400*time.Millisecond, unhold)
	defer timer.Stop()
	value := bytes.Repeat([]byte("0123456789abcdef"), (core.MaxRequestBytes-1024)/16)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	start := time.Now()
	_, putErr := c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("diag88"), Value: value})
	unhold()
	t.Logf("put=%v elapsed=%v held syncs=%d core failures=%d", putErr, time.Since(start), faults.Load(), failed.Load())
	if faults.Load() < 3 {
		t.Fatalf("wanted all three disks held, got %d sync calls", faults.Load())
	}
	if putErr != nil || failed.Load() != 0 {
		t.Fatalf("slow disk must not stop the leader: put=%v core failures=%d", putErr, failed.Load())
	}
	for id := uint64(1); id <= 3; id++ {
		select {
		case <-c.nodes[id].Done():
			t.Fatalf("node %d stopped", id)
		default:
		}
		resp, err := c.client(id, cert).Range(ctx, &pb.RangeRequest{Key: []byte("diag88")})
		if err != nil || len(resp.GetKvs()) != 1 || !bytes.Equal(resp.Kvs[0].Value, value) {
			t.Fatalf("node %d lost acknowledged write: %v", id, err)
		}
		m, err := c.nodes[id].Metrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("node %d: commit wait=%d ticks of %v; log write=%v", id, m.MaxCommitWait, c.tick, time.Duration(m.MaxLogWriteNanos))
	}
}
