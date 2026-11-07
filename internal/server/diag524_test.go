package server_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestDiag524Preload isolates the mixed-k8s preload: 300 ~60 KiB Txns/s,
// requests spread across all three nodes. It overwrites three keys, so the
// index stays small. Run on an isolated Actions host with netem on 10.
func TestDiag524Preload(t *testing.T) {
	if os.Getenv("DIAG524_NET") == "" {
		t.Skip("needs an isolated host with netem on the bench peer ports")
	}
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		for id, ln := range c.peerLns {
			if err := ln.Close(); err != nil {
				t.Fatal(err)
			}
			ln = listenPeer(t, fmt.Sprintf("127.0.0.1:%d", 20000+10*id+1))
			c.peerLns[id], c.peerAddrs[id] = ln, ln.Addr().String()
		}
	})
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	var clients []pb.KVClient
	for id := uint64(1); id <= 3; id++ {
		kv := c.client(id, cert)
		retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte("ready"), Value: []byte("1")})
		})
		clients = append(clients, kv)
	}
	value := make([]byte, 60<<10)
	jobs := []chan int{make(chan int, 2000), make(chan int, 2000), make(chan int, 2000)}
	ctx, cancelAll := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelAll()
	var sent, completed atomic.Int64
	var workers sync.WaitGroup
	var failed atomic.Int64
	var maxLatency atomic.Int64
	start := time.Now()
	// Each endpoint has its own 96 slots, as in the bench. A shared
	// worker pool would let one slow follower throttle the other clients
	// and hide the lag.
	for worker := 0; worker < 288; worker++ {
		endpoint := worker % 3
		workers.Go(func() {
			for i := range jobs[endpoint] {
				if ctx.Err() != nil {
					continue
				}
				at := time.Now()
				_, err := clients[endpoint].Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(fmt.Sprintf("key%d", i%3)), Value: value}}}}})
				d := time.Since(at).Nanoseconds()
				for old := maxLatency.Load(); d > old && !maxLatency.CompareAndSwap(old, d); old = maxLatency.Load() {
				}
				completed.Add(1)
				if err != nil && failed.Add(1) == 1 {
					t.Logf("first request error on node %d after %v: %v", i%3+1, time.Duration(d), err)
					cancelAll()
				}
			}
		})
	}
	// Fixed arrivals, not a closed loop. Each run offers the same work.
	for i := 0; i < 6000; i++ {
		time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second / 300)))
		if ctx.Err() != nil {
			break
		}
		jobs[i%3] <- i
		sent.Add(1)
	}
	for _, q := range jobs {
		close(q)
	}
	workers.Wait()
	t.Logf("300 Txns/s, 60 KiB each: sent=%d completed=%d elapsed=%v errors=%d max=%v", sent.Load(), completed.Load(), time.Since(start), failed.Load(), time.Duration(maxLatency.Load()))
	if failed.Load() > 0 {
		t.Fatalf("%d requests failed", failed.Load())
	}
}
