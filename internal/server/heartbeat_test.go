package server_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/peer"
)

// heartbeatLate is how far past its tick a leader's heartbeat may come,
// in the normal build, whatever its node is doing (#647): an election
// timeout is 10 ticks.
const heartbeatLate = 50 * time.Millisecond

// TestHeartbeatsUnderOverload offers the leader far more puts than one
// turn of its loop takes in a tick: 3 connections with 2,048 calls in
// flight each, for 3 s. Its heartbeats still go out every tick, at most
// heartbeatLate past it, so its followers keep it as leader (#647: a
// leader overloaded so had held them 1.5 s and lost its leadership).
func TestHeartbeatsUnderOverload(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("bounds the leader's heartbeats in the normal build under overload")
	}
	beats := &heartbeatTimes{at: map[[2]uint64][]time.Time{}}
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		c.wrap = func(id uint64, t peer.Transport) peer.Transport {
			return beatClock{Transport: t, from: id, times: beats}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 93)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	leader := c.nodes[1].Leader()
	value := make([]byte, 1024)
	const conns, inFlight, window = 3, 2048, 3 * time.Second
	start := time.Now()
	stop := start.Add(window)
	var wg sync.WaitGroup
	var puts, failed atomic.Int64
	for i := range conns {
		kv := c.client(leader, cert)
		for j := range inFlight {
			key := fmt.Appendf(nil, "load/%d/%d", i, j)
			wg.Go(func() {
				for time.Now().Before(stop) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
					cancel()
					if err != nil {
						failed.Add(1)
						return
					}
					puts.Add(1)
				}
			})
		}
	}
	wg.Wait()
	n, worst := beats.gaps(leader, start, stop)
	t.Logf("%d puts in %v with %d calls in flight; %d heartbeats of leader %d, the longest gap %v", puts.Load(), window, conns*inFlight, n, leader, worst)
	if failed.Load() > 0 {
		t.Errorf("%d puts failed under overload", failed.Load())
	}
	if l := c.nodes[1].Leader(); l != leader {
		t.Fatalf("the leader changed under overload, from %d to %d", leader, l)
	}
	if n < int(window/c.tick) {
		t.Fatalf("leader %d sent %d heartbeats in %v", leader, n, window)
	}
	if late := worst - c.tick; late > heartbeatLate {
		t.Fatalf("overload held leader %d's heartbeats %v past their tick", leader, late)
	}
}

// heartbeatTimes are when each node sent each peer a heartbeat.
type heartbeatTimes struct {
	mu sync.Mutex
	at map[[2]uint64][]time.Time
}

// gaps returns how many heartbeats node from sent between start and end,
// and the longest time between two to the same peer.
func (h *heartbeatTimes) gaps(from uint64, start, end time.Time) (int, time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n, worst := 0, time.Duration(0)
	for k, times := range h.at {
		if k[0] != from {
			continue
		}
		var last time.Time
		for _, at := range times {
			if at.Before(start) || at.After(end) {
				continue
			}
			if n++; !last.IsZero() {
				worst = max(worst, at.Sub(last))
			}
			last = at
		}
	}
	return n, worst
}

// beatClock notes when its node sends a heartbeat, on the node's loop.
type beatClock struct {
	peer.Transport
	from  uint64
	times *heartbeatTimes
}

func (b beatClock) Send(m *raftpb.Message) {
	if m.GetType() == raftpb.MsgHeartbeat {
		b.times.mu.Lock()
		k := [2]uint64{b.from, m.GetTo()}
		b.times.at[k] = append(b.times.at[k], time.Now())
		b.times.mu.Unlock()
	}
	b.Transport.Send(m)
}

// TestHeartbeatsUnderReadOverload offers the leader more reads than one
// turn of its loop takes in a tick: ranges of 100 keys of 1 KiB, 512 in
// flight on each of 3 connections, for 3 s, as a control plane's lists
// pile up. Its heartbeats still go out every tick, at most heartbeatLate
// past it (#647).
func TestHeartbeatsUnderReadOverload(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("bounds the leader's heartbeats in the normal build under overload")
	}
	beats := &heartbeatTimes{at: map[[2]uint64][]time.Time{}}
	c := newClusterWith(t, func(c *cluster) {
		c.tick = 100 * time.Millisecond
		c.wrap = func(id uint64, t peer.Transport) peer.Transport {
			return beatClock{Transport: t, from: id, times: beats}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	leader := c.nodes[1].Leader()
	kv := c.client(leader, cert)
	value := make([]byte, 1024)
	for i := range 1000 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "list/%04d", i), Value: value})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	const conns, inFlight, window = 3, 512, 3 * time.Second
	start := time.Now()
	stop := start.Add(window)
	var wg sync.WaitGroup
	var reads, failed atomic.Int64
	for range conns {
		kv := c.client(leader, cert)
		for j := range inFlight {
			from := fmt.Appendf(nil, "list/%04d", j%900)
			wg.Go(func() {
				for time.Now().Before(stop) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					_, err := kv.Range(ctx, &pb.RangeRequest{Key: from, RangeEnd: []byte("list0"), Limit: 100})
					cancel()
					if err != nil {
						failed.Add(1)
						return
					}
					reads.Add(1)
				}
			})
		}
	}
	wg.Wait()
	n, worst := beats.gaps(leader, start, stop)
	t.Logf("%d ranges of 100 keys in %v with %d in flight; %d heartbeats of leader %d, the longest gap %v", reads.Load(), window, conns*inFlight, n, leader, worst)
	if failed.Load() > 0 {
		t.Errorf("%d ranges failed under overload", failed.Load())
	}
	if l := c.nodes[1].Leader(); l != leader {
		t.Fatalf("the leader changed under overload, from %d to %d", leader, l)
	}
	if n < int(window/c.tick) {
		t.Fatalf("leader %d sent %d heartbeats in %v", leader, n, window)
	}
	if late := worst - c.tick; late > heartbeatLate {
		t.Fatalf("overload held leader %d's heartbeats %v past their tick", leader, late)
	}
}
