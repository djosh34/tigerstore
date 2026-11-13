package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestLargestRequest writes the largest request --max-request-bytes may
// allow, core.MaxRequestBytesCeiling, through a follower, so that it
// crosses the peers twice, and reads it back from every node, before and
// after they all restart from their logs. One byte more is refused.
func TestLargestRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 64 MiB request through three nodes")
	}
	const limit = core.MaxRequestBytesCeiling
	// Three nodes in one process hold several copies of the value each;
	// the garbage collector keeps them from adding up, which under the
	// race detector would take several GiB.
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(512 << 20))
	beats := &heartbeatTimes{at: map[[2]uint64][]time.Time{}}
	c := newClusterWith(t, func(c *cluster) {
		c.wrap = func(id uint64, t peer.Transport) peer.Transport {
			return beatClock{Transport: t, from: id, times: beats}
		}
		// A real node's ticks: under the race detector the leader takes
		// half a second to append the entry, five election timeouts at
		// the tests' 10 ms ticks.
		c.tick = 100 * time.Millisecond
		c.coreConfig = func(cfg *core.Config) {
			cfg.MaxRequestBytes = limit
			// Under the race detector, copying, encrypting and syncing
			// 64 MiB takes seconds at each step, longer than the peers'
			// 5 s write timeout for one frame.
			cfg.RequestTicks = int(time.Minute / c.tick)
			if simrun.Race {
				// The loop encodes and decodes the 64 MiB message on its own
				// thread, which under the race detector holds it 0.8 to 1.4 s
				// (20 ms without): longer than the 1 s election timeout, so
				// a follower campaigned, and the proposal was lost with the
				// leader change. 5 s outlasts it.
				cfg.ElectionTicks = 50
			}
		}
		c.serverConfig = func(cfg *server.Config) { cfg.MaxRequestBytes = limit }
		c.peerConfig = func(cfg *peer.Config) { cfg.WriteTimeout, cfg.ReadTimeout = time.Minute, time.Minute }
	})
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	kv := func(id uint64) pb.KVClient {
		conn, err := grpc.NewClient(c.running[id].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
		})), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(2*limit)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() }) //nolint:errcheck // closing a test client
		return pb.NewKVClient(conn)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	follower := c.nodes[1].Leader()%3 + 1
	// The size etcd limits: the InternalRaftRequest with the longest
	// request ID and the client certificate's name.
	key := []byte("big")
	size := func(n int) int {
		return proto.Size(&pb.InternalRaftRequest{
			Header: &pb.RequestHeader{ID: 1 << 63, Username: "client", AuthRevision: 1},
			Put:    &pb.PutRequest{Key: key, Value: make([]byte, n)},
		})
	}
	n := limit - 64
	for size(n+1) <= limit {
		n++
	}
	if size(n) != limit {
		t.Fatalf("no value makes a request of %d bytes", limit)
	}
	value := bytes.Repeat([]byte("0123456789abcdef"), n/16+1)[:n]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := kv(follower).Put(ctx, &pb.PutRequest{Key: key, Value: append(value, 'x')}); !errors.Is(err, rpctypes.ErrGRPCRequestTooLarge) {
		t.Fatalf("a put of %d bytes: %v", limit+1, err)
	}
	leader, start := c.nodes[1].Leader(), time.Now()
	if _, err := kv(follower).Put(ctx, &pb.PutRequest{Key: key, Value: value}); err != nil {
		t.Fatalf("a put of %d bytes through node %d: %v", limit, follower, err)
	}
	if !simrun.Race {
		// The leader's loop encodes and decodes the 64 MiB messages, and
		// sends heartbeats between its turns: a turn that holds it delays
		// them, and a run of large writes would add those holds up. Under
		// the race detector the hold is 0.8 to 1.4 s, so only the normal
		// build bounds it.
		gaps, worst := beats.gaps(leader, start, time.Now())
		t.Logf("%d heartbeats of leader %d during the put, the longest gap %v", gaps, leader, worst)
		if gaps < 3 {
			t.Fatalf("leader %d sent %d heartbeats during the put", leader, gaps)
		}
		if late := worst - c.tick; late > 50*time.Millisecond {
			t.Fatalf("the put held leader %d's heartbeats %v past their tick", leader, late)
		}
	}
	check := func(when string) {
		t.Helper()
		for id := uint64(1); id <= 3; id++ {
			r := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
				return kv(id).Range(ctx, &pb.RangeRequest{Key: key})
			})
			if len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, value) {
				t.Fatalf("%s, node %d reads %d keys", when, id, len(r.Kvs))
			}
		}
	}
	check("after the put")
	for id := uint64(1); id <= 3; id++ {
		c.running[id].stop()
		delete(c.running, id)
	}
	for id := uint64(1); id <= 3; id++ {
		c.start(id)
	}
	check("after the restart")
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
