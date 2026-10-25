package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"runtime/debug"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
)

// TestLargestRequest writes the largest request --max-request-bytes may
// allow, core.MaxRequestBytesCeiling, through a follower, so that it
// crosses the peers twice, and reads it back from every node, before and
// after they all restart from their logs. One byte more is refused.
func TestLargestRequest(t *testing.T) {
	const limit = core.MaxRequestBytesCeiling
	// Three nodes in one process hold several copies of the value each;
	// the garbage collector keeps them from adding up, which under the
	// race detector would take several GiB.
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(512 << 20))
	c := newClusterWith(t, func(c *cluster) {
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
	if _, err := kv(follower).Put(ctx, &pb.PutRequest{Key: key, Value: value}); err != nil {
		t.Fatalf("a put of %d bytes through node %d: %v", limit, follower, err)
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
