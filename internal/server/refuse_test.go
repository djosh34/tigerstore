package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestClientAtTheLimit runs etcd's Go client against a server whose one
// connection the memory budget holds is taken. A call that does not wait
// for a ready connection fails with Unavailable at once. clientv3's own
// calls wait for one (WaitForReady), so a put ends at its deadline, as
// against a node that is down, never later. Its reconnects keep gRPC's
// backoff instead of a tight loop.
func TestClientAtTheLimit(t *testing.T) {
	s := bareServer(t)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.conns, s.gone = map[closer]struct{}{}, newConnMetrics()
	s.maxConns = 1
	go func() { _ = s.loop.Run(t.Context()) }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.ServeListener(Listener{Listener: ln, Traffic: TrafficAll}) }()
	defer s.Stop() //nolint:errcheck // stopping a test server
	// The one connection the limit holds speaks HTTP/2 and stays.
	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close() //nolint:errcheck // closing a test connection
	if _, err := first.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	if err := http2.NewFramer(first, first).WriteSettings(); err != nil {
		t.Fatal(err)
	}
	for {
		s.mu.Lock()
		open := s.open
		s.mu.Unlock()
		if open == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ln.Addr().String()}, DialTimeout: time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close() //nolint:errcheck // closing a test client
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = pb.NewKVClient(cli.ActiveConnection()).Range(ctx, &pb.RangeRequest{Key: []byte("k")}, grpc.WaitForReady(false))
	if status.Code(err) != codes.Unavailable || time.Since(start) > 5*time.Second {
		t.Fatalf("a call that does not wait for a ready connection failed after %v with %v, want Unavailable at once", time.Since(start), err)
	}
	const timeout = 3 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start = time.Now()
	_, err = cli.Put(ctx, "k", "v")
	took := time.Since(start)
	if status.Code(err) != codes.Unavailable && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a put at the limit failed with %v, want Unavailable or its deadline", err)
	}
	if took > timeout+time.Second {
		t.Fatalf("a put at the limit took %v, over its %v timeout", took, timeout)
	}
	refused := testutil.ToFloat64(s.metrics.set.ClientConnsRefused)
	t.Logf("put failed after %v with %v; %g connections refused", took, err, refused)
	// gRPC's reconnect backoff starts at a second: a client that tried
	// again in a tight loop would have been refused hundreds of times.
	if refused == 0 || refused > 12 {
		t.Fatalf("%g connections refused in %v: want a few, as gRPC backs off", refused, time.Since(start))
	}
}
