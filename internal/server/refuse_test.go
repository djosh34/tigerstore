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
// connection the memory budget holds is taken. The refusal answers the
// first call on a connection with Unavailable and its reason, so a call
// that does not wait for a ready connection fails with them at once.
// clientv3's own calls wait for one (WaitForReady): a put fails with
// Unavailable or ends at its deadline, as against a node that is down,
// never later. Its reconnects keep gRPC's backoff instead of a tight loop.
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
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != refusal || time.Since(start) > 5*time.Second {
		t.Fatalf("a call that does not wait for a ready connection failed after %v with %v, want Unavailable with the reason at once", time.Since(start), err)
	}
	// For a fixed while, the client keeps calling, and gRPC keeps
	// reconnecting as its backoff allows.
	const watch = 3 * time.Second
	start = time.Now()
	before := testutil.ToFloat64(s.metrics.set.ClientConnsRefused)
	for time.Since(start) < watch {
		ctx, cancel := context.WithTimeout(context.Background(), watch-time.Since(start))
		_, err = cli.Put(ctx, "k", "v")
		cancel()
		if status.Code(err) != codes.Unavailable && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a put at the limit failed with %v, want Unavailable or its deadline", err)
		}
	}
	refused := testutil.ToFloat64(s.metrics.set.ClientConnsRefused) - before
	t.Logf("in %v of puts: %g connections refused", time.Since(start), refused)
	if time.Since(start) > watch+time.Second {
		t.Fatalf("a put at the limit ended %v after its deadline", time.Since(start)-watch)
	}
	// gRPC's reconnect backoff starts at a second and grows 1.6 times:
	// a client that tried again in a tight loop would have been refused
	// hundreds of times.
	if refused > 12 {
		t.Fatalf("%g connections refused in %v of puts: the client reconnects in a tight loop", refused, watch)
	}
}

// TestRefusalReadsOneFrame refuses a connection that sends a frame header
// claiming a megabyte: the refusal reads no further than HTTP/2's 16 KiB
// frame limit, so it answers at once instead of waiting for the bytes.
func TestRefusalReadsOneFrame(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	go func() {
		_, _ = b.Write([]byte(http2.ClientPreface))
		hdr := frameHeader(nil, 1<<20, http2.FrameHeaders, http2.FlagHeadersEndHeaders, 1)
		_, _ = b.Write(hdr)
	}()
	done := make(chan bool, 1)
	start := time.Now()
	go func() { done <- refuseCall(a, a) }()
	fr := http2.NewFramer(b, b)
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("no GOAWAY: %v", err)
		}
		if _, ok := f.(*http2.GoAwayFrame); ok {
			break
		}
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("the refusal answered after %v: it waited for the frame's megabyte", took)
	}
	<-done
}

// TestStopEndsRefusals stops a server while a refused client that sends
// nothing waits for its handshake: Stop returns at once, not after the
// handshake timeout.
func TestStopEndsRefusals(t *testing.T) {
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
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close() //nolint:errcheck // closing a test connection
		}
	}()
	// The first connection speaks HTTP/2 and holds the one place; the
	// second, refused, sends nothing.
	for i := range 2 {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		if i > 0 {
			break
		}
		if _, err := c.Write([]byte(http2.ClientPreface)); err != nil {
			t.Fatal(err)
		}
		if err := http2.NewFramer(c, c).WriteSettings(); err != nil {
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
	}
	for testutil.ToFloat64(s.metrics.set.ClientConnsRefused) == 0 {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop() }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for a refused connection's handshake")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("Stop took %v", took)
	}
}
