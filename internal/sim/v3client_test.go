package sim

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestClosingTransportRetries is the spin of #440 made short. A
// clientv3 client's connection breaks: its writer fails at once, but
// its reader sees the break only a second later, as it did when gosim
// ran another goroutine first. A call that starts in that second gets
// no stream from the transport, which the client still counts as ready,
// and gRPC retries it at once. Without the pause of v3Options that loop
// never blocks, so under gosim the reader never runs, the clock never
// moves, and the test runs until its time limit. With the pause the
// clock moves, the reader closes the transport, and the call goes
// through on a new connection.
func TestClosingTransportRetries(t *testing.T) {
	if simrun.Race {
		t.Skip("the spin is gosim's")
	}
	if simrun.Drive(t, 1) {
		return
	}
	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, emptyKV{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	var (
		mu   sync.Mutex
		last *breakingConn
	)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://kv"},
		Logger:    zap.NewNop(),
		DialOptions: v3Options(func(ctx context.Context, _ string) (net.Conn, error) {
			c, err := lis.DialContext(ctx)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			defer mu.Unlock()
			last = &breakingConn{Conn: c}
			return last, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	get := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := cli.Get(ctx, "k")
		return err
	}
	if err := get(); err != nil {
		t.Fatalf("the first call: %v", err)
	}

	mu.Lock()
	last.breakWrites()
	mu.Unlock()
	// The first call's write fails in a flush, whose error the
	// transport's writer does not check, and the second's ends that
	// writer. Both wait for their
	// answers until the reader sees the break, and then clientv3 tries
	// them again.
	waiting := make(chan error, 2)
	for range 2 {
		go func() { waiting <- get() }()
		time.Sleep(10 * time.Millisecond)
	}
	// This call starts after the writer ended and before the reader saw
	// the break.
	if err := get(); err != nil {
		t.Errorf("a call on the closing transport: %v", err)
	}
	for range 2 {
		if err := <-waiting; err != nil {
			t.Errorf("a call that broke the transport: %v", err)
		}
	}
}

// emptyKV answers each range with no keys.
type emptyKV struct{ pb.UnimplementedKVServer }

func (emptyKV) Range(context.Context, *pb.RangeRequest) (*pb.RangeResponse, error) {
	return &pb.RangeResponse{Header: &pb.ResponseHeader{}}, nil
}

// breakingConn is a connection whose writes fail once it broke, and
// whose reads end a second after that.
type breakingConn struct {
	net.Conn
	mu     sync.Mutex
	broken bool
	once   sync.Once
}

var errBroken = errors.New("the connection broke")

func (c *breakingConn) breakWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.broken = true
}

func (c *breakingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	broken := c.broken
	c.mu.Unlock()
	if broken {
		c.closeLater()
		return 0, errBroken
	}
	return c.Conn.Write(b)
}

func (c *breakingConn) Close() error {
	c.closeLater()
	return nil
}

// closeLater closes the connection, which ends its reads, a second from
// now.
func (c *breakingConn) closeLater() {
	c.once.Do(func() { time.AfterFunc(time.Second, func() { _ = c.Conn.Close() }) })
}
