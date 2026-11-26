package sim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
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

func dialTestConn(t *testing.T, dial func(context.Context, string) (net.Conn, error), extra ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts := append(v3Options(dial), grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, extra...)
	conn, err := grpc.NewClient("passthrough:///kv", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Exercise the real picker and dial error with both FailFast settings.
// Wait-for-ready's deadline is deliberately not reclassified.
func TestDialFailureBeforeStream(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		for _, ready := range []bool{false, true} {
			t.Run(fmt.Sprintf("clientv3=%v/ready=%v", v3, ready), func(t *testing.T) {
				simrun.Bubble(t, func(t *testing.T) {
					dial := func(context.Context, string) (net.Conn, error) { return nil, errConnRefused }
					var conn *grpc.ClientConn
					if v3 {
						cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://kv"}, Logger: zap.NewNop(), DialOptions: v3Options(dial)})
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = cli.Close() }()
						conn = cli.ActiveConnection()
					} else {
						conn = dialTestConn(t, dial)
					}
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					ctx, transport := observeCallTransport(ctx)
					_, original := pb.NewKVClient(conn).DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("k")}, grpc.WaitForReady(ready))
					err := transport.finish(original)
					if !transport.observed.Load() || transport.header.Load() {
						t.Fatalf("transport observation: begin %v, header %v", transport.observed.Load(), transport.header.Load())
					}
					wantCode := codes.Unavailable
					if ready {
						wantCode = codes.DeadlineExceeded
					}
					if status.Code(err) != wantCode || ambiguous(err) != ready {
						t.Fatalf("error %v, ambiguous %v; want code %v, ambiguous %v", err, ambiguous(err), wantCode, ready)
					}
					if !ready && status.Convert(original).Message() != refusedDialMessage {
						t.Fatalf("unexpected grpc dial description: %v", original)
					}
					if err.Error() != original.Error() || !errors.Is(err, original) {
						t.Fatalf("original error changed: %v -> %v", original, err)
					}
					if got, want := historyError(err, ambiguous(err)), historyError(original, ready); !reflect.DeepEqual(got, want) {
						t.Fatalf("history error %+v, want %+v", got, want)
					}
				})
			})
		}
	}
}

type dialTestKV struct {
	pb.UnimplementedKVServer
	put func(context.Context) error
}

func (s dialTestKV) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	return nil, s.put(ctx)
}

// Even a server returning the exact dial status must remain ambiguous.
func TestDialStatusAfterStream(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(strconv.FormatBool(disconnect), func(t *testing.T) {
			simrun.Bubble(t, func(t *testing.T) {
				lis := bufconn.Listen(1 << 16)
				srv := grpc.NewServer()
				defer srv.Stop()
				var applied atomic.Bool
				pb.RegisterKVServer(srv, dialTestKV{put: func(ctx context.Context) error {
					applied.Store(true)
					if disconnect {
						go srv.Stop()
						<-ctx.Done()
						return ctx.Err()
					}
					return status.Error(codes.Unavailable, refusedDialMessage)
				}})
				go func() { _ = srv.Serve(lis) }()
				conn := dialTestConn(t, func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx, transport := observeCallTransport(ctx)
				_, err := pb.NewKVClient(conn).Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
				err = transport.finish(err)
				if !applied.Load() || !transport.header.Load() || status.Code(err) != codes.Unavailable || !ambiguous(err) {
					t.Fatalf("applied %v, header %v, error %v, ambiguous %v", applied.Load(), transport.header.Load(), err, ambiguous(err))
				}
			})
		})
	}
}

// clientv3's context-error branch precedes mutable-RPC safety: a server
// DeadlineExceeded can be followed by a real refused dial, even for Put.
func TestDialFailureAfterClientV3Retry(t *testing.T) {
	simrun.Bubble(t, func(t *testing.T) {
		lis := bufconn.Listen(1 << 16)
		srv := grpc.NewServer()
		defer srv.Stop()
		var applied, attempts, dials atomic.Int32
		pb.RegisterKVServer(srv, dialTestKV{put: func(context.Context) error {
			applied.Add(1)
			return status.Error(codes.DeadlineExceeded, "server deadline after applying the write")
		}})
		go func() { _ = srv.Serve(lis) }()
		opts := v3Options(func(ctx context.Context, _ string) (net.Conn, error) {
			if dials.Add(1) > 1 {
				return nil, errConnRefused
			}
			return lis.DialContext(ctx)
		})
		opts = append(opts, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if attempts.Add(1) == 2 {
				srv.Stop()
				for state := cc.GetState(); state != connectivity.TransientFailure; state = cc.GetState() {
					cc.Connect()
					if !cc.WaitForStateChange(ctx, state) {
						return ctx.Err()
					}
				}
			}
			return invoke(ctx, method, req, reply, cc, opts...)
		}))
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://kv"}, Logger: zap.NewNop(), DialOptions: opts})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cli.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		ctx, transport := observeCallTransport(ctx)
		_, err = pb.NewKVClient(cli.ActiveConnection()).Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}, grpc.WaitForReady(false))
		err = transport.finish(err)
		if applied.Load() != 1 || attempts.Load() != 2 || status.Convert(err).Message() != refusedDialMessage || !transport.header.Load() || !ambiguous(err) {
			t.Fatalf("applied %d, attempts %d, dials %d, header %v, error %v, ambiguous %v", applied.Load(), attempts.Load(), dials.Load(), transport.header.Load(), err, ambiguous(err))
		}
	})
}

type dialRetryStats struct {
	retryPause
	transparent atomic.Int32
}

func (s *dialRetryStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if b, ok := event.(*stats.Begin); ok && b.IsTransparentRetryAttempt {
		s.transparent.Add(1)
	}
	s.retryPause.HandleRPC(ctx, event)
}

// Actual HTTP/2 refusals and GOAWAY exercise gRPC's transparent retry.
// Even known-unprocessed streams remain outside this narrow shortcut.
func TestDialFailureAfterUnprocessedStream(t *testing.T) {
	for _, goaway := range []bool{false, true} {
		t.Run(strconv.FormatBool(goaway), func(t *testing.T) {
			simrun.Bubble(t, func(t *testing.T) {
				var dials atomic.Int32
				retries := new(dialRetryStats)
				conn := dialTestConn(t, func(context.Context, string) (net.Conn, error) {
					if dials.Add(1) > 1 {
						return nil, errConnRefused
					}
					client, server := net.Pipe()
					go func() {
						defer func() { _ = server.Close() }()
						preface := make([]byte, len(http2.ClientPreface))
						if _, err := io.ReadFull(server, preface); err != nil {
							return
						}
						fr := http2.NewFramer(server, server)
						written := make(chan struct{})
						go func() { _ = fr.WriteSettings(); close(written) }()
						for {
							f, err := fr.ReadFrame()
							if err != nil {
								return
							}
							if h, ok := f.(*http2.HeadersFrame); ok {
								<-written
								if goaway {
									_ = fr.WriteGoAway(0, http2.ErrCodeNo, nil)
								} else {
									_ = fr.WriteRSTStream(h.StreamID, http2.ErrCodeRefusedStream)
								}
								return
							}
						}
					}()
					return client, nil
				}, grpc.WithStatsHandler(retries))
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx, transport := observeCallTransport(ctx)
				_, err := pb.NewKVClient(conn).DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("k")})
				err = transport.finish(err)
				if retries.transparent.Load() == 0 || status.Code(err) != codes.Unavailable || !transport.header.Load() || !ambiguous(err) {
					t.Fatalf("retries %d, dials %d, header %v, error %v, ambiguous %v", retries.transparent.Load(), dials.Load(), transport.header.Load(), err, ambiguous(err))
				}
			})
		})
	}
}

// The recorder's direct entry point must install the observer and share
// it over the simulator's own leader-changed retry loop, not just over
// gRPC's internal attempts.
func TestDialSimulatorCallUsesEvidence(t *testing.T) {
	simrun.Bubble(t, func(t *testing.T) {
		conn := dialTestConn(t, func(context.Context, string) (net.Conn, error) { return nil, errConnRefused })
		s := &Sim{t0: time.Now(), rng: rand.New(rand.NewPCG(1, 2))}
		n := &node{proc: &proc{ctx: t.Context(), wl: &wlProc{conn: conn}}}
		req := &pb.DeleteRangeRequest{Key: []byte("k")}
		_, err := s.call(n, req, Second)
		if status.Convert(err).Message() != refusedDialMessage || ambiguous(err) {
			t.Fatalf("the pre-stream call: %v, ambiguous %v", err, ambiguous(err))
		}
		ctx, transport := observeCallTransport(t.Context())
		retryPause{}.HandleRPC(ctx, &stats.Begin{Client: true})
		retryPause{}.HandleRPC(ctx, &stats.OutHeader{Client: true})
		_, err = s.callObserved(n, req, Second, transport)
		if status.Convert(err).Message() != refusedDialMessage || !ambiguous(err) {
			t.Fatalf("the retried call: %v, ambiguous %v", err, ambiguous(err))
		}
	})
}

// High-level clientv3 KV calls retain its default WaitForReady(true).
// Their eventual timeout still maps to the simulator's original error.
func TestDialClientV3DefaultWait(t *testing.T) {
	simrun.Bubble(t, func(t *testing.T) {
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://kv"}, Logger: zap.NewNop(), DialOptions: v3Options(func(context.Context, string) (net.Conn, error) { return nil, errConnRefused })})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cli.Close() }()
		c := &client{v3: cli, sim: &Sim{cfg: Config{ClientTimeout: Second}}}
		_, err = c.callV3(&pb.DeleteRangeRequest{Key: []byte("k")})
		if !errors.Is(err, errClientTimeout) || !ambiguous(err) {
			t.Fatalf("high-level clientv3 call: %v, ambiguous %v", err, ambiguous(err))
		}
	})
}

func TestDialClassificationRequiresEvidence(t *testing.T) {
	original := status.Error(codes.Unavailable, refusedDialMessage)
	for _, tc := range []struct {
		name          string
		err           error
		begin, header bool
		want          bool
	}{
		{"unobserved", original, false, false, true},
		{"pre-stream", original, true, false, false},
		{"prior-stream", original, true, true, true},
		{"other-unavailable", status.Error(codes.Unavailable, "connection reset"), true, false, true},
		{"wrong-code", status.Error(codes.Internal, refusedDialMessage), true, false, true},
		{"loose-text", status.Error(codes.Unavailable, "server said "+refusedDialMessage), true, false, true},
		{"canceled", status.Error(codes.Canceled, "context canceled"), true, false, true},
		{"timeout", errClientTimeout, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, transport := observeCallTransport(t.Context())
			if tc.begin {
				retryPause{}.HandleRPC(ctx, &stats.Begin{Client: true})
			}
			if tc.header {
				retryPause{}.HandleRPC(ctx, &stats.OutHeader{Client: true})
			}
			if got := ambiguous(transport.finish(tc.err)); got != tc.want {
				t.Fatalf("ambiguous %v, want %v", got, tc.want)
			}
		})
	}
}
