// Package server serves the etcd v3 gRPC API and the gRPC health service
// of one node, and etcd's /health, /readyz, /livez, /version and /metrics,
// on the client port. It speaks HTTP/2 and gRPC itself, on x/net's Framer
// and HPACK: a reader goroutine per connection decodes frames, a writer
// goroutine per connection encodes them, and handlers hand requests to the
// node's committer. Nothing here touches the store.
package server

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
)

// Config sets up the server.
type Config struct {
	ClusterID uint64
	// TLS is the client TLS configuration. Nil serves HTTP/2 without TLS
	// (prior knowledge, no HTTP/1), which only tests should do.
	TLS *tls.Config
	// BcryptCost is etcd's --bcrypt-cost; 0 means bcrypt's default.
	BcryptCost int
	// JWT, if set, makes and checks the auth tokens instead of the
	// node's simple tokens (--auth-token, ParseAuthToken).
	JWT *JWT
	// StopServiceOnDefrag is etcd's StopGRPCServiceOnDefrag feature.
	StopServiceOnDefrag bool
	// PeerVersions, if set, reports the peer transport's protocol
	// versions for /metrics.
	PeerVersions func() peer.Versions
	// NoGateway turns the gRPC gateway off (etcd's
	// --enable-grpc-gateway=false).
	NoGateway bool
	// CORS and HostWhitelist are etcd's --cors and --host-whitelist; nil
	// means etcd's default, which allows all.
	CORS, HostWhitelist map[string]struct{}
	// ExtensiveMetrics is etcd's --metrics=extensive: the gRPC handling
	// time histograms too.
	ExtensiveMetrics bool
	// Pprof is etcd's --enable-pprof: the Go profiles under
	// /debug/pprof/.
	Pprof bool
	// TracerProvider, if set, traces the gRPC calls as etcd's
	// --enable-distributed-tracing does (tracing.go).
	TracerProvider trace.TracerProvider
	// MaxRequestBytes is etcd's --max-request-bytes, 0 for its default;
	// a message may have 512 KiB more, as in etcd. MaxConcurrentStreams
	// is its --max-concurrent-streams, 0 for its default,
	// math.MaxUint32.
	MaxRequestBytes      int
	MaxConcurrentStreams uint32
	// WarningUnaryRequestDuration is etcd's
	// --warning-unary-request-duration: a unary call that takes longer
	// is logged with Logf. 0 means etcd's default, 300 ms.
	WarningUnaryRequestDuration time.Duration
	// Logf, if set, logs the slow unary calls.
	Logf func(format string, args ...any)
	// KeepaliveMinTime, KeepaliveTime and KeepaliveTimeout are etcd's
	// --grpc-keepalive-min-time, --grpc-keepalive-interval and
	// --grpc-keepalive-timeout as etcd hands them to grpc-go
	// (keepalive.go); 0 means grpc-go's default, 5 minutes, 2 hours and
	// 20 seconds.
	KeepaliveMinTime, KeepaliveTime, KeepaliveTimeout time.Duration
}

// handshakeTimeout bounds the TLS handshake and the HTTP/2 preface.
const handshakeTimeout = 10 * time.Second

// stopGrace is how long Stop lets connections finish after GOAWAY.
const stopGrace = time.Second

// Server serves one node's client port.
type Server struct {
	n   *node.Node
	cfg Config
	// gone sums the metrics of closed connections; guarded by mu.
	gone *connMetrics
	// metrics are what the server observes (metrics.go).
	metrics *serverMetrics
	// gateway serves /v3/ over HTTP (gateway.go) through gwConn.
	gateway http.Handler
	// pprof serves /debug/pprof/ with Config.Pprof (debug.go).
	pprof  http.Handler
	gwConn *grpc.ClientConn
	// tracing is set with Config.TracerProvider.
	tracing *tracing
	// cluster and member go in every response header.
	cluster, member uint64
	// serving is the gRPC health service's status.
	serving *healthState

	// ctx ends at Stop; it bounds TLS handshakes in progress.
	ctx    context.Context
	cancel context.CancelFunc

	// grpcTLS is Config.TLS for listeners that serve gRPC only.
	grpcTLS *tls.Config
	// maxRequestBytes, maxRecvMsg and maxStreams are the limits of
	// Config (setLimits), and streamWindow the receive window of each
	// stream, above the largest request, so a unary request never waits
	// for a window update.
	maxRequestBytes, maxRecvMsg, streamWindow int
	maxStreams                                uint32
	// slowUnary is Config.WarningUnaryRequestDuration with its default,
	// and the keepalive fields Config's.
	slowUnary                                     time.Duration
	keepaliveMin, keepaliveTime, keepaliveTimeout time.Duration

	mu      sync.Mutex
	lns     []net.Listener
	conns   map[closer]struct{}
	stopped bool
	wg      sync.WaitGroup
}

// closer is a live client connection that Stop must end.
type closer interface {
	// goAway asks the connection to end, gracefully if it can.
	goAway()
	close()
}

// New builds the server for a node.
func New(n *node.Node, cfg Config) (*Server, error) {
	if cfg.TLS != nil {
		cfg.TLS = cfg.TLS.Clone()
		// HTTP/1.1 stays for /health and /version probes that do not
		// speak HTTP/2; gRPC needs h2.
		cfg.TLS.NextProtos = []string{"h2", "http/1.1"}
	}
	if c := cfg.BcryptCost; c != 0 && (c < bcrypt.MinCost || c > bcrypt.MaxCost) {
		// etcd's NewAuthStore warns and takes the default too.
		if cfg.Logf != nil {
			cfg.Logf("use default bcrypt cost %d instead of the invalid given cost %d (min %d, max %d)", bcrypt.DefaultCost, c, bcrypt.MinCost, bcrypt.MaxCost)
		}
		cfg.BcryptCost = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{n: n, cfg: cfg, grpcTLS: grpcTLS(cfg.TLS), gone: newConnMetrics(), cluster: n.ClusterID(), member: n.ID(), ctx: ctx, cancel: cancel, conns: map[closer]struct{}{}, serving: newHealthState()}
	if err := s.newServerMetrics(); err != nil {
		cancel()
		return nil, err
	}
	if cfg.Pprof {
		s.pprof = pprofMux()
	}
	s.tracing = newTracing(cfg.TracerProvider)
	s.setLimits()
	if !cfg.NoGateway {
		var err error
		if s.gateway, s.gwConn, err = s.newGateway(); err != nil {
			cancel()
			return nil, err
		}
	}
	return s, nil
}

// Traffic is what a client listener serves. etcd serves gRPC and HTTP on
// each client URL; with --listen-client-http-urls, those serve HTTP only
// and the client URLs gRPC only.
type Traffic uint8

const (
	TrafficAll Traffic = iota
	TrafficGRPC
	TrafficHTTP
)

// Listener is a client listener and how it serves.
type Listener struct {
	net.Listener
	// TLS makes each connection start with a TLS handshake, with
	// Config.TLS.
	TLS     bool
	Traffic Traffic
}

// Serve accepts clients on ln until Stop, with TLS if Config.TLS is set,
// serving gRPC and HTTP.
func (s *Server) Serve(ln net.Listener) error {
	return s.ServeListener(Listener{Listener: ln, TLS: s.cfg.TLS != nil})
}

// ServeListener accepts clients on l until Stop. A server serves any
// number of listeners at once.
func (s *Server) ServeListener(l Listener) error {
	ln := l.Listener
	if l.TLS && s.cfg.TLS == nil {
		return errors.Join(errors.New("server: a TLS listener without Config.TLS"), ln.Close())
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ln.Close()
	}
	s.lns = append(s.lns, ln)
	s.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped {
				return nil
			}
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				continue
			}
			return err
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = nc.Close()
			return nil
		}
		// Added under mu, so never after Stop started waiting.
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			s.serveConn(nc, l)
		}()
	}
}

// serveConn finishes the TLS handshake and serves HTTP/2, or HTTP/1.1 for
// a client that did not offer h2 or, without TLS, did not start with
// HTTP/2's preface. As in etcd, a listener without TLS hands HTTP/2 to
// gRPC alone, and one that serves only HTTP drops HTTP/2 without TLS and
// one that serves only gRPC HTTP/1.1.
func (s *Server) serveConn(nc net.Conn, l Listener) {
	raw := nc
	traffic := l.Traffic
	var h2 bool
	if l.TLS {
		cfg := s.cfg.TLS
		if traffic == TrafficGRPC {
			cfg = s.grpcTLS
		}
		tc := tls.Server(nc, cfg)
		ctx, cancel := context.WithTimeout(s.ctx, handshakeTimeout)
		err := tc.HandshakeContext(ctx)
		cancel()
		if err != nil {
			_ = raw.Close()
			return
		}
		h2 = tc.ConnectionState().NegotiatedProtocol == "h2"
		nc = tc
	} else {
		pc, err := peekPreface(nc)
		if err != nil {
			_ = raw.Close()
			return
		}
		h2, nc = pc.h2, pc
		if h2 {
			traffic = TrafficGRPC
		}
	}
	if (h2 && l.Traffic == TrafficHTTP && !l.TLS) || (!h2 && l.Traffic == TrafficGRPC) {
		_ = raw.Close()
		return
	}
	if !h2 {
		h := &http1Conn{s: s, nc: nc, raw: raw}
		if !s.track(h) {
			return
		}
		defer s.untrack(h)
		h.serve()
		return
	}
	c := newConn(s, nc, raw)
	c.traffic = traffic
	if !s.track(c) {
		return
	}
	defer s.untrack(c)
	c.serve()
}

// servePlain serves HTTP/2 with prior knowledge on a connection of the
// gateway's.
func (s *Server) servePlain(nc net.Conn) {
	c := newConn(s, nc, nc)
	if !s.track(c) {
		return
	}
	defer s.untrack(c)
	c.serve()
}

func (s *Server) track(c closer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		c.close()
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c closer) {
	c.close()
	s.mu.Lock()
	if hc, ok := c.(*conn); ok {
		s.gone.add(hc.metrics)
	}
	delete(s.conns, c)
	s.mu.Unlock()
}

// Stop closes the port, sends every HTTP/2 client a GOAWAY, gives the
// streams in flight a moment to finish, and then closes every connection.
func (s *Server) Stop() error {
	s.cancel()
	var err error
	if s.gwConn != nil {
		err = s.gwConn.Close()
	}
	s.mu.Lock()
	s.stopped = true
	for _, ln := range s.lns {
		err = errors.Join(err, ln.Close())
	}
	for c := range s.conns {
		c.goAway()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopGrace):
		s.mu.Lock()
		for c := range s.conns {
			c.close()
		}
		s.mu.Unlock()
		<-done
	}
	return err
}

// learner reports whether the node is a learner. Tests run connections
// without a node.
func (s *Server) learner() bool { return s.n != nil && s.n.Learner() }

// grpcTLS is cfg for listeners that serve only gRPC, which offer only h2,
// as grpc-go's TLS does; nil without cfg.
func grpcTLS(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return nil
	}
	cfg = cfg.Clone()
	cfg.NextProtos = []string{"h2"}
	return cfg
}

// prefaceConn is a connection without TLS whose first bytes were read to
// tell HTTP/2 from HTTP/1.1; reads take them first.
type prefaceConn struct {
	net.Conn
	br *bufio.Reader
	h2 bool
}

func (c *prefaceConn) Read(p []byte) (int, error) { return c.br.Read(p) }

// peekPreface reads as much of a connection's first bytes as it takes to
// tell whether it starts with HTTP/2's client preface, as cmux does in
// etcd, within handshakeTimeout.
func peekPreface(nc net.Conn) (*prefaceConn, error) {
	if err := nc.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return nil, err
	}
	pc := &prefaceConn{Conn: nc, br: bufio.NewReaderSize(nc, len(http2.ClientPreface)), h2: true}
	for i := 1; i <= len(http2.ClientPreface) && pc.h2; i++ {
		b, err := pc.br.Peek(i)
		if err != nil {
			return nil, err
		}
		pc.h2 = b[i-1] == http2.ClientPreface[i-1]
	}
	return pc, nc.SetReadDeadline(time.Time{})
}

// setLimits sets the limits of Config, etcd's defaults for those not set.
func (s *Server) setLimits() {
	s.maxRequestBytes = cmp.Or(s.cfg.MaxRequestBytes, core.MaxRequestBytes)
	// etcd's MaxRequestBytesWithOverhead.
	s.maxRecvMsg = s.maxRequestBytes + 512<<10
	// gRPC's message prefix is 5 bytes.
	s.streamWindow = min(max(minStreamWindow, s.maxRecvMsg+5), math.MaxInt32)
	s.maxStreams = cmp.Or(s.cfg.MaxConcurrentStreams, math.MaxUint32)
	s.slowUnary = cmp.Or(s.cfg.WarningUnaryRequestDuration, 300*time.Millisecond)
	s.keepaliveMin = cmp.Or(s.cfg.KeepaliveMinTime, 5*time.Minute)
	s.keepaliveTime = cmp.Or(s.cfg.KeepaliveTime, 2*time.Hour)
	s.keepaliveTimeout = cmp.Or(s.cfg.KeepaliveTimeout, 20*time.Second)
}
