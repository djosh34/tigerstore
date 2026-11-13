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
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/loop"
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
	// Deferred, if set, runs on the loop each time a connection's turn
	// ran out of room while it held a whole HEADERS or DATA frame it had
	// read, a call's, which then waits for a later turn of the node's loop
	// (node.Node.TurnRoom): at most once a connection a turn. For tests.
	Deferred func()
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
	// loop is the node's event loop, which runs the HTTP/2 connections;
	// bufs are the buffers its connections borrow while they hold bytes,
	// bigBufs those that hold a whole write pass that waits for a socket,
	// and bw the write buffer every connection writes through. Loop only.
	loop    *loop.Loop
	bufs    *loop.Buffers
	bigBufs *loop.Buffers
	bw      *bufio.Writer
	// ctrlOut, framesOut and hdrOut are the write pass's scratch, which
	// every connection uses in turn. Loop only.
	ctrlOut   []ctrlFrame
	framesOut []wframe
	hdrOut    []byte
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
	// open counts the client connections from their accept until they end,
	// under mu, and maxConns bounds them, 0 for no bound (setLimits);
	// refusedLog is when a refused one was last logged.
	open       int
	maxConns   int
	refusedLog time.Time
	// refusing counts the refused connections that hear why (refuse), and
	// toldRefused holds when each client host last heard why.
	refusing    int
	toldRefused map[string]time.Time
	// outAll is the output that waits for the client connections'
	// sockets, and maxOut its bound, 0 for none: a connection whose next
	// write pass would go past it waits in outWaiting until sockets took
	// some. Loop only.
	outAll     int
	maxOut     int
	outWaiting []*conn
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
	s := &Server{n: n, loop: n.Loop(), bufs: loop.NewBuffers(connBuffer, keepBuffers), bigBufs: loop.NewBuffers(passBuffer, keepPassBuffers), bw: bufio.NewWriterSize(nil, ioBuffer), cfg: cfg, grpcTLS: grpcTLS(cfg.TLS), gone: newConnMetrics(), cluster: n.ClusterID(), member: n.ID(), ctx: ctx, cancel: cancel, conns: map[closer]struct{}{}, serving: newHealthState()}
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
		if s.maxConns > 0 && s.open >= s.maxConns {
			// Past the connections the memory budget holds.
			s.refuseLocked(nc, l)
			s.mu.Unlock()
			continue
		}
		// Added under mu, so never after Stop started waiting.
		s.wg.Add(1)
		s.open++
		s.mu.Unlock()
		go func() {
			// The loop ends a connection handed to it.
			if !s.serveConn(nc, l) {
				s.ended()
			}
		}()
	}
}

// serveConn finishes the TLS handshake and serves HTTP/2, or HTTP/1.1 for
// a client that did not offer h2 or, without TLS, did not start with
// HTTP/2's preface. As in etcd, a listener without TLS hands HTTP/2 to
// gRPC alone, and one that serves only HTTP drops HTTP/2 without TLS and
// one that serves only gRPC HTTP/1.1. An HTTP/2 connection goes to the
// node's loop, and serveConn reports that it handed it over.
func (s *Server) serveConn(nc net.Conn, l Listener) bool {
	raw := nc
	traffic := l.Traffic
	// TLS reads its records through the bridge's buffer, so one read
	// takes in as many as the kernel holds.
	bridge := loop.NewBridge(nc, ioBuffer)
	var h2 bool
	if l.TLS {
		cfg := s.cfg.TLS
		if traffic == TrafficGRPC {
			cfg = s.grpcTLS
		}
		// A TLS 1.3 connection's records go through our own record layer
		// after crypto/tls's handshake; one of TLS 1.2 stays crypto/tls's.
		ctx, cancel := context.WithTimeout(s.ctx, handshakeTimeout)
		records, tc, err := loop.Handshake(ctx, bridge, cfg, true)
		cancel()
		switch {
		case records != nil:
			nc = records
			h2 = records.ConnectionState().NegotiatedProtocol == "h2"
		case errors.Is(err, loop.ErrNotTLS13):
			nc = tc
			h2 = tc.ConnectionState().NegotiatedProtocol == "h2"
		default:
			_ = raw.Close()
			return false
		}
	} else {
		var err error
		if h2, err = peekPreface(bridge); err != nil {
			_ = raw.Close()
			return false
		}
		if h2 {
			traffic = TrafficGRPC
		}
		nc = bridge
	}
	if (h2 && l.Traffic == TrafficHTTP && !l.TLS) || (!h2 && l.Traffic == TrafficGRPC) {
		_ = raw.Close()
		return false
	}
	if !h2 {
		h := &http1Conn{s: s, nc: nc, raw: raw}
		if !s.track(h) {
			return false
		}
		defer s.untrack(h)
		h.serve()
		return false
	}
	c := newConn(s, nc, bridge)
	c.traffic = traffic
	return s.hand(c)
}

// servePlain serves HTTP/2 with prior knowledge on a connection of the
// gateway's, and reports whether it handed it to the loop.
func (s *Server) servePlain(nc net.Conn) bool {
	bridge := loop.NewBridge(nc, ioBuffer)
	return s.hand(newConn(s, bridge, bridge))
}

// hand gives an HTTP/2 connection to the loop, which serves it and ends
// it, and reports whether it did. Only then does the loop end the
// connection and its count in s.wg.
func (s *Server) hand(c *conn) bool {
	// The handshake's deadline goes; the loop keeps time itself.
	if err := c.raw.SetDeadline(time.Time{}); err != nil {
		_ = c.raw.Close()
		return false
	}
	if !s.track(c) {
		return false
	}
	if !s.loop.Post(c.attach) {
		s.untrack(c)
		return false
	}
	return true
}

// maxRefusing bounds the refused connections that wait for their
// handshake to hear why at once; past it, a refused one is closed.
// refuseEvery is how often one client host hears why, and maxToldRefused
// how many hosts the server remembers that for.
const (
	maxRefusing    = 16
	refuseEvery    = time.Second
	maxToldRefused = 1024
)

// remoteHost is the host of c's remote address.
func remoteHost(c net.Conn) string {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP.String()
	}
	return c.RemoteAddr().String()
}

// refuseLocked refuses a client connection past the limit, counts it in
// tigerstore_server_client_connections_refused_total, and logs it at most
// once a minute. The client hears why on a goroutine of its own (refuse),
// unless maxRefusing others wait already. s.mu is held.
func (s *Server) refuseLocked(nc net.Conn, l Listener) {
	s.metrics.set.ClientConnsRefused.Inc()
	if time.Since(s.refusedLog) >= time.Minute {
		s.refusedLog = time.Now()
		if s.cfg.Logf != nil {
			s.cfg.Logf("refused a client connection: %d are open, the most the memory budget holds", s.maxConns)
		}
	}
	// gRPC resets its reconnect backoff after any connection that got the
	// server's SETTINGS, so a client refused that way would come back at
	// once, in a loop. One that is closed before them backs off (a second,
	// then 1.6 times longer each time). So a client host hears why at most
	// once a second, and is closed at once in between.
	host := remoteHost(nc)
	now := time.Now()
	if last, ok := s.toldRefused[host]; s.refusing >= maxRefusing || ok && now.Sub(last) < refuseEvery {
		_ = nc.Close()
		return
	}
	if len(s.toldRefused) >= maxToldRefused {
		for h, at := range s.toldRefused {
			if now.Sub(at) >= refuseEvery {
				delete(s.toldRefused, h)
			}
		}
	}
	if len(s.toldRefused) < maxToldRefused {
		if s.toldRefused == nil {
			s.toldRefused = map[string]time.Time{}
		}
		s.toldRefused[host] = now
	}
	s.refusing++
	s.wg.Go(func() {
		s.refuse(nc, l)
		s.mu.Lock()
		s.refusing--
		s.mu.Unlock()
	})
}

// refusal is the debug data of the GOAWAY, and the body of the HTTP 503,
// that a refused client gets.
const refusal = "tigerstore: too many client connections; the server holds as many as its memory budget allows"

// refuse tells a refused client why, as its protocol allows, and closes
// the connection. An HTTP/2 client gets the server's SETTINGS; its first
// call, if one comes within a second, gets Unavailable with the reason,
// since grpc-go drops a GOAWAY's debug data; then a GOAWAY with
// ENHANCE_YOUR_CALM and the reason. An HTTP/1.1 client gets a 503. The
// client has up to a second to read it, and Stop ends it at once.
func (s *Server) refuse(nc net.Conn, l Listener) {
	defer nc.Close() //nolint:errcheck // the refused connection ends here
	stop := context.AfterFunc(s.ctx, func() { _ = nc.Close() })
	defer stop()
	if err := nc.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	var h2 bool
	var conn net.Conn
	if l.TLS {
		cfg := s.cfg.TLS
		if l.Traffic == TrafficGRPC {
			cfg = s.grpcTLS
		}
		tc := tls.Server(nc, cfg)
		if err := tc.HandshakeContext(s.ctx); err != nil {
			return
		}
		h2, conn = tc.ConnectionState().NegotiatedProtocol == "h2", tc
	} else {
		b := loop.NewBridge(nc, len(http2.ClientPreface))
		var err error
		if h2, err = peekPreface(b); err != nil {
			return
		}
		conn = b
	}
	if !h2 {
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(refusal), refusal)
	} else if !refuseCall(conn, nc) {
		return
	}
	// Reading what the client sends until it closes keeps a reset from
	// taking the answer with it.
	if err := nc.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn)
}

// refuseCall answers a refused HTTP/2 client: the server's SETTINGS, its
// first call's HEADERS within a second with trailers-only Unavailable and
// the reason, and a GOAWAY. It reports whether the client may still read.
func refuseCall(conn, nc net.Conn) bool {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return false
	}
	fr := http2.NewFramer(conn, conn)
	// HTTP/2's own limit, which the server never raises: a frame header
	// cannot make a refusal allocate more.
	fr.SetMaxReadFrameSize(maxReadFrame)
	if fr.WriteSettings() != nil || nc.SetReadDeadline(time.Now().Add(time.Second)) != nil {
		return false
	}
	var last uint32
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			break
		}
		if h, ok := f.(*http2.HeadersFrame); ok {
			last = h.StreamID
			if fr.WriteHeaders(http2.HeadersFrameParam{
				StreamID: last, EndStream: true, EndHeaders: true,
				BlockFragment: statusBlock(status.New(codes.Unavailable, refusal), true),
			}) != nil {
				return false
			}
			break
		}
	}
	return fr.WriteGoAway(last, http2.ErrCodeEnhanceYourCalm, []byte(refusal)) == nil
}

// ended counts the end of a connection that ServeListener or the gateway
// counted.
func (s *Server) ended() {
	s.mu.Lock()
	s.open--
	s.mu.Unlock()
	s.wg.Done()
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

// peekPreface reads as much of a connection's first bytes as it takes to
// tell whether it starts with HTTP/2's client preface, as cmux does in
// etcd, within handshakeTimeout. The bytes stay in the bridge.
func peekPreface(b *loop.Bridge) (bool, error) {
	if err := b.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return false, err
	}
	for i := 1; i <= len(http2.ClientPreface); i++ {
		p, err := b.Peek(i)
		if err != nil {
			return false, err
		}
		if p[i-1] != http2.ClientPreface[i-1] {
			return false, b.SetReadDeadline(time.Time{})
		}
	}
	return true, b.SetReadDeadline(time.Time{})
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
	if s.n == nil {
		return
	}
	if b := s.n.MemoryBudget(); b > 0 {
		// A quarter of the node's memory budget goes to the client
		// connections (connBytes each), and a sixteenth to their output
		// that waits for sockets; the rest to the peers, the store and its
		// value cache, which takes what the process leaves
		// (core.Node.SetOtherMemory). The floors are for the tiny budgets
		// of tests; a node's budget of 128 MiB or more is above both.
		s.maxConns = max(minConns, int(b/connShare/connBytes))
		s.maxOut = max(minOut, int(b/outShare))
	}
}

// connShare and outShare are the parts of the node's memory budget that
// the client connections and their waiting output take; minConns and
// minOut the least of each.
const (
	connShare = 4
	outShare  = 16
	minConns  = 64
	minOut    = 8 << 20
)
