// Package server serves the etcd v3 gRPC API and the gRPC health service
// of one node, and etcd's /health, /readyz, /livez, /version and /metrics,
// on the client port. It speaks HTTP/2 and gRPC itself, on x/net's Framer
// and HPACK: a reader goroutine per connection decodes frames, a writer
// goroutine per connection encodes them, and handlers hand requests to the
// node's committer. Nothing here touches the store.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
)

// Member is one cluster member as MemberList reports it.
type Member struct {
	ID         uint64
	Name       string
	PeerURLs   []string
	ClientURLs []string
}

// Config sets up the server.
type Config struct {
	ClusterID uint64
	Members   []Member
	// TLS is the client TLS configuration. Nil serves HTTP/2 without TLS
	// (prior knowledge, no HTTP/1), which only tests should do.
	TLS *tls.Config
	// PeerVersions, if set, reports the peer transport's protocol
	// versions for /metrics.
	PeerVersions func() peer.Versions
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
	gone *metrics
	// cluster and member go in every response header.
	cluster, member uint64

	// ctx ends at Stop; it bounds TLS handshakes in progress.
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	ln      net.Listener
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
func New(n *node.Node, cfg Config) *Server {
	if cfg.TLS != nil {
		cfg.TLS = cfg.TLS.Clone()
		// HTTP/1.1 stays for /health and /version probes that do not
		// speak HTTP/2; gRPC needs h2.
		cfg.TLS.NextProtos = []string{"h2", "http/1.1"}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{n: n, cfg: cfg, gone: newMetrics(), cluster: n.ClusterID(), member: n.ID(), ctx: ctx, cancel: cancel, conns: map[closer]struct{}{}}
}

// Serve accepts clients on ln until Stop.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ln.Close()
	}
	s.ln = ln
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
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
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
			s.serveConn(nc)
		}()
	}
}

// serveConn finishes the TLS handshake and serves HTTP/2, or HTTP/1.1 for
// a client that did not offer h2.
func (s *Server) serveConn(nc net.Conn) {
	raw := nc
	h2 := true
	if s.cfg.TLS != nil {
		tc := tls.Server(nc, s.cfg.TLS)
		ctx, cancel := context.WithTimeout(s.ctx, handshakeTimeout)
		err := tc.HandshakeContext(ctx)
		cancel()
		if err != nil {
			_ = raw.Close()
			return
		}
		h2 = tc.ConnectionState().NegotiatedProtocol == "h2"
		nc = tc
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
	s.mu.Lock()
	s.stopped = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
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

// memberList answers Cluster.MemberList from the configuration.
func (s *Server) memberList() *pb.MemberListResponse {
	resp := &pb.MemberListResponse{Header: &pb.ResponseHeader{ClusterId: s.cfg.ClusterID, MemberId: s.n.ID()}}
	for _, m := range s.cfg.Members {
		resp.Members = append(resp.Members, &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
	}
	return resp
}
