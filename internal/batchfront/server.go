// Package batchfront is a throwaway HTTP/2 gRPC batch frontend experiment.
// It uses x/net's Framer and HPACK, not a new wire protocol.
package batchfront

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2/hpack"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/server"
)

// Limits apply per connection, including flow-control-blocked responses.
const (
	MaxStreams       = 1024
	MaxInputBytes    = 16 << 20
	MaxOutputBytes   = 16 << 20
	MaxResponseBytes = MaxOutputBytes
	maxBatchCommands = 256
	maxBatchBytes    = 4 << 20
)

type counters struct {
	commands      atomic.Uint64
	sends         atomic.Uint64
	completions   atomic.Uint64
	replies       atomic.Uint64
	highwater     atomic.Uint64
	controlSends  atomic.Uint64
	responseSends atomic.Uint64
}

// Stats counts admission and grouped completions. OutputHighwater is the
// largest per-connection reservation, not a channel length.
type Stats struct {
	Commands, AdmissionSends, CompletionBatches, Replies, OutputHighwater uint64
	ControlSends, ResponseSends                                           uint64
}

type Server struct {
	n              *node.Node
	cfg            server.Config
	mode           string
	successHeader  []byte
	successTrailer []byte
	mu             sync.Mutex
	listener       net.Listener
	connections    map[*conn]struct{}
	stopped        bool
	wg             sync.WaitGroup
	stats          counters
}

// New selects rpc, admit, or full. The normal server is unchanged.
func New(n *node.Node, cfg server.Config, mode string) (*Server, error) {
	if mode == "" {
		mode = "full"
	}
	if mode != "rpc" && mode != "admit" && mode != "full" {
		return nil, errors.New("TIGERSTORE_BATCH_MODE must be rpc, admit, or full")
	}
	h, err := headerBlock(hpack.HeaderField{Name: ":status", Value: "200"}, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
	if err != nil {
		return nil, err
	}
	t, err := headerBlock(hpack.HeaderField{Name: "grpc-status", Value: "0"})
	if err != nil {
		return nil, err
	}
	return &Server{n: n, cfg: cfg, mode: mode, successHeader: h, successTrailer: t, connections: make(map[*conn]struct{})}, nil
}

func (s *Server) Stats() Stats {
	return Stats{Commands: s.stats.commands.Load(), AdmissionSends: s.stats.sends.Load(), CompletionBatches: s.stats.completions.Load(), Replies: s.stats.replies.Load(), OutputHighwater: s.stats.highwater.Load(), ControlSends: s.stats.controlSends.Load(), ResponseSends: s.stats.responseSends.Load()}
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ln.Close()
	}
	s.listener = ln
	s.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if s.cfg.TLS != nil {
			cfg := s.cfg.TLS.Clone()
			cfg.NextProtos = []string{"h2", "http/1.1"}
			nc = tls.Server(nc, cfg)
		}
		ctx, cancel := context.WithCancel(context.Background())
		c := &conn{Conn: nc, s: s, ctx: ctx, cancel: cancel, out: make(chan []encoded, 64), control: make(chan control, 256), budgets: make(map[uint32]*budget)}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			c.close()
			continue
		}
		s.connections[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer c.close()
			defer func() { s.mu.Lock(); delete(s.connections, c); s.mu.Unlock() }()
			if tc, ok := c.Conn.(*tls.Conn); ok {
				if err := tc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
					return
				}
				if err := tc.HandshakeContext(c.ctx); err != nil {
					return
				}
				if err := tc.SetDeadline(time.Time{}); err != nil {
					return
				}
				if tc.ConnectionState().NegotiatedProtocol != "h2" {
					c.serveHTTP1()
					return
				}
			}
			var writer sync.WaitGroup
			writer.Go(func() { defer c.close(); _ = c.writeLoop() })
			_ = c.readLoop()
			c.close()
			writer.Wait()
		}()
	}
}

func (s *Server) Stop() error {
	s.mu.Lock()
	s.stopped = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	for c := range s.connections {
		c.close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}
