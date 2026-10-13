// Package server serves the etcd v3 gRPC API of one node over client TLS.
// Handlers decode requests in gRPC's goroutines and hand them to the node's
// committer; nothing here touches the store.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
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
	// TLS is the client TLS configuration. Nil serves plain text, which
	// only tests should do.
	TLS *tls.Config
}

// maxStreams is the limit of concurrent streams per client connection.
const maxStreams = 1 << 20

// grpcOverhead is the room etcd leaves above --max-request-bytes for gRPC
// framing.
const grpcOverhead = 512 * 1024

// Server serves one node's client port.
type Server struct {
	n    *node.Node
	cfg  Config
	g    *grpc.Server
	http *http.Server

	mu      sync.Mutex
	clients *clientListener
	stopped bool
}

// New builds the server for a node.
func New(n *node.Node, cfg Config) *Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(core.MaxRequestBytes + grpcOverhead),
		grpc.MaxSendMsgSize(1<<31 - 1),
		grpc.MaxConcurrentStreams(maxStreams),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 2 * time.Hour, Timeout: 20 * time.Second}),
	}
	if cfg.TLS != nil {
		// cmux reads decrypted bytes. These credentials report the completed
		// TLS handshake to gRPC without attempting a second handshake.
		opts = append(opts, grpc.Creds(completedTLSCredentials{credentials.NewTLS(cfg.TLS)}))
	}
	g := grpc.NewServer(opts...)
	pb.RegisterKVServer(g, &kvServer{n: n})
	pb.RegisterClusterServer(g, &clusterServer{n: n, cfg: cfg})
	pb.RegisterWatchServer(g, &watchServer{n: n})
	pb.RegisterLeaseServer(g, &leaseServer{n: n})
	pb.RegisterMaintenanceServer(g, &maintenanceServer{n: n})
	pb.RegisterAuthServer(g, &pb.UnimplementedAuthServer{})
	s := &Server{n: n, cfg: cfg, g: g}
	s.http = &http.Server{
		Handler:           s.handler(),
		TLSConfig:         clientTLSConfig(cfg.TLS),
		ReadHeaderTimeout: 10 * time.Second,
		// net/http allows 250 streams per connection by default; one
		// kube-apiserver connection carries many more watches. etcd sets
		// math.MaxUint32.
		HTTP2: &http.HTTP2Config{MaxConcurrentStreams: maxStreams},
	}
	return s
}

// Serve accepts clients on ln until Stop. TLS clients share one port for
// native gRPC and HTTP/1 or HTTP/2 health and version requests. Without
// TLS, only gRPC is served; that is for tests. Serve is called once.
func (s *Server) Serve(ln net.Listener) error {
	if s.cfg.TLS == nil {
		return serveError(s.g.Serve(ln))
	}
	return s.serveTLS(ln)
}

// Stop closes the port and every client connection, including clients
// still in the TLS handshake or cmux's first-request matcher.
func (s *Server) Stop() error {
	s.mu.Lock()
	s.stopped = true
	clients := s.clients
	s.mu.Unlock()
	var err error
	if clients != nil {
		err = clients.Close()
	}
	s.g.Stop()
	return errors.Join(err, s.http.Close())
}

type maintenanceServer struct {
	pb.UnimplementedMaintenanceServer
	n *node.Node
}

func (s *maintenanceServer) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	st, err := s.n.Status(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return st, nil
}

type kvServer struct {
	pb.UnimplementedKVServer
	n *node.Node
}

// do runs op on the node and returns its response as T.
func do[T proto.Message](ctx context.Context, n *node.Node, op proto.Message) (T, error) {
	var zero T
	resp, err := n.Do(ctx, op)
	if err != nil {
		return zero, toStatus(err)
	}
	r, ok := resp.(T)
	if !ok {
		return zero, status.Errorf(codes.Internal, "response of type %T to %T", resp, op)
	}
	return r, nil
}

// toStatus turns context errors into the status etcd returns for them.
// Errors from the core are already etcd's gRPC errors.
func toStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unknown, err.Error())
}

func (s *kvServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	return do[*pb.RangeResponse](ctx, s.n, r)
}

func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	return do[*pb.PutResponse](ctx, s.n, r)
}

func (s *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	return do[*pb.DeleteRangeResponse](ctx, s.n, r)
}

func (s *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return do[*pb.TxnResponse](ctx, s.n, r)
}

func (s *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	return do[*pb.CompactionResponse](ctx, s.n, r)
}

type clusterServer struct {
	pb.UnimplementedClusterServer
	n   *node.Node
	cfg Config
}

func (s *clusterServer) MemberList(context.Context, *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	resp := &pb.MemberListResponse{Header: &pb.ResponseHeader{ClusterId: s.cfg.ClusterID, MemberId: s.n.ID()}}
	for _, m := range s.cfg.Members {
		resp.Members = append(resp.Members, &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
	}
	return resp, nil
}
