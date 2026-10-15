package server

import (
	"context"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// defragment answers Maintenance.Defragment as etcd's does: for an admin
// with auth on, and with a response without a header. With
// Config.StopServiceOnDefrag (etcd's StopGRPCServiceOnDefrag feature),
// the health service reports NOT_SERVING meanwhile.
func defragment(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	var r pb.DefragmentRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if err := s.n.Authorize(ctx, caller, &r); err != nil {
		return nil, toStatus(err)
	}
	if s.cfg.StopServiceOnDefrag {
		s.serving.defrags(1)
		defer s.serving.defrags(-1)
	}
	if err := s.n.Defragment(ctx); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DefragmentResponse{}, nil
}

// healthState is the status of the gRPC health service's service "":
// SERVING but while a defragmentation runs with StopServiceOnDefrag.
type healthState struct {
	mu      sync.Mutex
	defrag  int
	changed chan struct{}
}

func newHealthState() *healthState { return &healthState{changed: make(chan struct{})} }

func (h *healthState) defrags(delta int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.defrag += delta
	close(h.changed)
	h.changed = make(chan struct{})
}

// status is the status and a channel that closes when it may change.
func (h *healthState) status() (healthpb.HealthCheckResponse_ServingStatus, <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.defrag > 0 {
		return healthpb.HealthCheckResponse_NOT_SERVING, h.changed
	}
	return healthpb.HealthCheckResponse_SERVING, h.changed
}
