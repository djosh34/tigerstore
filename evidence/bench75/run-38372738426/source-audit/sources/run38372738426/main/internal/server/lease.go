package server

import (
	"context"
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/node"
)

type leaseServer struct {
	pb.UnimplementedLeaseServer
	n *node.Node
}

func (s *leaseServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	return do[*pb.LeaseGrantResponse](ctx, s.n, r)
}

func (s *leaseServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	return do[*pb.LeaseRevokeResponse](ctx, s.n, r)
}

func (s *leaseServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	return do[*pb.LeaseTimeToLiveResponse](ctx, s.n, r)
}

func (s *leaseServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	return do[*pb.LeaseLeasesResponse](ctx, s.n, r)
}

// LeaseKeepAlive renews leases one request at a time. A missing lease
// gets TTL 0, not an error, as in etcd.
func (s *leaseServer) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	ctx := stream.Context()
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		resp, err := do[*pb.LeaseKeepAliveResponse](ctx, s.n, r)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
