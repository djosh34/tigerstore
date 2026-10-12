package server

import (
	"errors"
	"io"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"

	"github.com/djosh34/tigerstore/internal/node"
)

// leaderCheck is how often a stream that requires a leader checks for one.
const leaderCheck = 500 * time.Millisecond

type watchServer struct {
	pb.UnimplementedWatchServer
	n *node.Node
}

// requiresLeader reports whether the client set etcd's "hasleader"
// metadata, as clientv3.WithRequireLeader does.
func requiresLeader(md metadata.MD) bool {
	return slices.Contains(md.Get(rpctypes.MetadataRequireLeaderKey), rpctypes.MetadataHasLeader)
}

// Watch serves one watch stream. A goroutine reads client requests into
// the node; this one sends the node's responses.
func (s *watchServer) Watch(stream pb.Watch_WatchServer) error {
	ctx := stream.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	needLeader := requiresLeader(md)
	if needLeader && s.n.Leader() == 0 {
		return rpctypes.ErrGRPCNoLeader
	}
	ws, err := s.n.OpenWatch(ctx)
	if err != nil {
		return toStatus(err)
	}
	defer ws.Close()

	recvErr := make(chan error, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				// The client sends no more requests; keep sending it
				// responses until it goes away, as etcd does.
				return
			}
			if err != nil {
				recvErr <- err
				return
			}
			if err := ws.Send(ctx, r); err != nil {
				recvErr <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(leaderCheck)
	defer ticker.Stop()
	for {
		select {
		case r := <-ws.C:
			if err := stream.Send(r); err != nil {
				return err
			}
		case <-ws.Done():
			return ws.Err()
		case <-s.n.Done():
			return rpctypes.ErrGRPCStopped
		case <-ctx.Done():
			return rpctypes.ErrGRPCWatchCanceled
		case err := <-recvErr:
			// The client closed its side or went away.
			if ctx.Err() != nil {
				return rpctypes.ErrGRPCWatchCanceled
			}
			return toStatus(err)
		case <-ticker.C:
			if needLeader && s.n.Leader() == 0 {
				return rpctypes.ErrGRPCNoLeader
			}
		}
	}
}
