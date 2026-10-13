package server

import (
	"errors"
	"io"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// A stream that requires a leader checks for one every election timeout
// (etcd's default 1 s) and closes after maxNoLeader checks in a row find
// none, as etcd's monitorLeader does.
const (
	leaderCheck = time.Second
	maxNoLeader = 3
)

// watch serves one Watch stream. A goroutine reads client requests into
// the node; this one sends the node's responses.
func (s *Server) watch(st *stream) error {
	ctx := st.ctx
	ws, err := s.n.OpenWatch(ctx)
	if err != nil {
		return toStatus(err)
	}
	defer ws.Close()

	recvErr := make(chan error, 1)
	go func() {
		for {
			r := &pb.WatchRequest{}
			err := st.Recv(r)
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
	noLeader := 0
	for {
		select {
		case r := <-ws.C:
			if err := st.Send(r); err != nil {
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
			if !st.needLeader {
				continue
			}
			if s.n.Leader() != 0 {
				noLeader = 0
				continue
			}
			if noLeader++; noLeader >= maxNoLeader {
				return rpctypes.ErrGRPCNoLeader
			}
		}
	}
}
