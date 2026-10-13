package server

import (
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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

	// The node drops a stream that falls too far behind; that must also
	// end a Send that waits for the client.
	st.abort = ws.Done()
	for {
		select {
		case r := <-ws.C:
			if err := st.Send(r); err != nil {
				if errors.Is(err, errAborted) {
					return ws.Err()
				}
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
		}
	}
}
