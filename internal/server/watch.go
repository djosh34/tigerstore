package server

import (
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// watch serves one Watch stream. A goroutine reads client requests into
// the node; this one sends the node's responses.
func (s *Server) watch(st *stream) error {
	ctx := st.ctx
	ws, err := s.n.OpenWatch(ctx, st.c.callerOf(st))
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
			ws.Taken(r)
			if err := sendFragments(st, r); err != nil {
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

// maxWatchBytes is etcd's MaxRequestBytesWithOverhead: --max-request-bytes
// and 512 KiB of gRPC overhead, the size from which etcd splits the
// responses of a watcher that asked for fragments.
const maxWatchBytes = core.MaxRequestBytes + 512<<10

// sendFragments is etcd's: a response the node marked for a watcher that
// asked for fragments goes out in parts of fewer than maxWatchBytes, each
// of at least one event and all but the last marked as fragments, if it
// is that large and has more than one event.
func sendFragments(st *stream, r *pb.WatchResponse) error {
	if !r.Fragment {
		return st.Send(r)
	}
	r.Fragment = false
	if proto.Size(r) < maxWatchBytes {
		return st.Send(r)
	}
	for evs := r.Events; len(evs) > 0; {
		cur := &pb.WatchResponse{
			Header: r.Header, WatchId: r.WatchId, Created: r.Created, Canceled: r.Canceled,
			CompactRevision: r.CompactRevision, CancelReason: r.CancelReason, Fragment: true,
		}
		n := 0
		for n < len(evs) {
			cur.Events = evs[:n+1]
			if n > 0 && proto.Size(cur) >= maxWatchBytes {
				break
			}
			n++
		}
		cur.Events = evs[:n]
		evs = evs[n:]
		cur.Fragment = len(evs) > 0
		if err := st.Send(cur); err != nil {
			return err
		}
	}
	return nil
}
