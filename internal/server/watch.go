package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// watch serves one Watch stream. A goroutine reads client requests into
// the node; this one sends the node's responses.
func (s *Server) watch(st *stream) error {
	ctx := st.ctx
	ws, err := s.n.OpenWatchParts(ctx, st.c.callerOf(st))
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
				s.streamFailed(st, "receive", "watch", err)
				recvErr <- err
				return
			}
			if err := ws.Send(ctx, r); err != nil {
				recvErr <- err
				return
			}
		}
	}()

	// The node can drop a stream; that must also end a Send that waits
	// for the client.
	st.abort = ws.Done()
	var parts watchParts
	for {
		select {
		case r := <-ws.P:
			ws.TakenPart(r)
			start := time.Now()
			var err error
			switch {
			case r.More || r.Cont:
				err = parts.send(st, r)
			case r.Fragmented:
				// One of etcd's fragments, cut by the node.
				err = st.Send(r.Resp)
			default:
				err = sendFragments(st, r.Resp)
			}
			if err != nil {
				if errors.Is(err, errAborted) {
					return ws.Err()
				}
				s.streamFailed(st, "send", "watch", err)
				return err
			}
			s.watchSent(r.Resp, time.Since(start))
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

// sendFragments is etcd's: a response the node marked for a watcher that
// asked for fragments goes out in parts of fewer than etcd's
// MaxRequestBytesWithOverhead (Server.maxRecvMsg) bytes, each
// of at least one event and all but the last marked as fragments, if it
// is that large and has more than one event.
func sendFragments(st *stream, r *pb.WatchResponse) error {
	if !r.Fragment {
		return st.Send(r)
	}
	r.Fragment = false
	maxWatchBytes := st.c.s.maxRecvMsg
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

// watchParts writes a watch response that comes in parts as one
// message: the first part's head with the size of the whole, then the
// events of each part as it comes. left is the bytes of events the
// message still owes.
type watchParts struct{ left int64 }

// errPartsShort is a response in parts whose events did not add up to
// the size its first part gave: a bug in the node.
var errPartsShort = status.Error(codes.Internal, "server: a watch response in parts did not match its size")

func (p *watchParts) send(st *stream, r core.WatchResponse) error {
	var b []byte
	if !r.Cont {
		head, err := proto.Marshal(&pb.WatchResponse{
			Header: r.Resp.Header, WatchId: r.Resp.WatchId, Created: r.Resp.Created, Canceled: r.Resp.Canceled,
			CompactRevision: r.Resp.CompactRevision, CancelReason: r.Resp.CancelReason,
		})
		if err != nil {
			return status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
		}
		size := int64(len(head)) + r.EventBytes
		if size > 1<<31-1 {
			return status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
		}
		b = binary.BigEndian.AppendUint32([]byte{0}, uint32(size))
		b = append(b, head...)
		p.left = r.EventBytes
	}
	events := len(b)
	for _, ev := range r.Resp.Events {
		b = protowire.AppendTag(b, 11, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(proto.Size(ev)))
		var err error
		if b, err = (proto.MarshalOptions{UseCachedSize: true}).MarshalAppend(b, ev); err != nil {
			return status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
		}
	}
	p.left -= int64(len(b) - events)
	if p.left < 0 || (!r.More && p.left != 0) {
		return errPartsShort
	}
	return st.push(b, false, nil, true)
}

// watchSent times a watch response as etcd's send loop does: one with
// events per response and per event, a created or canceled one as a
// control response, and an empty one as a progress notification.
func (s *Server) watchSent(r *pb.WatchResponse, d time.Duration) {
	m := s.metrics.set
	switch {
	case len(r.Events) > 0:
		m.WatchStream.Observe(d.Seconds())
		m.WatchStreamPerEvent.Observe(d.Seconds() / float64(len(r.Events)))
	case r.Created || r.Canceled:
		m.WatchControl.Observe(d.Seconds())
	default:
		m.WatchProgress.Observe(d.Seconds())
	}
}

// streamFailed counts a failed send or receive of a stream, unless the
// client ended the call, as etcd's isClientCtxErr tells.
func (s *Server) streamFailed(st *stream, dir, api string, err error) {
	if st.ctx.Err() != nil || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
		return
	}
	s.metrics.set.StreamFailures.WithLabelValues(dir, api).Inc()
}
