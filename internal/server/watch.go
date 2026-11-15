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

	"github.com/djosh34/tigerstore/internal/core"
)

// watch serves one Watch stream. A goroutine reads client requests into
// the node, and the node's loop encodes its responses into the stream's
// output (watchSink); this one waits for the stream's end.
func (s *Server) watch(st *stream) error {
	ctx := st.ctx
	sink := &watchSink{s: s, st: st}
	st.noWait = true
	ws, err := s.n.OpenWatchSink(ctx, st.c.callerOf(st), sink)
	if err != nil {
		return toStatus(err)
	}
	defer ws.Close()
	st.c.mu.Lock()
	st.drained = ws.Drained
	st.c.mu.Unlock()

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

	for {
		select {
		case <-ws.Done():
			// A send the sink could not make ended the stream; its error
			// is the stream's status. The node closed Done after the sink
			// set it.
			if sink.err != nil {
				return sink.err
			}
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

// watchSink encodes a watch stream's responses into its output on the
// node's loop (node.WatchSink). The node keeps the stream within its room,
// so a send never waits (stream.noWait).
type watchSink struct {
	s     *Server
	st    *stream
	parts watchParts
	// err is the first send that failed, which ends the stream with it.
	err error
}

func (w *watchSink) Watch(r core.WatchResponse) bool {
	start := time.Now()
	var err error
	switch {
	case r.More || r.Cont:
		err = w.parts.send(w.st, r)
	case r.Fragmented:
		// One of etcd's fragments, cut by the node.
		err = w.st.sendWatch(r.Resp, r.Events)
	default:
		err = sendFragments(w.st, r)
	}
	if err != nil {
		w.s.streamFailed(w.st, "send", "watch", err)
		w.err = err
		return false
	}
	w.s.watchSent(r, time.Since(start))
	return true
}

func (w *watchSink) Queued() int64 {
	c := w.st.c
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(w.st.queued)
}

// sendWatch queues a WatchResponse with events evs, which the node
// encoded once: the response is written around their bytes, with no
// message built (part 4 of docs/wire-loop.md).
func (st *stream) sendWatch(r *pb.WatchResponse, evs []*core.Event) error {
	var hb [watchHeadRoom]byte
	head := appendWatchHead(hb[:0], r)
	size := len(head) + eventsSize(evs)
	if size > 1<<31-1 {
		return status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
	}
	if st.encoding == gzipName {
		b := make([]byte, 5, 5+size)
		binary.BigEndian.PutUint32(b[1:], uint32(size))
		b, err := st.compress(appendEvents(append(b, head...), evs))
		if err != nil {
			return err
		}
		return st.push(b, false, nil, true)
	}
	b, ch := st.c.watchRoom(5 + size)
	b = binary.BigEndian.AppendUint32(append(b, 0), uint32(size))
	b = appendEvents(append(b, head...), evs)
	return st.pushIn(b, ch, false, nil, true)
}

// watchHeadRoom is room on the stack for the fields of most watch
// responses before their events.
const watchHeadRoom = 128

// appendWatchHead appends the fields of r before its events, in the
// order of their numbers, as protobuf's Marshal writes them.
func appendWatchHead(b []byte, r *pb.WatchResponse) []byte {
	if h := r.Header; h != nil {
		b = appendHeader(b, h.ClusterId, h.MemberId, h.Revision, h.RaftTerm)
	}
	if r.WatchId != 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(r.WatchId))
	}
	for _, f := range [...]struct {
		num protowire.Number
		on  bool
	}{{3, r.Created}, {4, r.Canceled}} {
		if f.on {
			b = protowire.AppendTag(b, f.num, protowire.VarintType)
			b = protowire.AppendVarint(b, 1)
		}
	}
	if r.CompactRevision != 0 {
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(r.CompactRevision))
	}
	if r.CancelReason != "" {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendString(b, r.CancelReason)
	}
	if r.Fragment {
		b = protowire.AppendTag(b, 7, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	return b
}

// appendEvents appends evs as the events of a WatchResponse.
func appendEvents(b []byte, evs []*core.Event) []byte {
	for _, e := range evs {
		b = protowire.AppendTag(b, 11, protowire.BytesType)
		b = protowire.AppendBytes(b, e.Bytes())
	}
	return b
}

// eventsSize is the size of evs as the events of a WatchResponse.
func eventsSize(evs []*core.Event) int {
	n := 0
	for _, e := range evs {
		n += eventSize(e)
	}
	return n
}

func eventSize(e *core.Event) int { return 1 + protowire.SizeBytes(len(e.Bytes())) }

// sendFragments is etcd's: a response the node marked for a watcher that
// asked for fragments goes out in parts of fewer than etcd's
// MaxRequestBytesWithOverhead (Server.maxRecvMsg) bytes, each
// of at least one event and all but the last marked as fragments, if it
// is that large and has more than one event.
func sendFragments(st *stream, r core.WatchResponse) error {
	resp := r.Resp
	if !resp.Fragment {
		return st.sendWatch(resp, r.Events)
	}
	resp.Fragment = false
	maxWatchBytes := st.c.s.maxRecvMsg
	var hb [watchHeadRoom]byte
	head := len(appendWatchHead(hb[:0], resp))
	if head+eventsSize(r.Events) < maxWatchBytes {
		return st.sendWatch(resp, r.Events)
	}
	// A fragment's head has the fragment field too.
	head += 2
	for evs := r.Events; len(evs) > 0; {
		n, size := 0, head
		for n < len(evs) {
			f := eventSize(evs[n])
			if n > 0 && size+f >= maxWatchBytes {
				break
			}
			size += f
			n++
		}
		cur := evs[:n]
		evs = evs[n:]
		resp.Fragment = len(evs) > 0
		if err := st.sendWatch(resp, cur); err != nil {
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
	// The part waits in the stream's queue: it takes no more room than
	// its bytes.
	size := eventsSize(r.Events)
	var b []byte
	if !r.Cont {
		var hb [watchHeadRoom]byte
		head := appendWatchHead(hb[:0], r.Resp)
		if whole := int64(len(head)) + r.EventBytes; whole > 1<<31-1 {
			return status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", whole)
		}
		b = make([]byte, 0, 5+len(head)+size)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(int64(len(head))+r.EventBytes))
		b = append(b, head...)
		p.left = r.EventBytes
	} else {
		b = make([]byte, 0, size)
	}
	events := len(b)
	b = appendEvents(b, r.Events)
	p.left -= int64(len(b) - events)
	if p.left < 0 || (!r.More && p.left != 0) {
		return errPartsShort
	}
	return st.push(b, false, nil, true)
}

// watchSent times a watch response as etcd's send loop does: one with
// events per response and per event, a created or canceled one as a
// control response, and an empty one as a progress notification.
func (s *Server) watchSent(r core.WatchResponse, d time.Duration) {
	m := s.metrics.set
	switch {
	case len(r.Events) > 0:
		m.WatchStream.Observe(d.Seconds())
		m.WatchStreamPerEvent.Observe(d.Seconds() / float64(len(r.Events)))
	case r.Resp.Created || r.Resp.Canceled:
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
