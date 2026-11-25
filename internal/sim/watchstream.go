package sim

import (
	"context"
	"math"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// watchStream is a client's watch stream on a node: main's in-process
// stream, or a wl node's gRPC Watch call.
type watchStream interface {
	// Send hands the node one request of the stream.
	Send(ctx context.Context, r *pb.WatchRequest) error
	// Responses carries the node's responses in order.
	Responses() <-chan *pb.WatchResponse
	// Taken reports that a response was taken, as main's stream counts
	// its room by it.
	Taken(r *pb.WatchResponse)
	// Done is closed once the stream ended; Err says why.
	Done() <-chan struct{}
	Err() error
	// Close ends the stream from the client's side.
	Close()
}

// wlStream is a gRPC Watch call on a wl node. A goroutine receives its
// responses and hands them on one at a time, so a client that does not
// take them stops reading, and the node's flow control holds it back.
type wlStream struct {
	cancel context.CancelFunc
	call   pb.Watch_WatchClient
	c      chan *pb.WatchResponse
	done   chan struct{}
	// mu guards err, which is set before done is closed.
	mu  sync.Mutex
	err error
}

// openWLStream opens a Watch call on p's node. The call ends with the
// process. It takes responses of any size, as clientv3's watches do
// (defaultMaxCallRecvMsgSize, client/v3 options.go): a catch-up response
// of large values passes gRPC's 4 MiB default (#820).
func openWLStream(p *proc) (*wlStream, error) {
	ctx, cancel := context.WithCancel(p.ctx)
	call, err := pb.NewWatchClient(p.wl.conn).Watch(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		cancel()
		return nil, err
	}
	// Commit the call's attempt now, before its receiver starts: gRPC
	// retries an uncommitted stream transparently, holding the stream's
	// lock while it picks a transport, which waits for a node that is
	// down. A Send waits on that lock, not durably, and under -race
	// synctest's clock stops for good, with the node's restart behind it
	// (#837). Committed, a transport that fails ends the call, and the
	// watch client opens another, as for any stream that ends. That
	// Context commits is grpc v1.84's clientStream.Context, which calls
	// commitAttempt: it is not documented, so a gRPC upgrade must check it
	// still does, as no test here would fail, only race-sim hang.
	_ = call.Context()
	ws := &wlStream{cancel: cancel, call: call, c: make(chan *pb.WatchResponse), done: make(chan struct{})}
	go ws.receive(ctx)
	return ws, nil
}

func (ws *wlStream) receive(ctx context.Context) {
	for {
		r, err := ws.call.Recv()
		if err != nil {
			ws.mu.Lock()
			ws.err = err
			ws.mu.Unlock()
			close(ws.done)
			return
		}
		select {
		case ws.c <- r:
		case <-ctx.Done():
		}
	}
}

func (ws *wlStream) Send(_ context.Context, r *pb.WatchRequest) error { return ws.call.Send(r) }
func (ws *wlStream) Responses() <-chan *pb.WatchResponse              { return ws.c }
func (ws *wlStream) Taken(*pb.WatchResponse)                          {}
func (ws *wlStream) Done() <-chan struct{}                            { return ws.done }
func (ws *wlStream) Close()                                           { ws.cancel() }

func (ws *wlStream) Err() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.err
}
