package sim

import (
	"context"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
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
// process.
func openWLStream(p *proc) (*wlStream, error) {
	ctx, cancel := context.WithCancel(p.ctx)
	call, err := pb.NewWatchClient(p.wl.conn).Watch(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
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
