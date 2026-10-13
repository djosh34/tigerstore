package server

import (
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/node"
)

// Batches of requests that one connection hands the node.
const (
	// maxBatchOps and maxBatchBytes send a batch before the reader runs
	// out of buffered frames.
	maxBatchOps   = 256
	maxBatchBytes = 4 << 20
	// maxInflight bounds the request bytes of one connection that the node
	// has not answered yet. Above it, the reader stops reading.
	maxInflight = 64 << 20
)

// opBatch is the requests the reader decoded since it last handed a batch
// to the node.
type opBatch struct {
	ops   []proto.Message
	tags  []any
	bytes int
}

// admit decodes the request of a unary call that the node runs and adds
// it to the reader's batch.
func (c *conn) admit(st *stream) {
	if st.msgs != 1 {
		st.finish(status.Errorf(codes.Internal, "grpc: unary call with %d request messages", st.msgs))
		return
	}
	if st.needLeader && c.s.n.Leader() == 0 {
		st.finish(rpctypes.ErrGRPCNoLeader)
		return
	}
	op := st.rpc.op()
	if err := unmarshal(st.body, op); err != nil {
		st.finish(err)
		return
	}
	size := len(st.body)
	st.body = nil
	c.mu.Lock()
	st.inflight = size
	c.inflight += size
	c.mu.Unlock()
	c.batch.ops = append(c.batch.ops, op)
	c.batch.tags = append(c.batch.tags, st)
	c.batch.bytes += size
}

// batchDue reports whether the reader should hand its batch to the node
// now: it is full, or the next frame is not buffered yet, so reading it
// could wait for the client. There is no timer.
func (c *conn) batchDue() bool {
	b := &c.batch
	if len(b.ops) == 0 {
		return false
	}
	if len(b.ops) >= maxBatchOps || b.bytes >= maxBatchBytes || c.br.Buffered() < frameHeaderLen {
		return true
	}
	h, err := c.br.Peek(frameHeaderLen)
	if err != nil {
		return true
	}
	n := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	return c.br.Buffered() < frameHeaderLen+n
}

// flush hands the batch to the node, then waits while too many request
// bytes of this connection are unanswered. It fails only when the
// connection closed.
func (c *conn) flush() error {
	b := c.batch
	c.batch = opBatch{}
	if err := c.s.n.SubmitBatch(c.ctx, b.ops, b.tags, c); err != nil {
		if c.ctx.Err() != nil {
			return err
		}
		for _, tag := range b.tags {
			c.release(tag.(*stream))
			tag.(*stream).finish(toStatus(err))
		}
		return nil
	}
	for {
		c.mu.Lock()
		over := c.inflight > maxInflight
		c.mu.Unlock()
		if !over {
			return nil
		}
		select {
		case <-c.room:
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// release ends the node's hold on a stream's request bytes.
func (c *conn) release(st *stream) {
	c.mu.Lock()
	c.releaseLocked(st)
	c.mu.Unlock()
}

func (c *conn) releaseLocked(st *stream) {
	if st.inflight == 0 {
		return
	}
	c.inflight -= st.inflight
	st.inflight = 0
	select {
	case c.room <- struct{}{}:
	default:
	}
}

// Complete takes replies from the node's committer. It only queues them;
// the writer encodes them.
func (c *conn) Complete(cs []node.Completion) {
	c.mu.Lock()
	if !c.closed {
		c.completions = append(c.completions, cs)
	}
	c.mu.Unlock()
	c.signal()
}

// finished is one encoded reply, or an error, for a stream.
type finished struct {
	st  *stream
	msg []byte
	err error
}

// complete encodes the replies the node handed over and queues them on
// their streams, taking the lock once.
func (c *conn) complete(batches [][]node.Completion) {
	var out []finished
	for _, cs := range batches {
		for _, cp := range cs {
			f := finished{st: cp.Tag.(*stream), err: cp.Err}
			if f.err == nil {
				f.msg, f.err = marshal(cp.Resp)
			}
			out = append(out, f)
		}
	}
	c.mu.Lock()
	for _, f := range out {
		c.releaseLocked(f.st)
		if f.st.done.Swap(true) {
			continue
		}
		var s *status.Status
		if f.err != nil {
			s = status.Convert(toStatus(f.err))
		}
		f.st.pushLocked(f.msg, true, s)
	}
	c.mu.Unlock()
}

// nodeStopped fails every call the node holds once it stopped; the node
// answers nothing after that.
func (c *conn) nodeStopped() {
	c.mu.Lock()
	var held []*stream
	for _, st := range c.streams {
		if st.inflight > 0 {
			c.releaseLocked(st)
			held = append(held, st)
		}
	}
	c.mu.Unlock()
	for _, st := range held {
		st.finish(rpctypes.ErrGRPCStopped)
	}
}
