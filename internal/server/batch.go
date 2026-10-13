package server

import (
	"encoding/binary"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
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
	reqs  []core.Request
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
	var req core.Request
	if st.rpc.wirePut {
		// The put stays in its wire form; the node copies it into the
		// Raft entry as it is.
		put, err := core.ParsePut(st.body)
		if err != nil {
			st.finish(status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err))
			return
		}
		req.Put = put
	} else {
		req.Op = st.rpc.op()
		if err := unmarshal(st.body, req.Op); err != nil {
			st.finish(err)
			return
		}
	}
	size := len(st.body)
	st.body = nil
	c.mu.Lock()
	st.inflight = size
	c.inflight += size
	c.mu.Unlock()
	c.batch.reqs = append(c.batch.reqs, req)
	c.batch.tags = append(c.batch.tags, st)
	c.batch.bytes += size
}

// batchDue reports whether the reader should hand its batch to the node
// now: it is full, or the next frame is not buffered yet, so reading it
// could wait for the client. There is no timer.
func (c *conn) batchDue() bool {
	b := &c.batch
	if len(b.reqs) == 0 {
		return false
	}
	if len(b.reqs) >= maxBatchOps || b.bytes >= maxBatchBytes || c.br.Buffered() < frameHeaderLen {
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
	if err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c); err != nil {
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
			switch {
			case f.err != nil:
			case cp.Resp == nil:
				f.msg, f.err = c.putResponse(&cp.Reply)
			default:
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

// putResponse encodes the response to a put in wire form, without
// building a protobuf message.
func (c *conn) putResponse(r *core.Reply) ([]byte, error) {
	b := make([]byte, 5, 5+putResponseSize)
	b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	binary.BigEndian.PutUint32(b[1:5], uint32(len(b)-5))
	return b, nil
}

// putResponseSize is the most appendPutResponse appends without a
// previous key: the header field with four varints.
const putResponseSize = 2 + 4*(1+binary.MaxVarintLen64)

// appendPutResponse appends a PutResponse in its wire form: the header
// with the cluster, member, revision and term, and the previous key if
// there is one.
func appendPutResponse(b []byte, cluster, member uint64, rev int64, term uint64, prev *mvccpb.KeyValue) ([]byte, error) {
	var h [4 * (1 + binary.MaxVarintLen64)]byte
	hb := h[:0]
	for i, v := range [...]uint64{cluster, member, uint64(rev), term} {
		if v != 0 {
			hb = protowire.AppendTag(hb, protowire.Number(i+1), protowire.VarintType)
			hb = protowire.AppendVarint(hb, v)
		}
	}
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, hb)
	if prev == nil {
		return b, nil
	}
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(proto.Size(prev)))
	return proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, prev)
}
