package server

import (
	"encoding/binary"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http2"
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
	// maxHeld bounds the bytes that the calls of one connection hold:
	// requests the node has not answered and replies not yet sent. Above
	// it, the connection's receive window is not credited back, so the
	// client stops sending until replies go out. The reader keeps reading
	// pings, resets and window updates.
	maxHeld = 64 << 20
	// callCost is what a call holds besides its request and reply bytes,
	// so that empty requests count too.
	callCost = 512
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
	st.reqSize = len(st.body)
	st.body = nil
	if st.slot != nil {
		c.queueManaged(st, req)
		return
	}
	c.batch.reqs = append(c.batch.reqs, req)
	c.batch.tags = append(c.batch.tags, st)
	c.batch.bytes += st.reqSize
}

// batchDue reports whether the reader should hand its batch to the node
// before it reads the next frame: the batch is full, or reading the next
// frame could wait for the client, because it is not buffered whole or
// starts a header block that continues in later frames. There is no
// timer.
func (c *conn) batchDue() bool {
	b := &c.batch
	if len(b.reqs) == 0 && c.managedCount == 0 {
		return false
	}
	if len(b.reqs)+c.managedCount >= maxBatchOps || b.bytes+c.managedBytes >= maxBatchBytes || c.br.Buffered() < frameHeaderLen {
		return true
	}
	// Peek cannot fail: the header is buffered.
	h, _ := c.br.Peek(frameHeaderLen)
	n := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	if http2.FrameType(h[3]) == http2.FrameHeaders && http2.Flags(h[4])&http2.FlagHeadersEndHeaders == 0 {
		return true
	}
	return c.br.Buffered() < frameHeaderLen+n
}

// flush hands the batch to the node. It never waits for the node's
// replies; the held bytes bound a connection through flow control.
func (c *conn) flush() error {
	if c.s.alloc != nil {
		if err := c.flushManaged(); err != nil {
			return err
		}
	}
	if len(c.batch.reqs) == 0 {
		return nil
	}
	b := c.batch
	c.batch = opBatch{}
	c.mu.Lock()
	for _, tag := range b.tags {
		st := tag.(*stream)
		st.atNode = true
		c.holdLocked(st, st.reqSize+callCost)
	}
	c.mu.Unlock()
	err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)
	if err == nil {
		return nil
	}
	if c.ctx.Err() != nil {
		return err
	}
	// The node stopped; it takes no more requests.
	c.mu.Lock()
	for _, tag := range b.tags {
		tag.(*stream).atNode = false
	}
	c.mu.Unlock()
	for _, tag := range b.tags {
		tag.(*stream).finish(toStatus(err))
	}
	return nil
}

// holdLocked adds n bytes to what a call holds. c.mu is held.
func (c *conn) holdLocked(st *stream, n int) {
	st.held += n
	c.held += n
}

// releaseLocked frees what a call held once it ended, and credits back
// the connection's receive window if that waited for it. c.mu is held.
func (c *conn) releaseLocked(st *stream) {
	c.held -= st.held
	st.held = 0
	c.creditLocked()
}

// creditLocked sends a connection window update once enough was received
// and the connection's calls hold less than maxHeld. c.mu is held.
func (c *conn) creditLocked() {
	if c.recvCredit >= connWindow/4 && c.held <= maxHeld {
		c.addCtrl(ctrlFrame{kind: ctrlWindowUpdate, val: uint32(c.recvCredit)})
		c.recvCredit = 0
	}
}

// Complete takes replies from the node's committer. It only queues them,
// under a lock of their own that nothing holds for long; the writer
// encodes them.
func (c *conn) Complete(cs []node.Completion) {
	c.compMu.Lock()
	c.completions = append(c.completions, cs)
	c.compMu.Unlock()
	c.signal()
}

// takeCompletions returns the replies the node queued.
func (c *conn) takeCompletions() [][]node.Completion {
	c.compMu.Lock()
	defer c.compMu.Unlock()
	cs := c.completions
	c.completions = nil
	return cs
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
			case f.st.rpc.wirePut:
				f.msg, f.err = c.putResponse(&cp.Reply)
			default:
				f.msg, f.err = marshal(cp.Resp)
			}
			out = append(out, f)
		}
	}
	c.mu.Lock()
	for _, f := range out {
		f.st.atNode = false
		if f.st.reset {
			// The client reset the call; its reply is dropped.
			c.releaseLocked(f.st)
			continue
		}
		if f.st.done.Swap(true) {
			// Ended already, at its deadline; endStream releases it.
			continue
		}
		c.holdLocked(f.st, len(f.msg))
		var s *status.Status
		if f.err != nil {
			s = status.Convert(toStatus(f.err))
		}
		f.st.pushLocked(f.msg, true, s)
	}
	c.mu.Unlock()
}

// nodeStopped fails every call the node still holds once it stopped. The
// node's last replies came in Complete calls before it stopped, so they
// are encoded first.
func (c *conn) nodeStopped() {
	if c.s.alloc != nil {
		if head := c.takeManagedCompletions(); head != nil {
			c.completeManaged(head)
		}
	}
	if cs := c.takeCompletions(); cs != nil {
		c.complete(cs)
	}
	c.mu.Lock()
	var held []*stream
	for _, st := range c.streams {
		if st.atNode {
			st.atNode = false
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
