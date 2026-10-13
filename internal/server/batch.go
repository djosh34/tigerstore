package server

import (
	"slices"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http2"
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
	st.reqSize = len(st.body)
	st.body = nil
	c.batch.ops = append(c.batch.ops, op)
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
	if len(b.ops) == 0 {
		return false
	}
	if len(b.ops) >= maxBatchOps || b.bytes >= maxBatchBytes || c.br.Buffered() < frameHeaderLen {
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
	b := c.batch
	c.batch = opBatch{}
	c.mu.Lock()
	// A call that ended at its deadline before this does not go out.
	keep := 0
	for i, tag := range b.tags {
		st := tag.(*stream)
		if st.done.Load() || st.reset {
			continue
		}
		b.ops[keep], b.tags[keep] = b.ops[i], tag
		keep++
		c.chargeLocked(&st.reqHeld, st.reqSize+callCost)
		c.atNode[st] = struct{}{}
	}
	c.mu.Unlock()
	clear(b.ops[keep:])
	clear(b.tags[keep:])
	b.ops, b.tags = b.ops[:keep], b.tags[:keep]
	if keep == 0 {
		return nil
	}
	err := c.s.n.SubmitBatch(c.ctx, b.ops, b.tags, c)
	if err == nil {
		return nil
	}
	if c.ctx.Err() != nil {
		return err
	}
	// The node stopped; it takes no more requests.
	c.mu.Lock()
	for _, tag := range b.tags {
		c.releaseReqLocked(tag.(*stream))
	}
	c.mu.Unlock()
	for _, tag := range b.tags {
		tag.(*stream).finish(toStatus(err))
	}
	return nil
}

// A call holds two charges against maxHeld: its request bytes plus
// callCost from when the reader hands it to the node until the node
// answers (reqHeld), and its reply bytes from then until the reply is
// sent or the call is reset (replyHeld).

// chargeLocked adds n bytes to one of a call's charges. c.mu is held.
func (c *conn) chargeLocked(charge *int, n int) {
	*charge += n
	c.held += n
}

// releaseLocked frees one of a call's charges and credits back the
// connection's receive window if that waited for it. c.mu is held.
func (c *conn) releaseLocked(charge *int) {
	c.held -= *charge
	*charge = 0
	c.creditLocked()
}

// releaseReqLocked frees a call's request charge once the node answered
// or stopped. c.mu is held.
func (c *conn) releaseReqLocked(st *stream) {
	delete(c.atNode, st)
	c.releaseLocked(&st.reqHeld)
}

// creditLocked sends a connection window update once enough was received
// and the connection's calls, and the replies waiting for the writer,
// hold less than maxHeld. c.mu is held.
func (c *conn) creditLocked() {
	if c.recvCredit >= connWindow/4 && c.held+int(c.waitingReplies.Load()) <= maxHeld {
		c.addCtrl(ctrlFrame{kind: ctrlWindowUpdate, val: uint32(c.recvCredit)})
		c.recvCredit = 0
	}
}

// completed is one Complete call's replies in a list the committer pushes
// onto without a lock, with an estimate of their encoded size.
type completed struct {
	cs    []node.Completion
	bytes int64
	next  *completed
}

// replyEstimate is the size counted for a reply that has no message, an
// error.
const replyEstimate = 64

// estimate is the size a reply will take once encoded.
func estimate(cp *node.Completion) int64 {
	if cp.Resp == nil {
		return replyEstimate
	}
	return int64(proto.Size(cp.Resp))
}

// Complete takes replies from the node's committer. It pushes them onto a
// lock-free list, so the committer never waits, counts their size against
// the connection's budget, and wakes the writer, which encodes them.
func (c *conn) Complete(cs []node.Completion) {
	n := &completed{cs: cs}
	for i := range cs {
		n.bytes += estimate(&cs[i])
	}
	c.waitingReplies.Add(n.bytes)
	for {
		n.next = c.completions.Load()
		if c.completions.CompareAndSwap(n.next, n) {
			break
		}
	}
	c.signal()
}

// takeCompletions returns the replies the node queued, oldest first,
// and their estimated size, which still counts as waiting until complete
// has charged them to their calls.
func (c *conn) takeCompletions() ([][]node.Completion, int64) {
	var out [][]node.Completion
	var bytes int64
	for n := c.completions.Swap(nil); n != nil; n = n.next {
		out = append(out, n.cs)
		bytes += n.bytes
	}
	slices.Reverse(out)
	return out, bytes
}

// completeWaiting encodes and queues the replies the node handed over.
func (c *conn) completeWaiting() {
	if cs, bytes := c.takeCompletions(); cs != nil {
		c.complete(cs, bytes)
	}
}

// finished is one encoded reply, or an error, for a stream.
type finished struct {
	st  *stream
	msg []byte
	err error
}

// complete encodes the replies the node handed over and queues them on
// their streams, taking the lock once.
// waiting is their estimated size, released once each reply is charged
// to its call.
func (c *conn) complete(batches [][]node.Completion, waiting int64) {
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
		c.releaseReqLocked(f.st)
		if f.st.reset || f.st.done.Swap(true) {
			// Reset by the client or ended at its deadline: the reply is
			// dropped.
			continue
		}
		c.chargeLocked(&f.st.replyHeld, len(f.msg))
		var s *status.Status
		if f.err != nil {
			s = status.Convert(toStatus(f.err))
		}
		f.st.pushLocked(f.msg, true, s)
	}
	c.waitingReplies.Add(-waiting)
	c.creditLocked()
	c.mu.Unlock()
}

// nodeStopped fails every call the node still holds once it stopped. The
// node's last replies came in Complete calls before it stopped, so they
// are encoded first.
func (c *conn) nodeStopped() {
	c.completeWaiting()
	c.mu.Lock()
	// The node answers none of these; calls that ended meanwhile, at a
	// deadline or a reset, still had their request charge.
	var held []*stream
	for st := range c.atNode {
		c.releaseReqLocked(st)
		held = append(held, st)
	}
	c.mu.Unlock()
	for _, st := range held {
		st.finish(rpctypes.ErrGRPCStopped)
	}
}
