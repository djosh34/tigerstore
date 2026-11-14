package server

import (
	"encoding/binary"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
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
	req := core.Request{Caller: c.callerOf(st)}
	if st.rpc.wirePut {
		// The put stays in its wire form; the node copies it into the
		// Raft entry as it is.
		put, err := core.ParsePut(st.body)
		if err != nil {
			st.finish(unmarshalError(err))
			return
		}
		req.Put = put
	} else {
		// The body goes into the Raft entry as it is if the request is a
		// write; apply decodes the entry again without copies. Here the
		// request is decoded to check it, by the fastest decoder for its
		// type, into messages from the connection's slabs where it decodes
		// many. The decoded request aliases the body, which the call holds
		// until the node answers it (arena.go).
		req.Wire = st.body
		var err error
		switch {
		case st.rpc == txnMethod:
			req.Op = c.slabs.Txn()
			err = core.DecodeTxn(&req, &c.slabs)
		case st.rpc == rangeMethod:
			req.Op = c.slabs.Range()
			err = core.DecodeAlias(st.body, req.Op)
		case st.rpc.alias:
			req.Op = st.rpc.op()
			err = core.DecodeAlias(st.body, req.Op)
		default:
			req.Op = st.rpc.op()
			err = proto.Unmarshal(st.body, req.Op)
		}
		if err != nil {
			st.finish(unmarshalError(err))
			return
		}
		switch r := req.Op.(type) {
		case *pb.RangeRequest:
			if st.rpc.learner == learnerSerializable && !r.Serializable && c.s.learner() {
				st.finish(rpctypes.ErrGRPCNotSupportedForLearner)
				return
			}
		case *pb.TxnRequest:
			st.readonlyTxn = readonlyTxn(r)
		}
	}
	c.startOpSpan(st, &req)
	// A timed call's start serves as its admission: one clock read less
	// per call.
	st.admitted = st.began
	if st.admitted.IsZero() {
		st.admitted = time.Now()
	}
	st.reqSize = len(st.body)
	st.body = nil
	c.batch.reqs = append(c.batch.reqs, req)
	c.batch.tags = append(c.batch.tags, st)
	c.batch.bytes += st.reqSize
}

// callerOf is the caller of a call on the connection. etcd's
// AuthInfoFromTLS ignores the certificate of a request from its gRPC
// gateway.
func (c *conn) callerOf(st *stream) core.Caller {
	caller := c.caller
	if st.gateway {
		caller = core.Caller{}
	}
	caller.Token = st.token
	if t := c.s.cfg.JWT; t != nil && caller.Token != "" {
		// A token that does not verify leaves the caller without a user,
		// which the node answers as an invalid token.
		caller.Verified = true
		if user, rev, err := t.info(caller.Token); err == nil {
			caller.User, caller.Revision = user, rev
		}
	}
	return caller
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
		b.reqs[keep], b.tags[keep] = b.reqs[i], tag
		keep++
		c.chargeLocked(&st.reqHeld, st.reqSize+callCost)
		c.atNode[st] = struct{}{}
	}
	c.mu.Unlock()
	clear(b.reqs[keep:])
	clear(b.tags[keep:])
	b.reqs, b.tags = b.reqs[:keep], b.tags[:keep]
	if keep == 0 {
		return nil
	}
	err := c.s.n.SubmitLocal(b.reqs, b.tags, c)
	if err == nil {
		c.mu.Lock()
		for i, tag := range b.tags {
			st := tag.(*stream)
			st.nodeID = b.reqs[i].ID
			// A call that ended while the node took the batch.
			if st.reset || st.done.Load() {
				c.cancelLocked(st)
			}
		}
		c.mu.Unlock()
		return nil
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

// cancelLocked tells the node that a call it runs ended early, reset or
// past its deadline, so that it drops the request if it waits for a
// leader, as etcd drops a proposal whose context ends. c.mu is held.
func (c *conn) cancelLocked(st *stream) {
	if _, ok := c.atNode[st]; ok && st.nodeID != 0 {
		c.s.n.Cancel(st.nodeID)
	}
}

// releaseReqLocked frees a call's request charge, and its request, once
// the node answered or stopped. c.mu is held.
func (c *conn) releaseReqLocked(st *stream) {
	delete(c.atNode, st)
	c.releaseLocked(&st.reqHeld)
	c.releaseBody(st)
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

// replyEstimate is the size counted for a reply that is an error.
const replyEstimate = 64

// estimate is the size a reply will take once encoded. A range answer,
// a piece of any answer, or a put in wire form counts its KeyValues, if
// any, and a header at its largest.
func estimate(cp *node.Completion) int64 {
	switch {
	case cp.Err != nil:
		return replyEstimate
	case cp.Resp != nil:
		if t, ok := cp.Resp.(*pb.TxnResponse); ok {
			return 5 + int64(txnRespSize(t))
		}
		return int64(proto.Size(cp.Resp))
	}
	return 5 + headerBound + fieldsOf(cp.Range.KVs) + 2 + 1 + binary.MaxVarintLen64
}

// Complete takes replies from the node's committer, on the loop. It
// pushes them onto a list, counts their size against the connection's
// budget, and lists the connection to flush at the end of the turn, which
// encodes and writes them.
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
	c.s.loop.Dirty(c)
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

// finished is one encoded reply, or an error, for a stream: a whole
// reply, or a piece of one (core.Reply.MorePieces). more says that pieces
// follow it, and cont that it follows a piece.
type finished struct {
	st         *stream
	msg        []byte
	err        error
	more, cont bool
	// pieces is set for the first piece of an answer in pieces.
	pieces bool
	// granted is the ID of a lease the node granted, for its span.
	granted int64
}

// complete encodes the replies the node handed over and queues them on
// their streams, taking the lock once.
// waiting is their estimated size, released once each reply is charged
// to its call.
func (c *conn) complete(batches [][]node.Completion, waiting int64) {
	n := 0
	for _, cs := range batches {
		n += len(cs)
	}
	out := make([]finished, 0, n)
	now := time.Now()
	for _, cs := range batches {
		for _, cp := range cs {
			st := cp.Tag.(*stream)
			f := finished{st: st, err: cp.Err, more: cp.MorePieces, cont: st.holes != nil}
			f.pieces = f.more && !f.cont
			if !f.cont {
				c.logSlow(st, now.Sub(st.admitted))
			}
			if g, ok := cp.Resp.(*pb.LeaseGrantResponse); ok {
				f.granted = g.ID
			}
			switch {
			case f.err != nil:
			case f.cont:
				f.msg = fillHole(st, cp.Range.KVs)
			case st.rpc.wirePut:
				// One buffer per reply: a slow stream keeps only its own
				// reply alive, which its reply charge counts.
				msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &cp.Reply)
				if f.err = err; err == nil {
					f.msg = msg
				}
			case st.rpc.compact && cp.Resp == nil:
				f.msg, f.err = c.rangeReply(st, &cp.Reply)
			default:
				switch r := cp.Resp.(type) {
				case *pb.TxnResponse:
					f.msg, st.holes, f.err = appendTxnPieces(r, cp.Rests)
				case *pb.DeleteRangeResponse:
					if len(cp.Rests) > 0 {
						f.msg, st.holes, f.err = appendDeletePieces(r, cp.Rests)
						break
					}
					f.msg, f.err = marshal(r)
				default:
					f.msg, f.err = marshal(cp.Resp)
				}
			}
			if f.err == nil && !f.more && !f.cont {
				// A message in pieces goes out uncompressed, as gRPC
				// allows any message to.
				f.msg, f.err = st.compress(f.msg)
			}
			if f.err != nil && f.more {
				// The node keeps the rest, which nobody takes now.
				c.s.n.Cancel(st.nodeID)
				f.more = false
			}
			out = append(out, f)
		}
		clear(cs)
	}
	var broken, next []*stream
	c.mu.Lock()
	for _, f := range out {
		st := f.st
		if !f.cont {
			if f.pieces {
				// The node reads the request for the pieces to come.
				c.abandonBody(st)
			}
			c.releaseReqLocked(st)
		}
		end := !f.more && f.err == nil
		if st.reset || (end && st.done.Swap(true)) || (!end && st.done.Load()) {
			// Reset by the client or ended at its deadline: the reply is
			// dropped, and the rest of an answer in pieces with it.
			if f.more {
				c.s.n.Cancel(st.nodeID)
			}
			continue
		}
		if f.cont && f.err != nil {
			// Part of the message went out: the stream can only break.
			broken = append(broken, st)
			continue
		}
		c.releaseLocked(&st.replyHeld)
		c.chargeLocked(&st.replyHeld, len(f.msg))
		if st.span != nil && f.granted != 0 {
			st.span.SetAttributes(attribute.Int64("id", f.granted))
		}
		if f.more {
			if st.space == nil {
				st.space = make(chan struct{}, 1)
			}
			st.pushLocked(f.msg, false, nil)
			next = append(next, st)
			continue
		}
		var s *status.Status
		if f.err != nil {
			s = status.Convert(toStatus(f.err))
		}
		st.pushLocked(f.msg, true, s)
	}
	c.waitingReplies.Add(-waiting)
	c.creditLocked()
	c.mu.Unlock()
	for _, st := range broken {
		c.resetStream(st.id, http2.ErrCodeInternal)
	}
	for _, st := range next {
		go c.nextPiece(st)
	}
}

// rangeReply encodes the answer of a Range: whole, or, if pieces
// follow, its first piece, with the message's prefix and header, and a
// hole for the rest (holes).
func (c *conn) rangeReply(st *stream, r *core.Reply) ([]byte, error) {
	if !r.MorePieces {
		return appendRangeReply(c.cluster, c.member, r.Term, &r.Range)
	}
	b, err := appendRangeStart(nil, c.cluster, c.member, r.Term, &r.Range)
	if err != nil {
		return nil, err
	}
	st.holes = []hole{{num: 2, left: r.Range.N - int64(len(r.Range.KVs)), after: appendRangeEnd(nil, &r.Range)}}
	return appendKVs(b, 2, r.Range.KVs), nil
}

// fillHole encodes a piece of an answer: the KeyValues of the stream's
// first hole, and once they are all out, what the message holds after
// them.
func fillHole(st *stream, kvs []mvcc.KV) []byte {
	h := &st.holes[0]
	b := appendKVs(make([]byte, 0, fieldsOf(kvs)+int64(len(h.after))), h.num, kvs)
	if h.left -= int64(len(kvs)); h.left <= 0 {
		b = append(b, h.after...)
		st.holes = st.holes[1:]
	}
	return b
}

// nextPiece asks the node for the next piece of st's answer once the
// writer took most of the last, so a slow client holds back the work and
// the node holds one piece of the answer at a time.
func (c *conn) nextPiece(st *stream) {
	for {
		c.mu.Lock()
		gone, ready := st.reset || c.closed, st.queued <= c.s.n.PieceBytes()/2
		c.mu.Unlock()
		switch {
		case gone:
			c.s.n.Cancel(st.nodeID)
			return
		case ready:
			c.s.n.Next(st.nodeID)
			return
		}
		select {
		case <-st.space:
		case <-c.ctx.Done():
			c.s.n.Cancel(st.nodeID)
			return
		}
	}
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

// appendPutReply appends the gRPC message of the response to a put in
// wire form, without building a protobuf message.
func (c *conn) appendPutReply(b []byte, r *core.Reply) ([]byte, error) {
	start := len(b)
	b = append(b, 0, 0, 0, 0, 0)
	b, err := appendPutResponse(b, c.cluster, c.member, r.Rev, r.Term, r.PrevKv)
	if err != nil {
		return b[:start], status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	binary.BigEndian.PutUint32(b[start+1:start+5], uint32(len(b)-start-5))
	return b, nil
}

// putResponseSize is the most appendPutResponse appends without a
// previous key: the header field with four varints.
const putResponseSize = 2 + 4*(1+binary.MaxVarintLen64)

// appendPutResponse appends a PutResponse in its wire form: the header
// with the cluster, member, revision and term, and the previous key if
// there is one.
func appendPutResponse(b []byte, cluster, member uint64, rev int64, term uint64, prev *mvccpb.KeyValue) ([]byte, error) {
	b = appendHeader(b, cluster, member, rev, term)
	if prev == nil {
		return b, nil
	}
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(proto.Size(prev)))
	return proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, prev)
}

// appendHeader appends field 1 of a response, its ResponseHeader.
func appendHeader(b []byte, cluster, member uint64, rev int64, term uint64) []byte {
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(headerSize(cluster, member, rev, term)))
	for i, v := range [...]uint64{cluster, member, uint64(rev), term} {
		if v != 0 {
			b = protowire.AppendTag(b, protowire.Number(i+1), protowire.VarintType)
			b = protowire.AppendVarint(b, v)
		}
	}
	return b
}

func headerSize(cluster, member uint64, rev int64, term uint64) int {
	n := 0
	for _, v := range [...]uint64{cluster, member, uint64(rev), term} {
		if v != 0 {
			n += 1 + protowire.SizeVarint(v)
		}
	}
	return n
}

// rangeSize is the size of the RangeResponse of res: its header, the
// KeyValue fields of the whole answer (res.Fields), more and count.
func rangeSize(cluster, member uint64, term uint64, res *mvcc.RangeResult) int64 {
	size := int64(1+protowire.SizeBytes(headerSize(cluster, member, res.Rev, term))) + res.Fields
	if res.More {
		size += 2
	}
	if res.Count != 0 {
		size += int64(1 + protowire.SizeVarint(uint64(res.Count)))
	}
	return size
}

// headerBound is the most the header field of a response takes.
const headerBound = 1 + 1 + 4*(1+binary.MaxVarintLen64)

// fieldsOf is the size of kvs as KeyValue fields.
func fieldsOf(kvs []mvcc.KV) int64 {
	n := int64(0)
	for i := range kvs {
		n += int64(kvs[i].FieldSize())
	}
	return n
}

// appendRangeReply encodes the gRPC message of a RangeResponse built from
// the index's values, whole: the value bytes are copied once, here, into
// the reply, with no KeyValue message per key.
func appendRangeReply(cluster, member uint64, term uint64, res *mvcc.RangeResult) ([]byte, error) {
	b, err := appendRangeStart(nil, cluster, member, term, res)
	if err != nil {
		return nil, err
	}
	return appendRangeEnd(appendKVs(b, 2, res.KVs), res), nil
}

// appendRangeStart appends the gRPC message prefix of the RangeResponse
// of res, uncompressed, and its header: what comes before its KeyValues.
// The message is sized from the whole answer, so its KeyValues can follow
// in pieces (core.Node.Next).
func appendRangeStart(b []byte, cluster, member uint64, term uint64, res *mvcc.RangeResult) ([]byte, error) {
	size := rangeSize(cluster, member, term, res)
	if size > 1<<31-1 {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
	}
	if b == nil {
		b = make([]byte, 0, 5+min(size, headerBound+fieldsOf(res.KVs)+12))
	}
	b = append(b, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(size))
	return appendHeader(b, cluster, member, res.Rev, term), nil
}

// appendRangeEnd appends what a RangeResponse holds after its KeyValues:
// more and count.
func appendRangeEnd(b []byte, res *mvcc.RangeResult) []byte {
	if res.More {
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	if res.Count != 0 {
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(res.Count))
	}
	return b
}

// appendKVs appends kvs as KeyValue fields num, those of a RangeResponse
// (2) or a DeleteRangeResponse (3).
func appendKVs(b []byte, num protowire.Number, kvs []mvcc.KV) []byte {
	for i := range kvs {
		kv := &kvs[i]
		b = protowire.AppendTag(b, num, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(kv.Size(len(kv.Value))))
		b = appendKV(b, kv)
	}
	return b
}

func appendKV(b []byte, kv *mvcc.KV) []byte {
	if len(kv.Key) > 0 {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, kv.Key)
	}
	for i, v := range [...]int64{kv.CreateRevision, kv.ModRevision, kv.Version} {
		if v != 0 {
			b = protowire.AppendTag(b, protowire.Number(i+2), protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(v))
		}
	}
	if len(kv.Value) > 0 {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, kv.Value)
	}
	if kv.Lease != 0 {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(kv.Lease))
	}
	return b
}

// readonlyTxn is etcd's IsTxnReadonly: a transaction whose branches only
// range. One with a nested transaction is not.
func readonlyTxn(r *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			if op.GetRequestRange() == nil {
				return false
			}
		}
	}
	return true
}
