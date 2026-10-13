package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// stream is one HTTP/2 stream: a gRPC call or an HTTP request.
type stream struct {
	c          *conn
	id         uint32
	ctx        context.Context // ends with the call: done, reset, timed out or connection closed
	cancel     context.CancelFunc
	rpc        *method // nil for HTTP requests
	needLeader bool    // etcd's "hasleader" metadata
	httpMethod string
	httpPath   string

	// Reader only: the gRPC message being received.
	hdr  [5]byte
	hdrN int
	msg  []byte
	// Reader only, until the unary handler starts: the request messages.
	body []byte
	msgs int

	// done is set once the server has queued the end of the stream; what
	// the client still sends is dropped.
	done atomic.Bool

	// Streaming calls only, capacity 1: a message or the client's end
	// arrived; the writer took queued output.
	inSignal chan struct{}
	space    chan struct{}
	// abort, if set by the handler, ends a Send that waits for space.
	abort <-chan struct{}
	// deadline ends the call at its grpc-timeout; guarded by c.mu.
	deadline *time.Timer

	// Guarded by c.mu.
	in            [][]byte // streaming calls: messages not yet taken by Recv
	inEOF         bool     // the client ended its side
	recvCredit    int      // stream bytes taken by Recv and not yet credited back
	recvUsed      int      // stream bytes received and not credited back
	sendWin       int64
	out           []outItem
	queued        int  // message bytes in out
	listed        bool // in c.ready
	headersQueued bool
	ended         bool // the writer took the end of the stream
	reset         bool // the stream or connection is gone; output is dropped
}

type outKind byte

const (
	outHeaders outKind = iota
	outData
	outTrailers
)

// outItem is one piece of a stream's output: a header block, data, or the
// trailer block that ends the stream.
type outItem struct {
	kind outKind
	b    []byte
	end  bool // for data: END_STREAM
}

var (
	errStreamGone = errors.New("server: stream is gone")
	errAborted    = errors.New("server: send aborted")
)

// A stream that requires a leader checks for one every election timeout
// (etcd's default 1 s) and ends after maxNoLeader checks in a row find
// none, as etcd's monitorLeader does for every stream.
const (
	leaderCheck = time.Second
	maxNoLeader = 3
)

// receive takes the payload of a DATA frame. Framer storage is reused by
// the next frame, so each message is copied once, into a buffer of its
// own size.
func (st *stream) receive(d []byte) error {
	for {
		if st.hdrN < len(st.hdr) {
			if len(d) == 0 {
				return nil
			}
			n := copy(st.hdr[st.hdrN:], d)
			st.hdrN += n
			d = d[n:]
			if st.hdrN < len(st.hdr) {
				return nil
			}
			if st.hdr[0] != 0 {
				return status.Error(codes.Unimplemented, "grpc: compressed messages are not supported")
			}
			size := binary.BigEndian.Uint32(st.hdr[1:])
			if size > maxRecvMsg {
				return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", size, maxRecvMsg)
			}
			st.msg = make([]byte, 0, size)
		}
		n := min(len(d), cap(st.msg)-len(st.msg))
		st.msg = append(st.msg, d[:n]...)
		d = d[n:]
		if len(st.msg) < cap(st.msg) {
			return nil
		}
		st.deliver(st.msg)
		st.msg, st.hdrN = nil, 0
	}
}

func (st *stream) deliver(m []byte) {
	if st.rpc.unary != nil {
		st.body = m
		st.msgs++
		return
	}
	c := st.c
	c.mu.Lock()
	st.in = append(st.in, m)
	c.mu.Unlock()
	select {
	case st.inSignal <- struct{}{}:
	default:
	}
}

// Recv takes the next request message of a streaming call. It returns
// io.EOF once the client ended its side.
func (st *stream) Recv(m proto.Message) error {
	c := st.c
	for {
		c.mu.Lock()
		if len(st.in) > 0 {
			b := st.in[0]
			st.in[0] = nil
			st.in = st.in[1:]
			st.recvCredit += len(b) + 5
			if st.recvCredit >= streamWindow/4 {
				c.addCtrl(ctrlFrame{kind: ctrlWindowUpdate, id: st.id, val: uint32(st.recvCredit)})
				st.recvUsed -= st.recvCredit
				st.recvCredit = 0
			}
			c.mu.Unlock()
			c.signal()
			if err := proto.Unmarshal(b, m); err != nil {
				return status.Errorf(codes.Internal, "grpc: failed to unmarshal the received message: %v", err)
			}
			return nil
		}
		eof := st.inEOF
		c.mu.Unlock()
		if eof {
			return io.EOF
		}
		select {
		case <-st.inSignal:
		case <-st.ctx.Done():
			return st.ctx.Err()
		}
	}
}

// Send queues one response message of a streaming call. It waits while
// the stream has too much output queued.
func (st *stream) Send(m proto.Message) error {
	b, err := marshal(m)
	if err != nil {
		return err
	}
	return st.push(b, false, nil, true)
}

// marshal encodes m behind a gRPC message prefix.
func marshal(m proto.Message) ([]byte, error) {
	size := proto.Size(m)
	if size > 1<<31-1 {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
	}
	b := make([]byte, 5, 5+size)
	binary.BigEndian.PutUint32(b[1:], uint32(size))
	b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	return b, nil
}

// reply ends a unary call with its response.
func (st *stream) reply(m proto.Message) {
	b, err := marshal(m)
	if err != nil {
		st.finish(err)
		return
	}
	if st.done.Swap(true) {
		return
	}
	_ = st.push(b, true, nil, false)
}

// finish ends the stream with err's status, or OK for nil. A second end
// is ignored.
func (st *stream) finish(err error) {
	if st.done.Swap(true) {
		return
	}
	var s *status.Status
	if err != nil {
		s = status.Convert(toStatus(err))
	}
	_ = st.push(nil, true, s, false)
}

// push queues a message (msg, with its prefix, if not nil) and the end of
// the stream (if end) with status s (nil for OK). With wait, it waits
// while the stream has more than sendQueue bytes queued.
func (st *stream) push(msg []byte, end bool, s *status.Status, wait bool) error {
	c := st.c
	for {
		c.mu.Lock()
		// After the end of the stream, which a deadline or a lost leader
		// can queue while the handler still runs, only the end itself
		// may be queued, never more messages.
		if st.reset || c.closed || (!end && st.done.Load()) {
			c.mu.Unlock()
			return errStreamGone
		}
		if wait && st.queued > 0 && st.queued+len(msg) > sendQueue {
			c.mu.Unlock()
			select {
			case <-st.space:
				continue
			case <-st.abort:
				return errAborted
			case <-st.ctx.Done():
				return st.ctx.Err()
			}
		}
		if msg != nil {
			if !st.headersQueued {
				st.out = append(st.out, outItem{kind: outHeaders, b: grpcHeaders})
				st.headersQueued = true
			}
			st.out = append(st.out, outItem{kind: outData, b: msg})
			st.queued += len(msg)
		}
		if end && s != nil && st.rpc != nil && st.rpc.stream != nil {
			// A stream that fails drops the replies its client has not
			// taken yet, so the status goes out without waiting for them.
			st.out = slices.DeleteFunc(st.out, func(it outItem) bool { return it.kind == outData })
			st.queued = 0
		}
		if end {
			var block []byte
			switch {
			case !st.headersQueued:
				block = statusBlock(s, true)
			case s == nil:
				block = okTrailers
			default:
				block = statusBlock(s, false)
			}
			st.headersQueued = true
			st.out = append(st.out, outItem{kind: outTrailers, b: block})
		}
		c.list(st)
		c.mu.Unlock()
		c.signal()
		return nil
	}
}

// pushHTTP queues a whole HTTP response.
func (st *stream) pushHTTP(headers, body []byte) {
	c := st.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if st.reset || c.closed {
		return
	}
	st.out = append(st.out, outItem{kind: outHeaders, b: headers}, outItem{kind: outData, b: body, end: true})
	st.queued += len(body)
	c.list(st)
	c.signal()
}

// runUnary runs a unary call once its request has arrived.
func (c *conn) runUnary(st *stream) {
	defer st.cancel()
	if st.msgs != 1 {
		st.finish(status.Errorf(codes.Internal, "grpc: unary call with %d request messages", st.msgs))
		return
	}
	if st.needLeader && c.s.n.Leader() == 0 {
		st.finish(rpctypes.ErrGRPCNoLeader)
		return
	}
	resp, err := st.rpc.unary(st.ctx, c.s, st.body)
	if err != nil {
		st.finish(err)
		return
	}
	st.reply(resp)
}

// runStream runs a streaming call from its first headers.
func (c *conn) runStream(st *stream) {
	defer st.cancel()
	if st.needLeader {
		if c.s.n.Leader() == 0 {
			st.finish(rpctypes.ErrGRPCNoLeader)
			return
		}
		go c.monitorLeader(st)
	}
	st.finish(st.rpc.stream(c.s, st))
}

// monitorLeader ends a stream that requires a leader once maxNoLeader
// checks in a row find none.
func (c *conn) monitorLeader(st *stream) {
	ticker := time.NewTicker(leaderCheck)
	defer ticker.Stop()
	noLeader := 0
	for {
		select {
		case <-st.ctx.Done():
			return
		case <-ticker.C:
		}
		if c.s.n.Leader() != 0 {
			noLeader = 0
			continue
		}
		if noLeader++; noLeader >= maxNoLeader {
			st.finish(rpctypes.ErrGRPCNoLeader)
			st.cancel()
			return
		}
	}
}

// expire ends a call whose grpc-timeout passed, whether or not its
// request is complete or its handler started.
func (st *stream) expire() {
	st.finish(status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()))
	if st.cancel != nil {
		st.cancel()
	}
}

// runHTTP answers an HTTP request on an HTTP/2 stream.
func (c *conn) runHTTP(st *stream) {
	defer st.cancel()
	st.done.Store(true)
	code, contentType, body := c.s.answerHTTP(st.ctx, st.httpMethod, st.httpPath)
	headers := httpHeaders(code, contentType, len(body))
	if st.httpMethod == http.MethodHead {
		body = ""
	}
	st.pushHTTP(headers, []byte(body))
}
