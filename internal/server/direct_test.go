package server

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/node"
)

// capture is a socket that takes and keeps every write, with nothing to
// read.
type capture struct{ bytes.Buffer }

func (s *capture) Read([]byte) (int, error) { return 0, loop.ErrWouldBlock }
func (s *capture) Close() error             { return nil }

// directConn is a connection without TLS, on the loop, whose socket keeps
// what it is sent; its first frames are written and dropped.
func directConn(t *testing.T) (*conn, *capture) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	br := loop.NewBridge(a, ioBuffer)
	sock := &capture{}
	br.Attach(sock, nil, nil)
	c := newConn(bareServer(t), br, br)
	c.sendWin = 1 << 30
	if _, err := c.write(); err != nil {
		t.Fatal(err)
	}
	sock.Reset()
	return c, sock
}

// directCall is a call of method that the node has, whose client ended
// its side.
func directCall(c *conn, id uint32, method string) *stream {
	st := &stream{c: c, id: id, rpc: methods[method], sendWin: 1 << 30, inEOF: true, reqSize: 100}
	c.mu.Lock()
	c.streams[id] = st
	c.chargeLocked(&st.reqHeld, st.reqSize+callCost)
	c.atNode[st] = struct{}{}
	c.mu.Unlock()
	return st
}

// sent is what a stream got on the wire.
type sent struct {
	headers, trailers []byte
	data              []byte
	dataFrames        int
	ended, rst        bool
}

// readSent reads the frames in b, by stream, and the types of the frames
// in their order.
func readSent(t *testing.T, b []byte) (map[uint32]*sent, []http2.FrameType) {
	t.Helper()
	fr := http2.NewFramer(nil, bytes.NewReader(b))
	got := map[uint32]*sent{}
	var order []http2.FrameType
	for {
		f, err := fr.ReadFrame()
		if errors.Is(err, io.EOF) {
			return got, order
		}
		if err != nil {
			t.Fatal(err)
		}
		order = append(order, f.Header().Type)
		id := f.Header().StreamID
		if id != 0 && got[id] == nil {
			got[id] = &sent{}
		}
		s := got[id]
		switch f := f.(type) {
		case *http2.HeadersFrame:
			if s.ended {
				t.Fatalf("stream %d: a header block after its end", id)
			}
			if f.StreamEnded() {
				s.trailers, s.ended = append([]byte(nil), f.HeaderBlockFragment()...), true
			} else {
				s.headers = append([]byte(nil), f.HeaderBlockFragment()...)
			}
		case *http2.DataFrame:
			s.data = append(s.data, f.Data()...)
			s.dataFrames++
		case *http2.RSTStreamFrame:
			if !s.ended || f.ErrCode != http2.ErrCodeNo {
				t.Fatalf("stream %d: RST_STREAM %v, after its end %v", id, f.ErrCode, s.ended)
			}
			s.rst = true
		}
	}
}

// replies are a reply of each kind the writer encodes for the node's
// unary calls, by method.
var replies = []struct {
	method string
	cp     node.Completion
}{
	{"/etcdserverpb.KV/Put", node.Completion{Rev: 5, Term: 2}},
	{"/etcdserverpb.KV/Put", node.Completion{Rev: 1 << 40, Term: 3, PrevKv: &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("old"), ModRevision: 4, Lease: 7}}},
	{"/etcdserverpb.KV/Range", node.Completion{Term: 2, Range: rangeOf(7, true, 9, mvcc.KV{Key: []byte("a"), Value: []byte("1"), CreateRevision: 2, ModRevision: 3, Version: 1}, mvcc.KV{Key: []byte("b"), ModRevision: 7, Lease: 3})}},
	{"/etcdserverpb.KV/Range", node.Completion{Term: 2, Range: rangeOf(7, false, 0)}},
	{"/etcdserverpb.KV/Txn", node.Completion{Resp: &pb.TxnResponse{
		Header:    &pb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 9, RaftTerm: 2},
		Succeeded: true,
		Responses: []*pb.ResponseOp{
			{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 9}}}},
			{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 9}, Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v"), ModRevision: 9}}, Count: 1}}},
			{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Deleted: 1}}},
			{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: &pb.TxnResponse{Succeeded: true}}},
		},
	}}},
	{"/etcdserverpb.KV/DeleteRange", node.Completion{Resp: &pb.DeleteRangeResponse{Header: &pb.ResponseHeader{Revision: 3}, Deleted: 1, PrevKvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v")}}}}},
}

func rangeOf(rev int64, more bool, count int64, kvs ...mvcc.KV) mvcc.RangeResult {
	res := mvcc.RangeResult{Rev: rev, KVs: kvs, More: more, Count: count, N: int64(len(kvs))}
	res.Fields = fieldsOf(kvs)
	return res
}

// TestRepliesWrittenInPlace writes a reply of each kind straight into the
// output: the frames carry the message the queue would have, the call
// ends and holds nothing, and its stream is kept for the next call.
func TestRepliesWrittenInPlace(t *testing.T) {
	for i, r := range replies {
		c, sock := directConn(t)
		st := directCall(c, 1, r.method)
		cp := r.cp
		cp.Tag = st
		queuedForm := finished{st: st}
		c.encode(&queuedForm, &cp)
		if queuedForm.err != nil {
			t.Fatal(queuedForm.err)
		}
		size, ok := c.directSize(st, &cp)
		if !ok || size != len(queuedForm.msg) {
			t.Fatalf("reply %d (%s): sized %d, %v, but encodes to %d bytes", i, r.method, size, ok, len(queuedForm.msg))
		}
		c.Complete([]node.Completion{cp})
		cs, waiting := c.takeCompletions()
		queued, err := c.complete(cs, waiting, maxTake)
		if err != nil || queued {
			t.Fatalf("reply %d (%s): queued %v, %v", i, r.method, queued, err)
		}
		if err := c.flushBridge(); err != nil {
			t.Fatal(err)
		}
		got, _ := readSent(t, sock.Bytes())
		s := got[1]
		if s == nil || !bytes.Equal(s.headers, grpcHeaders) || !bytes.Equal(s.data, queuedForm.msg) || s.dataFrames != 1 || !bytes.Equal(s.trailers, okTrailers) || s.rst {
			t.Fatalf("reply %d (%s): the stream got %+v, want the message %x", i, r.method, s, queuedForm.msg)
		}
		if !st.ended || c.streams[1] != nil || len(c.free) != 1 || c.held != 0 || c.waitingReplies.Load() != 0 || len(c.atNode) != 0 {
			t.Fatalf("reply %d (%s): ended %v, listed %v, %d free, %d held, %d waiting", i, r.method, st.ended, c.streams[1] != nil, len(c.free), c.held, c.waitingReplies.Load())
		}
		if c.sendWin != 1<<30-int64(size) {
			t.Fatalf("reply %d (%s): the send window is %d, want %d", i, r.method, c.sendWin, 1<<30-int64(size))
		}
	}
}

// TestRepliesQueuedInstead checks the replies that cannot go straight into
// the output: each is queued on its stream, and goes out whole in the same
// write pass when the windows and the pass allow.
func TestRepliesQueuedInstead(t *testing.T) {
	put := node.Completion{Rev: 5, Term: 2}
	big := node.Completion{Term: 2, Range: rangeOf(3, false, 0, mvcc.KV{Key: []byte("k"), Value: make([]byte, 20<<10)})}
	for _, tc := range []struct {
		name   string
		method string
		cp     node.Completion
		setup  func(c *conn, st *stream)
		budget int
		// frames is how many DATA frames the reply takes.
		frames int
	}{
		{name: "larger than a record", method: "/etcdserverpb.KV/Range", cp: big, budget: maxTake, frames: 2},
		{name: "compressed", method: "/etcdserverpb.KV/Put", cp: put, setup: func(_ *conn, st *stream) { st.encoding = gzipName }, budget: maxTake, frames: 1},
		{name: "past the pass's budget", method: "/etcdserverpb.KV/Put", cp: put, budget: 50, frames: 1},
		{name: "past the stream's window", method: "/etcdserverpb.KV/Put", cp: put, setup: func(_ *conn, st *stream) { st.sendWin = 4 }, budget: maxTake, frames: 1},
		{name: "past the connection's window", method: "/etcdserverpb.KV/Put", cp: put, setup: func(c *conn, _ *stream) { c.sendWin = 4 }, budget: maxTake, frames: 1},
	} {
		c, sock := directConn(t)
		st := directCall(c, 1, tc.method)
		if tc.setup != nil {
			tc.setup(c, st)
		}
		cp := tc.cp
		cp.Tag = st
		c.Complete([]node.Completion{cp})
		cs, waiting := c.takeCompletions()
		queued, err := c.complete(cs, waiting, tc.budget)
		if err != nil || !queued || len(st.out) == 0 {
			t.Fatalf("%s: queued %v with %d items, %v", tc.name, queued, len(st.out), err)
		}
		if sock.Len() != 0 || c.bridge.Pending() != 0 {
			t.Fatalf("%s: %d bytes written straight out", tc.name, c.bridge.Pending())
		}
		c.mu.Lock()
		if st.sendWin < 1<<20 || c.sendWin < 1<<20 {
			// The client credits the windows back.
			st.sendWin, c.sendWin = 1<<30, 1<<30
		}
		c.mu.Unlock()
		if _, err := c.write(); err != nil {
			t.Fatal(err)
		}
		got, _ := readSent(t, sock.Bytes())
		s := got[1]
		if s == nil || !s.ended || s.dataFrames < tc.frames || c.held != 0 || c.streams[1] != nil {
			t.Fatalf("%s: the stream got %+v, %d held", tc.name, s, c.held)
		}
	}
}

// TestRepliesInPlaceAfterControl checks the order of a pass: a control
// frame and a stream's output queued before the node's replies go out
// before them, and a reply to a client that did not end its side is
// followed by a RST_STREAM.
func TestRepliesInPlaceAfterControl(t *testing.T) {
	c, sock := directConn(t)
	watch := directCall(c, 1, "/etcdserverpb.KV/Range")
	c.mu.Lock()
	watch.pushLocked([]byte{0, 0, 0, 0, 1, 7}, false, nil)
	c.addCtrl(ctrlFrame{kind: ctrlPingAck})
	c.mu.Unlock()
	st := directCall(c, 3, "/etcdserverpb.KV/Put")
	st.inEOF = false
	c.Complete([]node.Completion{{Tag: st, Rev: 5, Term: 2}})
	if _, err := c.write(); err != nil {
		t.Fatal(err)
	}
	got, order := readSent(t, sock.Bytes())
	want := []http2.FrameType{http2.FramePing, http2.FrameHeaders, http2.FrameData, http2.FrameHeaders, http2.FrameData, http2.FrameHeaders, http2.FrameRSTStream}
	if !slices.Equal(order, want) {
		t.Fatalf("frames %v, want %v", order, want)
	}
	if s := got[3]; s == nil || !s.ended || !s.rst || got[1].ended {
		t.Fatalf("the reply's stream got %+v, the other %+v", s, got[1])
	}
	if len(st.out) != 0 || st.listed {
		t.Fatal("the reply was queued")
	}
}

// TestRepliesInPlaceAllocate checks that a put's reply written straight
// into the output allocates less than a queued one: neither the stream's
// queue nor the message gets a buffer of its own.
func TestRepliesInPlaceAllocate(t *testing.T) {
	allocs := func(direct bool) float64 {
		c, sock := directConn(t)
		if !direct {
			c.direct = nil
		}
		st := &stream{}
		return testing.AllocsPerRun(100, func() {
			*st = stream{c: c, id: 1, rpc: methods["/etcdserverpb.KV/Put"], sendWin: 1 << 20, inEOF: true}
			c.mu.Lock()
			c.streams[1] = st
			c.atNode[st] = struct{}{}
			c.mu.Unlock()
			c.free = c.free[:0]
			c.Complete([]node.Completion{{Tag: st, Rev: 5, Term: 2}})
			if _, err := c.write(); err != nil {
				t.Fatal(err)
			}
			sock.Reset()
		})
	}
	in, queued := allocs(true), allocs(false)
	if in > queued-2 {
		t.Fatalf("a reply written in place allocates %v times, a queued one %v", in, queued)
	}
}

// TestReplySizesMatch encodes random replies of each kind in place and
// queued: directSize is the size of the queued message, and the bytes are
// the same, or the reply does not go in place.
func TestReplySizesMatch(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	num := func() int64 {
		if r.IntN(4) == 0 {
			return 0
		}
		return r.Int64N(1 << (1 + r.IntN(62)))
	}
	bytesOf := func() []byte {
		b := make([]byte, r.IntN(1+r.IntN(3000)))
		for i := range b {
			b[i] = byte(r.IntN(256))
		}
		return b
	}
	kv := func() *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: bytesOf(), Value: bytesOf(), CreateRevision: num(), ModRevision: num(), Version: num(), Lease: num()}
	}
	header := func() *pb.ResponseHeader {
		return &pb.ResponseHeader{ClusterId: uint64(num()), MemberId: uint64(num()), Revision: num(), RaftTerm: uint64(num())}
	}
	c, _ := directConn(t)
	c.cluster, c.member = uint64(num()), uint64(num())
	inPlace := 0
	for i := range 3000 {
		var method string
		var cp node.Completion
		switch i % 5 {
		case 0:
			method = "/etcdserverpb.KV/Put"
			cp = node.Completion{Rev: num(), Term: uint64(num())}
			if r.IntN(2) == 0 {
				cp.PrevKv = kv()
			}
		case 1:
			method = "/etcdserverpb.KV/Range"
			var kvs []mvcc.KV
			for range r.IntN(5) {
				m := kv()
				kvs = append(kvs, mvcc.KV{Key: m.Key, Value: m.Value, CreateRevision: m.CreateRevision, ModRevision: m.ModRevision, Version: m.Version, Lease: m.Lease})
			}
			cp = node.Completion{Term: uint64(num()), Range: rangeOf(num(), r.IntN(2) == 0, num(), kvs...)}
		case 2:
			method = "/etcdserverpb.KV/Txn"
			tr := &pb.TxnResponse{Header: header(), Succeeded: r.IntN(2) == 0}
			for range r.IntN(4) {
				switch r.IntN(4) {
				case 0:
					tr.Responses = append(tr.Responses, &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: header(), Kvs: []*mvccpb.KeyValue{kv()}, Count: num(), More: r.IntN(2) == 0}}})
				case 1:
					tr.Responses = append(tr.Responses, &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: header(), PrevKv: kv()}}})
				case 2:
					tr.Responses = append(tr.Responses, &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Header: header(), Deleted: num()}}})
				default:
					tr.Responses = append(tr.Responses, &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: &pb.TxnResponse{Succeeded: true}}})
				}
			}
			cp = node.Completion{Resp: tr}
		case 3:
			method = "/etcdserverpb.KV/DeleteRange"
			cp = node.Completion{Resp: &pb.DeleteRangeResponse{Header: header(), Deleted: num(), PrevKvs: []*mvccpb.KeyValue{kv()}}}
		default:
			// Other replies are encoded by protobuf, which can fail: they
			// are queued.
			method = "/etcdserverpb.Lease/LeaseGrant"
			cp = node.Completion{Resp: &pb.LeaseGrantResponse{Header: header(), ID: num(), TTL: num()}}
		}
		st := &stream{c: c, id: 1, rpc: methods[method]}
		queuedForm := finished{st: st}
		c.encode(&queuedForm, &cp)
		if queuedForm.err != nil {
			t.Fatal(queuedForm.err)
		}
		size, ok := c.directSize(st, &cp)
		if !ok {
			if method != "/etcdserverpb.Lease/LeaseGrant" && directFrames(int64(len(queuedForm.msg)), true) <= loop.MaxReserve {
				t.Fatalf("reply %d (%s) of %d bytes does not go in place", i, method, len(queuedForm.msg))
			}
			continue
		}
		inPlace++
		b, err := c.appendReply(make([]byte, 0, size), st, &cp)
		if err != nil || size != len(queuedForm.msg) || !bytes.Equal(b, queuedForm.msg) {
			t.Fatalf("reply %d (%s): sized %d, encoded in place to %x (%v), queued to %x", i, method, size, b, err, queuedForm.msg)
		}
	}
	if inPlace < 1000 {
		t.Fatalf("only %d of 3000 replies went in place", inPlace)
	}
	if size, ok := c.directSize(&stream{rpc: methods["/etcdserverpb.Lease/LeaseGrant"]}, &node.Completion{Resp: &pb.LeaseGrantResponse{Error: "\xff"}}); ok {
		t.Fatalf("a LeaseGrant reply, whose encoding can fail, goes in place at %d bytes", size)
	}
}
