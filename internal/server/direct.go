package server

import (
	"encoding/binary"
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/node"
)

// A reply of the node to a put, a range, a Txn or a DeleteRange usually
// goes straight into the connection's output: its HEADERS, its one DATA
// frame and its trailers are encoded in place at the end of the TLS
// record being written (loop.Records.Reserve),
// or of the bridge's write buffer without TLS. It needs no queue of its
// own on the stream, and is not copied through the server's write buffer
// and again into the record. A reply goes there when it is whole and
// uncompressed, fits one record with its frames, and the stream has
// nothing queued, both send windows take it, and the write pass has room
// for it (direct). Any other reply is queued on its stream as before
// (stream.pushLocked), and the write pass takes it from there.

// directOut is an output that replies can be encoded into in place: the
// connection's TLS records, or its bridge without TLS.
type directOut interface {
	Reserve(n int) []byte
	Commit(n int)
}

// errReplySize is the error of a reply whose encoding did not take the
// size it was sized at; the connection then breaks, since its frames
// would not be whole.
var errReplySize = errors.New("server: a reply's encoding differs from its size")

// errOutputClosed is the error of a reply whose connection's output takes
// no more writes.
var errOutputClosed = errors.New("server: the connection's output is closed")

// directSize is the size of the gRPC message of cp, prefix included, and
// whether it may go straight into the connection's output: a whole,
// uncompressed reply that fits one record with its frames, of a put, a
// range, a Txn or a DeleteRange. Their encoding cannot fail, which
// protobuf's can for a message with a string field: once its frames are
// begun, a reply can only end its connection.
func (c *conn) directSize(st *stream, cp *node.Completion) (int, bool) {
	if c.direct == nil || cp.Err != nil || cp.MorePieces || len(cp.Rests) > 0 || st.holes != nil || st.encoding == gzipName {
		return 0, false
	}
	var size int64
	switch r := cp.Resp.(type) {
	case nil:
		switch {
		case st.rpc.wirePut:
			size = int64(putReplySize(c.cluster, c.member, &cp.Reply))
		case st.rpc.compact:
			size = rangeSize(c.cluster, c.member, cp.Term, &cp.Range)
		default:
			return 0, false
		}
	case *pb.TxnResponse:
		size = int64(txnRespSize(r))
	case *pb.DeleteRangeResponse:
		size = int64(proto.Size(r))
	default:
		return 0, false
	}
	size += 5
	if directFrames(size, true) > loop.MaxReserve {
		return 0, false
	}
	return int(size), true
}

// directFrames is what a reply whose message takes size bytes takes on
// the wire: its HEADERS, its DATA frame and its trailers, and with rst a
// RST_STREAM after them, for a client that has not ended its side.
func directFrames(size int64, rst bool) int64 {
	n := 3*frameHeaderLen + int64(len(grpcHeaders)+len(okTrailers)) + size
	if rst {
		n += frameHeaderLen + 4
	}
	return n
}

// takeDirect reports whether f's reply goes straight into the output,
// and if so takes what it spends: the send windows, the pass's budget,
// and the end of the call in the metrics. c.mu is held.
func (c *conn) takeDirect(f *finished, budget *int) bool {
	st := f.st
	f.rst = !st.inEOF
	frames := int(directFrames(int64(f.size), f.rst))
	size := int64(f.size)
	if len(st.out) > 0 || st.listed || frames > *budget || f.size > c.maxFrame || size > c.sendWin || size > st.sendWin {
		return false
	}
	*budget -= frames
	c.sendWin -= size
	st.sendWin -= size
	c.metrics.sent.add(size - 5)
	if st.calls != nil {
		st.calls.sent.add(1)
	}
	st.countEnd(nil)
	return true
}

// writeDirect encodes f's reply, which takeDirect let through, straight
// into the connection's output.
func (c *conn) writeDirect(f *finished) error {
	st := f.st
	b := c.direct.Reserve(int(directFrames(int64(f.size), f.rst)))
	if b == nil {
		return errOutputClosed
	}
	b = frameHeader(b, len(grpcHeaders), http2.FrameHeaders, http2.FlagHeadersEndHeaders, st.id)
	b = append(b, grpcHeaders...)
	b = frameHeader(b, f.size, http2.FrameData, 0, st.id)
	at := len(b)
	b, err := c.appendReply(b, st, f.cp, f.size-5)
	if err != nil {
		return err
	}
	// An encoding longer than its room went to a buffer of its own.
	if len(b)-at != f.size {
		return errReplySize
	}
	b = frameHeader(b, len(okTrailers), http2.FrameHeaders, http2.FlagHeadersEndHeaders|http2.FlagHeadersEndStream, st.id)
	b = append(b, okTrailers...)
	if f.rst {
		b = frameHeader(b, 4, http2.FrameRSTStream, 0, st.id)
		b = binary.BigEndian.AppendUint32(b, uint32(http2.ErrCodeNo))
	}
	c.direct.Commit(len(b))
	return nil
}

// appendReply appends the gRPC message of a reply that directSize sized
// at size bytes without its prefix.
func (c *conn) appendReply(b []byte, st *stream, cp *node.Completion, size int) ([]byte, error) {
	switch r := cp.Resp.(type) {
	case *pb.TxnResponse:
		e := txnEnc{b: appendPrefix(b, size)}
		e.appendTxnResp(r)
		return e.b, nil
	case *pb.DeleteRangeResponse:
		// proto.Size in directSize cached the sizes MarshalAppend uses.
		return proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(appendPrefix(b, size), r)
	}
	if st.rpc.wirePut {
		return c.appendPutReply(b, &cp.Reply)
	}
	b, err := appendRangeStart(b, c.cluster, c.member, cp.Term, &cp.Range)
	if err != nil {
		return b, err
	}
	return appendRangeEnd(appendKVs(b, 2, cp.Range.KVs), &cp.Range), nil
}

// appendPrefix appends the prefix of an uncompressed gRPC message of size
// bytes.
func appendPrefix(b []byte, size int) []byte {
	return binary.BigEndian.AppendUint32(append(b, 0), uint32(size))
}

// putReplySize is the size of the PutResponse appendPutReply encodes.
func putReplySize(cluster, member uint64, r *core.Reply) int {
	n := 1 + protowire.SizeBytes(headerSize(cluster, member, r.Rev, r.Term))
	if r.PrevKv != nil {
		n += 1 + protowire.SizeBytes(proto.Size(r.PrevKv))
	}
	return n
}
