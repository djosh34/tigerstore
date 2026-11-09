package server

import (
	"encoding/binary"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The TxnResponse encoder below writes what protobuf's deterministic
// encoding writes for the messages the store builds, which carry no
// unknown fields, without protobuf's generic walk. Fields at their zero
// value are left out, a set message field is written even when empty, and
// fields go in number order.

// appendTxnPieces encodes the gRPC message of a TxnResponse, uncompressed,
// up to the KeyValues of the first message in rests, and returns what
// follows as holes: the KeyValues of each message in rests that later
// pieces bring, and the bytes after them (core.Reply.Rests).
func appendTxnPieces(r *pb.TxnResponse, rests []mvcc.Rest) ([]byte, []hole, error) {
	e := newTxnEnc(rests)
	return e.message(e.txnRespSize(r), func() { e.appendTxnResp(r) })
}

// appendDeletePieces is appendTxnPieces for a DeleteRangeResponse.
func appendDeletePieces(r *pb.DeleteRangeResponse, rests []mvcc.Rest) ([]byte, []hole, error) {
	e := newTxnEnc(rests)
	return e.message(e.deleteRespSize(r), func() { e.appendDeleteFields(r) })
}

func newTxnEnc(rests []mvcc.Rest) *txnEnc {
	e := &txnEnc{}
	if len(rests) > 0 {
		// Most transactions have no reads to come, and a lookup in a nil
		// map finds nothing.
		e.rests = make(map[proto.Message]*mvcc.Rest, len(rests))
	}
	for i := range rests {
		e.rests[rests[i].Msg] = &rests[i]
	}
	return e
}

// message encodes a gRPC message of size bytes whose fields fields
// appends.
func (e *txnEnc) message(size int, fields func()) ([]byte, []hole, error) {
	if size > 1<<31-1 {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
	}
	e.b = make([]byte, 0, 5+min(size, 1<<20))
	e.b = append(e.b, 0)
	e.b = binary.BigEndian.AppendUint32(e.b, uint32(size))
	fields()
	return e.end()
}

// txnEnc encodes a response whose messages in rests have KeyValues to
// come: each such message's size counts all of them, and its encoding
// stops after the KeyValues it holds, at a hole.
type txnEnc struct {
	rests map[proto.Message]*mvcc.Rest
	b     []byte
	head  []byte
	holes []hole
}

// hole is where later pieces bring left KeyValues as field num; after
// is what the message holds after them, up to the next hole.
type hole struct {
	num   protowire.Number
	left  int64
	after []byte
}

// gap ends the bytes before a hole of n KeyValues as field num.
func (e *txnEnc) gap(num protowire.Number, n int64) {
	if len(e.holes) == 0 {
		e.head = e.b
	} else {
		e.holes[len(e.holes)-1].after = e.b
	}
	e.holes = append(e.holes, hole{num: num, left: n})
	e.b = nil
}

// end returns the bytes before the first hole and the holes.
func (e *txnEnc) end() ([]byte, []hole, error) {
	if len(e.holes) == 0 {
		return e.b, nil, nil
	}
	e.holes[len(e.holes)-1].after = e.b
	return e.head, e.holes, nil
}

// kvs is the size of the KeyValue fields of msg, which holds kvs: all
// those it has in the whole answer if it is in rests.
func (e *txnEnc) kvs(msg proto.Message, kvs []*mvccpb.KeyValue) int {
	if r, ok := e.rests[msg]; ok {
		return int(r.Fields)
	}
	return kvsSize(kvs)
}

// appendKVs appends kvs as field num of msg, and a hole for the rest if
// msg is in rests.
func (e *txnEnc) appendKVs(msg proto.Message, num protowire.Number, kvs []*mvccpb.KeyValue) {
	for _, kv := range kvs {
		e.b = appendKeyValue(e.b, num, kv)
	}
	if r, ok := e.rests[msg]; ok {
		e.gap(num, r.N-int64(len(kvs)))
	}
}

func sizeMsg(n int) int { return 1 + protowire.SizeBytes(n) }

func appendMsgHeader(b []byte, num protowire.Number, size int) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendVarint(b, uint64(size))
}

func sizeVarint(v uint64) int {
	if v == 0 {
		return 0
	}
	return 1 + protowire.SizeVarint(v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func sizeBool(v bool) int {
	if v {
		return 2
	}
	return 0
}

func appendBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}
	return appendVarint(b, num, 1)
}

func sizeBytes(v []byte) int {
	if len(v) == 0 {
		return 0
	}
	return 1 + protowire.SizeBytes(len(v))
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func respHeaderSize(h *pb.ResponseHeader) int {
	return headerSize(h.ClusterId, h.MemberId, h.Revision, h.RaftTerm)
}

func appendRespHeader(b []byte, h *pb.ResponseHeader) []byte {
	if h == nil {
		return b
	}
	return appendHeader(b, h.ClusterId, h.MemberId, h.Revision, h.RaftTerm)
}

// headerField is the size of an optional header field 1.
func headerField(h *pb.ResponseHeader) int {
	if h == nil {
		return 0
	}
	return sizeMsg(respHeaderSize(h))
}

func keyValueSize(kv *mvccpb.KeyValue) int {
	return sizeBytes(kv.Key) + sizeVarint(uint64(kv.CreateRevision)) + sizeVarint(uint64(kv.ModRevision)) +
		sizeVarint(uint64(kv.Version)) + sizeBytes(kv.Value) + sizeVarint(uint64(kv.Lease))
}

func appendKeyValue(b []byte, num protowire.Number, kv *mvccpb.KeyValue) []byte {
	b = appendMsgHeader(b, num, keyValueSize(kv))
	b = appendBytes(b, 1, kv.Key)
	b = appendVarint(b, 2, uint64(kv.CreateRevision))
	b = appendVarint(b, 3, uint64(kv.ModRevision))
	b = appendVarint(b, 4, uint64(kv.Version))
	b = appendBytes(b, 5, kv.Value)
	return appendVarint(b, 6, uint64(kv.Lease))
}

func kvsSize(kvs []*mvccpb.KeyValue) int {
	n := 0
	for _, kv := range kvs {
		n += sizeMsg(keyValueSize(kv))
	}
	return n
}

func (e *txnEnc) rangeRespSize(r *pb.RangeResponse) int {
	return headerField(r.Header) + e.kvs(r, r.Kvs) + sizeBool(r.More) + sizeVarint(uint64(r.Count))
}

func putRespSize(r *pb.PutResponse) int {
	n := headerField(r.Header)
	if r.PrevKv != nil {
		n += sizeMsg(keyValueSize(r.PrevKv))
	}
	return n
}

func (e *txnEnc) deleteRespSize(r *pb.DeleteRangeResponse) int {
	return headerField(r.Header) + sizeVarint(uint64(r.Deleted)) + e.kvs(r, r.PrevKvs)
}

func (e *txnEnc) txnRespSize(r *pb.TxnResponse) int {
	n := headerField(r.Header) + sizeBool(r.Succeeded)
	for _, op := range r.Responses {
		n += sizeMsg(e.respOpSize(op))
	}
	return n
}

// txnRespSize is the size of a TxnResponse with every KeyValue in it.
func txnRespSize(r *pb.TxnResponse) int { return (&txnEnc{}).txnRespSize(r) }

// respOpSize is the size of a ResponseOp. A set member of its oneof
// counts even when nil, as an empty message, as protobuf writes it.
func (e *txnEnc) respOpSize(op *pb.ResponseOp) int {
	switch r := op.Response.(type) {
	case *pb.ResponseOp_ResponseRange:
		return sizeMsg(e.rangeRespSize(orEmpty(r.ResponseRange)))
	case *pb.ResponseOp_ResponsePut:
		return sizeMsg(putRespSize(orEmpty(r.ResponsePut)))
	case *pb.ResponseOp_ResponseDeleteRange:
		return sizeMsg(e.deleteRespSize(orEmpty(r.ResponseDeleteRange)))
	case *pb.ResponseOp_ResponseTxn:
		return sizeMsg(e.txnRespSize(orEmpty(r.ResponseTxn)))
	}
	return 0
}

// orEmpty returns m, or an empty message for a nil one.
func orEmpty[M any](m *M) *M {
	if m == nil {
		return new(M)
	}
	return m
}

func (e *txnEnc) appendTxnResp(r *pb.TxnResponse) {
	e.b = appendRespHeader(e.b, r.Header)
	e.b = appendBool(e.b, 2, r.Succeeded)
	for _, op := range r.Responses {
		e.b = appendMsgHeader(e.b, 3, e.respOpSize(op))
		e.appendRespOp(op)
	}
}

// appendDeleteFields appends the fields of a DeleteRangeResponse.
func (e *txnEnc) appendDeleteFields(dr *pb.DeleteRangeResponse) {
	e.b = appendRespHeader(e.b, dr.Header)
	e.b = appendVarint(e.b, 2, uint64(dr.Deleted))
	e.appendKVs(dr, 3, dr.PrevKvs)
}

func (e *txnEnc) appendRespOp(op *pb.ResponseOp) {
	switch r := op.Response.(type) {
	case *pb.ResponseOp_ResponseRange:
		rr := orEmpty(r.ResponseRange)
		e.b = appendMsgHeader(e.b, 1, e.rangeRespSize(rr))
		e.b = appendRespHeader(e.b, rr.Header)
		e.appendKVs(rr, 2, rr.Kvs)
		e.b = appendBool(e.b, 3, rr.More)
		e.b = appendVarint(e.b, 4, uint64(rr.Count))
	case *pb.ResponseOp_ResponsePut:
		pr := orEmpty(r.ResponsePut)
		e.b = appendMsgHeader(e.b, 2, putRespSize(pr))
		e.b = appendRespHeader(e.b, pr.Header)
		if pr.PrevKv != nil {
			e.b = appendKeyValue(e.b, 2, pr.PrevKv)
		}
	case *pb.ResponseOp_ResponseDeleteRange:
		dr := orEmpty(r.ResponseDeleteRange)
		e.b = appendMsgHeader(e.b, 3, e.deleteRespSize(dr))
		e.appendDeleteFields(dr)
	case *pb.ResponseOp_ResponseTxn:
		tr := orEmpty(r.ResponseTxn)
		e.b = appendMsgHeader(e.b, 4, e.txnRespSize(tr))
		e.appendTxnResp(tr)
	}
}
