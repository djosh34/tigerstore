package server

import (
	"encoding/binary"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// The TxnResponse encoder below writes what protobuf's deterministic
// encoding writes for the messages the store builds, which carry no
// unknown fields, without protobuf's generic walk. Fields at their zero
// value are left out, a set message field is written even when empty, and
// fields go in number order.

// appendTxnReply encodes the gRPC message of a TxnResponse.
func appendTxnReply(r *pb.TxnResponse) ([]byte, error) {
	size := txnRespSize(r)
	if size > 1<<31-1 {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: message of %d bytes is too large to send", size)
	}
	b := make([]byte, 0, 5+size)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(size))
	return appendTxnResp(b, r), nil
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

func rangeRespSize(r *pb.RangeResponse) int {
	return headerField(r.Header) + kvsSize(r.Kvs) + sizeBool(r.More) + sizeVarint(uint64(r.Count))
}

func putRespSize(r *pb.PutResponse) int {
	n := headerField(r.Header)
	if r.PrevKv != nil {
		n += sizeMsg(keyValueSize(r.PrevKv))
	}
	return n
}

func deleteRespSize(r *pb.DeleteRangeResponse) int {
	return headerField(r.Header) + sizeVarint(uint64(r.Deleted)) + kvsSize(r.PrevKvs)
}

func txnRespSize(r *pb.TxnResponse) int {
	n := headerField(r.Header) + sizeBool(r.Succeeded)
	for _, op := range r.Responses {
		n += sizeMsg(respOpSize(op))
	}
	return n
}

// respOpSize is the size of a ResponseOp. A set member of its oneof
// counts even when nil, as an empty message, as protobuf writes it.
func respOpSize(op *pb.ResponseOp) int {
	switch r := op.Response.(type) {
	case *pb.ResponseOp_ResponseRange:
		return sizeMsg(rangeRespSize(orEmpty(r.ResponseRange)))
	case *pb.ResponseOp_ResponsePut:
		return sizeMsg(putRespSize(orEmpty(r.ResponsePut)))
	case *pb.ResponseOp_ResponseDeleteRange:
		return sizeMsg(deleteRespSize(orEmpty(r.ResponseDeleteRange)))
	case *pb.ResponseOp_ResponseTxn:
		return sizeMsg(txnRespSize(orEmpty(r.ResponseTxn)))
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

func appendTxnResp(b []byte, r *pb.TxnResponse) []byte {
	b = appendRespHeader(b, r.Header)
	b = appendBool(b, 2, r.Succeeded)
	for _, op := range r.Responses {
		b = appendMsgHeader(b, 3, respOpSize(op))
		b = appendRespOp(b, op)
	}
	return b
}

func appendRespOp(b []byte, op *pb.ResponseOp) []byte {
	switch r := op.Response.(type) {
	case *pb.ResponseOp_ResponseRange:
		rr := orEmpty(r.ResponseRange)
		b = appendMsgHeader(b, 1, rangeRespSize(rr))
		b = appendRespHeader(b, rr.Header)
		for _, kv := range rr.Kvs {
			b = appendKeyValue(b, 2, kv)
		}
		b = appendBool(b, 3, rr.More)
		b = appendVarint(b, 4, uint64(rr.Count))
	case *pb.ResponseOp_ResponsePut:
		pr := orEmpty(r.ResponsePut)
		b = appendMsgHeader(b, 2, putRespSize(pr))
		b = appendRespHeader(b, pr.Header)
		if pr.PrevKv != nil {
			b = appendKeyValue(b, 2, pr.PrevKv)
		}
	case *pb.ResponseOp_ResponseDeleteRange:
		dr := orEmpty(r.ResponseDeleteRange)
		b = appendMsgHeader(b, 3, deleteRespSize(dr))
		b = appendRespHeader(b, dr.Header)
		b = appendVarint(b, 2, uint64(dr.Deleted))
		for _, kv := range dr.PrevKvs {
			b = appendKeyValue(b, 3, kv)
		}
	case *pb.ResponseOp_ResponseTxn:
		tr := orEmpty(r.ResponseTxn)
		b = appendMsgHeader(b, 4, txnRespSize(tr))
		b = appendTxnResp(b, tr)
	}
	return b
}
