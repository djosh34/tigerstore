package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// txnSize is proto.Size of a TxnRequest, written out: every transaction
// that writes is sized against the request limit, and protobuf's generic
// walk of the tree was 3.3% of the leader's CPU per compare-and-swap. A
// test checks it against proto.Size.
func txnSize(t *pb.TxnRequest) int {
	n := unknownSize(t)
	for _, c := range t.Compare {
		n += fieldMsg(1, compareSize(c))
	}
	for _, op := range t.Success {
		n += fieldMsg(2, requestOpSize(op))
	}
	for _, op := range t.Failure {
		n += fieldMsg(3, requestOpSize(op))
	}
	return n
}

func unknownSize(m proto.Message) int { return len(m.ProtoReflect().GetUnknown()) }

// fieldMsg is the size of a message field of the given size.
func fieldMsg(num protowire.Number, size int) int {
	return protowire.SizeTag(num) + protowire.SizeBytes(size)
}

// fieldVarint is the size of a varint field, left out at zero.
func fieldVarint(num protowire.Number, v uint64) int {
	if v == 0 {
		return 0
	}
	return protowire.SizeTag(num) + protowire.SizeVarint(v)
}

// fieldBytes is the size of a bytes field, left out when empty.
func fieldBytes(num protowire.Number, b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return protowire.SizeTag(num) + protowire.SizeBytes(len(b))
}

func fieldBool(num protowire.Number, v bool) int {
	if !v {
		return 0
	}
	return protowire.SizeTag(num) + 1
}

func compareSize(c *pb.Compare) int {
	n := unknownSize(c) + fieldVarint(1, uint64(int64(c.Result))) + fieldVarint(2, uint64(int64(c.Target))) +
		fieldBytes(3, c.Key) + fieldBytes(64, c.RangeEnd)
	// A field of a oneof counts when set, even at zero.
	switch u := c.TargetUnion.(type) {
	case *pb.Compare_Version:
		n += protowire.SizeTag(4) + protowire.SizeVarint(uint64(u.Version))
	case *pb.Compare_CreateRevision:
		n += protowire.SizeTag(5) + protowire.SizeVarint(uint64(u.CreateRevision))
	case *pb.Compare_ModRevision:
		n += protowire.SizeTag(6) + protowire.SizeVarint(uint64(u.ModRevision))
	case *pb.Compare_Value:
		n += protowire.SizeTag(7) + protowire.SizeBytes(len(u.Value))
	case *pb.Compare_Lease:
		n += protowire.SizeTag(8) + protowire.SizeVarint(uint64(u.Lease))
	}
	return n
}

func requestOpSize(op *pb.RequestOp) int {
	n := unknownSize(op)
	switch r := op.Request.(type) {
	case *pb.RequestOp_RequestRange:
		n += fieldMsg(1, rangeSize(r.RequestRange))
	case *pb.RequestOp_RequestPut:
		n += fieldMsg(2, putSize(r.RequestPut))
	case *pb.RequestOp_RequestDeleteRange:
		n += fieldMsg(3, deleteSize(r.RequestDeleteRange))
	case *pb.RequestOp_RequestTxn:
		n += fieldMsg(4, txnSize(r.RequestTxn))
	}
	return n
}

func rangeSize(r *pb.RangeRequest) int {
	if r == nil {
		return 0
	}
	return unknownSize(r) + fieldBytes(1, r.Key) + fieldBytes(2, r.RangeEnd) +
		fieldVarint(3, uint64(r.Limit)) + fieldVarint(4, uint64(r.Revision)) +
		fieldVarint(5, uint64(int64(r.SortOrder))) + fieldVarint(6, uint64(int64(r.SortTarget))) +
		fieldBool(7, r.Serializable) + fieldBool(8, r.KeysOnly) + fieldBool(9, r.CountOnly) +
		fieldVarint(10, uint64(r.MinModRevision)) + fieldVarint(11, uint64(r.MaxModRevision)) +
		fieldVarint(12, uint64(r.MinCreateRevision)) + fieldVarint(13, uint64(r.MaxCreateRevision))
}

func putSize(r *pb.PutRequest) int {
	if r == nil {
		return 0
	}
	return unknownSize(r) + fieldBytes(1, r.Key) + fieldBytes(2, r.Value) + fieldVarint(3, uint64(r.Lease)) +
		fieldBool(4, r.PrevKv) + fieldBool(5, r.IgnoreValue) + fieldBool(6, r.IgnoreLease)
}

func deleteSize(r *pb.DeleteRangeRequest) int {
	if r == nil {
		return 0
	}
	return unknownSize(r) + fieldBytes(1, r.Key) + fieldBytes(2, r.RangeEnd) + fieldBool(3, r.PrevKv)
}
