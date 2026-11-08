package core

import (
	"bytes"
	"encoding/binary"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// A short transaction is the transaction kube-apiserver sends for every
// write, with its key written once instead of three times: a compare of the
// key's mod revision (or another number), a put or a delete of the key if
// it holds, and a read of the key if it does not, or nothing. Its entry
// holds (from entry format 4, originVersion):
//
//	u8 flags | uvarint result | uvarint target | u8 number field |
//	uvarint number (unless the field is 0) | uvarint lease (for a put) |
//	uvarint key length | key | value (for a put: the rest)
//
// It decodes to the same TxnRequest as the client's encoding, so it is the
// same write to every node and to the checks; only fewer bytes go to the
// disks and the peers. The value comes last, so the node that took the
// request knows where it is without decoding the entry.

const (
	shortDelete      = 1 << iota // the success branch deletes the key, else puts it
	shortRead                    // the failure branch reads the key
	shortPrevKV                  // the put or delete returns the previous key-value
	shortIgnoreValue             // the put keeps the value
	shortIgnoreLease             // the put keeps the lease
)

// Compare.TargetUnion fields that hold a number.
const (
	numberNone = iota
	numberVersion
	numberCreate
	numberMod
	numberLease
)

// isShort reports whether t has the shape of a short transaction, with no
// field it would lose.
func isShort(t *pb.TxnRequest) bool {
	if len(t.Compare) != 1 || len(t.Success) != 1 || len(t.Failure) > 1 || hasUnknown(t) {
		return false
	}
	c := t.Compare[0]
	if len(c.Key) == 0 || len(c.RangeEnd) != 0 || hasUnknown(c) {
		return false
	}
	switch c.TargetUnion.(type) {
	case nil, *pb.Compare_Version, *pb.Compare_CreateRevision, *pb.Compare_ModRevision, *pb.Compare_Lease:
	default:
		return false
	}
	op := t.Success[0]
	if hasUnknown(op) {
		return false
	}
	switch r := op.Request.(type) {
	case *pb.RequestOp_RequestPut:
		if p := r.RequestPut; !bytes.Equal(p.Key, c.Key) || hasUnknown(p) {
			return false
		}
	case *pb.RequestOp_RequestDeleteRange:
		if d := r.RequestDeleteRange; !bytes.Equal(d.Key, c.Key) || len(d.RangeEnd) != 0 || hasUnknown(d) {
			return false
		}
	default:
		return false
	}
	if len(t.Failure) == 0 {
		return true
	}
	f := t.Failure[0]
	r := f.GetRequestRange()
	return r != nil && !hasUnknown(f) && !hasUnknown(r) && bytes.Equal(r.Key, c.Key) && len(r.RangeEnd) == 0 &&
		r.Limit == 0 && r.Revision == 0 && r.SortOrder == 0 && r.SortTarget == 0 && !r.Serializable &&
		!r.KeysOnly && !r.CountOnly && r.MinModRevision == 0 && r.MaxModRevision == 0 &&
		r.MinCreateRevision == 0 && r.MaxCreateRevision == 0
}

func hasUnknown(m proto.Message) bool { return len(m.ProtoReflect().GetUnknown()) > 0 }

// shortSize is the size of the short form of t, which isShort accepted.
func shortSize(t *pb.TxnRequest) int {
	c := t.Compare[0]
	n := 1 + protowire.SizeVarint(uint64(uint32(c.Result))) + protowire.SizeVarint(uint64(uint32(c.Target))) + 1
	switch u := c.TargetUnion.(type) {
	case *pb.Compare_Version:
		n += protowire.SizeVarint(uint64(u.Version))
	case *pb.Compare_CreateRevision:
		n += protowire.SizeVarint(uint64(u.CreateRevision))
	case *pb.Compare_ModRevision:
		n += protowire.SizeVarint(uint64(u.ModRevision))
	case *pb.Compare_Lease:
		n += protowire.SizeVarint(uint64(u.Lease))
	}
	if put := t.Success[0].GetRequestPut(); put != nil {
		n += protowire.SizeVarint(uint64(put.Lease)) + len(put.Value)
	}
	return n + protowire.SizeBytes(len(c.Key))
}

// appendShort appends the short form of t, which isShort accepted.
func appendShort(b []byte, t *pb.TxnRequest) []byte {
	c := t.Compare[0]
	var flags byte
	var put *pb.PutRequest
	switch r := t.Success[0].Request.(type) {
	case *pb.RequestOp_RequestPut:
		put = r.RequestPut
		if put.PrevKv {
			flags |= shortPrevKV
		}
		if put.IgnoreValue {
			flags |= shortIgnoreValue
		}
		if put.IgnoreLease {
			flags |= shortIgnoreLease
		}
	case *pb.RequestOp_RequestDeleteRange:
		flags |= shortDelete
		if r.RequestDeleteRange.PrevKv {
			flags |= shortPrevKV
		}
	}
	if len(t.Failure) == 1 {
		flags |= shortRead
	}
	b = append(b, flags)
	b = binary.AppendUvarint(b, uint64(uint32(c.Result)))
	b = binary.AppendUvarint(b, uint64(uint32(c.Target)))
	switch u := c.TargetUnion.(type) {
	case nil:
		b = append(b, numberNone)
	case *pb.Compare_Version:
		b = binary.AppendUvarint(append(b, numberVersion), uint64(u.Version))
	case *pb.Compare_CreateRevision:
		b = binary.AppendUvarint(append(b, numberCreate), uint64(u.CreateRevision))
	case *pb.Compare_ModRevision:
		b = binary.AppendUvarint(append(b, numberMod), uint64(u.ModRevision))
	case *pb.Compare_Lease:
		b = binary.AppendUvarint(append(b, numberLease), uint64(u.Lease))
	}
	if put != nil {
		b = binary.AppendUvarint(b, uint64(put.Lease))
	}
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	if put != nil {
		b = append(b, put.Value...)
	}
	return b
}

// decodeShort decodes a short transaction that starts at offset at of its
// entry into m, and records in pl where the value of its put starts. The
// bytes fields are slices of b.
func decodeShort(b []byte, m *pb.TxnRequest, at int, pl *placer) error {
	if len(b) == 0 {
		return errBadBatch
	}
	whole := b
	flags := b[0]
	if flags >= shortIgnoreLease<<1 || flags&shortDelete != 0 && flags&(shortIgnoreValue|shortIgnoreLease) != 0 {
		return errBadBatch
	}
	rest := b[1:]
	result, rest, ok := uvarint(rest)
	if !ok {
		return errBadBatch
	}
	target, rest, ok := uvarint(rest)
	if !ok || len(rest) == 0 || result > 1<<32-1 || target > 1<<32-1 {
		return errBadBatch
	}
	c := &pb.Compare{Result: pb.Compare_CompareResult(int32(uint32(result))), Target: pb.Compare_CompareTarget(int32(uint32(target)))}
	field := rest[0]
	rest = rest[1:]
	if field != numberNone {
		var n uint64
		if n, rest, ok = uvarint(rest); !ok {
			return errBadBatch
		}
		switch field {
		case numberVersion:
			c.TargetUnion = &pb.Compare_Version{Version: int64(n)}
		case numberCreate:
			c.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: int64(n)}
		case numberMod:
			c.TargetUnion = &pb.Compare_ModRevision{ModRevision: int64(n)}
		case numberLease:
			c.TargetUnion = &pb.Compare_Lease{Lease: int64(n)}
		default:
			return errBadBatch
		}
	}
	var lease uint64
	if flags&shortDelete == 0 {
		if lease, rest, ok = uvarint(rest); !ok {
			return errBadBatch
		}
	}
	n, rest, ok := uvarint(rest)
	if !ok || n == 0 || n > uint64(len(rest)) {
		return errBadBatch
	}
	key := rest[:n:n]
	c.Key = key
	value := rest[n:]
	m.Compare = []*pb.Compare{c}
	prev := flags&shortPrevKV != 0
	if flags&shortDelete != 0 {
		if len(value) != 0 {
			return errBadBatch
		}
		m.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: key, PrevKv: prev}}}}
	} else {
		value = slices.Clip(value)
		pl.add(value, at+len(whole)-len(value))
		m.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{
			Key: key, Value: value, Lease: int64(lease), PrevKv: prev,
			IgnoreValue: flags&shortIgnoreValue != 0, IgnoreLease: flags&shortIgnoreLease != 0,
		}}}}
	}
	if flags&shortRead != 0 {
		m.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}}
	}
	return nil
}
