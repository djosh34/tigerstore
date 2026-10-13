package core

import (
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// The decoders below read the requests of a transaction (and a
// DeleteRange) from their wire form, with protobuf's rules, into the
// generated structs, the way UnmarshalAlias does: bytes fields are slices
// of the input. They are written out per message, without reflection,
// because a transaction is decoded on every replica: UnmarshalAlias took
// about 1.6 times as long as proto.Unmarshal for a 1 KiB compare-and-swap.
// TestTxnDecodeMatchesProtobuf compares them with proto.Unmarshal.

// wireField is one field: its number and type, its varint or its bytes,
// and all of its encoding, tag included.
type wireField struct {
	num protowire.Number
	typ protowire.Type
	v   uint64
	b   []byte
	raw []byte
}

// nextField reads the first field of b and returns the rest.
func nextField(b []byte) (wireField, []byte, error) {
	num, typ, n := protowire.ConsumeTag(b)
	if n < 0 || num > protowire.MaxValidNumber {
		return wireField{}, nil, errBadMessage
	}
	f := wireField{num: num, typ: typ}
	var m int
	switch typ {
	case protowire.VarintType:
		f.v, m = protowire.ConsumeVarint(b[n:])
	case protowire.BytesType:
		f.b, m = protowire.ConsumeBytes(b[n:])
		f.b = slices.Clip(f.b)
	case protowire.Fixed32Type, protowire.Fixed64Type, protowire.StartGroupType, protowire.EndGroupType:
		m = protowire.ConsumeFieldValue(num, typ, b[n:])
	default:
		// Wire types 6 and 7 do not exist.
		return wireField{}, nil, errBadMessage
	}
	if m < 0 {
		return wireField{}, nil, errBadMessage
	}
	f.raw = b[:n+m]
	return f, b[n+m:], nil
}

// bytesField and varintField report whether f is field num of that type;
// a known field of the wrong type is an unknown field to protobuf.
func (f *wireField) bytesField(num protowire.Number) bool {
	return f.num == num && f.typ == protowire.BytesType
}

func (f *wireField) varintField(num protowire.Number) bool {
	return f.num == num && f.typ == protowire.VarintType
}

// keepUnknown appends a field that m does not know to its unknown fields,
// as protobuf does.
func keepUnknown(m proto.Message, raw []byte) {
	r := m.ProtoReflect()
	r.SetUnknown(append(r.GetUnknown(), raw...))
}

// DecodeAlias decodes m from b like UnmarshalAlias, with the written-out
// decoders for transactions and deletes, the requests decoded most.
func DecodeAlias(b []byte, m proto.Message) error {
	switch m := m.(type) {
	case *pb.TxnRequest:
		return decodeTxnAlias(b, m)
	case *pb.DeleteRangeRequest:
		return decodeDelete(b, m)
	}
	return UnmarshalAlias(b, m)
}

// decodeTxnAlias decodes a TxnRequest, as UnmarshalAlias does.
func decodeTxnAlias(b []byte, m *pb.TxnRequest) error {
	// The top message counts as one level, as in protobuf.
	return decodeTxn(b, m, protowire.DefaultRecursionLimit-1)
}

// decodeTxn decodes a TxnRequest; depth is how many more levels of nested
// messages may follow.
func decodeTxn(b []byte, m *pb.TxnRequest, depth int) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		switch {
		case f.bytesField(1):
			if depth <= 0 {
				return errTooDeep
			}
			c := &pb.Compare{}
			m.Compare = append(m.Compare, c)
			err = decodeCompare(f.b, c)
		case f.bytesField(2), f.bytesField(3):
			if depth <= 0 {
				return errTooDeep
			}
			op := &pb.RequestOp{}
			if f.num == 2 {
				m.Success = append(m.Success, op)
			} else {
				m.Failure = append(m.Failure, op)
			}
			err = decodeOp(f.b, op, depth-1)
		default:
			keepUnknown(m, f.raw)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeCompare(b []byte, m *pb.Compare) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		switch {
		case f.varintField(1):
			m.Result = pb.Compare_CompareResult(int32(f.v))
		case f.varintField(2):
			m.Target = pb.Compare_CompareTarget(int32(f.v))
		case f.bytesField(3):
			m.Key = f.b
		case f.varintField(4):
			m.TargetUnion = &pb.Compare_Version{Version: int64(f.v)}
		case f.varintField(5):
			m.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: int64(f.v)}
		case f.varintField(6):
			m.TargetUnion = &pb.Compare_ModRevision{ModRevision: int64(f.v)}
		case f.bytesField(7):
			m.TargetUnion = &pb.Compare_Value{Value: f.b}
		case f.varintField(8):
			m.TargetUnion = &pb.Compare_Lease{Lease: int64(f.v)}
		case f.bytesField(64):
			m.RangeEnd = f.b
		default:
			keepUnknown(m, f.raw)
		}
	}
	return nil
}

// decodeOp decodes a RequestOp. A request that comes again merges into
// the first if it is of the same kind, and replaces it if not, as in
// protobuf.
func decodeOp(b []byte, m *pb.RequestOp, depth int) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		if f.typ != protowire.BytesType || f.num < 1 || f.num > 4 {
			keepUnknown(m, f.raw)
			continue
		}
		if depth <= 0 {
			return errTooDeep
		}
		switch int(f.num) {
		case 1:
			r, ok := m.Request.(*pb.RequestOp_RequestRange)
			if !ok {
				r = &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{}}
				m.Request = r
			}
			err = decodeRange(f.b, r.RequestRange)
		case 2:
			r, ok := m.Request.(*pb.RequestOp_RequestPut)
			if !ok {
				r = &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{}}
				m.Request = r
			}
			err = decodePutRequest(f.b, r.RequestPut)
		case 3:
			r, ok := m.Request.(*pb.RequestOp_RequestDeleteRange)
			if !ok {
				r = &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{}}
				m.Request = r
			}
			err = decodeDelete(f.b, r.RequestDeleteRange)
		default:
			r, ok := m.Request.(*pb.RequestOp_RequestTxn)
			if !ok {
				r = &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{}}
				m.Request = r
			}
			err = decodeTxn(f.b, r.RequestTxn, depth-1)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeRange(b []byte, m *pb.RangeRequest) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		switch {
		case f.bytesField(1):
			m.Key = f.b
		case f.bytesField(2):
			m.RangeEnd = f.b
		case f.varintField(3):
			m.Limit = int64(f.v)
		case f.varintField(4):
			m.Revision = int64(f.v)
		case f.varintField(5):
			m.SortOrder = pb.RangeRequest_SortOrder(int32(f.v))
		case f.varintField(6):
			m.SortTarget = pb.RangeRequest_SortTarget(int32(f.v))
		case f.varintField(7):
			m.Serializable = f.v != 0
		case f.varintField(8):
			m.KeysOnly = f.v != 0
		case f.varintField(9):
			m.CountOnly = f.v != 0
		case f.varintField(10):
			m.MinModRevision = int64(f.v)
		case f.varintField(11):
			m.MaxModRevision = int64(f.v)
		case f.varintField(12):
			m.MinCreateRevision = int64(f.v)
		case f.varintField(13):
			m.MaxCreateRevision = int64(f.v)
		default:
			keepUnknown(m, f.raw)
		}
	}
	return nil
}

func decodePutRequest(b []byte, m *pb.PutRequest) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		switch {
		case f.bytesField(1):
			m.Key = f.b
		case f.bytesField(2):
			m.Value = f.b
		case f.varintField(3):
			m.Lease = int64(f.v)
		case f.varintField(4):
			m.PrevKv = f.v != 0
		case f.varintField(5):
			m.IgnoreValue = f.v != 0
		case f.varintField(6):
			m.IgnoreLease = f.v != 0
		default:
			keepUnknown(m, f.raw)
		}
	}
	return nil
}

func decodeDelete(b []byte, m *pb.DeleteRangeRequest) error {
	for len(b) > 0 {
		f, rest, err := nextField(b)
		if err != nil {
			return err
		}
		b = rest
		switch {
		case f.bytesField(1):
			m.Key = f.b
		case f.bytesField(2):
			m.RangeEnd = f.b
		case f.varintField(3):
			m.PrevKv = f.v != 0
		default:
			keepUnknown(m, f.raw)
		}
	}
	return nil
}
