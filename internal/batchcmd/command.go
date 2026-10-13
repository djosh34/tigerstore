// Package batchcmd decodes a small etcd write subset into immutable wire-backed
// descriptors. Callers must not mutate or pool a body while a Command uses it.
package batchcmd

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

const (
	PutPath = "/etcdserverpb.KV/Put"
	TxnPath = "/etcdserverpb.KV/Txn"
)

type Kind uint8

const (
	Invalid Kind = iota
	Put
	Txn
)

// Command owns its descriptors, but borrows every byte slice from Body.
// The body must remain immutable until the core encodes or finishes the request.
// Committed entry bytes have a separate, potentially indefinite MVCC lifetime.
// A zero Command is invalid. Only Decode constructs commands.
type Command struct {
	kind                   Kind
	body                   []byte
	put                    operation
	compares               []comparison
	success, failure       []operation
	size                   int
	readOnly, serializable bool
	validated              bool
}

// operation holds Put fields or a point Range's key, limit and revision.
// For Put, a is lease. For Range, a is limit and b is revision.
type operation struct {
	kind       Kind // Invalid denotes a point Range inside a transaction.
	key, value []byte
	a, b       int64
	flags      uint8
}

type comparison struct {
	key, value     []byte
	operand        int64
	result, target int32
	union          protowire.Number
}

func (c Command) Kind() Kind         { return c.kind }
func (c Command) Valid() bool        { return c.validated }
func (c Command) Body() []byte       { return c.body }
func (c Command) Size() int          { return c.size }
func (c Command) ReadOnly() bool     { return c.readOnly }
func (c Command) Serializable() bool { return c.serializable }

// Legacy adapts descriptors to existing MVCC requests without copying values.
// It allocates typed request objects. It never uses generic protobuf decoding.
func (c Command) Legacy() proto.Message {
	switch c.kind {
	case Put:
		return c.put.legacyPut()
	case Txn:
		r := &pb.TxnRequest{}
		if len(c.compares) > 0 {
			r.Compare = make([]*pb.Compare, len(c.compares))
		}
		for i, v := range c.compares {
			x := &pb.Compare{Key: v.key, Target: pb.Compare_CompareTarget(v.target), Result: pb.Compare_CompareResult(v.result)}
			switch v.union {
			case 4:
				x.TargetUnion = &pb.Compare_Version{Version: v.operand}
			case 5:
				x.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: v.operand}
			case 6:
				x.TargetUnion = &pb.Compare_ModRevision{ModRevision: v.operand}
			case 7:
				x.TargetUnion = &pb.Compare_Value{Value: v.value}
			case 8:
				x.TargetUnion = &pb.Compare_Lease{Lease: v.operand}
			}
			r.Compare[i] = x
		}
		r.Success = legacyOps(c.success)
		r.Failure = legacyOps(c.failure)
		return r
	default:
		return nil
	}
}

func (v operation) legacyPut() *pb.PutRequest {
	return &pb.PutRequest{Key: v.key, Value: v.value, Lease: v.a, PrevKv: v.flags&1 != 0, IgnoreValue: v.flags&2 != 0, IgnoreLease: v.flags&4 != 0}
}

func (v operation) legacyRange() *pb.RangeRequest {
	return &pb.RangeRequest{Key: v.key, Limit: v.a, Revision: v.b, Serializable: v.flags&1 != 0, KeysOnly: v.flags&2 != 0, CountOnly: v.flags&4 != 0}
}

func legacyOps(ops []operation) []*pb.RequestOp {
	if len(ops) == 0 {
		return nil
	}
	r := make([]*pb.RequestOp, len(ops))
	for i, v := range ops {
		if v.kind == Put {
			r[i] = &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: v.legacyPut()}}
		} else {
			r[i] = &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: v.legacyRange()}}
		}
	}
	return r
}

var errWire = status.Error(codes.InvalidArgument, "batchcmd: malformed protobuf")

func unsupported(feature string) error {
	return status.Error(codes.Unimplemented, "batchcmd: unsupported "+feature)
}

// DecodeEntry walks an immutable entry body without generic protobuf decoding.
// It does not repeat admission validation or compute the canonical request size.
// Its returned command cannot be submitted again without Decode.
func DecodeEntry(path string, body []byte) (Command, error) {
	c := Command{body: body}
	var err error
	switch path {
	case PutPath:
		c.kind = Put
		c.put, err = decodePut(body)
	case TxnPath:
		c.kind = Txn
		err = c.decodeTxn(body)
	default:
		return Command{}, unsupported("method " + path)
	}
	if err != nil {
		return Command{}, err
	}
	return c, nil
}

// Decode accepts full Put and a non-nested CAS Txn subset: single-key compares
// for all targets and results, Put branches, and point Range branches with
// limit, revision, serializable, keys_only and count_only. Other fields return
// Unimplemented rather than being silently dropped. Ordinary validation errors
// retain etcd's error codes. Unknown fields are unsupported in this prototype.
func Decode(path string, body []byte) (Command, error) {
	c, err := DecodeEntry(path, body)
	if err != nil {
		return Command{}, err
	}
	// Reuse the proven structural validator and canonical protobuf size. These
	// temporary typed adapters are not retained in a command or pending record.
	op := c.Legacy()
	if c.kind == Put {
		err = mvcc.CheckPut(op.(*pb.PutRequest))
	} else {
		t := op.(*pb.TxnRequest)
		err = mvcc.CheckTxn(t)
		c.readOnly = mvcc.IsTxnReadOnly(t)
		c.serializable = mvcc.IsTxnSerializable(t)
	}
	if err != nil {
		return Command{}, err
	}
	c.size = proto.Size(op)
	c.validated = true
	return c, nil
}

// field borrows a bytes field or consumes a scalar. Other valid wire types
// reach the field check, which reports unsupported fields or a type mismatch.
type field struct {
	number protowire.Number
	typ    protowire.Type
	bytes  []byte
	value  uint64
}

func next(body []byte) (field, []byte, error) {
	n, typ, used := protowire.ConsumeTag(body)
	if used < 0 {
		return field{}, nil, errWire
	}
	body = body[used:]
	f := field{number: n, typ: typ}
	switch typ {
	case protowire.BytesType:
		f.bytes, used = protowire.ConsumeBytes(body)
	case protowire.VarintType:
		f.value, used = protowire.ConsumeVarint(body)
	default:
		used = protowire.ConsumeFieldValue(n, typ, body)
	}
	if used < 0 {
		return field{}, nil, errWire
	}
	return f, body[used:], nil
}

func (f field) want(t protowire.Type) error {
	if f.typ != t {
		return errWire
	}
	return nil
}

func setFlag(flags *uint8, bit uint8, value uint64) {
	*flags &^= bit
	if value != 0 {
		*flags |= bit
	}
}

func decodePut(body []byte) (operation, error) {
	v := operation{kind: Put}
	for len(body) > 0 {
		f, rest, err := next(body)
		if err != nil {
			return operation{}, err
		}
		body = rest
		switch f.number {
		case 1, 2:
			if err := f.want(protowire.BytesType); err != nil {
				return operation{}, err
			}
			if f.number == 1 {
				v.key = f.bytes
			} else {
				v.value = f.bytes
			}
		case 3, 4, 5, 6:
			if err := f.want(protowire.VarintType); err != nil {
				return operation{}, err
			}
			if f.number == 3 {
				v.a = int64(f.value)
			} else {
				setFlag(&v.flags, 1<<uint(f.number-4), f.value)
			}
		default:
			return operation{}, unsupported("Put field")
		}
	}
	return v, nil
}

func decodeRange(body []byte) (operation, error) {
	var v operation
	for len(body) > 0 {
		f, rest, err := next(body)
		if err != nil {
			return operation{}, err
		}
		body = rest
		switch f.number {
		case 1, 2:
			if err := f.want(protowire.BytesType); err != nil {
				return operation{}, err
			}
			if f.number == 1 {
				v.key = f.bytes
			} else if len(f.bytes) > 0 {
				return operation{}, unsupported("Range range_end")
			}
		case 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13:
			if err := f.want(protowire.VarintType); err != nil {
				return operation{}, err
			}
			switch f.number {
			case 3:
				v.a = int64(f.value)
			case 4:
				v.b = int64(f.value)
			case 7, 8, 9:
				setFlag(&v.flags, 1<<uint(f.number-7), f.value)
			default:
				if f.value != 0 {
					return operation{}, unsupported("Range sort or revision filter")
				}
			}
		default:
			return operation{}, unsupported("Range field")
		}
	}
	return v, nil
}

func decodeCompare(body []byte) (comparison, error) {
	var v comparison
	for len(body) > 0 {
		f, rest, err := next(body)
		if err != nil {
			return comparison{}, err
		}
		body = rest
		switch f.number {
		case 1, 2, 4, 5, 6, 8:
			if err := f.want(protowire.VarintType); err != nil {
				return comparison{}, err
			}
			switch f.number {
			case 1:
				v.result = int32(f.value)
			case 2:
				v.target = int32(f.value)
			default:
				v.operand, v.union = int64(f.value), f.number
			}
		case 3, 7, 64:
			if err := f.want(protowire.BytesType); err != nil {
				return comparison{}, err
			}
			switch f.number {
			case 3:
				v.key = f.bytes
			case 7:
				v.value, v.union = f.bytes, f.number
			case 64:
				if len(f.bytes) > 0 {
					return comparison{}, unsupported("Compare range_end")
				}
			}
		default:
			return comparison{}, unsupported("Compare field")
		}
	}
	if v.result < 0 || v.result > 3 || v.target < 0 || v.target > 4 {
		return comparison{}, unsupported("Compare enum")
	}
	return v, nil
}

func decodeOp(body []byte) (operation, error) {
	var v operation
	seen := false
	for len(body) > 0 {
		f, rest, err := next(body)
		if err != nil {
			return operation{}, err
		}
		body = rest
		if f.number != 1 && f.number != 2 {
			return operation{}, unsupported("Txn operation")
		}
		if err := f.want(protowire.BytesType); err != nil {
			return operation{}, err
		}
		// Repeated message fields require protobuf merge semantics, deliberately
		// excluded rather than approximated with last-message-wins.
		if seen {
			return operation{}, unsupported("repeated RequestOp message")
		}
		seen = true
		if f.number == 1 {
			v, err = decodeRange(f.bytes)
		} else {
			v, err = decodePut(f.bytes)
		}
		if err != nil {
			return operation{}, err
		}
	}
	if !seen {
		return operation{}, unsupported("empty RequestOp")
	}
	return v, nil
}

func (c *Command) decodeTxn(body []byte) error {
	for len(body) > 0 {
		f, rest, err := next(body)
		if err != nil {
			return err
		}
		body = rest
		if f.number < 1 || f.number > 3 {
			return unsupported("Txn field")
		}
		if err := f.want(protowire.BytesType); err != nil {
			return err
		}
		if f.number == 1 {
			v, err := decodeCompare(f.bytes)
			if err != nil {
				return err
			}
			c.compares = append(c.compares, v)
		} else {
			v, err := decodeOp(f.bytes)
			if err != nil {
				return err
			}
			if f.number == 2 {
				c.success = append(c.success, v)
			} else {
				c.failure = append(c.failure, v)
			}
		}
		if max(len(c.compares), len(c.success), len(c.failure)) > mvcc.MaxTxnOps {
			// Use the same etcd error as the ordinary validator.
			return rpctypes.ErrGRPCTooManyOps
		}
	}
	return nil
}
