package core

import (
	"encoding/binary"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// A Raft entry carries a batch of writes:
//
//	uvarint count, then per write:
//	uvarint origin node | uvarint origin boot | uvarint request ID |
//	u8 kind | uvarint length | protobuf request
//
// The origin node, boot and request ID let the node that received the
// request answer it once the entry applies.

const (
	kindPut     = 1
	kindDelete  = 2
	kindTxn     = 3
	kindCompact = 4
)

var errBadBatch = errors.New("core: malformed batch")

func encodeBatch(origin, boot uint64, ps []*pending) ([]byte, error) {
	buf := binary.AppendUvarint(nil, uint64(len(ps)))
	for _, p := range ps {
		var kind byte
		switch p.req.Op.(type) {
		case *pb.PutRequest:
			kind = kindPut
		case *pb.DeleteRangeRequest:
			kind = kindDelete
		case *pb.TxnRequest:
			kind = kindTxn
		case *pb.CompactionRequest:
			kind = kindCompact
		default:
			return nil, fmt.Errorf("core: cannot propose %T", p.req.Op)
		}
		buf = binary.AppendUvarint(buf, origin)
		buf = binary.AppendUvarint(buf, boot)
		buf = binary.AppendUvarint(buf, p.req.ID)
		buf = append(buf, kind)
		buf = binary.AppendUvarint(buf, uint64(proto.Size(p.req.Op)))
		var err error
		buf, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(buf, p.req.Op)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// DecodeBatch calls fn for every write in the data of a Raft entry.
func DecodeBatch(data []byte, fn func(origin, boot, id uint64, op proto.Message) error) error {
	n, data, ok := uvarint(data)
	if !ok {
		return errBadBatch
	}
	for range n {
		var origin, boot, id, size uint64
		if origin, data, ok = uvarint(data); !ok {
			return errBadBatch
		}
		if boot, data, ok = uvarint(data); !ok {
			return errBadBatch
		}
		if id, data, ok = uvarint(data); !ok || len(data) == 0 {
			return errBadBatch
		}
		kind := data[0]
		if size, data, ok = uvarint(data[1:]); !ok || size > uint64(len(data)) {
			return errBadBatch
		}
		var op proto.Message
		switch kind {
		case kindPut:
			op = &pb.PutRequest{}
		case kindDelete:
			op = &pb.DeleteRangeRequest{}
		case kindTxn:
			op = &pb.TxnRequest{}
		case kindCompact:
			op = &pb.CompactionRequest{}
		default:
			return errBadBatch
		}
		if err := proto.Unmarshal(data[:size], op); err != nil {
			return fmt.Errorf("%w: %w", errBadBatch, err)
		}
		data = data[size:]
		if err := fn(origin, boot, id, op); err != nil {
			return err
		}
	}
	if len(data) != 0 {
		return errBadBatch
	}
	return nil
}

func uvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, b, false
	}
	return v, b[n:], true
}

// readCtx identifies one ReadIndex request of this run of the node.
func readCtx(boot, seq uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, boot)
	return binary.BigEndian.AppendUint64(b, seq)
}

func parseReadCtx(boot uint64, b []byte) (uint64, bool) {
	if len(b) != 16 || binary.BigEndian.Uint64(b) != boot {
		return 0, false
	}
	return binary.BigEndian.Uint64(b[8:]), true
}
