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
	kindGrant   = 5
	kindRevoke  = 6
	kindRenew   = 7
	kindExpiry  = 8
	kindAlarm   = 9
)

var errBadBatch = errors.New("core: malformed batch")

// encodeBatch packs writes into the data of one Raft entry. A write in
// wire form goes in as the client sent it; other writes are marshaled.
func encodeBatch(origin, boot uint64, ps []*pending, size int) ([]byte, error) {
	buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))
	for _, p := range ps {
		var kind byte
		switch p.req.Op.(type) {
		case nil:
			kind = kindPut
		case *pb.PutRequest:
			kind = kindPut
		case *pb.DeleteRangeRequest:
			kind = kindDelete
		case *pb.TxnRequest:
			kind = kindTxn
		case *pb.CompactionRequest:
			kind = kindCompact
		case *pb.LeaseGrantRequest:
			kind = kindGrant
		case *pb.LeaseRevokeRequest:
			kind = kindRevoke
		case *pb.LeaseKeepAliveRequest:
			kind = kindRenew
		case *pb.LeaseCheckpoint:
			kind = kindExpiry
		case *pb.AlarmRequest:
			kind = kindAlarm
		default:
			return nil, fmt.Errorf("core: cannot propose %T", p.req.Op)
		}
		buf = binary.AppendUvarint(buf, origin)
		buf = binary.AppendUvarint(buf, boot)
		buf = binary.AppendUvarint(buf, p.req.ID)
		buf = append(buf, kind)
		if p.req.Op == nil {
			buf = binary.AppendUvarint(buf, uint64(len(p.req.Put.Wire)))
			buf = append(buf, p.req.Put.Wire...)
			continue
		}
		if p.req.Wire != nil {
			buf = binary.AppendUvarint(buf, uint64(len(p.req.Wire)))
			buf = append(buf, p.req.Wire...)
			continue
		}
		buf = binary.AppendUvarint(buf, uint64(proto.Size(p.req.Op)))
		var err error
		buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// walkBatch calls fn for every write in the data of a Raft entry, with
// the write's bytes in place and where they start in data.
func walkBatch(data []byte, fn func(origin, boot, id uint64, kind byte, b []byte, at int) error) error {
	whole := data
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
		if err := fn(origin, boot, id, kind, data[:size:size], len(whole)-len(data)); err != nil {
			return err
		}
		data = data[size:]
	}
	if len(data) != 0 {
		return errBadBatch
	}
	return nil
}

// newOp returns an empty request of an entry kind.
func newOp(kind byte) (proto.Message, error) {
	switch kind {
	case kindPut:
		return &pb.PutRequest{}, nil
	case kindDelete:
		return &pb.DeleteRangeRequest{}, nil
	case kindTxn:
		return &pb.TxnRequest{}, nil
	case kindCompact:
		return &pb.CompactionRequest{}, nil
	case kindGrant:
		return &pb.LeaseGrantRequest{}, nil
	case kindRevoke:
		return &pb.LeaseRevokeRequest{}, nil
	case kindRenew:
		return &pb.LeaseKeepAliveRequest{}, nil
	case kindExpiry:
		return &pb.LeaseCheckpoint{}, nil
	case kindAlarm:
		return &pb.AlarmRequest{}, nil
	}
	return nil, errBadBatch
}

// DecodeBatch calls fn for every write in the data of a Raft entry,
// decoded. The checks use it; the core applies writes in place.
func DecodeBatch(data []byte, fn func(origin, boot, id uint64, op proto.Message) error) error {
	return walkBatch(data, func(origin, boot, id uint64, kind byte, b []byte, _ int) error {
		op, err := newOp(kind)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(b, op); err != nil {
			return fmt.Errorf("%w: %w", errBadBatch, err)
		}
		return fn(origin, boot, id, op)
	})
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
