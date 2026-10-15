package core

import (
	"encoding/binary"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// A Raft entry carries a batch of writes:
//
//	uvarint count, then per write:
//	uvarint origin node | uvarint origin boot | uvarint request ID |
//	u8 kind | uvarint length | protobuf request
//
// The origin node, boot and request ID let the node that received the
// request answer it once the entry applies.
//
// That is entry format 1. Every later format starts with a 0 byte and its
// number, then the batch; a format 1 batch never starts with 0, as it
// holds at least one write. Format 2 adds only that header. Format 3
// adds the kinds from kindAlarm on (featureVersion). Which format a node
// writes is the cluster version (version.go).

const (
	kindPut     = 1
	kindDelete  = 2
	kindTxn     = 3
	kindCompact = 4
	kindGrant   = 5
	kindRevoke  = 6
	kindRenew   = 7
	kindExpiry  = 8
	// kindNoSpace stands for a forwarded write that the leader refused
	// because its disk is past its budget. It has no request.
	kindNoSpace = 9
	// kindVersion is a node's announcement of the newest version it
	// supports, a wrapperspb.UInt32Value. It changes no key.
	kindVersion = 10
	kindAlarm   = 11
	// kindMember is a member's name and client URLs, which it publishes
	// once it starts (members.go).
	kindMember = 12
	// kindAuth is an auth request, an InternalRaftRequest with the user
	// and auth revision in its header (auth.go).
	kindAuth = 13
	// kindAuthed wraps a write with the identity it was taken with while
	// auth is enabled: uvarint auth revision | uvarint length | user |
	// u8 kind | the write.
	kindAuthed = 14
	// kindLeaseCheckpoint is the leader's checkpoint of leases' remaining
	// TTLs, a LeaseCheckpointRequest (lease.go).
	kindLeaseCheckpoint = 15
)

var errBadBatch = errors.New("core: malformed batch")

// encodeBatch packs writes into the data of one Raft entry of a format. A
// write in wire form goes in as the client sent it; other writes are
// marshaled.
func encodeBatch(format uint32, origin, boot uint64, ps []*pending, size int) ([]byte, error) {
	buf := make([]byte, 0, 2+binary.MaxVarintLen64+size)
	if format > 1 {
		buf = append(buf, 0, byte(format))
	}
	buf = binary.AppendUvarint(buf, uint64(len(ps)))
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
		case *wrapperspb.UInt32Value:
			kind = kindVersion
		case *pb.AlarmRequest:
			kind = kindAlarm
		case *pb.Member:
			kind = kindMember
		case *pb.InternalRaftRequest:
			kind = kindAuth
		case *pb.LeaseCheckpointRequest:
			kind = kindLeaseCheckpoint
		default:
			return nil, fmt.Errorf("core: cannot propose %T", p.req.Op)
		}
		buf = binary.AppendUvarint(buf, origin)
		buf = binary.AppendUvarint(buf, boot)
		buf = binary.AppendUvarint(buf, p.req.ID)
		var body []byte
		switch {
		case p.req.Op == nil:
			body = p.req.Put.Wire
		case p.req.Wire != nil:
			body = p.req.Wire
		default:
			var err error
			if body, err = (proto.MarshalOptions{Deterministic: true}).Marshal(p.req.Op); err != nil {
				return nil, err
			}
		}
		if w := p.who; w != nil {
			head := binary.AppendUvarint(nil, w.revision)
			head = binary.AppendUvarint(head, uint64(len(w.user)))
			head = append(append(head, w.user...), kind)
			buf = append(buf, kindAuthed)
			buf = binary.AppendUvarint(buf, uint64(len(head)+len(body)))
			buf = append(buf, head...)
		} else {
			buf = append(buf, kind)
			buf = binary.AppendUvarint(buf, uint64(len(body)))
		}
		buf = append(buf, body...)
	}
	return buf, nil
}

// unwrapAuth takes the identity off a write of kindAuthed, whose bytes b
// start at offset at of the entry, and returns the write's kind, bytes
// and offset, and the identity: the zero one for a write without it.
func unwrapAuth(kind byte, b []byte, at int) (byte, []byte, int, authInfo, error) {
	if kind != kindAuthed {
		return kind, b, at, authInfo{}, nil
	}
	rest := b
	rev, rest, ok := uvarint(rest)
	if !ok {
		return 0, nil, 0, authInfo{}, errBadBatch
	}
	n, rest, ok := uvarint(rest)
	if !ok || n >= uint64(len(rest)) {
		return 0, nil, 0, authInfo{}, errBadBatch
	}
	ai := authInfo{user: string(rest[:n]), revision: rev}
	kind, rest = rest[n], rest[n+1:]
	return kind, rest, at + len(b) - len(rest), ai, nil
}

// skipFormat returns the data of a Raft entry without its format header.
func skipFormat(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != 0 {
		return data, nil
	}
	if len(data) < 2 {
		return nil, errBadBatch
	}
	if f := data[1]; f < 2 || f > MaxVersion {
		return nil, fmt.Errorf("%w: entry format %d; this build reads formats up to %d", errBadBatch, f, MaxVersion)
	}
	return data[2:], nil
}

// walkBatch calls fn for every write in the data of a Raft entry, with
// the write's bytes in place and where they start in data.
func walkBatch(data []byte, fn func(origin, boot, id uint64, kind byte, b []byte, at int) error) error {
	whole := data
	data, err := skipFormat(data)
	if err != nil {
		return err
	}
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

// refuseSpace returns the data of a Raft entry with every write that
// TakesSpace replaced by a kindNoSpace record. A transaction in an entry
// is never read-only, so all of them take space.
func refuseSpace(data []byte) ([]byte, error) {
	batch, err := skipFormat(data)
	if err != nil {
		return nil, err
	}
	n, _, ok := uvarint(batch)
	if !ok {
		return nil, errBadBatch
	}
	out := append(make([]byte, 0, len(data)), data[:len(data)-len(batch)]...)
	out = binary.AppendUvarint(out, n)
	err = walkBatch(data, func(origin, boot, id uint64, kind byte, b []byte, at int) error {
		inner, _, _, _, err := unwrapAuth(kind, b, at)
		if err != nil {
			return err
		}
		if inner == kindPut || inner == kindTxn || inner == kindGrant {
			kind, b = kindNoSpace, nil
		}
		out = binary.AppendUvarint(out, origin)
		out = binary.AppendUvarint(out, boot)
		out = binary.AppendUvarint(out, id)
		out = append(out, kind)
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
		return nil
	})
	return out, err
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
	case kindMember:
		return &pb.Member{}, nil
	case kindAuth:
		return &pb.InternalRaftRequest{}, nil
	case kindLeaseCheckpoint:
		return &pb.LeaseCheckpointRequest{}, nil
	}
	return nil, errBadBatch
}

// DecodeBatch calls fn for every write in the data of a Raft entry,
// decoded, and skips refused writes and version announcements, which change no key. The
// checks use it; the core applies writes in place.
func DecodeBatch(data []byte, fn func(origin, boot, id uint64, op proto.Message) error) error {
	return walkBatch(data, func(origin, boot, id uint64, kind byte, b []byte, at int) error {
		kind, b, _, _, err := unwrapAuth(kind, b, at)
		if err != nil {
			return err
		}
		if kind == kindNoSpace || kind == kindVersion {
			// A refused write did not apply, and an announcement changes
			// no key.
			return nil
		}
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
