// Package floor contains experiment fixtures matching loadgen's put/txn-1k.
package floor

import (
	"encoding/binary"
	"fmt"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func field(b []byte, n protowire.Number, v []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, n, protowire.BytesType), v)
}
func Key(i int) []byte {
	const a = "bcdfghjklmnpqrstvwxz2456789"
	x := uint64(i) * 0x9e3779b97f4a7c15
	var name [15]byte
	for j := range name {
		name[j] = a[x%uint64(len(a))]
		x /= uint64(len(a))
	}
	return fmt.Appendf(nil, "/registry/pods/bench-%03d/web-%s-%s", i%100, name[:10], name[10:])
}
func Put(i int) *pb.PutRequest { return &pb.PutRequest{Key: Key(i), Value: make([]byte, 1024)} }
func Txn(i int, rev int64) *pb.TxnRequest {
	p := Put(i)
	return &pb.TxnRequest{Compare: []*pb.Compare{{Target: pb.Compare_MOD, Key: p.Key, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: p}}}, Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: p.Key}}}}}
}

// Batch reproduces core.encodeBatch format 3 with a 64-bit origin and steady-state IDs.
func Batch(txn bool, n int) []byte {
	w := Wire(txn)
	b := binary.AppendUvarint([]byte{0, 3}, uint64(n))
	for i := range n {
		b = binary.AppendUvarint(b, 0x123456789abcdef0)
		b = binary.AppendUvarint(b, 1)
		b = binary.AppendUvarint(b, uint64(1000000+i))
		kind := byte(1)
		if txn {
			kind = 3
		}
		b = append(b, kind)
		b = binary.AppendUvarint(b, uint64(len(w)))
		b = append(b, w...)
	}
	return b
}
func Wire(txn bool) []byte {
	p := Put(0)
	put, _ := proto.Marshal(p)
	if !txn {
		return put
	}
	// loadgen's mod_revision is a nine-byte non-minimal varint.
	cmp := protowire.AppendVarint(protowire.AppendTag(nil, 2, protowire.VarintType), 2)
	cmp = field(cmp, 3, p.Key)
	cmp = protowire.AppendTag(cmp, 6, protowire.VarintType)
	cmp = append(cmp, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0)
	rng, _ := proto.Marshal(&pb.RangeRequest{Key: p.Key})
	b := field(nil, 1, cmp)
	b = field(b, 2, field(nil, 2, put))
	return field(b, 3, field(nil, 1, rng))
}
