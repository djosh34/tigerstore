package core

import (
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// Put is a PutRequest read in place from its wire form. Key and Value
// are slices of Wire, which nobody may change afterwards.
type Put struct {
	Wire []byte
	mvcc.PutOp
}

var errBadPut = errors.New("core: malformed PutRequest")

// ParsePut reads a PutRequest from its wire form without copying it, with
// protobuf's rules: the last value of a field wins, and unknown fields and
// fields of the wrong wire type are skipped.
func ParsePut(b []byte) (Put, error) {
	p := Put{Wire: b}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return Put{}, errBadPut
		}
		b = b[n:]
		switch {
		case typ == protowire.BytesType && (num == 1 || num == 2):
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return Put{}, errBadPut
			}
			if num == 1 {
				p.Key = v
			} else {
				p.Value = v
			}
			b = b[n:]
		case typ == protowire.VarintType && num >= 3 && num <= 6:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return Put{}, errBadPut
			}
			switch int(num) {
			case 3:
				p.Lease = int64(v)
			case 4:
				p.PrevKv = v != 0
			case 5:
				p.IgnoreValue = v != 0
			case 6:
				p.IgnoreLease = v != 0
			}
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return Put{}, errBadPut
			}
			b = b[n:]
		}
	}
	return p, nil
}

// PutResponse builds the response of a put in wire form from its Reply,
// as a node that decodes it gets.
func (n *Node) PutResponse(r Reply) *pb.PutResponse {
	resp := &pb.PutResponse{Header: &pb.ResponseHeader{Revision: r.Rev}, PrevKv: r.PrevKv}
	n.fillHeader(resp.Header)
	resp.Header.RaftTerm = r.Term
	return resp
}
