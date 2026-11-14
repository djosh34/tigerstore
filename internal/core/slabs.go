package core

import pb "go.etcd.io/etcd/api/v3/etcdserverpb"

// slabSize is how many messages of one type a slab allocates at once.
// A message that lives on keeps its slab alive, and a slab with messages
// left keeps those it handed out, and what they alias, so a slab stays
// small: a few KiB of messages.
const slabSize = 16

// slab hands out pointers to the elements of an array that it allocates
// slabSize at a time. Nothing is ever handed out twice: the garbage
// collector frees an array once no message in it is referred to.
type slab[T any] struct{ free []T }

func (s *slab[T]) take() *T {
	if len(s.free) == 0 {
		s.free = make([]T, slabSize)
	}
	m := &s.free[0]
	s.free = s.free[1:]
	return m
}

// Slabs are the messages one client connection decodes its requests
// into: one allocation per slabSize messages instead of one per message.
// A nil *Slabs allocates each message on its own. Not safe for
// concurrent use.
type Slabs struct {
	txns      slab[pb.TxnRequest]
	compares  slab[pb.Compare]
	ops       slab[pb.RequestOp]
	ranges    slab[pb.RangeRequest]
	puts      slab[pb.PutRequest]
	deletes   slab[pb.DeleteRangeRequest]
	rangeOps  slab[pb.RequestOp_RequestRange]
	putOps    slab[pb.RequestOp_RequestPut]
	deleteOps slab[pb.RequestOp_RequestDeleteRange]
}

// Txn returns an empty TxnRequest.
func (s *Slabs) Txn() *pb.TxnRequest {
	if s == nil {
		return &pb.TxnRequest{}
	}
	return s.txns.take()
}

// Range returns an empty RangeRequest.
func (s *Slabs) Range() *pb.RangeRequest {
	if s == nil {
		return &pb.RangeRequest{}
	}
	return s.ranges.take()
}

func (s *Slabs) compare() *pb.Compare {
	if s == nil {
		return &pb.Compare{}
	}
	return s.compares.take()
}

func (s *Slabs) op() *pb.RequestOp {
	if s == nil {
		return &pb.RequestOp{}
	}
	return s.ops.take()
}

func (s *Slabs) rangeOp() *pb.RequestOp_RequestRange {
	if s == nil {
		return &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{}}
	}
	r := s.rangeOps.take()
	r.RequestRange = s.ranges.take()
	return r
}

func (s *Slabs) putOp() *pb.RequestOp_RequestPut {
	if s == nil {
		return &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{}}
	}
	r := s.putOps.take()
	r.RequestPut = s.puts.take()
	return r
}

func (s *Slabs) deleteOp() *pb.RequestOp_RequestDeleteRange {
	if s == nil {
		return &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{}}
	}
	r := s.deleteOps.take()
	r.RequestDeleteRange = s.deletes.take()
	return r
}
