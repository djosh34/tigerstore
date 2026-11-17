package mvcc

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// These checks run before a request enters the Raft log, like etcd's
// server/etcdserver/api/v3rpc/key.go in v3.7.2.

// MaxTxnOps is etcd's default --max-txn-ops.
const MaxTxnOps = 128

// IsTxnReadOnly reports whether a transaction only reads.
func IsTxnReadOnly(r *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			if op.GetRequestRange() == nil {
				return false
			}
		}
	}
	return true
}

// IsTxnSerializable reports whether a transaction only has serializable
// reads.
func IsTxnSerializable(r *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			if rr := op.GetRequestRange(); rr == nil || !rr.Serializable {
				return false
			}
		}
	}
	return true
}
