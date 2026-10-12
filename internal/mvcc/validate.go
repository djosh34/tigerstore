package mvcc

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// These checks run before a request enters the Raft log, like etcd's
// server/etcdserver/api/v3rpc/key.go in v3.7.2.

// MaxTxnOps is etcd's default --max-txn-ops.
const MaxTxnOps = 128

// CheckRange validates a range request.
func CheckRange(r *pb.RangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	if _, ok := pb.RangeRequest_SortOrder_name[int32(r.SortOrder)]; !ok {
		return rpctypes.ErrGRPCInvalidSortOption
	}
	if _, ok := pb.RangeRequest_SortTarget_name[int32(r.SortTarget)]; !ok {
		return rpctypes.ErrGRPCInvalidSortOption
	}
	return nil
}

// CheckPut validates a put request.
func CheckPut(r *pb.PutRequest) error {
	switch {
	case len(r.Key) == 0:
		return rpctypes.ErrGRPCEmptyKey
	case r.IgnoreValue && len(r.Value) != 0:
		return rpctypes.ErrGRPCValueProvided
	case r.IgnoreLease && r.Lease != 0:
		return rpctypes.ErrGRPCLeaseProvided
	}
	return nil
}

// CheckDelete validates a delete request.
func CheckDelete(r *pb.DeleteRangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
}

// CheckTxn validates a transaction, including that no two writes in it
// touch the same key.
func CheckTxn(r *pb.TxnRequest) error {
	if err := validateTxn(r, MaxTxnOps); err != nil {
		return err
	}
	if _, _, err := checkIntervals(r.Success); err != nil {
		return err
	}
	_, _, err := checkIntervals(r.Failure)
	return err
}

func validateTxn(r *pb.TxnRequest, maxOps int) error {
	opc := max(len(r.Compare), len(r.Success), len(r.Failure))
	if opc > maxOps {
		return rpctypes.ErrGRPCTooManyOps
	}
	for _, c := range r.Compare {
		if len(c.Key) == 0 {
			return rpctypes.ErrGRPCEmptyKey
		}
	}
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			if err := validateRequestOp(op, maxOps-opc); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRequestOp(op *pb.RequestOp, maxOps int) error {
	switch t := op.Request.(type) {
	case *pb.RequestOp_RequestRange:
		return CheckRange(t.RequestRange)
	case *pb.RequestOp_RequestPut:
		return CheckPut(t.RequestPut)
	case *pb.RequestOp_RequestDeleteRange:
		return CheckDelete(t.RequestDeleteRange)
	case *pb.RequestOp_RequestTxn:
		return validateTxn(t.RequestTxn, maxOps)
	}
	// etcd answers an empty operation with "key not found".
	return rpctypes.ErrGRPCKeyNotFound
}

// interval is [lo, hi) with plain string order. A delete without a range
// end is the point [key, key+"\x00").
type interval struct{ lo, hi string }

func (iv interval) has(k string) bool { return iv.lo <= k && k < iv.hi }

func deleteInterval(d *pb.DeleteRangeRequest) interval {
	if len(d.RangeEnd) == 0 {
		return interval{string(d.Key), string(d.Key) + "\x00"}
	}
	return interval{string(d.Key), string(d.RangeEnd)}
}

func hits(dels []interval, k string) bool {
	for _, d := range dels {
		if d.has(k) {
			return true
		}
	}
	return false
}

// checkIntervals returns the puts and deletes of ops and nested
// transactions, and fails if a put overlaps another put or a delete. A key
// put in both branches of one nested transaction is fine.
func checkIntervals(ops []*pb.RequestOp) (map[string]struct{}, []interval, error) {
	var dels []interval
	for _, op := range ops {
		if d := op.GetRequestDeleteRange(); d != nil {
			dels = append(dels, deleteInterval(d))
		}
	}
	puts := make(map[string]struct{})
	for _, op := range ops {
		t := op.GetRequestTxn()
		if t == nil {
			continue
		}
		putsThen, delsThen, err := checkIntervals(t.Success)
		if err != nil {
			return nil, nil, err
		}
		putsElse, delsElse, err := checkIntervals(t.Failure)
		if err != nil {
			return nil, nil, err
		}
		for k := range putsThen {
			if _, ok := puts[k]; ok || hits(dels, k) {
				return nil, nil, rpctypes.ErrGRPCDuplicateKey
			}
			puts[k] = struct{}{}
		}
		for k := range putsElse {
			if _, ok := puts[k]; ok {
				if _, safe := putsThen[k]; !safe {
					return nil, nil, rpctypes.ErrGRPCDuplicateKey
				}
			}
			if hits(dels, k) {
				return nil, nil, rpctypes.ErrGRPCDuplicateKey
			}
			puts[k] = struct{}{}
		}
		dels = append(dels, delsThen...)
		dels = append(dels, delsElse...)
	}
	for _, op := range ops {
		p := op.GetRequestPut()
		if p == nil {
			continue
		}
		k := string(p.Key)
		if _, ok := puts[k]; ok || hits(dels, k) {
			return nil, nil, rpctypes.ErrGRPCDuplicateKey
		}
		puts[k] = struct{}{}
	}
	return puts, dels, nil
}

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
