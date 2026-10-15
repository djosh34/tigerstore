package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// errSessionExpired is etcd's answer to a Lock whose key went away, with
// its lease, while it waited.
var errSessionExpired = status.Error(codes.Unknown, "mutex: session is expired")

// lock serves Lock.Lock as etcd v3.7.2 does, with the steps of clientv3's
// Mutex.Lock. The caller's key is the lock's name, a slash and its lease
// in hex. The oldest key under that prefix holds the lock; the others
// wait for every older key to go. A wait that fails deletes the caller's
// key again.
func lock(ctx context.Context, s *Server, b []byte) (proto.Message, error) {
	var r v3lockpb.LockRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	lease, err := s.sessionLease(ctx, r.Lease)
	if err != nil {
		return nil, clientErr(err)
	}
	prefix := []byte(string(r.Name) + "/")
	key := sessionKey(prefix, lease)
	owner := rangeOp(firstCreated(prefix))
	resp, err := s.txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{createdIs(key, 0)},
		Success: []*pb.RequestOp{putOp(key, nil, lease), owner},
		Failure: []*pb.RequestOp{rangeOp(&pb.RangeRequest{Key: key}), owner},
	})
	if err != nil {
		return nil, clientErr(err)
	}
	rev := resp.Header.Revision
	if !resp.Succeeded {
		rev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	first := resp.Responses[1].GetResponseRange().Kvs
	if len(first) == 0 || first[0].CreateRevision == rev {
		return &v3lockpb.LockResponse{Header: resp.Header, Key: key}, nil
	}
	if err := s.waitDeletes(ctx, prefix, rev-1); err != nil {
		s.release(ctx, key)
		return nil, clientErr(err)
	}
	// The key may have gone with its lease while the lock was taken.
	get, err := s.get(ctx, &pb.RangeRequest{Key: key})
	if err != nil {
		s.release(ctx, key)
		return nil, clientErr(err)
	}
	if len(get.Kvs) == 0 {
		return nil, errSessionExpired
	}
	return &v3lockpb.LockResponse{Header: get.Header, Key: key}, nil
}

// release deletes the key of a Lock that failed, even when the caller
// has gone. Like etcd, it ignores the answer: a key it could not delete
// still goes with its lease.
func (s *Server) release(ctx context.Context, key []byte) {
	_, _ = s.n.Do(context.WithoutCancel(ctx), &pb.DeleteRangeRequest{Key: key})
}

// unlock serves Lock.Unlock: it deletes the key, whatever it is.
func unlock(ctx context.Context, s *Server, b []byte) (proto.Message, error) {
	var r v3lockpb.UnlockRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	resp, err := s.n.Do(ctx, &pb.DeleteRangeRequest{Key: r.Key})
	if err != nil {
		return nil, clientErr(err)
	}
	return &v3lockpb.UnlockResponse{Header: resp.(*pb.DeleteRangeResponse).Header}, nil
}
