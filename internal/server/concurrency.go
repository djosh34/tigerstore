package server

import (
	"context"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

// etcd v3.7.2 serves its Lock and Election services with an in-process
// client of itself, running the recipes of clientv3's concurrency
// package. The functions here do the same requests in the same order on
// the node, so a client sees the same keys, revisions and errors.

// client runs the requests of one Lock or Election call on the node as
// the call's caller. etcd's in-process client runs them with the call's
// context, so the caller's certificate counts in the size limit of its
// writes as it does for a KV write.
type client struct {
	s      *Server
	caller core.Caller
}

func (c client) do(ctx context.Context, op proto.Message) (proto.Message, error) {
	return c.s.n.DoAs(ctx, c.caller, op)
}

// sessionTTL is the TTL of the lease that etcd's concurrency session
// grants when a request names lease 0.
const sessionTTL = 60

// sessionLease returns the lease of the session that etcd's services
// make for a request: the request's lease, or a new lease of 60 s for
// lease 0. etcd orphans the session at once, so nothing keeps that lease
// alive, and it does not check that the request's lease exists.
func (c client) sessionLease(ctx context.Context, lease int64) (int64, error) {
	if lease != 0 {
		return lease, nil
	}
	resp, err := c.do(ctx, &pb.LeaseGrantRequest{TTL: sessionTTL})
	if err != nil {
		return 0, err
	}
	return resp.(*pb.LeaseGrantResponse).ID, nil
}

// sessionKey is the key of a session's lease under prefix, as the
// concurrency package names it.
func sessionKey(prefix []byte, lease int64) []byte {
	return fmt.Appendf(nil, "%s%x", prefix, lease)
}

// clientErr turns the error of a request that a service made into the
// one etcd's service answers. Its in-process client turns etcd's own
// errors into plain errors with the same text, which grpc-go sends with
// code Unknown. Other errors keep their status.
func clientErr(err error) error {
	err = toStatus(err)
	if e, ok := errors.AsType[rpctypes.EtcdError](rpctypes.Error(err)); ok {
		return status.Error(codes.Unknown, e.Error())
	}
	return err
}

// prefixEnd is the range end of every key with prefix, as clientv3's
// WithPrefix makes it.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}

// createdIs compares the create revision of key with rev.
func createdIs(key []byte, rev int64) *pb.Compare {
	return &pb.Compare{Key: key, Target: pb.Compare_CREATE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_CreateRevision{CreateRevision: rev}}
}

func putOp(key, value []byte, lease int64) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: value, Lease: lease}}}
}

func rangeOp(r *pb.RangeRequest) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: r}}
}

// firstCreated reads the key under prefix that was created first, as
// clientv3's WithFirstCreate does.
func firstCreated(prefix []byte) *pb.RangeRequest {
	return &pb.RangeRequest{Key: prefix, RangeEnd: prefixEnd(prefix), Limit: 1, SortOrder: pb.RangeRequest_ASCEND, SortTarget: pb.RangeRequest_CREATE}
}

func (c client) txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	resp, err := c.do(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func (c client) get(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := c.do(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RangeResponse), nil
}

// waitDeletes waits until every key under prefix created at or before
// maxCreate is deleted, as the concurrency package's waitDeletes does:
// it waits for the newest of them to go, then looks again.
func (c client) waitDeletes(ctx context.Context, prefix []byte, maxCreate int64) error {
	for {
		resp, err := c.get(ctx, &pb.RangeRequest{Key: prefix, RangeEnd: prefixEnd(prefix), Limit: 1, SortOrder: pb.RangeRequest_DESCEND, SortTarget: pb.RangeRequest_CREATE, MaxCreateRevision: maxCreate})
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}
		err = c.watchEvents(ctx, &pb.WatchCreateRequest{Key: resp.Kvs[0].Key, StartRevision: resp.Header.Revision}, func(w *pb.WatchResponse) bool {
			for _, ev := range w.Events {
				if ev.Type == mvccpb.DELETE {
					return true
				}
			}
			return false
		})
		if err != nil {
			return err
		}
	}
}

// watchEvents watches as clientv3 does for the concurrency package. It
// hands f every response with events until f reports that it is done.
// When the node drops the watch stream, because it fell behind or the
// node installed a snapshot, it watches again after the last event, as
// clientv3 resumes a watch.
func (c client) watchEvents(ctx context.Context, req *pb.WatchCreateRequest, f func(*pb.WatchResponse) bool) error {
	req = proto.CloneOf(req)
	for {
		err := c.watchOnce(ctx, req, f)
		if !errors.Is(err, node.ErrWatchTooSlow) && !errors.Is(err, node.ErrSnapshotInstalled) {
			return err
		}
	}
}

// watchOnce runs watchEvents on one watch stream. It moves the start of
// req past each event it hands f.
func (c client) watchOnce(ctx context.Context, req *pb.WatchCreateRequest, f func(*pb.WatchResponse) bool) error {
	ws, err := c.s.n.OpenWatch(ctx, c.caller)
	if err != nil {
		return err
	}
	defer ws.Close()
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: req}}); err != nil {
		return err
	}
	for {
		select {
		case r := <-ws.C:
			ws.Taken(r)
			if r.Canceled {
				return watchErr(r)
			}
			if len(r.Events) == 0 {
				continue
			}
			req.StartRevision = r.Events[len(r.Events)-1].Kv.ModRevision + 1
			if f(r) {
				return nil
			}
		case <-ws.Done():
			return ws.Err()
		case <-c.s.n.Done():
			return rpctypes.ErrGRPCStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// watchErr is clientv3's error for a watch that the server canceled.
func watchErr(r *pb.WatchResponse) error {
	switch {
	case r.CompactRevision != 0:
		return rpctypes.ErrGRPCCompacted
	case r.CancelReason != "":
		return status.Error(codes.FailedPrecondition, r.CancelReason)
	}
	return rpctypes.ErrGRPCFutureRev
}
