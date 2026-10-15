package server

import (
	"bytes"
	"context"
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	epb "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// etcd's election errors. Its service returns plain errors, which grpc-go
// sends with code Unknown.
var (
	errMissingLeaderKey = status.Error(codes.Unknown, `"leader" field must be provided`)
	errNotLeader        = status.Error(codes.Unknown, "election: not leader")
	errNoLeader         = status.Error(codes.Unknown, "election: no leader")
)

// campaign serves Election.Campaign as etcd v3.7.2 does, with the steps
// of clientv3's Election.Campaign. The candidate's key is the election's
// name, a slash and its lease in hex, holding its value. The oldest key
// under that prefix is the leader; Campaign answers once every older key
// is gone. A campaign that the caller gives up resigns.
func campaign(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	c := client{s: s, caller: caller}
	var r epb.CampaignRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	lease, err := c.sessionLease(ctx, r.Lease)
	if err != nil {
		return nil, clientErr(err)
	}
	prefix := []byte(string(r.Name) + "/")
	key := sessionKey(prefix, lease)
	resp, err := c.txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{createdIs(key, 0)},
		Success: []*pb.RequestOp{putOp(key, r.Value, lease)},
		Failure: []*pb.RequestOp{rangeOp(&pb.RangeRequest{Key: key})},
	})
	if err != nil {
		return nil, clientErr(err)
	}
	rev := resp.Header.Revision
	if !resp.Succeeded {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		rev = kv.CreateRevision
		if !bytes.Equal(kv.Value, r.Value) {
			if _, err := c.proclaim(ctx, key, rev, lease, r.Value); err != nil {
				// etcd resigns here too, but after "not leader" it
				// has forgotten its key, so its resign does nothing.
				if !errors.Is(err, errNotLeader) {
					_, _ = c.resign(ctx, key, rev)
				}
				return nil, clientErr(err)
			}
		}
	}
	if err := c.waitDeletes(ctx, prefix, rev-1); err != nil {
		if ctx.Err() != nil {
			// Like etcd, it ignores the answer: a key it could not
			// delete still goes with its lease.
			_, _ = c.resign(context.WithoutCancel(ctx), key, rev)
		}
		return nil, clientErr(err)
	}
	return &epb.CampaignResponse{Header: resp.Header, Leader: &epb.LeaderKey{Name: r.Name, Key: key, Rev: rev, Lease: lease}}, nil
}

// proclaim puts a new value in the leader's key if the key still has the
// create revision rev.
func (c client) proclaim(ctx context.Context, key []byte, rev, lease int64, value []byte) (*pb.TxnResponse, error) {
	resp, err := c.txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{createdIs(key, rev)},
		Success: []*pb.RequestOp{putOp(key, value, lease)},
	})
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		return nil, errNotLeader
	}
	return resp, nil
}

// resign deletes the leader's key if it still has the create revision
// rev.
func (c client) resign(ctx context.Context, key []byte, rev int64) (*pb.TxnResponse, error) {
	return c.txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{createdIs(key, rev)},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: key}}}},
	})
}

// electionProclaim serves Election.Proclaim. Like etcd, it grants a new
// lease for the value when the leader key names lease 0.
func electionProclaim(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	c := client{s: s, caller: caller}
	var r epb.ProclaimRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Leader == nil {
		return nil, errMissingLeaderKey
	}
	lease, err := c.sessionLease(ctx, r.Leader.Lease)
	if err != nil {
		return nil, clientErr(err)
	}
	resp, err := c.proclaim(ctx, r.Leader.Key, r.Leader.Rev, lease, r.Value)
	if err != nil {
		return nil, clientErr(err)
	}
	return &epb.ProclaimResponse{Header: resp.Header}, nil
}

// electionResign serves Election.Resign. A leader key that is already
// gone resigns without an error. Like etcd, it grants a lease that
// nothing uses when the leader key names lease 0.
func electionResign(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	c := client{s: s, caller: caller}
	var r epb.ResignRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Leader == nil {
		return nil, errMissingLeaderKey
	}
	if _, err := c.sessionLease(ctx, r.Leader.Lease); err != nil {
		return nil, clientErr(err)
	}
	resp, err := c.resign(ctx, r.Leader.Key, r.Leader.Rev)
	if err != nil {
		return nil, clientErr(err)
	}
	return &epb.ResignResponse{Header: resp.Header}, nil
}

// electionLeader serves Election.Leader: the oldest key under the
// election's prefix.
func electionLeader(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	c := client{s: s, caller: caller}
	var r epb.LeaderRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	resp, err := c.get(ctx, firstCreated([]byte(string(r.Name)+"/")))
	if err != nil {
		return nil, clientErr(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, errNoLeader
	}
	return &epb.LeaderResponse{Header: resp.Header, Kv: resp.Kvs[0]}, nil
}

// observe serves Election.Observe as etcd v3.7.2 does, with the steps of
// clientv3's Election.Observe. It sends the current leader, or waits for
// the first put under the prefix, then sends every change of the leader's
// key until it is deleted, and starts again. A failed read or watch ends
// the stream without an error, as in etcd.
func (s *Server) observe(st *stream) error {
	r := &epb.LeaderRequest{}
	if err := st.RecvOne(r); err != nil {
		return err
	}
	ctx := st.ctx
	c := client{s: s, caller: st.c.callerOf(st)}
	prefix := []byte(string(r.Name) + "/")
	for {
		resp, err := c.get(ctx, firstCreated(prefix))
		if err != nil {
			return observeEnd(ctx)
		}
		var header *pb.ResponseHeader
		var leader *mvccpb.KeyValue
		if len(resp.Kvs) > 0 {
			header, leader = resp.Header, resp.Kvs[0]
		} else {
			err := c.watchEvents(ctx, &pb.WatchCreateRequest{Key: prefix, RangeEnd: prefixEnd(prefix), StartRevision: resp.Header.Revision}, func(w *pb.WatchResponse) bool {
				for _, ev := range w.Events {
					if ev.Type == mvccpb.PUT {
						// The header revision of the put, not of
						// the response, which may hold later events.
						header, leader = proto.CloneOf(w.Header), ev.Kv
						header.Revision = ev.Kv.ModRevision
						return true
					}
				}
				return false
			})
			if err != nil {
				return observeEnd(ctx)
			}
		}
		if err := st.Send(&epb.LeaderResponse{Header: header, Kv: leader}); err != nil {
			return err
		}
		var sendErr error
		err = c.watchEvents(ctx, &pb.WatchCreateRequest{Key: leader.Key, StartRevision: header.Revision + 1}, func(w *pb.WatchResponse) bool {
			for _, ev := range w.Events {
				if ev.Type == mvccpb.DELETE {
					return true
				}
				if sendErr = st.Send(&epb.LeaderResponse{Header: w.Header, Kv: ev.Kv}); sendErr != nil {
					return true
				}
			}
			return false
		})
		if sendErr != nil {
			return sendErr
		}
		if err != nil {
			return observeEnd(ctx)
		}
	}
}

// observeEnd is how an Observe stream ends after a failed read or watch:
// with the caller's context error, or else without an error.
func observeEnd(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return toStatus(err)
	}
	return nil
}
