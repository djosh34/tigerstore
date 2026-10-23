// Package frometcd converts etcd's requests and responses to the history
// format. The converters refuse request fields that the format cannot
// hold, so a checker never sees a different request than the cluster did.
// The simulator and the real-process chaos run both use it.
package frometcd

import (
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
)

func rangeRequest(r *pb.RangeRequest) (*history.RangeRequest, error) {
	if r.SortOrder != pb.RangeRequest_NONE || r.SortTarget != pb.RangeRequest_KEY ||
		r.MinModRevision != 0 || r.MaxModRevision != 0 || r.MinCreateRevision != 0 || r.MaxCreateRevision != 0 {
		return nil, fmt.Errorf("the history format has no sort options or revision filters: %v", r)
	}
	return &history.RangeRequest{
		Key: string(r.Key), End: string(r.RangeEnd), Revision: r.Revision, Limit: r.Limit,
		CountOnly: r.CountOnly, KeysOnly: r.KeysOnly, Serializable: r.Serializable,
	}, nil
}

func putRequest(r *pb.PutRequest) (*history.PutRequest, error) {
	if r.IgnoreValue || r.IgnoreLease {
		return nil, fmt.Errorf("the history format has no ignore_value or ignore_lease: %v", r)
	}
	return &history.PutRequest{Key: string(r.Key), Value: string(r.Value), Lease: r.Lease, PrevKV: r.PrevKv}, nil
}

func deleteRequest(r *pb.DeleteRangeRequest) *history.DeleteRequest {
	return &history.DeleteRequest{Key: string(r.Key), End: string(r.RangeEnd), PrevKV: r.PrevKv}
}

// The history format numbers compare targets and results from 1 in etcd's
// order.
func compare(c *pb.Compare) (history.Compare, error) {
	if len(c.RangeEnd) != 0 {
		return history.Compare{}, fmt.Errorf("the history format has no compare over a range (%q to %q)", c.Key, c.RangeEnd)
	}
	out := history.Compare{Key: string(c.Key), Target: history.Target(c.Target) + 1, Result: history.Result(c.Result) + 1}
	switch c.Target {
	case pb.Compare_VERSION:
		out.Number = c.GetVersion()
	case pb.Compare_CREATE:
		out.Number = c.GetCreateRevision()
	case pb.Compare_MOD:
		out.Number = c.GetModRevision()
	case pb.Compare_LEASE:
		out.Number = c.GetLease()
	case pb.Compare_VALUE:
		out.Value = string(c.GetValue())
	}
	return out, nil
}

func txnOps(ops []*pb.RequestOp) ([]history.TxnOp, error) {
	var out []history.TxnOp
	for _, op := range ops {
		var hop history.TxnOp
		var err error
		switch t := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			hop.Range, err = rangeRequest(t.RequestRange)
		case *pb.RequestOp_RequestPut:
			hop.Put, err = putRequest(t.RequestPut)
		case *pb.RequestOp_RequestDeleteRange:
			hop.Delete = deleteRequest(t.RequestDeleteRange)
		default:
			return nil, fmt.Errorf("the history format has no %T in a transaction", t)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, hop)
	}
	return out, nil
}

// Request converts a request.
func Request(m proto.Message) (history.Request, error) {
	switch r := m.(type) {
	case *pb.RangeRequest:
		hr, err := rangeRequest(r)
		return history.Request{Range: hr}, err
	case *pb.PutRequest:
		hp, err := putRequest(r)
		return history.Request{Put: hp}, err
	case *pb.DeleteRangeRequest:
		return history.Request{Delete: deleteRequest(r)}, nil
	case *pb.CompactionRequest:
		return history.Request{Compact: &history.CompactRequest{Revision: r.Revision, Physical: r.Physical}}, nil
	case *pb.LeaseGrantRequest:
		return history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: r.ID, TTL: r.TTL}}, nil
	case *pb.LeaseRevokeRequest:
		return history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: r.ID}}, nil
	case *pb.TxnRequest:
		t := &history.TxnRequest{}
		for _, c := range r.Compare {
			hc, err := compare(c)
			if err != nil {
				return history.Request{}, err
			}
			t.Compare = append(t.Compare, hc)
		}
		var err error
		if t.Success, err = txnOps(r.Success); err != nil {
			return history.Request{}, err
		}
		if t.Failure, err = txnOps(r.Failure); err != nil {
			return history.Request{}, err
		}
		return history.Request{Txn: t}, nil
	}
	return history.Request{}, fmt.Errorf("the history format has no %T", m)
}

// KeyValue converts a key-value.
func KeyValue(kv *mvccpb.KeyValue) history.KeyValue {
	return history.KeyValue{
		Key: string(kv.Key), Value: string(kv.Value), CreateRevision: kv.CreateRevision,
		ModRevision: kv.ModRevision, Version: kv.Version, Lease: kv.Lease,
	}
}

func keyValues(kvs []*mvccpb.KeyValue) []history.KeyValue {
	var out []history.KeyValue
	for _, kv := range kvs {
		out = append(out, KeyValue(kv))
	}
	return out
}

// PrevKV converts a previous key-value, nil when it is nil.
func PrevKV(kv *mvccpb.KeyValue) *history.KeyValue {
	if kv == nil {
		return nil
	}
	v := KeyValue(kv)
	return &v
}

func rangeResponse(r *pb.RangeResponse) *history.RangeResponse {
	return &history.RangeResponse{KVs: keyValues(r.Kvs), Count: r.Count, More: r.More}
}

// Response converts the response to a request.
func Response(m proto.Message) (*history.Response, error) {
	switch r := m.(type) {
	case *pb.RangeResponse:
		return &history.Response{Revision: r.Header.Revision, Range: rangeResponse(r)}, nil
	case *pb.PutResponse:
		return &history.Response{Revision: r.Header.Revision, Put: &history.PutResponse{PrevKV: PrevKV(r.PrevKv)}}, nil
	case *pb.LeaseGrantResponse:
		return &history.Response{Revision: r.Header.Revision, LeaseGrant: &history.LeaseGrantResponse{ID: r.ID, TTL: r.TTL}}, nil
	case *pb.LeaseRevokeResponse:
		return &history.Response{Revision: r.Header.Revision, LeaseRevoke: &history.LeaseRevokeResponse{}}, nil
	case *pb.CompactionResponse:
		return &history.Response{Revision: r.Header.Revision, Compact: &history.CompactResponse{}}, nil
	case *pb.DeleteRangeResponse:
		return &history.Response{Revision: r.Header.Revision, Delete: &history.DeleteResponse{Deleted: r.Deleted, PrevKVs: keyValues(r.PrevKvs)}}, nil
	case *pb.TxnResponse:
		t := &history.TxnResponse{Succeeded: r.Succeeded}
		for _, op := range r.Responses {
			switch o := op.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				t.Results = append(t.Results, history.TxnOpResponse{Range: rangeResponse(o.ResponseRange)})
			case *pb.ResponseOp_ResponsePut:
				t.Results = append(t.Results, history.TxnOpResponse{Put: &history.PutResponse{PrevKV: PrevKV(o.ResponsePut.PrevKv)}})
			case *pb.ResponseOp_ResponseDeleteRange:
				d := o.ResponseDeleteRange
				t.Results = append(t.Results, history.TxnOpResponse{Delete: &history.DeleteResponse{Deleted: d.Deleted, PrevKVs: keyValues(d.PrevKvs)}})
			default:
				return nil, fmt.Errorf("the history format has no %T in a transaction", o)
			}
		}
		return &history.Response{Revision: r.Header.Revision, Txn: t}, nil
	}
	return nil, fmt.Errorf("the history format has no %T", m)
}
