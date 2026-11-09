package main

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// The methods the log decodes. Others keep only their sizes.
const (
	methodRange          = "/etcdserverpb.KV/Range"
	methodRangeStream    = "/etcdserverpb.KV/RangeStream"
	methodPut            = "/etcdserverpb.KV/Put"
	methodDelete         = "/etcdserverpb.KV/DeleteRange"
	methodTxn            = "/etcdserverpb.KV/Txn"
	methodCompact        = "/etcdserverpb.KV/Compact"
	methodWatch          = "/etcdserverpb.Watch/Watch"
	methodLeaseGrant     = "/etcdserverpb.Lease/LeaseGrant"
	methodLeaseRevoke    = "/etcdserverpb.Lease/LeaseRevoke"
	methodLeaseKeepAlive = "/etcdserverpb.Lease/LeaseKeepAlive"
	methodLeaseTTL       = "/etcdserverpb.Lease/LeaseTimeToLive"
)

// decodeRequest decodes a request message of method, or returns nil for a
// method the log does not decode.
func decodeRequest(method string, b []byte) (*k8strace.Request, error) {
	switch method {
	case methodRange, methodRangeStream:
		var m pb.RangeRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Range: rangeReq(&m)}, nil
	case methodPut:
		var m pb.PutRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Put: putReq(&m)}, nil
	case methodDelete:
		var m pb.DeleteRangeRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Delete: deleteReq(&m)}, nil
	case methodTxn:
		var m pb.TxnRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Txn: txnReq(&m)}, nil
	case methodCompact:
		var m pb.CompactionRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Compact: &k8strace.Compact{Rev: m.Revision, Physical: m.Physical}}, nil
	case methodLeaseGrant:
		var m pb.LeaseGrantRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{LeaseGrant: &k8strace.LeaseGrant{TTL: m.TTL, ID: m.ID}}, nil
	case methodLeaseRevoke:
		var m pb.LeaseRevokeRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Lease: &m.ID}, nil
	case methodLeaseKeepAlive:
		var m pb.LeaseKeepAliveRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Lease: &m.ID}, nil
	case methodLeaseTTL:
		var m pb.LeaseTimeToLiveRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Lease: &m.ID}, nil
	case methodWatch:
		var m pb.WatchRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Request{Watch: watchReq(&m)}, nil
	}
	return nil, nil
}

func rangeReq(m *pb.RangeRequest) *k8strace.Range {
	return &k8strace.Range{
		Key: string(m.Key), End: string(m.RangeEnd), Limit: m.Limit, Rev: m.Revision,
		Serializable: m.Serializable, KeysOnly: m.KeysOnly, CountOnly: m.CountOnly,
		SortOrder: int32(m.SortOrder), SortTarget: int32(m.SortTarget),
		MinMod: m.MinModRevision, MaxMod: m.MaxModRevision, MinCreate: m.MinCreateRevision, MaxCreate: m.MaxCreateRevision,
	}
}

func putReq(m *pb.PutRequest) *k8strace.Put {
	return &k8strace.Put{Key: string(m.Key), Size: len(m.Value), Lease: m.Lease, PrevKV: m.PrevKv, IgnoreValue: m.IgnoreValue, IgnoreLease: m.IgnoreLease}
}

func deleteReq(m *pb.DeleteRangeRequest) *k8strace.Delete {
	return &k8strace.Delete{Key: string(m.Key), End: string(m.RangeEnd), PrevKV: m.PrevKv}
}

func txnReq(m *pb.TxnRequest) *k8strace.Txn {
	t := &k8strace.Txn{Success: ops(m.Success), Failure: ops(m.Failure)}
	for _, c := range m.Compare {
		cmp := k8strace.Compare{Target: int32(c.Target), Result: int32(c.Result), Key: string(c.Key), End: string(c.RangeEnd)}
		switch u := c.TargetUnion.(type) {
		case *pb.Compare_Version:
			cmp.Value = u.Version
		case *pb.Compare_CreateRevision:
			cmp.Value = u.CreateRevision
		case *pb.Compare_ModRevision:
			cmp.Value = u.ModRevision
		case *pb.Compare_Value:
			cmp.Value = int64(len(u.Value))
		case *pb.Compare_Lease:
			cmp.Value = u.Lease
		}
		t.Compare = append(t.Compare, cmp)
	}
	return t
}

func ops(in []*pb.RequestOp) []k8strace.Op {
	var out []k8strace.Op
	for _, o := range in {
		var op k8strace.Op
		switch r := o.Request.(type) {
		case *pb.RequestOp_RequestRange:
			op.Range = rangeReq(r.RequestRange)
		case *pb.RequestOp_RequestPut:
			op.Put = putReq(r.RequestPut)
		case *pb.RequestOp_RequestDeleteRange:
			op.Delete = deleteReq(r.RequestDeleteRange)
		case *pb.RequestOp_RequestTxn:
			op.Txn = txnReq(r.RequestTxn)
		}
		out = append(out, op)
	}
	return out
}

func watchReq(m *pb.WatchRequest) *k8strace.WatchRequest {
	switch r := m.RequestUnion.(type) {
	case *pb.WatchRequest_CreateRequest:
		c := r.CreateRequest
		w := &k8strace.WatchCreate{
			Key: string(c.Key), End: string(c.RangeEnd), StartRev: c.StartRevision, Progress: c.ProgressNotify,
			PrevKV: c.PrevKv, ID: c.WatchId, Fragment: c.Fragment,
		}
		for _, f := range c.Filters {
			w.Filters = append(w.Filters, int32(f))
		}
		return &k8strace.WatchRequest{Create: w}
	case *pb.WatchRequest_CancelRequest:
		id := r.CancelRequest.WatchId
		return &k8strace.WatchRequest{Cancel: &id}
	case *pb.WatchRequest_ProgressRequest:
		return &k8strace.WatchRequest{Progress: true}
	}
	return &k8strace.WatchRequest{}
}

// decodeResponse decodes a response message of method, or returns nil for
// a method the log does not decode.
func decodeResponse(method string, b []byte) (*k8strace.Response, error) {
	switch method {
	case methodRange:
		var m pb.RangeResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return rangeResp(&m), nil
	case methodRangeStream:
		var m pb.RangeStreamResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		if m.RangeResponse == nil {
			return &k8strace.Response{}, nil
		}
		return rangeResp(m.RangeResponse), nil
	case methodPut:
		var m pb.PutResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		r := &k8strace.Response{Rev: m.GetHeader().GetRevision()}
		if m.PrevKv != nil {
			r.Prev = []k8strace.KV{kv(m.PrevKv)}
		}
		return r, nil
	case methodDelete:
		var m pb.DeleteRangeResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return deleteResp(&m), nil
	case methodTxn:
		var m pb.TxnResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return txnResp(&m), nil
	case methodCompact:
		var m pb.CompactionResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision()}, nil
	case methodLeaseGrant:
		var m pb.LeaseGrantResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision(), Lease: m.ID, TTL: m.TTL}, nil
	case methodLeaseRevoke:
		var m pb.LeaseRevokeResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision()}, nil
	case methodLeaseKeepAlive:
		var m pb.LeaseKeepAliveResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision(), Lease: m.ID, TTL: m.TTL}, nil
	case methodLeaseTTL:
		var m pb.LeaseTimeToLiveResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision(), Lease: m.ID, TTL: m.TTL}, nil
	case methodWatch:
		var m pb.WatchResponse
		if err := proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		w := &k8strace.WatchResponse{ID: m.WatchId, Created: m.Created, Canceled: m.Canceled, CompactRev: m.CompactRevision, Fragment: m.Fragment}
		for _, e := range m.Events {
			ev := k8strace.Event{Delete: e.Type == mvccpb.Event_DELETE, KV: kv(e.Kv)}
			if e.PrevKv != nil {
				prev := kv(e.PrevKv)
				ev.Prev = &prev
			}
			w.Events = append(w.Events, ev)
		}
		return &k8strace.Response{Rev: m.GetHeader().GetRevision(), Watch: w}, nil
	}
	return nil, nil
}

func rangeResp(m *pb.RangeResponse) *k8strace.Response {
	r := &k8strace.Response{Rev: m.GetHeader().GetRevision(), Count: m.Count, More: m.More}
	for _, x := range m.Kvs {
		r.KVs = append(r.KVs, kv(x))
	}
	return r
}

func deleteResp(m *pb.DeleteRangeResponse) *k8strace.Response {
	r := &k8strace.Response{Rev: m.GetHeader().GetRevision(), Deleted: m.Deleted}
	for _, x := range m.PrevKvs {
		r.Prev = append(r.Prev, kv(x))
	}
	return r
}

func txnResp(m *pb.TxnResponse) *k8strace.Response {
	r := &k8strace.Response{Rev: m.GetHeader().GetRevision(), Succeeded: m.Succeeded}
	for _, o := range m.Responses {
		var op k8strace.Response
		switch x := o.Response.(type) {
		case *pb.ResponseOp_ResponseRange:
			op = *rangeResp(x.ResponseRange)
		case *pb.ResponseOp_ResponsePut:
			op.Rev = x.ResponsePut.GetHeader().GetRevision()
			if x.ResponsePut.PrevKv != nil {
				op.Prev = []k8strace.KV{kv(x.ResponsePut.PrevKv)}
			}
		case *pb.ResponseOp_ResponseDeleteRange:
			op = *deleteResp(x.ResponseDeleteRange)
		case *pb.ResponseOp_ResponseTxn:
			op = *txnResp(x.ResponseTxn)
		}
		r.Ops = append(r.Ops, op)
	}
	return r
}

func kv(m *mvccpb.KeyValue) k8strace.KV {
	if m == nil {
		return k8strace.KV{}
	}
	return k8strace.KV{Key: string(m.Key), Mod: m.ModRevision, Create: m.CreateRevision, Version: m.Version, Size: len(m.Value), Lease: m.Lease}
}
