package main

import (
	"context"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A transport-only toy, not a durable or MVCC etcd implementation.
// Both servers execute the same Put and use the same protobuf codec.
type backend struct {
	pb.UnimplementedKVServer
	pb.UnimplementedWatchServer
	mu       sync.Mutex
	rev      int64
	kv       map[string]*mvccpb.KeyValue
	watchers map[*subscription]bool
}
type subscription struct {
	key, end []byte
	prev     bool
	id       int64
	send     func(*pb.WatchResponse)
}

func newBackend() *backend {
	return &backend{rev: 1, kv: make(map[string]*mvccpb.KeyValue), watchers: make(map[*subscription]bool)}
}
func (b *backend) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: b.rev, RaftTerm: 1}
}
func (b *backend) Put(_ context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rev++
	old := b.kv[string(r.Key)]
	k := &mvccpb.KeyValue{Key: r.Key, Value: r.Value, CreateRevision: b.rev, ModRevision: b.rev, Version: 1, Lease: r.Lease}
	if old != nil {
		k.CreateRevision = old.CreateRevision
		k.Version = old.Version + 1
	}
	b.kv[string(r.Key)] = k
	for s := range b.watchers {
		if matches(r.Key, s.key, s.end) {
			e := &mvccpb.Event{Type: mvccpb.Event_PUT, Kv: k}
			if s.prev {
				e.PrevKv = old
			}
			s.send(&pb.WatchResponse{Header: b.header(), WatchId: s.id, Events: []*mvccpb.Event{e}})
		}
	}
	out := &pb.PutResponse{Header: b.header()}
	if r.PrevKv {
		out.PrevKv = old
	}
	return out, nil
}
func matches(k, start, end []byte) bool {
	if len(end) == 0 {
		return string(k) == string(start)
	}
	return string(k) >= string(start) && (string(end) == "\x00" || string(k) < string(end))
}
func (b *backend) Range(_ context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := &pb.RangeResponse{Header: b.header()}
	for _, k := range b.kv {
		if matches(k.Key, r.Key, r.RangeEnd) {
			out.Count++
			if !r.CountOnly {
				c := proto.Clone(k).(*mvccpb.KeyValue)
				if r.KeysOnly {
					c.Value = nil
				}
				out.Kvs = append(out.Kvs, c)
			}
		}
	}
	return out, nil
}
func (b *backend) subscribe(r *pb.WatchCreateRequest, id int64, send func(*pb.WatchResponse)) *subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &subscription{key: r.Key, end: r.RangeEnd, prev: r.PrevKv, id: id, send: send}
	b.watchers[s] = true
	send(&pb.WatchResponse{Header: b.header(), WatchId: id, Created: true})
	return s
}
func (b *backend) unsubscribe(s *subscription) { b.mu.Lock(); delete(b.watchers, s); b.mu.Unlock() }
func (b *backend) Watch(stream pb.Watch_WatchServer) error {
	// One watch per stream suffices for the smoke. Stock clientv3 multiplexes;
	// production must support several IDs, cancel, replay and progress semantics.
	out := make(chan *pb.WatchResponse, 256)
	reqs := make(chan *pb.WatchRequest)
	errs := make(chan error, 1)
	go func() {
		for {
			r, e := stream.Recv()
			if e != nil {
				errs <- e
				return
			}
			select {
			case reqs <- r:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	var s *subscription
	defer func() {
		if s != nil {
			b.unsubscribe(s)
		}
	}()
	for {
		select {
		case r := <-reqs:
			if c := r.GetCreateRequest(); c != nil {
				if s != nil {
					return status.Error(codes.Unimplemented, "multiple watch IDs")
				}
				s = b.subscribe(c, 1, func(r *pb.WatchResponse) {
					select {
					case out <- r:
					case <-stream.Context().Done():
					}
				})
			}
		case r := <-out:
			if e := stream.Send(r); e != nil {
				return e
			}
		case e := <-errs:
			return e
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
func (b *backend) unary(path string, payload []byte) ([]byte, error) {
	var out proto.Message
	switch path {
	case "/etcdserverpb.KV/Put":
		r := new(pb.PutRequest)
		if e := proto.Unmarshal(payload, r); e != nil {
			return nil, status.Error(codes.InvalidArgument, e.Error())
		}
		v, e := b.Put(context.Background(), r)
		if e != nil {
			return nil, e
		}
		out = v
	case "/etcdserverpb.KV/Range":
		r := new(pb.RangeRequest)
		if e := proto.Unmarshal(payload, r); e != nil {
			return nil, status.Error(codes.InvalidArgument, e.Error())
		}
		v, e := b.Range(context.Background(), r)
		if e != nil {
			return nil, e
		}
		out = v
	default:
		return nil, status.Error(codes.Unimplemented, "prototype implements Put, Range and Watch only")
	}
	return proto.Marshal(out)
}
