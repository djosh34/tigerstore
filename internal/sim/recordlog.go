package sim

import (
	"errors"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A run's calls and watch responses would be most of its heap, and the
// garbage collector would walk every pointer of them again in every
// cycle, for as long as the run lasts (#500). So a run keeps them
// encoded, in memory that holds no pointers, until its checks decode
// them: each request and answer marshaled without its values, and each
// distinct value once.
//
// Nothing here uses a Go map: under gosim an insert into a map past its
// fourth entry draws from the run's random stream, so keeping a call
// would change every later choice of the run.

// recordLog holds a run's calls, oldest first, and its watch responses.
type recordLog struct {
	recs []loggedRecord
	// data holds the marshaled messages, and ids the ids of their values
	// in vals, in the order eachValue visits them.
	data arena
	ids  []uint32
	vals valueTable
	// types are the messages' types, by index.
	types []protoreflect.MessageType
	// errs are the calls' errors, one of each kind (errKey), and
	// errIndex finds them by the hash of their kind.
	errs     []error
	errIndex hashTable
	// scratch holds the values of the message being encoded.
	scratch [][]byte
}

// loggedRecord is a Record, without pointers.
type loggedRecord struct {
	client, node int32
	call, ret    int64
	req, resp    loggedMessage
	// err is the index of the call's error in errs, or -1.
	err             int32
	ambiguous, isV3 bool
}

// loggedMessage is a message in a recordLog: its type, plus one, or 0 for
// none, its bytes in data, and its values' ids, ids[ids0:ids1]. A nil
// pointer of a message type, as clientv3's answer to a call that failed,
// is null.
type loggedMessage struct {
	typ        uint8
	null       bool
	at         span
	ids0, ids1 uint32
}

// errKind is the kind of an error: its type and message. Errors of one
// kind are the same to the checks, so a run keeps the first of each.
func errKind(err error) string { return fmt.Sprintf("%T %s", err, err) }

// newRecordLog returns an empty recordLog. Protobuf sets up a message
// type the first time it marshals or reads one, with gosim maps that draw
// from the run's random stream; so each run sets up the types of the
// history at its start, with simrun.Aside, and the moment a node or a
// client first uses one changes nothing.
func newRecordLog() (*recordLog, error) {
	var err error
	simrun.Aside(func() {
		for _, m := range []proto.Message{
			&pb.ResponseHeader{}, &mvccpb.KeyValue{}, &mvccpb.Event{},
			&pb.RangeRequest{}, &pb.RangeResponse{}, &pb.PutRequest{}, &pb.PutResponse{},
			&pb.DeleteRangeRequest{}, &pb.DeleteRangeResponse{}, &pb.Compare{},
			&pb.RequestOp{}, &pb.ResponseOp{}, &pb.TxnRequest{}, &pb.TxnResponse{},
			&pb.LeaseGrantRequest{}, &pb.LeaseGrantResponse{}, &pb.LeaseRevokeRequest{}, &pb.LeaseRevokeResponse{},
			&pb.LeaseKeepAliveRequest{}, &pb.LeaseKeepAliveResponse{}, &pb.CompactionRequest{}, &pb.CompactionResponse{},
			&pb.WatchResponse{},
		} {
			var b []byte
			if b, err = proto.Marshal(m); err != nil {
				return
			}
			if err = proto.Unmarshal(b, m.ProtoReflect().New().Interface()); err != nil {
				return
			}
		}
	})
	return &recordLog{}, err
}

// errTooManyTypes is the error of a run whose calls have more message
// types than a recordLog numbers.
var errTooManyTypes = errors.New("sim: too many message types in the history")

// add keeps r.
func (l *recordLog) add(r *Record) error {
	req, err := l.encode(r.req)
	if err != nil {
		return err
	}
	resp, err := l.encode(r.Resp)
	if err != nil {
		return err
	}
	l.recs = append(l.recs, loggedRecord{
		client: int32(r.Client), node: int32(r.Node), call: r.Call, ret: r.Return,
		req: req, resp: resp, err: l.errorIndex(r.Err), ambiguous: r.Ambiguous, isV3: r.v3,
	})
	return nil
}

// errorIndex returns the index in errs of err's kind, or -1 for nil.
func (l *recordLog) errorIndex(err error) int32 {
	if err == nil {
		return -1
	}
	kind := errKind(err)
	i, ok := l.errIndex.find(fnv64a([]byte(kind)), func(i uint32) bool { return errKind(l.errs[i]) == kind })
	if !ok {
		i = uint32(len(l.errs))
		l.errs = append(l.errs, err)
		l.errIndex.add(fnv64a([]byte(kind)), i)
	}
	return int32(i)
}

// encode keeps m without its values, and its values in vals.
func (l *recordLog) encode(m proto.Message) (loggedMessage, error) {
	if m == nil {
		return loggedMessage{}, nil
	}
	t := m.ProtoReflect().Type()
	typ := uint8(slices.Index(l.types, t) + 1)
	if typ == 0 {
		if len(l.types) == 255 {
			return loggedMessage{}, errTooManyTypes
		}
		l.types = append(l.types, t)
		typ = uint8(len(l.types))
	}
	if !m.ProtoReflect().IsValid() {
		return loggedMessage{typ: typ, null: true}, nil
	}
	l.scratch = l.scratch[:0]
	bare := withoutValues(m, func(v []byte) { l.scratch = append(l.scratch, v) })
	b, err := proto.Marshal(bare)
	if err != nil {
		return loggedMessage{}, err
	}
	ids0 := uint32(len(l.ids))
	for _, v := range l.scratch {
		l.ids = append(l.ids, l.vals.add(v))
	}
	return loggedMessage{typ: typ, at: l.data.add(b), ids0: ids0, ids1: uint32(len(l.ids))}, nil
}

// decode returns the message m, whose values share vals' bytes.
func (l *recordLog) decode(m loggedMessage) (proto.Message, error) {
	switch {
	case m.typ == 0:
		return nil, nil
	case m.null:
		return l.types[m.typ-1].Zero().Interface(), nil
	}
	out := l.types[m.typ-1].New().Interface()
	if err := proto.Unmarshal(l.data.bytes(m.at), out); err != nil {
		return nil, err
	}
	ids := l.ids[m.ids0:m.ids1]
	n := 0
	eachValue(out, func(v *[]byte) {
		if n < len(ids) {
			*v = l.vals.get(ids[n])
		}
		n++
	})
	if n != len(ids) {
		return nil, fmt.Errorf("sim: a logged %s had %d values, decoded it has %d", out.ProtoReflect().Descriptor().FullName(), len(ids), n)
	}
	return out, nil
}

// records returns the calls.
func (l *recordLog) records() ([]*Record, error) {
	out := make([]*Record, len(l.recs))
	for i, lr := range l.recs {
		req, err := l.decode(lr.req)
		if err != nil {
			return nil, err
		}
		resp, err := l.decode(lr.resp)
		if err != nil {
			return nil, err
		}
		r := &Record{
			Client: int(lr.client), Node: int(lr.node), Call: lr.call, Return: lr.ret,
			req: req, Resp: resp, Ambiguous: lr.ambiguous, v3: lr.isV3,
		}
		if lr.err >= 0 {
			r.Err = l.errs[lr.err]
		}
		out[i] = r
	}
	return out, nil
}

// withoutValues returns m with its values left out, and hands each value
// to each, in the order eachValue visits them. It shares everything but
// the messages on the way to a value with m, and changes nothing in m,
// which the nodes and other clients may share. A message of a type that
// holds no value is m itself.
func withoutValues(m proto.Message, each func([]byte)) proto.Message {
	kv := func(kv *mvccpb.KeyValue) *mvccpb.KeyValue {
		if kv == nil {
			return nil
		}
		each(kv.Value)
		return &mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision, ModRevision: kv.ModRevision, Version: kv.Version, Lease: kv.Lease}
	}
	kvs := func(in []*mvccpb.KeyValue) []*mvccpb.KeyValue {
		if in == nil {
			return nil
		}
		out := make([]*mvccpb.KeyValue, len(in))
		for i, v := range in {
			out[i] = kv(v)
		}
		return out
	}
	switch m := m.(type) {
	case *pb.PutRequest:
		each(m.Value)
		return &pb.PutRequest{Key: m.Key, Lease: m.Lease, PrevKv: m.PrevKv, IgnoreValue: m.IgnoreValue, IgnoreLease: m.IgnoreLease}
	case *pb.TxnRequest:
		ops := func(in []*pb.RequestOp) []*pb.RequestOp {
			if in == nil {
				return nil
			}
			out := make([]*pb.RequestOp, len(in))
			for i, op := range in {
				switch r := op.Request.(type) {
				case *pb.RequestOp_RequestPut:
					out[i] = &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: withoutValues(r.RequestPut, each).(*pb.PutRequest)}}
				case *pb.RequestOp_RequestTxn:
					out[i] = &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: withoutValues(r.RequestTxn, each).(*pb.TxnRequest)}}
				default:
					out[i] = op
				}
			}
			return out
		}
		return &pb.TxnRequest{Compare: m.Compare, Success: ops(m.Success), Failure: ops(m.Failure)}
	case *pb.RangeResponse:
		return &pb.RangeResponse{Header: m.Header, Kvs: kvs(m.Kvs), More: m.More, Count: m.Count}
	case *pb.PutResponse:
		return &pb.PutResponse{Header: m.Header, PrevKv: kv(m.PrevKv)}
	case *pb.DeleteRangeResponse:
		return &pb.DeleteRangeResponse{Header: m.Header, Deleted: m.Deleted, PrevKvs: kvs(m.PrevKvs)}
	case *pb.TxnResponse:
		out := &pb.TxnResponse{Header: m.Header, Succeeded: m.Succeeded}
		if m.Responses != nil {
			out.Responses = make([]*pb.ResponseOp, len(m.Responses))
		}
		for i, op := range m.Responses {
			switch r := op.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				out.Responses[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{ResponseRange: withoutValues(r.ResponseRange, each).(*pb.RangeResponse)}}
			case *pb.ResponseOp_ResponsePut:
				out.Responses[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{ResponsePut: withoutValues(r.ResponsePut, each).(*pb.PutResponse)}}
			case *pb.ResponseOp_ResponseDeleteRange:
				out.Responses[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: withoutValues(r.ResponseDeleteRange, each).(*pb.DeleteRangeResponse)}}
			case *pb.ResponseOp_ResponseTxn:
				out.Responses[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: withoutValues(r.ResponseTxn, each).(*pb.TxnResponse)}}
			default:
				out.Responses[i] = op
			}
		}
		return out
	case *pb.WatchResponse:
		out := &pb.WatchResponse{
			Header: m.Header, WatchId: m.WatchId, Created: m.Created, Canceled: m.Canceled,
			CompactRevision: m.CompactRevision, CancelReason: m.CancelReason, Fragment: m.Fragment,
		}
		if m.Events != nil {
			out.Events = make([]*mvccpb.Event, len(m.Events))
		}
		for i, ev := range m.Events {
			out.Events[i] = &mvccpb.Event{Type: ev.Type, Kv: kv(ev.Kv), PrevKv: kv(ev.PrevKv)}
		}
		return out
	}
	return m
}

// eachValue calls f with each value of m, in the order withoutValues
// visits them.
func eachValue(m proto.Message, f func(*[]byte)) {
	kvs := func(kvs ...*mvccpb.KeyValue) {
		for _, kv := range kvs {
			if kv != nil {
				f(&kv.Value)
			}
		}
	}
	switch m := m.(type) {
	case *pb.PutRequest:
		f(&m.Value)
	case *pb.TxnRequest:
		for _, ops := range [][]*pb.RequestOp{m.Success, m.Failure} {
			for _, op := range ops {
				switch r := op.Request.(type) {
				case *pb.RequestOp_RequestPut:
					eachValue(r.RequestPut, f)
				case *pb.RequestOp_RequestTxn:
					eachValue(r.RequestTxn, f)
				}
			}
		}
	case *pb.RangeResponse:
		kvs(m.Kvs...)
	case *pb.PutResponse:
		kvs(m.PrevKv)
	case *pb.DeleteRangeResponse:
		kvs(m.PrevKvs...)
	case *pb.TxnResponse:
		for _, op := range m.Responses {
			switch r := op.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				eachValue(r.ResponseRange, f)
			case *pb.ResponseOp_ResponsePut:
				eachValue(r.ResponsePut, f)
			case *pb.ResponseOp_ResponseDeleteRange:
				eachValue(r.ResponseDeleteRange, f)
			case *pb.ResponseOp_ResponseTxn:
				eachValue(r.ResponseTxn, f)
			}
		}
	case *pb.WatchResponse:
		for _, ev := range m.Events {
			kvs(ev.Kv, ev.PrevKv)
		}
	}
}

// span is a run of bytes in an arena.
type span struct{ chunk, off, n uint32 }

// arenaChunk is the size of an arena's chunks; a longer slice gets a
// chunk of its own.
const arenaChunk = 1 << 20

// arena keeps byte slices, copied into chunks that hold no pointers.
type arena struct{ chunks [][]byte }

// add copies b into the arena.
func (a *arena) add(b []byte) span {
	last := len(a.chunks) - 1
	if last < 0 || len(b) > cap(a.chunks[last])-len(a.chunks[last]) {
		a.chunks = append(a.chunks, make([]byte, 0, max(arenaChunk, len(b))))
		last++
	}
	c := a.chunks[last]
	a.chunks[last] = append(c, b...)
	return span{chunk: uint32(last), off: uint32(len(c)), n: uint32(len(b))}
}

// bytes returns the bytes of s, which the arena keeps.
func (a *arena) bytes(s span) []byte {
	return a.chunks[s.chunk][s.off : s.off+s.n : s.off+s.n]
}

// valueTable keeps each distinct value once.
type valueTable struct {
	bytes arena
	spans []span
	// ids finds the ids of values by their hash (fnv64a).
	ids hashTable
}

// fnv64a is the FNV-1a hash of b.
func fnv64a(b []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, c := range b {
		h = (h ^ uint64(c)) * 0x100000001b3
	}
	return h
}

// add returns the id of v, keeping it if it is new.
func (t *valueTable) add(v []byte) uint32 {
	h := fnv64a(v)
	if id, ok := t.ids.find(h, func(id uint32) bool { return string(t.get(id)) == string(v) }); ok {
		return id
	}
	id := uint32(len(t.spans))
	t.spans = append(t.spans, t.bytes.add(v))
	t.ids.add(h, id)
	return id
}

// get returns the value id, which the table keeps.
func (t *valueTable) get(id uint32) []byte { return t.bytes.bytes(t.spans[id]) }

// hashTable finds numbers by a hash of what they stand for, with linear
// probing in a table at most half full. It holds no pointers and draws
// nothing from a run's random stream.
type hashTable struct {
	hashes []uint64
	// ids holds each slot's number plus one, or 0 for an empty slot.
	ids []uint32
	n   int
}

// find returns the number with hash h for which is reports true.
func (t *hashTable) find(h uint64, is func(uint32) bool) (uint32, bool) {
	if len(t.ids) == 0 {
		return 0, false
	}
	mask := uint64(len(t.ids) - 1)
	for i := h & mask; t.ids[i] != 0; i = (i + 1) & mask {
		if t.hashes[i] == h && is(t.ids[i]-1) {
			return t.ids[i] - 1, true
		}
	}
	return 0, false
}

// add adds id with hash h.
func (t *hashTable) add(h uint64, id uint32) {
	if 2*(t.n+1) > len(t.ids) {
		old, oldIDs := t.hashes, t.ids
		size := max(16, 2*len(t.ids))
		t.hashes, t.ids, t.n = make([]uint64, size), make([]uint32, size), 0
		for i, o := range oldIDs {
			if o != 0 {
				t.add(old[i], o-1)
			}
		}
	}
	mask := uint64(len(t.ids) - 1)
	i := h & mask
	for t.ids[i] != 0 {
		i = (i + 1) & mask
	}
	t.hashes[i], t.ids[i] = h, id+1
	t.n++
}
