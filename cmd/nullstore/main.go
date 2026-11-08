// Command nullstore answers the etcd v3 calls of cmd/loadgen at once and
// keeps nothing, so that the bench can measure loadgen's own ceiling: how
// fast it goes when the store costs nothing. It is not a store and checks
// nothing.
//
//	nullstore -listen <host:port>[,<host:port>...] -cert <file> -key <file>
//
// Every write succeeds with the next revision. A Range of one key answers
// one key-value with a value of -value-size bytes, a Range over a range of
// keys -list-keys of them, and a RangeStream the same in one message. A
// watch gets an event for every put or delete under its range, with the
// put's value, and a previous key-value of the same size after a Txn that
// compared a mod revision other than 0, as an update does.
package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// Field numbers from etcd v3.7.2 api/etcdserverpb/rpc.proto and
// api/mvccpb/kv.proto.
const (
	respHeader     = 1 // every response
	headerRevision = 3 // ResponseHeader

	rangeKey   = 1 // RangeRequest
	rangeEnd   = 2
	rangeLimit = 3
	rangeKVs   = 2 // RangeResponse
	rangeMore  = 3
	rangeCount = 4

	txnCompare      = 1 // TxnRequest
	txnSuccess      = 2
	cmpModRevision  = 6 // Compare
	opPut           = 2 // RequestOp and ResponseOp
	opDelete        = 3
	putKey          = 1 // PutRequest
	putValue        = 2
	txnSucceeded    = 2 // TxnResponse
	txnResponses    = 3
	delDeleted      = 2 // DeleteRangeResponse
	leaseResponseID = 2 // LeaseGrantResponse
	leaseRespTTL    = 3
	leaseTTL        = 1 // LeaseGrantRequest

	watchCreate  = 1 // WatchRequest
	watchKey     = 1 // WatchCreateRequest
	watchEnd     = 2
	watchCreated = 3 // WatchResponse
	watchEvents  = 11
	eventType    = 1 // mvccpb.Event
	eventKV      = 2
	eventPrevKV  = 3
	kvKey        = 1 // mvccpb.KeyValue
	kvCreate     = 2
	kvMod        = 3
	kvVersion    = 4
	kvValue      = 5
	deleteEvent  = 1
)

var errNotBytes = errors.New("nullstore: a message is not *[]byte")

// rawCodec hands the handlers the bytes of each message, so nothing is
// decoded that a handler does not read.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, errNotBytes
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return errNotBytes
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

type server struct {
	valueSize, listKeys int
	revision            atomic.Int64
	lease               atomic.Int64

	mu       sync.RWMutex
	watchers map[*watcher]struct{}
}

type watcher struct {
	start, end []byte
	events     chan event
	gone       chan struct{}
}

type event struct {
	key, value []byte
	rev        int64
	deleted    bool
	prev       bool
}

func main() {
	listen := flag.String("listen", "127.0.0.1:2379", "comma-separated host:port to serve on")
	cert := flag.String("cert", "", "server certificate")
	key := flag.String("key", "", "server key")
	s := &server{watchers: make(map[*watcher]struct{})}
	flag.IntVar(&s.valueSize, "value-size", 1024, "value bytes of each key-value a Range answers")
	flag.IntVar(&s.listKeys, "list-keys", 300, "key-values a Range over a range of keys answers")
	flag.Parse()
	if err := run(s, strings.Split(*listen, ","), *cert, *key); err != nil {
		fmt.Fprintln(os.Stderr, "nullstore:", err)
		os.Exit(1)
	}
}

func run(s *server, addrs []string, cert, key string) error {
	kp, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return err
	}
	var lns []net.Listener
	for _, addr := range addrs {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, l := range lns {
				err = errors.Join(err, l.Close())
			}
			return err
		}
		lns = append(lns, ln)
	}
	srv, errs := s.serve(kp, lns)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-stop:
		srv.Stop()
		return nil
	case err := <-errs:
		srv.Stop()
		return err
	}
}

// serve serves s on every listener with TLS 1.3 and kp. The channel gets
// the error of a listener that stopped.
func (s *server) serve(kp tls.Certificate, lns []net.Listener) (*grpc.Server, <-chan error) {
	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{kp}, MinVersion: tls.VersionTLS13})
	srv := grpc.NewServer(grpc.Creds(creds), grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(s.handle))
	errs := make(chan error, len(lns))
	for _, ln := range lns {
		go func() { errs <- srv.Serve(ln) }()
	}
	return srv, errs
}

func (s *server) handle(_ any, ss grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(ss)
	if method == "/etcdserverpb.Watch/Watch" {
		return s.watch(ss)
	}
	var req []byte
	if err := ss.RecvMsg(&req); err != nil {
		return err
	}
	if !wellFormed(req) {
		return status.Error(codes.InvalidArgument, "nullstore: malformed request")
	}
	var resp []byte
	switch method {
	case "/etcdserverpb.KV/Put":
		k, v := field(req, putKey), field(req, putValue)
		rev := s.revision.Add(1)
		s.publish(event{key: k, value: v, rev: rev})
		resp = header(rev)
	case "/etcdserverpb.KV/DeleteRange":
		rev := s.revision.Add(1)
		s.publish(event{key: field(req, putKey), rev: rev, deleted: true, prev: true})
		resp = protowire.AppendVarint(protowire.AppendTag(header(rev), delDeleted, protowire.VarintType), 1)
	case "/etcdserverpb.KV/Txn":
		resp = s.txn(req)
	case "/etcdserverpb.KV/Range":
		resp = s.rangeAnswer(req)
	case "/etcdserverpb.KV/RangeStream":
		resp = appendBytesField(nil, 1, s.rangeAnswer(req))
	case "/etcdserverpb.Lease/LeaseGrant":
		resp = protowire.AppendVarint(protowire.AppendTag(header(s.revision.Load()), leaseResponseID, protowire.VarintType), uint64(s.lease.Add(1)))
		resp = protowire.AppendVarint(protowire.AppendTag(resp, leaseRespTTL, protowire.VarintType), varint(req, leaseTTL))
	default:
		return status.Error(codes.Unimplemented, method)
	}
	return ss.SendMsg(&resp)
}

// txn succeeds with one put or delete response per operation of the
// success branch.
func (s *server) txn(req []byte) []byte {
	rev := s.revision.Add(1)
	update := varint(field(req, txnCompare), cmpModRevision) != 0
	resp := protowire.AppendVarint(protowire.AppendTag(header(rev), txnSucceeded, protowire.VarintType), 1)
	each(req, txnSuccess, func(op []byte) {
		if put := field(op, opPut); put != nil {
			s.publish(event{key: field(put, putKey), value: field(put, putValue), rev: rev, prev: update})
			resp = appendBytesField(resp, txnResponses, appendBytesField(nil, opPut, header(rev)))
			return
		}
		s.publish(event{key: field(field(op, opDelete), putKey), rev: rev, deleted: true, prev: true})
		deleted := protowire.AppendVarint(protowire.AppendTag(header(rev), delDeleted, protowire.VarintType), 1)
		resp = appendBytesField(resp, txnResponses, appendBytesField(nil, opDelete, deleted))
	})
	return resp
}

// rangeAnswer answers one key-value for a Range of one key, and -list-keys
// within the limit for a Range over a range of keys.
func (s *server) rangeAnswer(req []byte) []byte {
	rev := s.revision.Load()
	key := field(req, rangeKey)
	n := 1
	if field(req, rangeEnd) != nil {
		n = s.listKeys
		if limit := int(varint(req, rangeLimit)); limit > 0 {
			n = min(n, limit)
		}
	}
	value := make([]byte, s.valueSize)
	resp := header(rev)
	for i := range n {
		k := fmt.Appendf(append([]byte(nil), key...), "%06d", i)
		resp = appendBytesField(resp, rangeKVs, keyValue(k, value, rev))
	}
	resp = protowire.AppendVarint(protowire.AppendTag(resp, rangeMore, protowire.VarintType), 1)
	return protowire.AppendVarint(protowire.AppendTag(resp, rangeCount, protowire.VarintType), uint64(n+1))
}

// watch answers the create request, then sends the events of its range
// until the stream ends.
func (s *server) watch(ss grpc.ServerStream) error {
	var req []byte
	if err := ss.RecvMsg(&req); err != nil {
		return err
	}
	if !wellFormed(req) {
		return status.Error(codes.InvalidArgument, "nullstore: malformed watch request")
	}
	create := field(req, watchCreate)
	w := &watcher{start: field(create, watchKey), end: field(create, watchEnd), events: make(chan event, 1<<16), gone: make(chan struct{})}
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.watchers, w)
		s.mu.Unlock()
	}()
	created := protowire.AppendVarint(protowire.AppendTag(header(s.revision.Load()), watchCreated, protowire.VarintType), 1)
	if err := ss.SendMsg(&created); err != nil {
		return err
	}
	// Progress requests and the like need no answer; the reader only
	// notices the end of the stream.
	go func() {
		var more []byte
		for ss.RecvMsg(&more) == nil {
		}
		close(w.gone)
	}()
	for {
		var e event
		select {
		case e = <-w.events:
		case <-w.gone:
			return nil
		}
		resp := header(e.rev)
		for {
			resp = appendBytesField(resp, watchEvents, e.encode())
			if len(resp) > 64<<10 || len(w.events) == 0 {
				break
			}
			e = <-w.events
		}
		if err := ss.SendMsg(&resp); err != nil {
			return err
		}
	}
}

func (e event) encode() []byte {
	var b []byte
	if e.deleted {
		b = protowire.AppendVarint(protowire.AppendTag(b, eventType, protowire.VarintType), deleteEvent)
	}
	b = appendBytesField(b, eventKV, keyValue(e.key, e.value, e.rev))
	if e.prev {
		b = appendBytesField(b, eventPrevKV, keyValue(e.key, e.value, e.rev-1))
	}
	return b
}

// publish hands e to every watcher of its key, waiting while a watcher's
// queue is full.
func (s *server) publish(e event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for w := range s.watchers {
		if bytes.Compare(e.key, w.start) < 0 || len(w.end) > 0 && bytes.Compare(e.key, w.end) >= 0 {
			continue
		}
		select {
		case w.events <- e:
		case <-w.gone:
		}
	}
}

func header(rev int64) []byte {
	h := protowire.AppendVarint(protowire.AppendTag(nil, headerRevision, protowire.VarintType), uint64(rev))
	return appendBytesField(nil, respHeader, h)
}

func keyValue(key, value []byte, rev int64) []byte {
	kv := appendBytesField(nil, kvKey, key)
	for _, f := range [...]struct {
		n protowire.Number
		v int64
	}{{kvCreate, 1}, {kvMod, rev}, {kvVersion, 1}} {
		kv = protowire.AppendVarint(protowire.AppendTag(kv, f.n, protowire.VarintType), uint64(f.v))
	}
	return appendBytesField(kv, kvValue, value)
}

func appendBytesField(b []byte, n protowire.Number, v []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, n, protowire.BytesType), v)
}

// each calls f with every bytes field n of m. A malformed message ends it.
func each(m []byte, n protowire.Number, f func([]byte)) {
	for len(m) > 0 {
		num, typ, l := protowire.ConsumeTag(m)
		if l < 0 {
			return
		}
		m = m[l:]
		if typ == protowire.BytesType && num == n {
			b, l := protowire.ConsumeBytes(m)
			if l < 0 {
				return
			}
			f(b)
			m = m[l:]
			continue
		}
		l = protowire.ConsumeFieldValue(num, typ, m)
		if l < 0 {
			return
		}
		m = m[l:]
	}
}

// wellFormed reports whether m parses as protobuf fields at its top level.
// The handlers read nested messages leniently: a part that does not parse
// reads as absent.
func wellFormed(m []byte) bool {
	for len(m) > 0 {
		num, typ, l := protowire.ConsumeTag(m)
		if l < 0 {
			return false
		}
		m = m[l:]
		if l = protowire.ConsumeFieldValue(num, typ, m); l < 0 {
			return false
		}
		m = m[l:]
	}
	return true
}

// field returns the last bytes field n of m, or nil.
func field(m []byte, n protowire.Number) []byte {
	var out []byte
	each(m, n, func(b []byte) { out = b })
	return out
}

// varint returns the last varint field n of m, or 0.
func varint(m []byte, n protowire.Number) uint64 {
	var out uint64
	for len(m) > 0 {
		num, typ, l := protowire.ConsumeTag(m)
		if l < 0 {
			return out
		}
		m = m[l:]
		if typ == protowire.VarintType && num == n {
			v, l := protowire.ConsumeVarint(m)
			if l < 0 {
				return out
			}
			out = v
			m = m[l:]
			continue
		}
		l = protowire.ConsumeFieldValue(num, typ, m)
		if l < 0 {
			return out
		}
		m = m[l:]
	}
	return out
}
