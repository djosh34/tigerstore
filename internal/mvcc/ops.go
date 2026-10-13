package mvcc

import (
	"bytes"
	"math"
	"sort"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/planted"
)

// The rules below follow etcd v3.7.2 (server/etcdserver/txn and
// server/storage/mvcc) so that responses match etcd byte for byte.

// writer applies one request. Its changes get main revision begin+1.
type writer struct {
	s       *Store
	begin   int64
	changed bool
}

func (s *Store) start() *writer {
	s.changes = s.changes[:0]
	return &writer{s: s, begin: s.rev}
}

// finish moves the store to the next revision if the request changed a key.
func (w *writer) finish() {
	if w.changed {
		w.s.rev = w.begin + 1
		if planted.On("B09a") {
			w.s.rev = w.begin + int64(len(w.s.changes))
		}
	}
}

// cur is the revision a read inside the request sees.
func (w *writer) cur() int64 {
	if w.changed {
		return w.begin + 1
	}
	return w.begin
}

func header(r int64) *pb.ResponseHeader { return &pb.ResponseHeader{Revision: r} }

// KV is one key's version as a range returns it: the fields of an
// mvccpb.KeyValue, as a value. Key and Value are the index's slices.
type KV struct {
	Key, Value                                  []byte
	CreateRevision, ModRevision, Version, Lease int64
}

// RangeResult is a range's answer without its header: the store's
// revision, for the header, the keys, their count and whether more were
// left out.
type RangeResult struct {
	Rev   int64
	KVs   []KV
	Count int64
	More  bool
}

// Range reads keys at the current or a past revision.
func (s *Store) Range(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	w := &writer{s: s, begin: s.rev}
	return w.rangeKeys(r)
}

// RangeKVs is Range without building protobuf messages: one slice of
// values instead of a message per key, for drivers that encode the
// response themselves.
func (s *Store) RangeKVs(r *pb.RangeRequest) (RangeResult, error) {
	w := &writer{s: s, begin: s.rev}
	return w.rangeResult(r)
}

func (w *writer) rangeKeys(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	res, err := w.rangeResult(r)
	if err != nil {
		return nil, err
	}
	return res.Response(), nil
}

// Response is the range's answer as etcd's message, with the revision in
// its header.
func (res *RangeResult) Response() *pb.RangeResponse {
	resp := &pb.RangeResponse{Header: header(res.Rev), Count: res.Count, More: res.More}
	if len(res.KVs) > 0 {
		kvs := make([]mvccpb.KeyValue, len(res.KVs))
		resp.Kvs = make([]*mvccpb.KeyValue, len(res.KVs))
		for i, kv := range res.KVs {
			kvs[i] = mvccpb.KeyValue{
				Key: kv.Key, Value: kv.Value, CreateRevision: kv.CreateRevision,
				ModRevision: kv.ModRevision, Version: kv.Version, Lease: kv.Lease,
			}
			resp.Kvs[i] = &kvs[i]
		}
	}
	return resp
}

func (w *writer) rangeResult(r *pb.RangeRequest) (RangeResult, error) {
	w.s.counts.Ranges++
	cur := w.cur()
	at := r.Revision
	if at <= 0 {
		at = cur
	}
	if at > cur {
		return RangeResult{}, rpctypes.ErrGRPCFutureRev
	}
	if at < w.s.compactRev {
		return RangeResult{}, rpctypes.ErrGRPCCompacted
	}

	// etcd pushes limit+1 into the index only with the default order and no
	// revision filters. It decides the default order before it turns a NONE
	// order with a non-key target into ASCEND.
	pushLimit := int64(0)
	if r.Limit > 0 && r.Limit < math.MaxInt64 && IsDefaultOrdering(r.SortTarget, r.SortOrder) && !hasRevisionFilters(r) {
		pushLimit = r.Limit + 1
	}
	var kvs []KV
	count := int64(0)
	w.s.ascend(r.Key, r.RangeEnd, func(k *keyIndex) bool {
		v, ok := k.live(at)
		if !ok {
			return true
		}
		count++
		if !r.CountOnly && (pushLimit == 0 || int64(len(kvs)) < pushLimit) {
			kvs = append(kvs, k.kvValue(v))
		}
		return true
	})

	kvs = filterKVs(kvs, r)
	sortKVs(kvs, r)

	res := RangeResult{Rev: cur, Count: count}
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs = kvs[:r.Limit]
		res.More = true
	}
	if r.KeysOnly {
		// Without a sort on values, etcd reads only the key index, which
		// has no lease.
		fast := r.SortTarget != pb.RangeRequest_VALUE
		for i := range kvs {
			kvs[i].Value = nil
			if fast {
				kvs[i].Lease = 0
			}
		}
	}
	res.KVs = kvs
	return res, nil
}

// IsDefaultOrdering reports whether a range returns keys in the order the
// index holds them.
func IsDefaultOrdering(t pb.RangeRequest_SortTarget, o pb.RangeRequest_SortOrder) bool {
	return o == pb.RangeRequest_NONE || (t == pb.RangeRequest_KEY && o == pb.RangeRequest_ASCEND)
}

func hasRevisionFilters(r *pb.RangeRequest) bool {
	return r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0
}

func filterKVs(kvs []KV, r *pb.RangeRequest) []KV {
	if !hasRevisionFilters(r) {
		return kvs
	}
	out := kvs[:0]
	for _, kv := range kvs {
		switch {
		case r.MaxModRevision != 0 && kv.ModRevision > r.MaxModRevision:
		case r.MinModRevision != 0 && kv.ModRevision < r.MinModRevision:
		case r.MaxCreateRevision != 0 && kv.CreateRevision > r.MaxCreateRevision:
		case r.MinCreateRevision != 0 && kv.CreateRevision < r.MinCreateRevision:
		default:
			out = append(out, kv)
		}
	}
	return out
}

// kvSort sorts with sort.Sort like etcd, so that ties end in the same order.
type kvSort struct {
	kvs  []KV
	less func(a, b *KV) bool
}

func (s kvSort) Len() int           { return len(s.kvs) }
func (s kvSort) Swap(i, j int)      { s.kvs[i], s.kvs[j] = s.kvs[j], s.kvs[i] }
func (s kvSort) Less(i, j int) bool { return s.less(&s.kvs[i], &s.kvs[j]) }

func sortKVs(kvs []KV, r *pb.RangeRequest) {
	order := r.SortOrder
	if r.SortTarget != pb.RangeRequest_KEY && order == pb.RangeRequest_NONE {
		order = pb.RangeRequest_ASCEND
	}
	if IsDefaultOrdering(r.SortTarget, order) {
		return
	}
	var less func(a, b *KV) bool
	switch r.SortTarget {
	case pb.RangeRequest_KEY:
		less = func(a, b *KV) bool { return bytes.Compare(a.Key, b.Key) < 0 }
	case pb.RangeRequest_VERSION:
		less = func(a, b *KV) bool { return a.Version < b.Version }
	case pb.RangeRequest_CREATE:
		less = func(a, b *KV) bool { return a.CreateRevision < b.CreateRevision }
	case pb.RangeRequest_MOD:
		less = func(a, b *KV) bool { return a.ModRevision < b.ModRevision }
	case pb.RangeRequest_VALUE:
		less = func(a, b *KV) bool { return bytes.Compare(a.Value, b.Value) < 0 }
	default:
		return
	}
	var s sort.Interface = kvSort{kvs, less}
	if order == pb.RangeRequest_DESCEND {
		s = sort.Reverse(s)
	}
	sort.Sort(s)
}

// PutOp is a put request's fields. Key and Value are kept without
// copying.
type PutOp struct {
	Key, Value                       []byte
	Lease                            int64
	PrevKv, IgnoreValue, IgnoreLease bool
}

func putOp(p *pb.PutRequest) PutOp {
	return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}
}

// Put writes one key.
func (s *Store) Put(p *pb.PutRequest) (*pb.PutResponse, error) {
	op := putOp(p)
	prev, err := s.PutKV(&op)
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Header: header(s.rev), PrevKv: prev}, nil
}

// PutKV writes one key and returns its previous version if op asks for it
// and the key existed. The store's revision is then the put's revision.
func (s *Store) PutKV(op *PutOp) (*mvccpb.KeyValue, error) {
	w := s.start()
	if err := w.checkPut(op); err != nil {
		return nil, err
	}
	prev := w.put(op)
	w.finish()
	return prev, nil
}

func (w *writer) checkPut(p *PutOp) error {
	if p.Lease != 0 && (w.s.LeaseExists == nil || !w.s.LeaseExists(p.Lease)) {
		return rpctypes.ErrGRPCLeaseNotFound
	}
	if p.IgnoreValue || p.IgnoreLease {
		k, ok := w.s.keys.Get(&keyIndex{key: p.Key})
		if !ok {
			return rpctypes.ErrGRPCKeyNotFound
		}
		if _, ok := k.live(w.cur()); !ok {
			return rpctypes.ErrGRPCKeyNotFound
		}
	}
	return nil
}

// latest returns the newest version of key if the key exists now.
func (w *writer) latest(key []byte) (*keyIndex, *version) {
	k, ok := w.s.keys.Get(&keyIndex{key: key})
	if !ok {
		return nil, nil
	}
	v := &k.vers[len(k.vers)-1]
	if v.ver == 0 {
		return k, nil
	}
	return k, v
}

func (w *writer) add(k *keyIndex, v version) {
	v.mod = rev{main: w.begin + 1, sub: int64(len(w.s.changes))}
	if planted.On("B09a") {
		v.mod = rev{main: w.begin + 1 + int64(len(w.s.changes))}
	}
	k.vers = append(k.vers, v)
	w.changed = true
	w.s.changes = append(w.s.changes, Change{k: k, i: len(k.vers) - 1})
}

// put writes one key and returns its previous version if p asks for it.
func (w *writer) put(p *PutOp) *mvccpb.KeyValue {
	w.s.counts.Puts++
	var prevKV *mvccpb.KeyValue
	k, prev := w.latest(p.Key)
	v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}
	if prev != nil {
		if p.PrevKv {
			prevKV = k.kv(prev)
		}
		if p.IgnoreValue {
			v.value = prev.value
		}
		if p.IgnoreLease {
			v.lease = prev.lease
		}
		v.create = prev.create
		v.ver = prev.ver + 1
	} else if planted.On("B09b") && k != nil && len(k.vers) >= 2 && k.vers[len(k.vers)-2].ver != 0 {
		old := k.vers[len(k.vers)-2]
		v.create, v.ver = old.create, old.ver+1
	}
	if k == nil {
		k = &keyIndex{key: p.Key}
		w.s.keys.ReplaceOrInsert(k)
	}
	w.add(k, v)
	return prevKV
}

// DeleteRange deletes keys.
func (s *Store) DeleteRange(d *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	w := s.start()
	resp := w.deleteRange(d)
	w.finish()
	return resp, nil
}

func (w *writer) deleteRange(d *pb.DeleteRangeRequest) *pb.DeleteRangeResponse {
	w.s.counts.Deletes++
	resp := &pb.DeleteRangeResponse{}
	var del []*keyIndex
	w.s.ascend(d.Key, d.RangeEnd, func(k *keyIndex) bool {
		if k.vers[len(k.vers)-1].ver != 0 {
			del = append(del, k)
		}
		return true
	})
	if d.PrevKv {
		resp.PrevKvs = make([]*mvccpb.KeyValue, len(del))
		for i, k := range del {
			resp.PrevKvs[i] = k.kv(&k.vers[len(k.vers)-1])
		}
	}
	for _, k := range del {
		w.add(k, version{})
	}
	resp.Deleted = int64(len(del))
	resp.Header = header(w.cur())
	return resp
}

// Txn runs a transaction. Compares and checks read the state before the
// transaction; reads inside it see its own earlier writes. An error after
// checkTxn passed is a bug, and the caller must stop using the store.
func (s *Store) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	w := s.start()
	path := w.compareToPath(r)
	if _, err := w.checkTxn(r, path); err != nil {
		return nil, err
	}
	resp, _, err := w.executeTxn(r, path)
	if err != nil {
		return nil, err
	}
	resp.Header = header(w.cur())
	w.finish()
	return resp, nil
}

func (w *writer) compareToPath(r *pb.TxnRequest) []bool {
	path := []bool{w.applyCompares(r.Compare)}
	ops := r.Success
	if !path[0] {
		ops = r.Failure
	}
	for _, op := range ops {
		if t := op.GetRequestTxn(); t != nil {
			path = append(path, w.compareToPath(t)...)
		}
	}
	return path
}

func (w *writer) applyCompares(cs []*pb.Compare) bool {
	for _, c := range cs {
		if !w.applyCompare(c) {
			return false
		}
	}
	return true
}

func (w *writer) applyCompare(c *pb.Compare) bool {
	end := c.RangeEnd
	matched := false
	ok := true
	w.s.ascend(c.Key, end, func(k *keyIndex) bool {
		v, live := k.live(w.cur())
		if !live {
			return true
		}
		matched = true
		if !compareKV(c, k.kv(v)) {
			ok = false
			return false
		}
		return true
	})
	if !matched {
		// A missing key has no value to compare, so VALUE compares fail.
		if c.Target == pb.Compare_VALUE {
			return false
		}
		return compareKV(c, &mvccpb.KeyValue{})
	}
	return ok
}

func compareKV(c *pb.Compare, kv *mvccpb.KeyValue) bool {
	var result int
	switch c.Target {
	case pb.Compare_VALUE:
		result = bytes.Compare(kv.Value, c.GetValue())
	case pb.Compare_CREATE:
		result = cmpInt(kv.CreateRevision, c.GetCreateRevision())
	case pb.Compare_MOD:
		result = cmpInt(kv.ModRevision, c.GetModRevision())
	case pb.Compare_VERSION:
		result = cmpInt(kv.Version, c.GetVersion())
	case pb.Compare_LEASE:
		result = cmpInt(kv.Lease, c.GetLease())
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	}
	return true
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func branch(r *pb.TxnRequest, ok bool) []*pb.RequestOp {
	if ok {
		return r.Success
	}
	return r.Failure
}

// checkTxn checks the chosen branch against the state before the
// transaction and returns the number of nested transactions in it.
func (w *writer) checkTxn(r *pb.TxnRequest, path []bool) (int, error) {
	n := 0
	for _, op := range branch(r, path[0]) {
		var err error
		switch t := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			err = w.checkRange(t.RequestRange)
		case *pb.RequestOp_RequestPut:
			op := putOp(t.RequestPut)
			err = w.checkPut(&op)
		case *pb.RequestOp_RequestTxn:
			var sub int
			sub, err = w.checkTxn(t.RequestTxn, path[1:])
			n += sub + 1
			path = path[sub+1:]
		}
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (w *writer) checkRange(r *pb.RangeRequest) error {
	switch {
	case r.Revision == 0:
		return nil
	case r.Revision > w.begin:
		return rpctypes.ErrGRPCFutureRev
	case r.Revision < w.s.compactRev:
		return rpctypes.ErrGRPCCompacted
	}
	return nil
}

// executeTxn runs the chosen branch and returns its response and the
// number of nested transactions in it. Nested responses keep an empty
// header, as in etcd.
func (w *writer) executeTxn(r *pb.TxnRequest, path []bool) (*pb.TxnResponse, int, error) {
	ops := branch(r, path[0])
	resp := &pb.TxnResponse{
		Header:    &pb.ResponseHeader{},
		Succeeded: path[0],
		Responses: make([]*pb.ResponseOp, len(ops)),
	}
	n := 0
	for i, op := range ops {
		var out pb.ResponseOp
		switch t := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			rr, err := w.rangeKeys(t.RequestRange)
			if err != nil {
				return nil, 0, err
			}
			out.Response = &pb.ResponseOp_ResponseRange{ResponseRange: rr}
		case *pb.RequestOp_RequestPut:
			op := putOp(t.RequestPut)
			out.Response = &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&op)}}
		case *pb.RequestOp_RequestDeleteRange:
			out.Response = &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: w.deleteRange(t.RequestDeleteRange)}
		case *pb.RequestOp_RequestTxn:
			sub, subN, err := w.executeTxn(t.RequestTxn, path[1:])
			if err != nil {
				return nil, 0, err
			}
			out.Response = &pb.ResponseOp_ResponseTxn{ResponseTxn: sub}
			n += subN + 1
			path = path[subN+1:]
		}
		resp.Responses[i] = &out
	}
	return resp, n, nil
}
