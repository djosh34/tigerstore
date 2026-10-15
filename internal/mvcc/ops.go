package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
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
	s.fills = nil
	return &writer{s: s, begin: s.rev}
}

// reader starts a read outside any write.
func (s *Store) reader() *writer {
	s.fills = nil
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
// mvccpb.KeyValue, as a value. Key is the index's slice, Value the one
// Values returned; neither may change.
type KV struct {
	Key, Value                                  []byte
	CreateRevision, ModRevision, Version, Lease int64
	ref                                         Ref
}

// RangeResult is a range's answer without its header: the store's
// revision, for the header, the keys, their count and whether more were
// left out.
type RangeResult struct {
	Rev   int64
	KVs   []KV
	Count int64
	More  bool
	// values is set when the KVs still need their values.
	values bool
}

// Range reads keys at the current or a past revision.
func (s *Store) Range(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	return s.reader().rangeKeys(r, true)
}

// RangeKVs is Range without building protobuf messages: one slice of
// values instead of a message per key, for drivers that encode the
// response themselves.
func (s *Store) RangeKVs(r *pb.RangeRequest) (RangeResult, error) {
	w := s.reader()
	res, err := w.rangeResult(r, true)
	if err != nil || !res.values {
		return res, err
	}
	for i := range res.KVs {
		kv := &res.KVs[i]
		if err := s.fill(kv.Key, kv.ref, &kv.Value); err != nil {
			return RangeResult{}, err
		}
	}
	return res, nil
}

// rangeKeys answers r as etcd's message. Without total, as rangeResult.
func (w *writer) rangeKeys(r *pb.RangeRequest, total bool) (*pb.RangeResponse, error) {
	res, err := w.rangeResult(r, total)
	if err != nil {
		return nil, err
	}
	resp := res.Response()
	if res.values {
		for i := range res.KVs {
			if err := w.s.fill(res.KVs[i].Key, res.KVs[i].ref, &resp.Kvs[i].Value); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
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

// RangeChunk answers one chunk of a RangeStream as etcd v3.7.2's range
// without a total count does: the index walk stops once it has the limit
// plus one keys, and Count holds the keys it saw, not every key in the
// range.
func (s *Store) RangeChunk(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	return s.reader().rangeKeys(r, false)
}

// rangeResult answers r without the values, unless it sorts by them, and
// sets values if they are still to be read. Without total, the walk stops
// at the pushed-down limit and Count holds only the keys it saw.
func (w *writer) rangeResult(r *pb.RangeRequest, total bool) (RangeResult, error) {
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
		if !total && pushLimit > 0 && int64(len(kvs)) >= pushLimit {
			return false
		}
		count++
		if !r.CountOnly && (pushLimit == 0 || int64(len(kvs)) < pushLimit) {
			kvs = append(kvs, k.kvValue(v))
		}
		return true
	})

	kvs = filterKVs(kvs, r)
	byValue := r.SortTarget == pb.RangeRequest_VALUE
	if byValue {
		if err := w.s.readNow(kvs); err != nil {
			return RangeResult{}, err
		}
	}
	sortKVs(kvs, r)

	res := RangeResult{Rev: cur, Count: count, values: !r.KeysOnly && !byValue}
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs = kvs[:r.Limit]
		res.More = true
	}
	if r.KeysOnly {
		// Without a sort on values, etcd reads only the key index, which
		// has no lease.
		for i := range kvs {
			kvs[i].Value = nil
			if !byValue {
				kvs[i].Lease = 0
			}
		}
	}
	res.KVs = kvs
	return res, nil
}

// readNow reads the value of every KV at once, for a sort on them. If
// some are only on disk, it returns a ColdError that needs them all.
func (s *Store) readNow(kvs []KV) error {
	var cold []Fill
	for i := range kvs {
		kv := &kvs[i]
		b, ok, err := s.value(kv.Key, kv.ref)
		if err != nil {
			return err
		}
		if !ok {
			cold = append(cold, Fill{Key: kv.Key, Ref: kv.ref})
		}
		kv.Value = b
	}
	if len(cold) > 0 {
		return &ColdError{Need: cold}
	}
	return nil
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
		// etcd reads the previous version before this check fails.
		w.countPrevRead(op)
		return nil, err
	}
	prev, err := w.put(op)
	if err != nil {
		return nil, err
	}
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

// countPrevRead counts the read of the previous version that etcd makes
// for a put with prev_kv, ignore_value or ignore_lease.
func (w *writer) countPrevRead(p *PutOp) {
	if p.PrevKv || p.IgnoreValue || p.IgnoreLease {
		w.s.counts.Ranges++
	}
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
	if n := len(k.vers); n > 0 && w.s.Replaced != nil {
		if old := k.vers[n-1]; old.ver != 0 && old.value.Len > 0 && old.value != v.value {
			w.s.Replaced(old.value)
		}
	}
	c := cap(k.vers)
	k.vers = append(k.vers, v)
	w.s.size += rowBytes(k.key, &v)
	w.s.versions += int64(cap(k.vers) - c)
	if w.s.track {
		w.s.sum ^= versionHash(k.key, &v)
	}
	w.changed = true
	w.s.changes = append(w.s.changes, Change{s: w.s, k: k, i: len(k.vers) - 1})
}

// put writes one key and returns its previous version if p asks for it.
// An error is a failed value read or placement; the store is then
// unchanged.
func (w *writer) put(p *PutOp) (*mvccpb.KeyValue, error) {
	w.s.counts.Puts++
	w.countPrevRead(p)
	var prevKV *mvccpb.KeyValue
	k, prev := w.latest(p.Key)
	v := version{create: w.begin + 1, ver: 1, lease: p.Lease}
	if !p.IgnoreValue || prev == nil {
		ref, err := w.s.place(p.Value)
		if err != nil {
			return nil, err
		}
		v.value = ref
	}
	if prev != nil {
		if p.PrevKv {
			var err error
			if prevKV, err = w.s.kv(k, prev); err != nil {
				return nil, err
			}
		}
		if p.IgnoreValue {
			v.value = prev.value
		}
		if p.IgnoreLease {
			v.lease = prev.lease
		}
		v.create = prev.create
		v.ver = prev.ver + 1
	} else {
		w.s.counts.Keys++
	}
	if prev == nil && planted.On("B09b") && k != nil && len(k.vers) >= 2 && k.vers[len(k.vers)-2].ver != 0 {
		old := k.vers[len(k.vers)-2]
		v.create, v.ver = old.create, old.ver+1
	}
	if k == nil {
		// A copy, so that the index keeps no Raft entry alive.
		k = &keyIndex{key: bytes.Clone(p.Key)}
		w.s.keys.ReplaceOrInsert(k)
		w.s.keyBytes += int64(len(k.key))
	}
	w.add(k, v)
	w.s.counts.PutBytes += int64(len(p.Key)) + int64(v.value.Len)
	if w.s.trackWritten && v.value.Len > 0 && (!p.IgnoreValue || prev == nil) {
		w.s.written = append(w.s.written, written{k: k, ref: v.value})
	}
	return prevKV, nil
}

// place names a value that the request being applied writes.
func (s *Store) place(value []byte) (Ref, error) {
	if len(value) == 0 {
		return Ref{}, nil
	}
	r, err := s.values.Place(value)
	if err != nil {
		return Ref{}, fmt.Errorf("mvcc: place a value: %w", err)
	}
	r.Sum = crc32.Checksum(value, castagnoli)
	return r, nil
}

// DeleteRange deletes keys.
func (s *Store) DeleteRange(d *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	w := s.start()
	resp, err := w.deleteRange(d)
	if err != nil {
		return nil, err
	}
	w.finish()
	return resp, nil
}

func (w *writer) deleteRange(d *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	w.s.counts.Deletes++
	if d.PrevKv {
		// etcd reads the previous values with a range.
		w.s.counts.Ranges++
	}
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
			kv, err := w.s.kv(k, &k.vers[len(k.vers)-1])
			if err != nil {
				return nil, err
			}
			resp.PrevKvs[i] = kv
		}
	}
	for _, k := range del {
		w.add(k, version{})
	}
	w.s.counts.Keys -= int64(len(del))
	resp.Deleted = int64(len(del))
	resp.Header = header(w.cur())
	return resp, nil
}

// Txn runs a transaction. Compares and checks read the state before the
// transaction; reads inside it see its own earlier writes. An error after
// checkTxn passed is a bug, and the caller must stop using the store.
func (s *Store) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	w := s.start()
	ops := s.counts.Puts + s.counts.Deletes + s.counts.Ranges
	defer func() {
		if s.counts.Puts+s.counts.Deletes+s.counts.Ranges-ops > 1 {
			s.counts.Txns++
		}
	}()
	path, err := w.compareToPath(r)
	if err != nil {
		return nil, err
	}
	if _, err := w.checkTxn(r, path); err != nil {
		return nil, err
	}
	resp, _, err := w.executeTxn(r, path)
	if err != nil {
		if cold := (*ColdError)(nil); errors.As(err, &cold) {
			// checkTxn found every value it sorts by in memory; the
			// transaction may have changed keys already, so it cannot run
			// again.
			return nil, fmt.Errorf("mvcc: a transaction needs %d values from disk after its check", len(cold.Need))
		}
		return nil, err
	}
	resp.Header = header(w.cur())
	w.finish()
	return resp, nil
}

func (w *writer) compareToPath(r *pb.TxnRequest) ([]bool, error) {
	ok, err := w.applyCompares(r.Compare)
	if err != nil {
		return nil, err
	}
	path := []bool{ok}
	ops := r.Success
	if !path[0] {
		ops = r.Failure
	}
	for _, op := range ops {
		if t := op.GetRequestTxn(); t != nil {
			sub, err := w.compareToPath(t)
			if err != nil {
				return nil, err
			}
			path = append(path, sub...)
		}
	}
	return path, nil
}

func (w *writer) applyCompares(cs []*pb.Compare) (bool, error) {
	for _, c := range cs {
		ok, err := w.applyCompare(c)
		if !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *writer) applyCompare(c *pb.Compare) (bool, error) {
	// etcd reads each compare's keys with a range.
	w.s.counts.Ranges++
	end := c.RangeEnd
	matched := false
	ok := true
	var err error
	// cold are the values only the disk has. Any key that fails the
	// compare decides it without them.
	var cold []Fill
	w.s.ascend(c.Key, end, func(k *keyIndex) bool {
		v, live := k.live(w.cur())
		if !live {
			return true
		}
		matched = true
		kv := k.kvValue(v)
		if c.Target == pb.Compare_VALUE {
			// Only a compare on the value reads it.
			var in bool
			if kv.Value, in, err = w.s.value(k.key, v.value); err != nil {
				return false
			}
			if !in {
				cold = append(cold, Fill{Key: k.key, Ref: v.value})
				return true
			}
		}
		if !compareKV(c, &kv) {
			ok = false
			return false
		}
		return true
	})
	if err != nil {
		return false, err
	}
	if ok && len(cold) > 0 {
		return false, &ColdError{Need: cold}
	}
	if !matched {
		// A missing key has no value to compare, so VALUE compares fail.
		if c.Target == pb.Compare_VALUE {
			return false, nil
		}
		return compareKV(c, &KV{}), nil
	}
	return ok, nil
}

func compareKV(c *pb.Compare, kv *KV) bool {
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
			if err = w.checkRange(t.RequestRange); err == nil {
				err = w.checkSortValues(t.RequestRange)
			}
		case *pb.RequestOp_RequestPut:
			op := putOp(t.RequestPut)
			// etcd's check of a put in a transaction reads the previous
			// version once more.
			w.countPrevRead(&op)
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

// checkSortValues returns a ColdError if a range that sorts by value
// would need values only the disk has, so that the transaction waits for
// them before it changes anything. Its own writes are in memory; the keys
// it reads otherwise are those at r's revision, or before the
// transaction.
func (w *writer) checkSortValues(r *pb.RangeRequest) error {
	if r.SortTarget != pb.RangeRequest_VALUE || r.CountOnly {
		return nil
	}
	at := r.Revision
	if at <= 0 {
		at = w.begin
	}
	var kvs []KV
	w.s.ascend(r.Key, r.RangeEnd, func(k *keyIndex) bool {
		if v, ok := k.live(at); ok {
			kvs = append(kvs, k.kvValue(v))
		}
		return true
	})
	return w.s.readNow(kvs)
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
			rr, err := w.rangeKeys(t.RequestRange, true)
			if err != nil {
				return nil, 0, err
			}
			out.Response = &pb.ResponseOp_ResponseRange{ResponseRange: rr}
		case *pb.RequestOp_RequestPut:
			op := putOp(t.RequestPut)
			prev, err := w.put(&op)
			if err != nil {
				return nil, 0, err
			}
			out.Response = &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: header(w.begin + 1), PrevKv: prev}}
		case *pb.RequestOp_RequestDeleteRange:
			d, err := w.deleteRange(t.RequestDeleteRange)
			if err != nil {
				return nil, 0, err
			}
			out.Response = &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: d}
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
