package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

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
	// subs counts the changes of the request so far: the next one's sub
	// revision.
	subs int64
	// room is what the KeyValues of the request's answer may take; the
	// rest of them come in pieces (TakeRests).
	room int
	// done are the results of a transaction's finished compares, and
	// resume how far an earlier run of it got.
	done   []bool
	resume *Compares
}

func (s *Store) start() *writer {
	s.resetChanges()
	return s.reader()
}

// reader starts a read outside any write.
func (s *Store) reader() *writer {
	s.fills, s.rests = nil, nil
	room := s.PieceBytes
	if room <= 0 {
		room = math.MaxInt
	}
	return &writer{s: s, begin: s.rev, room: room}
}

// finish moves the store to the next revision if the request changed a key.
func (w *writer) finish() {
	if w.changed {
		w.s.rev = w.begin + 1
		if planted.On("B09a") {
			w.s.rev = w.begin + w.subs
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
		k, ok := w.s.get(p.Key)
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
	k, ok := w.s.get(key)
	if !ok {
		return nil, nil
	}
	v := &k.vers()[len(k.vers())-1]
	if v.ver == 0 {
		return k, nil
	}
	return k, v
}

func (w *writer) add(k *keyIndex, v version) {
	v.mod = rev{main: w.begin + 1, sub: w.subs}
	if planted.On("B09a") {
		v.mod = rev{main: w.begin + 1 + w.subs}
	}
	w.subs++
	if n := len(k.vers()); n > 0 && w.s.Replaced != nil {
		if old := k.vers()[n-1]; old.ver != 0 && old.value.Len > 0 && old.value != v.value {
			w.s.Replaced(old.value)
		}
	}
	w.s.versions += k.push(v)
	w.s.size += rowBytes(w.s.key(k), &v)
	if w.s.track {
		w.s.sum ^= versionHash(w.s.key(k), &v)
	}
	w.changed = true
	w.s.changes = append(w.s.changes, Change{s: w.s, k: k, i: len(k.vers()) - 1})
	w.s.hist.add(k, v.mod.main)
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
	if prev == nil && planted.On("B09b") && k != nil && len(k.vers()) >= 2 && k.vers()[len(k.vers())-2].ver != 0 {
		old := k.vers()[len(k.vers())-2]
		v.create, v.ver = old.create, old.ver+1
	}
	if k == nil {
		// insert copies the key, so the index keeps no Raft entry alive.
		k = w.s.insert(p.Key, nil)
	}
	w.add(k, v)
	w.s.counts.PutBytes += int64(len(p.Key)) + int64(v.value.Len)
	if w.s.trackWritten && v.value.Len > 0 && (!p.IgnoreValue || prev == nil) {
		w.s.written = append(w.s.written, written{id: k.id, ref: v.value})
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
	if d.PrevKv {
		// The previous values are a range just before the delete, read
		// through a cursor as any range's, so a large one comes in pieces.
		res, c, err := w.rangeAt(&pb.RangeRequest{Key: d.Key, RangeEnd: d.RangeEnd}, true, w.room, false)
		if err != nil {
			return nil, err
		}
		resp.PrevKvs = KeyValues(res.KVs)
		w.took(resp, &res, c)
		if err := w.s.fillValues(res.KVs, resp.PrevKvs); err != nil {
			return nil, err
		}
	}
	var del []*keyIndex
	w.s.ascend(d.Key, d.RangeEnd, func(k *keyIndex) bool {
		if k.vers()[len(k.vers())-1].ver != 0 {
			del = append(del, k)
		}
		return true
	})
	for _, k := range del {
		w.add(k, version{})
	}
	w.s.counts.Keys -= int64(len(del))
	resp.Deleted = int64(len(del))
	resp.Header = header(w.cur())
	return resp, nil
}

// Deletion deletes keys at one revision over several calls, as a lease
// revoke of many keys does (#505). The store moves to the revision only
// when it is done, so a read sees all of its deletes or none; nothing
// else may write meanwhile. As in etcd's lessor, each key is a delete of
// its own in the counts, and the write counts as a transaction when it
// has more than one; a key already gone is skipped.
type Deletion struct {
	w *writer
	n int
}

// StartDeletes starts a Deletion.
func (s *Store) StartDeletes() *Deletion { return &Deletion{w: s.start()} }

// Delete deletes keys, which are sorted and come after those of earlier
// calls. Changes then returns the changes of this call alone.
func (d *Deletion) Delete(keys []string) {
	s := d.w.s
	s.resetChanges()
	for _, key := range keys {
		if k, ok := s.get([]byte(key)); ok && k.vers()[len(k.vers())-1].ver != 0 {
			d.w.add(k, version{})
			s.counts.Keys--
		}
	}
	s.counts.Deletes += int64(len(keys))
	d.n += len(keys)
}

// Finish moves the store to the Deletion's revision.
func (d *Deletion) Finish() {
	if d.n > 1 {
		d.w.s.counts.Txns++
	}
	d.w.finish()
}

// Txn runs a transaction. Compares and checks read the state before the
// transaction; reads inside it see its own earlier writes. An error after
// checkTxn passed is a bug, and the caller must stop using the store.
func (s *Store) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	w := s.start()
	if c := s.resume; c != nil && c.Rev == w.cur() {
		w.resume = c
	}
	s.resume = nil
	ops := s.counts.Puts + s.counts.Deletes + s.counts.Ranges
	defer func() {
		if s.counts.Puts+s.counts.Deletes+s.counts.Ranges-ops > 1 {
			s.counts.Txns++
		}
	}()
	path, err := w.compareToPath(r)
	if err == nil {
		_, err = w.checkTxn(r, path)
	}
	if cold, ok := errors.AsType[*ColdError](err); ok {
		if cold.Compares == nil {
			cold.Compares = &Compares{Done: w.done}
		}
		cold.Compares.Rev = w.cur()
		return nil, cold
	}
	if err != nil {
		return nil, err
	}
	resp, _, err := w.executeTxn(r, path)
	if err != nil {
		if cold, ok := errors.AsType[*ColdError](err); ok {
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
	i := len(w.done)
	if r := w.resume; r != nil && i < len(r.Done) {
		w.done = append(w.done, r.Done[i])
		return r.Done[i], nil
	}
	ok, err := w.compare(c)
	if err == nil {
		w.done = append(w.done, ok)
	}
	return ok, err
}

// compare runs one compare. It stops at the first key that fails it. The
// values only the disk has are read in batches: the walk stops once one
// is full, and the next run goes on after the last key that matched
// before the batch.
func (w *writer) compare(c *pb.Compare) (bool, error) {
	start, end := c.Key, c.RangeEnd
	var after []byte
	if r := w.resume; r != nil && len(r.Done) == len(w.done) && r.After != nil && len(end) > 0 {
		after = r.After
		start = append(bytes.Clone(after), 0)
	}
	matched := after != nil
	ok := true
	var err error
	// cold are the values only the disk has, size their bytes and last the
	// last key that matched before the first of them.
	var cold []Fill
	var size int64
	last := after
	w.s.ascend(start, end, func(k *keyIndex) bool {
		v, live := k.live(w.cur())
		if !live {
			return true
		}
		matched = true
		kv := w.s.kvValue(k, v)
		if c.Target == pb.Compare_VALUE {
			// Only a compare on the value reads it.
			var in bool
			if kv.Value, in, err = w.s.value(kv.Key, v.value, nil); err != nil {
				return false
			}
			if !in {
				cold = append(cold, Fill{Key: kv.Key, Ref: v.value})
				size += int64(v.value.Len)
				return size < compareBatch
			}
		}
		if !compareKV(c, &kv) {
			ok = false
			return false
		}
		if len(cold) == 0 {
			last = w.s.key(k)
		}
		return true
	})
	if err != nil {
		return false, err
	}
	if ok && len(cold) > 0 {
		return false, &ColdError{Need: cold, Compares: &Compares{Done: w.done, After: last, Batch: true}}
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

// Resume makes the next Txn go on from where an earlier run of it got,
// unless a write came between. Only that Txn uses c.
func (s *Store) Resume(c *Compares) { s.resume = c }

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
			kvs = append(kvs, w.s.kvValue(k, v))
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
	t := new(txnParts)
	resp := &t.resp
	resp.Header, resp.Succeeded = &t.header, path[0]
	parts, outs := t.one[:], t.outs[:]
	if len(ops) > 1 {
		parts, outs = make([]opParts, len(ops)), make([]*pb.ResponseOp, len(ops))
	}
	resp.Responses = outs[:len(ops)]
	n := 0
	for i, op := range ops {
		p := &parts[i]
		out := &p.out
		switch t := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			w.s.counts.Ranges++
			rr, err := w.rangeMessage(t.RequestRange, true)
			if err != nil {
				return nil, 0, err
			}
			p.rng.ResponseRange = rr
			out.Response = &p.rng
		case *pb.RequestOp_RequestPut:
			op := putOp(t.RequestPut)
			prev, err := w.put(&op)
			if err != nil {
				return nil, 0, err
			}
			p.header.Revision = w.begin + 1
			p.putResp.Header, p.putResp.PrevKv = &p.header, prev
			p.put.ResponsePut = &p.putResp
			out.Response = &p.put
		case *pb.RequestOp_RequestDeleteRange:
			d, err := w.deleteRange(t.RequestDeleteRange)
			if err != nil {
				return nil, 0, err
			}
			p.del.ResponseDeleteRange = d
			out.Response = &p.del
		case *pb.RequestOp_RequestTxn:
			sub, subN, err := w.executeTxn(t.RequestTxn, path[1:])
			if err != nil {
				return nil, 0, err
			}
			p.txn.ResponseTxn = sub
			out.Response = &p.txn
			n += subN + 1
			path = path[subN+1:]
		}
		resp.Responses[i] = out
	}
	return resp, n, nil
}

// txnParts is a transaction's response with its header and, for one op,
// the op's parts, so that the response to a Kubernetes write, a Txn of one
// op, is one allocation instead of six (#602).
type txnParts struct {
	resp   pb.TxnResponse
	header pb.ResponseHeader
	outs   [1]*pb.ResponseOp
	one    [1]opParts
}

// opParts are the messages of one op's response.
type opParts struct {
	out     pb.ResponseOp
	rng     pb.ResponseOp_ResponseRange
	put     pb.ResponseOp_ResponsePut
	putResp pb.PutResponse
	header  pb.ResponseHeader
	del     pb.ResponseOp_ResponseDeleteRange
	txn     pb.ResponseOp_ResponseTxn
}
