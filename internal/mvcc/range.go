package mvcc

import (
	"bytes"
	"math"
	"sort"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Every Range reads through a Cursor, a piece at a time (#505). A piece
// holds at most room bytes of encoded keys and values, and the first
// piece comes with the count and the encoded size of the whole answer,
// which a walk of the index gives without reading a value. An answer that
// fits in one piece is read in one walk. A Cursor reads at a fixed
// position in the store's history, so its later pieces read what its
// first saw, until a compaction passes that position.

// KV is one key's version as a range returns it: the fields of an
// mvccpb.KeyValue, as a value. Key is the index's slice, Value the one
// Values returned; neither may change.
type KV struct {
	Key, Value                                  []byte
	CreateRevision, ModRevision, Version, Lease int64
	ref                                         Ref
}

// Size is the size of the KeyValue message of kv as protobuf encodes it,
// with a value of valueLen bytes; fields at their zero value are left
// out.
func (kv *KV) Size(valueLen int) int {
	n := 0
	if len(kv.Key) > 0 {
		n += 1 + protowire.SizeBytes(len(kv.Key))
	}
	for _, v := range [...]int64{kv.CreateRevision, kv.ModRevision, kv.Version} {
		if v != 0 {
			n += 1 + protowire.SizeVarint(uint64(v))
		}
	}
	if valueLen > 0 {
		n += 1 + protowire.SizeBytes(valueLen)
	}
	if kv.Lease != 0 {
		n += 1 + protowire.SizeVarint(uint64(kv.Lease))
	}
	return n
}

// FieldSize is the size kv takes as one element of a repeated KeyValue
// field: its tag, its length and its message.
func (kv *KV) FieldSize() int { return 1 + protowire.SizeBytes(kv.Size(len(kv.Value))) }

// RangeResult is a range's answer without its header: the store's
// revision, for the header, the keys of this piece, the count and
// whether more were left out, and the number of keys and the encoded size
// of the KeyValue fields of the whole answer.
type RangeResult struct {
	Rev   int64
	KVs   []KV
	Count int64
	More  bool
	// N is the number of KVs of the whole answer, and Fields the size of
	// their KeyValue fields as one RangeResponse encodes them.
	N      int64
	Fields int64
}

// Response is the range's answer as etcd's message, with the revision in
// its header. The KVs must be the whole answer.
func (res *RangeResult) Response() *pb.RangeResponse {
	resp := &pb.RangeResponse{Header: header(res.Rev), Count: res.Count, More: res.More}
	resp.Kvs = KeyValues(res.KVs)
	return resp
}

// KeyValues returns kvs as etcd's messages.
func KeyValues(kvs []KV) []*mvccpb.KeyValue {
	if len(kvs) == 0 {
		return nil
	}
	msgs := make([]mvccpb.KeyValue, len(kvs))
	out := make([]*mvccpb.KeyValue, len(kvs))
	for i, kv := range kvs {
		msgs[i] = mvccpb.KeyValue{
			Key: kv.Key, Value: kv.Value, CreateRevision: kv.CreateRevision,
			ModRevision: kv.ModRevision, Version: kv.Version, Lease: kv.Lease,
		}
		out[i] = &msgs[i]
	}
	return out
}

// kvBytes is what a KV takes in a sorted list, as measured on a 64-bit
// platform (TestKVBytes).
const kvBytes = 104

// ErrTooLarge refuses a sorted Range whose list, which the sort needs at
// once, passes Store.SortBytes: a KV for each key, and each value for a
// sort by value. The client can ask for key order or a narrower range.
var ErrTooLarge = status.Error(codes.ResourceExhausted, "tigerstore: a sorted Range needs more keys at once than the node has room for; narrow the range or read it in key order")

// Cursor reads the rest of a Range answer, a piece at a time.
type Cursor struct {
	s *Store
	r *pb.RangeRequest
	// pos is the position read at: versions before it are visible.
	pos rev
	// next is the key the next piece of a walk starts at, and left how
	// many keys the answer still hands out. list holds the rest of a
	// sorted answer instead.
	next []byte
	left int64
	list []KV
}

// at is the position a read at revision r sees: every version of r.
func at(r int64) rev { return rev{main: r + 1} }

// visible returns the newest version of k before position p, if the key
// exists there.
func (k *keyIndex) visible(p rev) (*version, bool) {
	i := k.at(p.main)
	for i >= 0 && k.vs()[i].mod.main == p.main && k.vs()[i].mod.sub >= p.sub {
		i--
	}
	if i < 0 || k.vs()[i].ver == 0 {
		return nil, false
	}
	return &k.vs()[i], true
}

// position is where a range of the request being applied reads: at
// r.Revision, or after the request's own writes so far.
func (w *writer) position(r *pb.RangeRequest) (rev, error) {
	cur := w.cur()
	switch {
	case r.Revision > cur:
		return rev{}, rpctypes.ErrGRPCFutureRev
	case r.Revision > 0 && r.Revision < w.s.compactRev:
		return rev{}, rpctypes.ErrGRPCCompacted
	case r.Revision > 0:
		return at(r.Revision), nil
	case !w.changed:
		return at(cur), nil
	}
	return rev{main: w.begin + 1, sub: w.subs}, nil
}

// compacted reports whether a compaction removed versions a read at p may
// need: one past its revision, or at the revision of a read that sees
// only part of it.
func (s *Store) compacted(p rev) bool {
	read := p.main - 1
	if p.sub > 0 {
		read = p.main
	}
	return read < s.compactRev || (p.sub > 0 && read == s.compactRev)
}

// pushLimit is the limit etcd pushes into its index walk: limit+1 with
// the default order and no revision filters, else 0. etcd decides the
// default order before it turns a NONE order with a non-key target into
// ASCEND.
func pushLimit(r *pb.RangeRequest) int64 {
	if r.Limit > 0 && r.Limit < math.MaxInt64 && IsDefaultOrdering(r.SortTarget, r.SortOrder) && !hasRevisionFilters(r) {
		return r.Limit + 1
	}
	return 0
}

// Range answers r whole, as etcd's message: for reads whose answer is
// small, such as a node's own, and for tests.
func (s *Store) Range(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	s.counts.Ranges++
	w := s.reader()
	w.room = math.MaxInt
	return w.rangeMessage(r, true)
}

// Rest is the part of a message's KeyValues that comes in later pieces:
// Msg, a *pb.RangeResponse or a *pb.DeleteRangeResponse in a Txn's or a
// DeleteRange's answer, holds the first piece, N and Fields count and
// size every KeyValue it has in the whole answer, and Cursor reads the
// rest.
type Rest struct {
	Msg       proto.Message
	N, Fields int64
	Cursor    *Cursor
}

// TakeRests returns the Rests of the answer of the last Txn or
// DeleteRange, in the order its message holds them, and forgets them.
// Their KeyValues share the room of PieceBytes; past it, a message gets
// the first keys that fit, or none.
func (s *Store) TakeRests() []Rest {
	r := s.rests
	s.rests = nil
	return r
}

// rangeMessage answers r as etcd's message, with as many KeyValues as
// the room of the request's answer holds, and a Rest for the others.
// Values only the log has come as fills into the message.
func (w *writer) rangeMessage(r *pb.RangeRequest, total bool) (*pb.RangeResponse, error) {
	res, c, err := w.rangeAt(r, total, w.room, false)
	if err != nil {
		return nil, err
	}
	resp := res.Response()
	w.took(resp, &res, c)
	return resp, w.s.fillValues(res.KVs, resp.Kvs)
}

// took charges the first piece of res to the room of the answer, and
// keeps a Rest for msg if c reads more.
func (w *writer) took(msg proto.Message, res *RangeResult, c *Cursor) {
	for i := range res.KVs {
		// The values are not filled yet: they count by their length.
		kv := &res.KVs[i]
		w.room -= 1 + protowire.SizeBytes(kv.Size(int(kv.ref.Len)))
	}
	if c != nil {
		w.s.rests = append(w.s.rests, Rest{Msg: msg, N: res.N, Fields: res.Fields, Cursor: c})
	}
}

// RangeFirst reads the first piece of r, of at most room bytes of
// KeyValue fields but at least one key, with the count and size of the
// whole answer. The Cursor reads the rest, nil if the piece holds it all.
// Without total, a walk with a pushed-down limit stops at it, and Count
// holds only the keys it saw, as one chunk of etcd's RangeStream does.
// Values only the log has come as fills (TakeFills).
func (s *Store) RangeFirst(r *pb.RangeRequest, total bool, room int) (RangeResult, *Cursor, error) {
	s.counts.Ranges++
	return s.reader().rangeAt(r, total, room, true)
}

// rangeAt is RangeFirst for a range of the request being applied. Without
// fill, the KVs keep their values to read, for fillValues.
func (w *writer) rangeAt(r *pb.RangeRequest, total bool, room int, fill bool) (RangeResult, *Cursor, error) {
	s := w.s
	p, err := w.position(r)
	if err != nil {
		return RangeResult{}, nil, err
	}
	c := &Cursor{s: s, r: r, pos: p, left: math.MaxInt64}
	if r.Limit > 0 {
		c.left = r.Limit
	}
	res := RangeResult{Rev: w.cur()}
	if !IsDefaultOrdering(r.SortTarget, sortOrder(r)) {
		return c.sorted(res, total, room, fill)
	}
	// A walk in key order: it collects the first piece, then, if the
	// answer goes on, counts and sizes the rest without reading values.
	push := int64(0)
	if !total {
		push = pushLimit(r)
	}
	var kvs []KV
	size := 0
	full := false
	s.ascend(r.Key, r.RangeEnd, func(k *keyIndex) bool {
		v, ok := k.visible(p)
		if !ok {
			return true
		}
		if push > 0 && !r.CountOnly && res.Count >= push {
			return false
		}
		res.Count++
		if r.CountOnly {
			return true
		}
		kv := s.kvValue(k, v)
		if kept := keeps(r, &kv); !kept || res.N == c.left {
			res.More = res.More || kept
			return true
		}
		if r.KeysOnly {
			kv.ref, kv.Lease = Ref{}, 0
		}
		f := int64(1 + protowire.SizeBytes(kv.Size(int(kv.ref.Len))))
		res.N++
		res.Fields += f
		if full {
			return true
		}
		if (len(kvs) > 0 || room <= 0) && size+int(f) > room {
			full, c.next = true, kv.Key
			return true
		}
		size += int(f)
		kvs = append(kvs, kv)
		return true
	})
	if fill {
		if err := s.fillValues(kvs, nil); err != nil {
			return RangeResult{}, nil, err
		}
	}
	res.KVs = kvs
	if !full {
		return res, nil, nil
	}
	c.left = res.N - int64(len(kvs))
	return res, c, nil
}

// keeps reports whether kv passes r's revision filters.
func keeps(r *pb.RangeRequest, kv *KV) bool {
	switch {
	case r.MaxModRevision != 0 && kv.ModRevision > r.MaxModRevision:
	case r.MinModRevision != 0 && kv.ModRevision < r.MinModRevision:
	case r.MaxCreateRevision != 0 && kv.CreateRevision > r.MaxCreateRevision:
	case r.MinCreateRevision != 0 && kv.CreateRevision < r.MinCreateRevision:
	default:
		return true
	}
	return false
}

// sorted reads an answer in another order than the keys': every key goes
// into a list, which is sorted, and the pieces come from it. A sort by
// value reads every value at once. The list stays until the last piece,
// so a Range (fill) is refused once it, with the values a sort by value
// reads, passes SortBytes. A range of a write being applied never is:
// the limit is the node's own, and every node must apply the write alike. As etcd does, an order of NONE with a target other than the
// key sorts only the first limit+1 keys.
func (c *Cursor) sorted(res RangeResult, total bool, room int, fill bool) (RangeResult, *Cursor, error) {
	r, s := c.r, c.s
	byValue := r.SortTarget == pb.RangeRequest_VALUE
	push := pushLimit(r)
	var kvs []KV
	held := int64(0)
	tooLarge := false
	s.ascend(r.Key, r.RangeEnd, func(k *keyIndex) bool {
		v, ok := k.visible(c.pos)
		if !ok {
			return true
		}
		pushed := push > 0 && int64(len(kvs)) >= push
		if pushed && !total {
			return false
		}
		res.Count++
		kv := s.kvValue(k, v)
		if pushed || r.CountOnly || !keeps(r, &kv) {
			return true
		}
		held += kvBytes
		if byValue {
			held += int64(kv.ref.Len)
		}
		if fill && s.SortBytes > 0 && held > s.SortBytes {
			tooLarge = true
			return false
		}
		kvs = append(kvs, kv)
		return true
	})
	if tooLarge {
		return RangeResult{}, nil, ErrTooLarge
	}
	if byValue {
		if err := s.readNow(kvs); err != nil {
			return RangeResult{}, nil, err
		}
	}
	sortKVs(kvs, r)
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs, res.More = kvs[:r.Limit], true
	}
	for i := range kvs {
		kv := &kvs[i]
		if r.KeysOnly {
			// Without a sort on values, etcd reads only the key index,
			// which has no lease.
			kv.Value, kv.ref = nil, Ref{}
			if !byValue {
				kv.Lease = 0
			}
		}
		res.Fields += int64(1 + protowire.SizeBytes(kv.Size(int(kv.ref.Len))))
	}
	res.N = int64(len(kvs))
	c.list = kvs
	piece, done, err := c.take(room, byValue || !fill)
	if err != nil {
		return RangeResult{}, nil, err
	}
	res.KVs = piece
	if done {
		return res, nil, nil
	}
	return res, c, nil
}

// take hands out the next piece of a sorted answer. Values a sort by
// value read are in place already.
func (c *Cursor) take(room int, read bool) ([]KV, bool, error) {
	n, size := 0, 0
	for n < len(c.list) {
		f := 1 + protowire.SizeBytes(c.list[n].Size(int(c.list[n].ref.Len)))
		if (n > 0 || room <= 0) && size+f > room {
			break
		}
		size += f
		n++
	}
	piece := c.list[:n:n]
	c.list = c.list[n:]
	if !read {
		if err := c.s.fillValues(piece, nil); err != nil {
			return nil, false, err
		}
	}
	return piece, len(c.list) == 0, nil
}

// Next reads the next piece of the answer, of at most room bytes of
// KeyValue fields but at least one key, and reports whether it is the
// last. A compaction that passed the cursor's position fails it with
// ErrCompacted. Values only the log has come as fills (TakeFills).
func (c *Cursor) Next(room int) ([]KV, bool, error) {
	s, r := c.s, c.r
	if s.compacted(c.pos) {
		return nil, false, rpctypes.ErrGRPCCompacted
	}
	if c.list != nil {
		return c.take(room, r.SortTarget == pb.RangeRequest_VALUE)
	}
	var kvs []KV
	size := 0
	end := r.RangeEnd
	if len(end) == 0 {
		// A single key is one piece; a cursor never reads one.
		end = []byte{0}
	}
	stopped := false
	s.ascend(c.next, end, func(k *keyIndex) bool {
		if c.left == 0 {
			stopped = true
			return false
		}
		v, ok := k.visible(c.pos)
		if !ok {
			return true
		}
		kv := c.s.kvValue(k, v)
		if !keeps(r, &kv) {
			return true
		}
		if r.KeysOnly {
			kv.ref, kv.Lease = Ref{}, 0
		}
		f := 1 + protowire.SizeBytes(kv.Size(int(kv.ref.Len)))
		if len(kvs) > 0 && size+f > room {
			c.next, stopped = c.s.key(k), true
			return false
		}
		size += f
		kvs = append(kvs, kv)
		c.left--
		return true
	})
	if err := s.fillValues(kvs, nil); err != nil {
		return nil, false, err
	}
	// The walk sees what the first piece's walk saw, so it ends with the
	// last key the answer holds.
	return kvs, c.left == 0 || !stopped, nil
}

// fillValues sets the value of each KV that is in memory, or of its
// message in msgs if that is not nil, and keeps a Fill for each that only
// the log has. The values copied out of Raft entries share one allocation
// of their exact size; a value from the cache stays its own (#430).
func (s *Store) fillValues(kvs []KV, msgs []*mvccpb.KeyValue) error {
	copied := 0
	for i := range kvs {
		if r := kvs[i].ref; r.Len > 0 && s.values.Copies(r) {
			copied += int(r.Len)
		}
	}
	var arena *[]byte
	if copied > 0 && len(kvs) > 1 {
		b := make([]byte, 0, copied)
		arena = &b
	}
	for i := range kvs {
		dst := &kvs[i].Value
		if msgs != nil {
			dst = &msgs[i].Value
		}
		if err := s.fill(kvs[i].Key, kvs[i].ref, dst, arena); err != nil {
			return err
		}
	}
	return nil
}

// readNow reads the value of every KV at once, for a sort on them. If
// some are only on disk, it returns a ColdError that needs them all.
func (s *Store) readNow(kvs []KV) error {
	var cold []Fill
	for i := range kvs {
		kv := &kvs[i]
		b, ok, err := s.value(kv.Key, kv.ref, nil)
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

// kvSort sorts with sort.Sort like etcd, so that ties end in the same order.
type kvSort struct {
	kvs  []KV
	less func(a, b *KV) bool
}

func (s kvSort) Len() int           { return len(s.kvs) }
func (s kvSort) Swap(i, j int)      { s.kvs[i], s.kvs[j] = s.kvs[j], s.kvs[i] }
func (s kvSort) Less(i, j int) bool { return s.less(&s.kvs[i], &s.kvs[j]) }

// sortOrder is the order r is sorted in: etcd turns NONE into ASCEND for
// a target other than the key.
func sortOrder(r *pb.RangeRequest) pb.RangeRequest_SortOrder {
	if r.SortTarget != pb.RangeRequest_KEY && r.SortOrder == pb.RangeRequest_NONE {
		return pb.RangeRequest_ASCEND
	}
	return r.SortOrder
}

func sortKVs(kvs []KV, r *pb.RangeRequest) {
	order := sortOrder(r)
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
