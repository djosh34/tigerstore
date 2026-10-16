// Package mvcc is the multi-version key index of one node.
//
// The committer owns a Store and is the only caller, so it takes no locks.
// Every write request gets one main revision; each change inside it gets a
// sub revision. The index holds no values: it names each value by a Ref,
// its position in the Raft log, and reads the bytes through Values.
package mvcc

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/google/btree"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/planted"
)

// rev orders changes: main is the revision of the request, sub the position
// of the change inside it.
type rev struct{ main, sub int64 }

// Ref names a value by where the Raft log holds it, the same on every
// node: the index of the entry that wrote it, its offset and length in the
// entry's data, and the CRC-32C of its bytes. An empty value has Len 0.
type Ref struct {
	Index    uint64
	Off, Len uint32
	Sum      uint32
}

// Values keeps the bytes of a store's values.
type Values interface {
	// Place returns the Ref of value, which the request being applied
	// writes, without its Sum. value is not empty. Place may keep it as
	// it is.
	Place(value []byte) (Ref, error)
	// Read returns the bytes r names if they are in memory, and false if
	// only the log on disk has them. The caller may keep the bytes, as a
	// response or a watch event does, but must not change them. Bytes it
	// copies go to the end of *arena if arena is not nil, so that the
	// values of one answer take one allocation.
	Read(r Ref, arena *[]byte) ([]byte, bool, error)
}

// Fill is a value a response or a watch event waits for because only the
// log on disk has it: the bytes Ref names go to *Dst. The committer never
// reads the disk; a helper reads them and calls Set.
type Fill struct {
	Key []byte
	Ref Ref
	Dst *[]byte
}

// Set checks b, the bytes read for f, against the checksum and stores
// them in *Dst.
func (f Fill) Set(b []byte) error {
	if err := check(f.Key, f.Ref, b); err != nil {
		return err
	}
	*f.Dst = b
	return nil
}

// ColdError is the answer of an operation that needs values at once, to
// compare or sort them, that only the log on disk has. The store is
// unchanged. The operation runs again once the values in Need are read
// and Values.Read returns them; their Dst is nil.
type ColdError struct{ Need []Fill }

func (e *ColdError) Error() string {
	return fmt.Sprintf("mvcc: %d values must be read from disk first", len(e.Need))
}

// ErrBadValue means that the bytes read for a value do not match its
// checksum: the disk was damaged, or they were read from the wrong place.
var ErrBadValue = errors.New("mvcc: a value does not match its checksum")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Heap keeps values in memory, for stores whose writes come from no Raft
// entry, such as tests and the simulator's model of the log.
type Heap struct{ vals [][]byte }

// Place keeps value and names it by its number.
func (h *Heap) Place(value []byte) (Ref, error) {
	h.vals = append(h.vals, value)
	return Ref{Index: uint64(len(h.vals)), Len: uint32(len(value))}, nil
}

// Read returns a value Place kept.
func (h *Heap) Read(r Ref, _ *[]byte) ([]byte, bool, error) {
	if r.Index == 0 || r.Index > uint64(len(h.vals)) {
		return nil, false, fmt.Errorf("mvcc: no value %d in memory", r.Index)
	}
	return h.vals[r.Index-1], true, nil
}

// version is one state of a key. ver is 0 for a tombstone.
type version struct {
	mod    rev
	create int64
	ver    int64
	lease  int64
	value  Ref
}

// keyIndex holds every version of one key, oldest first.
type keyIndex struct {
	key  []byte
	vers []version
}

// at returns the position of the newest version with main revision <= r,
// or -1.
func (k *keyIndex) at(r int64) int {
	lo, hi := 0, len(k.vers)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if k.vers[m].mod.main <= r {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}

// live returns the version visible at revision r, if the key exists then.
func (k *keyIndex) live(r int64) (*version, bool) {
	i := k.at(r)
	if i < 0 || k.vers[i].ver == 0 {
		return nil, false
	}
	return &k.vers[i], true
}

// kvValue returns a version without its value, which fill adds.
func (k *keyIndex) kvValue(v *version) KV {
	return KV{Key: k.key, CreateRevision: v.create, ModRevision: v.mod.main, Version: v.ver, Lease: v.lease, ref: v.value}
}

// kv returns a version with its value, or with a Fill for it.
func (s *Store) kv(k *keyIndex, v *version) (*mvccpb.KeyValue, error) {
	kv := &mvccpb.KeyValue{
		Key:            k.key,
		CreateRevision: v.create,
		ModRevision:    v.mod.main,
		Version:        v.ver,
		Lease:          v.lease,
	}
	if err := s.fill(k.key, v.value, &kv.Value, nil); err != nil {
		return nil, err
	}
	return kv, nil
}

// fill stores the value r names in *dst if it is in memory. Otherwise the
// store keeps a Fill for it, which TakeFills hands out.
func (s *Store) fill(key []byte, r Ref, dst *[]byte, arena *[]byte) error {
	b, ok, err := s.value(key, r, arena)
	if err != nil {
		return err
	}
	if !ok {
		s.fills = append(s.fills, Fill{Key: key, Ref: r, Dst: dst})
		return nil
	}
	*dst = b
	return nil
}

// TakeFills returns the values that the responses and events built since
// the last call wait for, and forgets them. The caller owns the slice.
func (s *Store) TakeFills() []Fill {
	f := s.fills
	s.fills = nil
	return f
}

// value returns the bytes of a value of key k, checked against the
// checksum, if they are in memory, and false if only the disk has them.
// arena is as in Values.Read.
func (s *Store) value(key []byte, r Ref, arena *[]byte) ([]byte, bool, error) {
	if r.Len == 0 {
		return nil, true, nil
	}
	b, ok, err := s.values.Read(r, arena)
	if err != nil {
		return nil, false, fmt.Errorf("mvcc: read the value of %q: %w", key, err)
	}
	if !ok {
		return nil, false, nil
	}
	if err := check(key, r, b); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// check checks the bytes read for a value of key against its Ref.
func check(key []byte, r Ref, b []byte) error {
	if uint32(len(b)) != r.Len || (crc32.Checksum(b, castagnoli) != r.Sum && !planted.On("B25")) {
		return fmt.Errorf("%w: key %q, entry %d, offset %d", ErrBadValue, key, r.Index, r.Off)
	}
	return nil
}

// Store is the key index with every revision since the last compaction.
type Store struct {
	keys       *btree.BTreeG[*keyIndex]
	rev        int64
	compactRev int64
	values     Values

	// frozen is the checkpoint being copied, if any.
	frozen *frozen
	// sum is the XOR of the hashes of every version, kept only with
	// track.
	sum   uint64
	track bool

	// LeaseExists reports whether a lease is granted. Nil means that no
	// lease exists, so every Put with a lease fails.
	LeaseExists func(id int64) bool
	// Replaced, if set, gets the value of a key's newest version when a
	// put or a delete replaces that version and the new one does not keep
	// the value. Older revisions may still read it.
	Replaced func(Ref)

	// changes collects the changes of the request being applied.
	changes []Change
	// fills are the values that responses and events wait for.
	fills []Fill

	counts Counts
	// compactHash is the hash the last compaction took.
	compactHash KVHash
	// size is the bytes etcd's database would hold for the versions
	// (rowBytes), the size the space quota counts.
	size int64

	// versions and keyBytes count the room for versions that the keys'
	// arrays have and the key bytes in the index, for Bytes.
	versions, keyBytes int64

	// written holds the values that puts wrote since the last Leaving,
	// oldest first, with TrackWritten.
	written      []written
	trackWritten bool
}

// Bytes estimates what the index takes in memory: per key its record,
// its place in the B-tree and its bytes, and 64 bytes per version its
// array has room for.
func (s *Store) Bytes() int64 {
	return int64(s.keys.Len())*64 + s.keyBytes + s.versions*64
}

// written is a value a put wrote to key k.
type written struct {
	k   *keyIndex
	ref Ref
}

// TrackWritten makes the store keep the value of every put until
// Leaving hands it out.
func (s *Store) TrackWritten() { s.trackWritten = true }

// Leaving calls fn, oldest first, with each value written in an entry up
// to index that is still its key's newest value, and forgets every value
// written up to index.
func (s *Store) Leaving(index uint64, fn func(Ref)) {
	n := 0
	for ; n < len(s.written) && s.written[n].ref.Index <= index; n++ {
		if w := s.written[n]; w.k.current(w.ref) {
			fn(w.ref)
		}
	}
	clear(s.written[:n])
	s.written = s.written[n:]
}

// Current reports whether r names the value of key's newest version.
func (s *Store) Current(key []byte, r Ref) bool {
	k, ok := s.keys.Get(&keyIndex{key: key})
	return ok && k.current(r)
}

// current reports whether r names the value of k's newest version. A
// key that a compaction removed has no versions left.
func (k *keyIndex) current(r Ref) bool {
	if len(k.vers) == 0 {
		return false
	}
	v := &k.vers[len(k.vers)-1]
	return v.ver != 0 && v.value == r
}

// Counts are the operations a store has seen, for etcd's metrics
// etcd_mvcc_put_total, etcd_mvcc_delete_total and etcd_mvcc_range_total:
// every put, delete range and range the store runs, also inside
// transactions. Ranges also count the reads etcd makes inside other
// operations: one per compare, one per delete with prev_kv, and one per
// put with prev_kv, ignore_value or ignore_lease, two in a transaction.
// Txns counts the requests with more than one of those, as etcd's
// etcd_mvcc_txn_total does, PutBytes the key and value bytes of the puts
// (etcd_debugging_mvcc_total_put_size_in_bytes) and Compacted the
// versions compactions dropped (etcd_debugging_mvcc_db_compaction_keys_total).
// Keys is the number of keys whose newest version is not a delete
// (etcd_debugging_mvcc_keys_total).
type Counts struct{ Puts, Deletes, Ranges, Txns, PutBytes, Compacted, Keys int64 }

// New returns an empty store at revision 1, the revision etcd starts at,
// that keeps its values in v.
func New(v Values) *Store {
	return &Store{
		keys:   btree.NewG(32, func(a, b *keyIndex) bool { return bytes.Compare(a.key, b.key) < 0 }),
		values: v,
		rev:    1,
		// etcd's compacted revision starts at -1, so a read at revision
		// -1 inside a transaction is not "compacted".
		compactRev: -1,
	}
}

// Counts returns the operations the store has seen.
func (s *Store) Counts() Counts { return s.counts }

// Size is about the bytes etcd v3.7.2's database would hold for the
// versions in the index: per version, the row etcd writes, a revision key
// and the marshaled KeyValue. The versions a compaction drops no longer
// count, even while a checkpoint copy keeps them. etcd's file also holds
// free pages and its other buckets.
func (s *Store) Size() int64 { return s.size }

// rowBytes is the size of etcd's row for a version: its 17-byte revision
// key, one more for a tombstone's mark, and the KeyValue, which for a
// tombstone holds only the key.
func rowBytes(key []byte, v *version) int64 {
	n := 17 + protowire.SizeTag(1) + protowire.SizeBytes(len(key))
	if v.ver == 0 {
		return int64(n + 1)
	}
	n += 3*protowire.SizeTag(2) + protowire.SizeVarint(uint64(v.create)) + protowire.SizeVarint(uint64(v.mod.main)) + protowire.SizeVarint(uint64(v.ver))
	if v.value.Len > 0 {
		n += protowire.SizeTag(5) + protowire.SizeBytes(int(v.value.Len))
	}
	if v.lease != 0 {
		n += protowire.SizeTag(6) + protowire.SizeVarint(uint64(v.lease))
	}
	return int64(n)
}

// Rev is the current revision.
func (s *Store) Rev() int64 { return s.rev }

// CompactRev is the revision of the last compaction, -1 if none.
func (s *Store) CompactRev() int64 { return s.compactRev }

// ascend calls fn for every key in [key, end) in order until fn returns
// false. An empty end means only key; end "\x00" means every key >= key.
func (s *Store) ascend(key, end []byte, fn func(*keyIndex) bool) {
	switch {
	case len(end) == 0:
		if k, ok := s.keys.Get(&keyIndex{key: key}); ok {
			fn(k)
		}
	case len(end) == 1 && end[0] == 0:
		s.keys.AscendGreaterOrEqual(&keyIndex{key: key}, fn)
	default:
		s.keys.AscendRange(&keyIndex{key: key}, &keyIndex{key: end}, fn)
	}
}

// Change is one key change made by a write request.
type Change struct {
	s *Store
	k *keyIndex
	i int
}

// Key is the changed key.
func (c Change) Key() []byte { return c.k.key }

// ModRev is the main revision of the change.
func (c Change) ModRev() int64 { return c.k.vers[c.i].mod.main }

// Lease is the lease of the key after the change, 0 for a delete.
func (c Change) Lease() int64 { return c.k.vers[c.i].lease }

// PrevLease is the lease of the key before the change, 0 if it did not
// exist or had none.
func (c Change) PrevLease() int64 {
	if c.i > 0 && c.k.vers[c.i-1].ver != 0 {
		return c.k.vers[c.i-1].lease
	}
	return 0
}

// Event builds the watch event of the change. The previous value is set
// when withPrev is true and the key existed before. Values that only the
// disk has come as Fills, from TakeFills; the event holds their positions,
// so a later compaction does not change it.
func (c Change) Event(withPrev bool) (*mvccpb.Event, error) {
	s := c.s
	v := &c.k.vers[c.i]
	ev := &mvccpb.Event{Type: mvccpb.Event_DELETE, Kv: &mvccpb.KeyValue{Key: c.k.key, ModRevision: v.mod.main}}
	if v.ver != 0 {
		kv, err := s.kv(c.k, v)
		if err != nil {
			return nil, err
		}
		ev = &mvccpb.Event{Type: mvccpb.Event_PUT, Kv: kv}
	}
	if withPrev && c.i > 0 && c.k.vers[c.i-1].ver != 0 {
		prev, err := s.kv(c.k, &c.k.vers[c.i-1])
		if err != nil {
			return nil, err
		}
		ev.PrevKv = prev
	}
	return ev, nil
}

// Changes returns the changes of the last applied write request. The slice
// is reused by the next write.
func (s *Store) Changes() []Change { return s.changes }

// Refs returns the positions of the values of every version of a key.
// The simulator uses them to damage a value on disk.
func (s *Store) Refs(key []byte) []Ref {
	k, ok := s.keys.Get(&keyIndex{key: key})
	if !ok {
		return nil
	}
	var out []Ref
	for _, v := range k.vers {
		if v.value.Len > 0 {
			out = append(out, v.value)
		}
	}
	return out
}

// Hash returns a hash of every version of every key, with the Ref of its
// value, the current revision and the compaction revision. Replicas that
// applied the same log have the same hash. With TrackHash it costs
// nothing; without, it walks every key.
func (s *Store) Hash() uint64 {
	sum := s.sum
	if !s.track {
		sum = s.versionSum()
	}
	return mix(sum ^ mix(uint64(s.rev)) ^ mix(mix(uint64(s.compactRev))))
}

// versionSum is the XOR of the hashes of every version, leaving out those
// that a compaction waiting for a checkpoint copy drops.
func (s *Store) versionSum() uint64 {
	var pending int64
	if s.frozen != nil {
		pending = s.frozen.compact
	}
	var sum uint64
	s.keys.Ascend(func(k *keyIndex) bool {
		for i := k.dropped(pending); i < len(k.vers); i++ {
			sum ^= versionHash(k.key, &k.vers[i])
		}
		return true
	})
	return sum
}

// TrackHash keeps the hash up to date as versions come and go, so that
// Hash takes no walk. The simulator checks it after every entry.
func (s *Store) TrackHash() {
	s.sum, s.track = s.versionSum(), true
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

// versionHash hashes one version of a key, FNV-1a over its fields and
// then mixed, so that the XOR of many stays well spread.
func versionHash(key []byte, v *version) uint64 {
	h := uint64(fnvOffset)
	for _, b := range key {
		h = (h ^ uint64(b)) * fnvPrime
	}
	fields := [...]uint64{
		uint64(len(key)), uint64(v.mod.main), uint64(v.mod.sub), uint64(v.create), uint64(v.ver),
		uint64(v.lease), v.value.Index, uint64(v.value.Off)<<32 | uint64(v.value.Len), uint64(v.value.Sum),
	}
	for _, x := range fields {
		for range 8 {
			h = (h ^ x&0xff) * fnvPrime
			x >>= 8
		}
	}
	return mix(h)
}

// mix is the finalizer of splitmix64.
func mix(x uint64) uint64 {
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

// History returns every change to keys in [key, end) at main revision
// from or later, in revision order. end follows Range: empty means only
// key, "\x00" means every key from key on.
func (s *Store) History(key, end []byte, from int64) []Change {
	var out []Change
	s.ascend(key, end, func(k *keyIndex) bool {
		i := k.at(from - 1)
		for i++; i < len(k.vers); i++ {
			out = append(out, Change{s: s, k: k, i: i})
		}
		return true
	})
	slices.SortFunc(out, func(a, b Change) int {
		x, y := a.k.vers[a.i].mod, b.k.vers[b.i].mod
		if x.main != y.main {
			return cmp.Compare(x.main, y.main)
		}
		return cmp.Compare(x.sub, y.sub)
	})
	return out
}

// InRange reports whether key is in [start, end) with Range's meaning of
// end.
func InRange(key, start, end []byte) bool {
	switch {
	case len(end) == 0:
		return bytes.Equal(key, start)
	case len(end) == 1 && end[0] == 0:
		return bytes.Compare(key, start) >= 0
	}
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}

// Compact drops every version that no read at rev or later can see, as
// etcd's compaction does: per key, the newest version at or below rev
// stays, unless it is a tombstone from before rev. A tombstone at rev
// stays, so a watch from rev still gets that delete. Reads below rev then
// fail with ErrCompacted. Compact does not change the revision. In the
// same walk it takes the hash of rev (CompactHash).
func (s *Store) Compact(rev int64) error {
	switch {
	case rev <= s.compactRev:
		return rpctypes.ErrGRPCCompacted
	case rev > s.rev:
		return rpctypes.ErrGRPCFutureRev
	}
	s.changes = s.changes[:0]
	pending := s.pendingCompact()
	h := newRevHash(rev, s.compactRev)
	s.compactRev = rev
	if s.frozen != nil {
		// A checkpoint copy is under way and reads the versions in place;
		// they go once it is done, but no longer count.
		s.frozen.compact = rev
	}
	var gone []*keyIndex
	s.keys.Ascend(func(k *keyIndex) bool {
		from, to := k.dropped(pending), k.dropped(rev)
		h.key(k, from)
		s.counts.Compacted += int64(to - from)
		for i := from; i < to; i++ {
			s.size -= rowBytes(k.key, &k.vers[i])
			if s.track {
				s.sum ^= versionHash(k.key, &k.vers[i])
			}
		}
		if s.frozen == nil && s.dropFirst(k, to) {
			gone = append(gone, k)
		}
		return true
	})
	s.remove(gone)
	s.compactHash = h.sum()
	return nil
}

// dropped returns how many of k's oldest versions a compaction at rev
// drops: per key, the newest version at or below rev stays, unless it is
// a tombstone from before rev.
func (k *keyIndex) dropped(rev int64) int {
	i := k.at(rev)
	if i >= 0 && k.vers[i].ver == 0 && (k.vers[i].mod.main < rev || planted.On("B08")) {
		i++
	}
	return max(i, 0)
}

// drop removes the versions that a compaction at rev drops, and the keys
// left without any.
func (s *Store) drop(rev int64) {
	var gone []*keyIndex
	s.keys.Ascend(func(k *keyIndex) bool {
		if s.dropFirst(k, k.dropped(rev)) {
			gone = append(gone, k)
		}
		return true
	})
	s.remove(gone)
}

// dropFirst removes k's i oldest versions and reports whether none is
// left.
func (s *Store) dropFirst(k *keyIndex, i int) bool {
	if i > 0 {
		c := cap(k.vers)
		k.vers = slices.Delete(k.vers, 0, i)
		// The array stays; a key that lost most of its versions gets a
		// smaller one, so that the memory goes.
		if len(k.vers) <= c/4 {
			k.vers = slices.Clone(k.vers)
		}
		s.versions -= int64(c - cap(k.vers))
	}
	return len(k.vers) == 0
}

// remove takes keys without versions out of the index.
func (s *Store) remove(gone []*keyIndex) {
	for _, k := range gone {
		s.keys.Delete(k)
		s.keyBytes -= int64(len(k.key))
	}
}
