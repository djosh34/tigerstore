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
	// Read returns the bytes r names. The caller must not change them.
	Read(r Ref) ([]byte, error)
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
func (h *Heap) Read(r Ref) ([]byte, error) {
	if r.Index == 0 || r.Index > uint64(len(h.vals)) {
		return nil, fmt.Errorf("mvcc: no value %d in memory", r.Index)
	}
	return h.vals[r.Index-1], nil
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

// kvValue returns a version without its value, which readValues adds.
func (k *keyIndex) kvValue(v *version) KV {
	return KV{Key: k.key, CreateRevision: v.create, ModRevision: v.mod.main, Version: v.ver, Lease: v.lease, ref: v.value}
}

func (s *Store) kv(k *keyIndex, v *version) (*mvccpb.KeyValue, error) {
	val, err := s.value(k.key, v.value)
	if err != nil {
		return nil, err
	}
	return &mvccpb.KeyValue{
		Key:            k.key,
		CreateRevision: v.create,
		ModRevision:    v.mod.main,
		Version:        v.ver,
		Lease:          v.lease,
		Value:          val,
	}, nil
}

// value reads the bytes of a value of key k and checks them against the
// checksum.
func (s *Store) value(key []byte, r Ref) ([]byte, error) {
	if r.Len == 0 {
		return nil, nil
	}
	b, err := s.values.Read(r)
	if err != nil {
		return nil, fmt.Errorf("mvcc: read the value of %q: %w", key, err)
	}
	if uint32(len(b)) != r.Len || crc32.Checksum(b, castagnoli) != r.Sum {
		return nil, fmt.Errorf("%w: key %q, entry %d, offset %d", ErrBadValue, key, r.Index, r.Off)
	}
	return b, nil
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

	// changes collects the changes of the request being applied.
	changes []Change

	counts Counts
}

// Counts are the operations a store has seen, for etcd's metrics
// etcd_mvcc_put_total, etcd_mvcc_delete_total and etcd_mvcc_range_total:
// every put, delete range and range the store runs, also inside
// transactions. Ranges also count the reads etcd makes inside other
// operations: one per compare, one per delete with prev_kv, and one per
// put with prev_kv, ignore_value or ignore_lease, two in a transaction.
type Counts struct{ Puts, Deletes, Ranges int64 }

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
// when withPrev is true and the key existed before. Its values are copies:
// a watcher can hold an event long after the entries that wrote them left
// memory.
func (c Change) Event(withPrev bool) (*mvccpb.Event, error) {
	s := c.s
	v := &c.k.vers[c.i]
	ev := &mvccpb.Event{Type: mvccpb.Event_DELETE, Kv: &mvccpb.KeyValue{Key: c.k.key, ModRevision: v.mod.main}}
	if v.ver != 0 {
		kv, err := s.kv(c.k, v)
		if err != nil {
			return nil, err
		}
		kv.Value = bytes.Clone(kv.Value)
		ev = &mvccpb.Event{Type: mvccpb.Event_PUT, Kv: kv}
	}
	if withPrev && c.i > 0 && c.k.vers[c.i-1].ver != 0 {
		prev, err := s.kv(c.k, &c.k.vers[c.i-1])
		if err != nil {
			return nil, err
		}
		prev.Value = bytes.Clone(prev.Value)
		ev.PrevKv = prev
	}
	return ev, nil
}

// Changes returns the changes of the last applied write request. The slice
// is reused by the next write.
func (s *Store) Changes() []Change { return s.changes }

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
	for _, x := range [...]uint64{uint64(len(key)), uint64(v.mod.main), uint64(v.mod.sub), uint64(v.create), uint64(v.ver),
		uint64(v.lease), v.value.Index, uint64(v.value.Off)<<32 | uint64(v.value.Len), uint64(v.value.Sum)} {
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
// fail with ErrCompacted. Compact does not change the revision.
func (s *Store) Compact(rev int64) error {
	switch {
	case rev <= s.compactRev:
		return rpctypes.ErrGRPCCompacted
	case rev > s.rev:
		return rpctypes.ErrGRPCFutureRev
	}
	s.changes = s.changes[:0]
	if s.track {
		var pending int64
		if s.frozen != nil {
			pending = s.frozen.compact
		}
		s.keys.Ascend(func(k *keyIndex) bool {
			for i := k.dropped(pending); i < k.dropped(rev); i++ {
				s.sum ^= versionHash(k.key, &k.vers[i])
			}
			return true
		})
	}
	s.compactRev = rev
	if s.frozen != nil {
		// A checkpoint copy is under way and reads the versions in place;
		// they go once it is done.
		s.frozen.compact = rev
		return nil
	}
	s.drop(rev)
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
		if i := k.dropped(rev); i > 0 {
			k.vers = slices.Delete(k.vers, 0, i)
		}
		if len(k.vers) == 0 {
			gone = append(gone, k)
		}
		return true
	})
	for _, k := range gone {
		s.keys.Delete(k)
	}
}
