// Package mvcc is the multi-version key index of one node.
//
// The committer owns a Store and is the only caller, so it takes no locks.
// Every write request gets one main revision; each change inside it gets a
// sub revision. Values are byte slices that the caller does not modify
// afterwards, usually slices of the Raft entry that carried the write.
package mvcc

import (
	"bytes"
	"cmp"
	"hash"
	"hash/fnv"
	"slices"
	"unsafe"

	"github.com/google/btree"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/planted"
)

// rev orders changes: main is the revision of the request, sub the position
// of the change inside it.
type rev struct{ main, sub int64 }

// Ref names a value by its Raft position: the entry index, the offset of
// the value in the entry's data and its length. It is the same on every
// node. Prototype E (#133).
type Ref struct {
	Idx    uint64
	Off, N uint32
}

// Values resolves value references. The core sets it: Hot returns the
// bytes if the entry is still in the RAM window, Read returns them from
// anywhere, from disk if need be.
var Values interface {
	Hot(r Ref) ([]byte, bool)
	Read(r Ref) []byte
}

// version is one state of a key. ver is 0 for a tombstone.
type version struct {
	mod    rev
	create int64
	ver    int64
	lease  int64
	value  Ref
}

func readValue(r Ref) []byte {
	if r.N == 0 {
		return nil
	}
	return Values.Read(r)
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

func (k *keyIndex) kvValue(v *version) KV {
	kv := KV{Key: k.key, CreateRevision: v.create, ModRevision: v.mod.main, Version: v.ver, Lease: v.lease, Ref: v.value}
	if v.value.N > 0 {
		if b, ok := Values.Hot(v.value); ok {
			kv.Value = b
		}
	}
	return kv
}

func (k *keyIndex) kv(v *version) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            k.key,
		CreateRevision: v.create,
		ModRevision:    v.mod.main,
		Version:        v.ver,
		Lease:          v.lease,
		Value:          readValue(v.value),
	}
}

// Store is the key index with every revision since the last compaction.
type Store struct {
	keys       *btree.BTreeG[*keyIndex]
	rev        int64
	compactRev int64

	// LeaseExists reports whether a lease is granted. Nil means that no
	// lease exists, so every Put with a lease fails.
	LeaseExists func(id int64) bool

	// changes collects the changes of the request being applied.
	changes []Change

	// entIdx and ent are the Raft entry being applied: values are named
	// by their position in it.
	entIdx uint64
	ent    []byte

	// A checkpoint in progress: versions above freezeRev are left out,
	// and physical compaction waits until it is done.
	freezing       bool
	freezeRev      int64
	cursor         []byte
	cursorStarted  bool
	pendingCompact int64

	counts Counts
}

// Counts are the operations a store has seen, for etcd's metrics
// etcd_mvcc_put_total, etcd_mvcc_delete_total and etcd_mvcc_range_total:
// every put, delete range and range the store runs, also inside
// transactions. Ranges also count the reads etcd makes inside other
// operations: one per compare, one per delete with prev_kv, and one per
// put with prev_kv, ignore_value or ignore_lease, two in a transaction.
type Counts struct{ Puts, Deletes, Ranges int64 }

// New returns an empty store at revision 1, the revision etcd starts at.
func New() *Store {
	return &Store{
		keys: btree.NewG(32, func(a, b *keyIndex) bool { return bytes.Compare(a.key, b.key) < 0 }),
		rev:  1,
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
// when withPrev is true and the key existed before.
func (c Change) Event(withPrev bool) *mvccpb.Event {
	v := &c.k.vers[c.i]
	ev := &mvccpb.Event{Type: mvccpb.Event_PUT, Kv: c.k.kv(v)}
	if v.ver == 0 {
		ev.Type = mvccpb.Event_DELETE
		ev.Kv = &mvccpb.KeyValue{Key: c.k.key, ModRevision: v.mod.main}
	}
	if withPrev && c.i > 0 && c.k.vers[c.i-1].ver != 0 {
		ev.PrevKv = c.k.kv(&c.k.vers[c.i-1])
	}
	return ev
}

// Changes returns the changes of the last applied write request. The slice
// is reused by the next write.
func (s *Store) Changes() []Change { return s.changes }

// Hash returns a hash of every version of every key, the current revision
// and the compaction revision. Replicas that applied the same log have the
// same hash.
func (s *Store) Hash() uint64 {
	h := fnv.New64a()
	writeInt(h, s.rev)
	writeInt(h, s.compactRev)
	s.keys.Ascend(func(k *keyIndex) bool {
		writeInt(h, int64(len(k.key)))
		h.Write(k.key)
		for _, v := range k.vers {
			writeInt(h, v.mod.main)
			writeInt(h, v.mod.sub)
			writeInt(h, v.create)
			writeInt(h, v.ver)
			writeInt(h, v.lease)
			writeInt(h, int64(v.value.Idx))
			writeInt(h, int64(v.value.Off)<<32|int64(v.value.N))
		}
		return true
	})
	return h.Sum64()
}

func writeInt(h hash.Hash64, v int64) {
	var b [8]byte
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	h.Write(b[:])
}

// History returns every change to keys in [key, end) at main revision
// from or later, in revision order. end follows Range: empty means only
// key, "\x00" means every key from key on.
func (s *Store) History(key, end []byte, from int64) []Change {
	var out []Change
	s.ascend(key, end, func(k *keyIndex) bool {
		i := k.at(from - 1)
		for i++; i < len(k.vers); i++ {
			out = append(out, Change{k: k, i: i})
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
	if s.freezing {
		s.pendingCompact = rev
		s.compactRev = rev
		return nil
	}
	s.physicalCompact(rev)
	s.compactRev = rev
	return nil
}

func (s *Store) physicalCompact(rev int64) {
	s.changes = s.changes[:0]
	var gone []*keyIndex
	s.keys.Ascend(func(k *keyIndex) bool {
		i := k.at(rev)
		if i >= 0 && k.vers[i].ver == 0 && (k.vers[i].mod.main < rev || planted.On("B08")) {
			i++
		}
		if i > 0 {
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

// SetEntry names the Raft entry the next writes come from.
func (s *Store) SetEntry(idx uint64, data []byte) { s.entIdx, s.ent = idx, data }

// refOf returns the Raft position of b, a slice of the current entry.
func (s *Store) refOf(b []byte) Ref {
	if len(b) == 0 {
		return Ref{}
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(s.ent)))
	p := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	if p < base || p+uintptr(len(b)) > base+uintptr(len(s.ent)) {
		panic("mvcc: value is not a slice of the entry being applied")
	}
	return Ref{Idx: s.entIdx, Off: uint32(p - base), N: uint32(len(b))}
}
