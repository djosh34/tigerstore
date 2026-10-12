// Package mvcc is the multi-version key index of one node.
//
// The committer owns a Store and is the only caller, so it takes no locks.
// Every write request gets one main revision; each change inside it gets a
// sub revision. Values are byte slices that the caller does not modify
// afterwards, usually slices of the Raft entry that carried the write.
package mvcc

import (
	"bytes"
	"hash"
	"hash/fnv"

	"github.com/google/btree"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// rev orders changes: main is the revision of the request, sub the position
// of the change inside it.
type rev struct{ main, sub int64 }

// version is one state of a key. ver is 0 for a tombstone.
type version struct {
	mod    rev
	create int64
	ver    int64
	lease  int64
	value  []byte
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

func (k *keyIndex) kv(v *version) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            k.key,
		CreateRevision: v.create,
		ModRevision:    v.mod.main,
		Version:        v.ver,
		Lease:          v.lease,
		Value:          v.value,
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
}

// New returns an empty store at revision 1, the revision etcd starts at.
func New() *Store {
	return &Store{
		keys: btree.NewG(32, func(a, b *keyIndex) bool { return bytes.Compare(a.key, b.key) < 0 }),
		rev:  1,
	}
}

// Rev is the current revision.
func (s *Store) Rev() int64 { return s.rev }

// CompactRev is the revision of the last compaction, 0 if none.
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
			writeInt(h, int64(len(v.value)))
			h.Write(v.value)
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
