package mvcc

import (
	"encoding/binary"
	"hash/crc32"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// KVHash is the answer to etcd's Maintenance.HashKV: a CRC-32C of every
// version up to Revision, with the compaction revision it was taken at.
type KVHash struct {
	Hash            uint32
	CompactRevision int64
	Revision        int64
}

// HashKV hashes the versions up to rev, or the current revision if rev is
// 0, with etcd v3.7.2's rules (server/storage/mvcc/hash.go) for which
// versions count: one above rev never does; one at or below the
// compaction revision C counts only if it is the version a read at rev
// sees, unless no key exists at rev; a tombstone at C never does. So
// replicas agree on the hash of a revision only at the same C, as etcd's
// corruption check compares them. A rev below C is ErrCompacted, one
// above the current revision ErrFutureRev.
//
// etcd hashes its database rows in revision order, each a revision key and
// the marshaled KeyValue. This walks the index in key order instead, and
// hashes each version's checksum instead of its value, so it reads nothing
// from disk: the hash tells replicas apart as etcd's does, but its number
// differs from etcd's for the same data.
func (s *Store) HashKV(rev int64) (KVHash, error) {
	c := s.compactRev
	switch {
	case rev > 0 && rev < c:
		return KVHash{}, rpctypes.ErrGRPCCompacted
	case rev > s.rev:
		return KVHash{}, rpctypes.ErrGRPCFutureRev
	case rev == 0:
		rev = s.rev
	}
	h := newRevHash(rev, c)
	pending := s.pendingCompact()
	s.keys.Ascend(func(k *keyIndex) bool {
		h.key(k, k.dropped(pending))
		return true
	})
	return h.sum(), nil
}

// revHash sums the versions up to rev with HashKV's rules, a key at a
// time, with c as the compaction revision.
type revHash struct {
	rev, c     int64
	live, dead *kvHasher
	anyLive    bool
}

func newRevHash(rev, c int64) *revHash {
	return &revHash{rev: rev, c: c, live: newKVHasher(), dead: newKVHasher()}
}

// key adds k's versions from its from-th on. etcd's rule for versions at
// or below c depends on whether any key exists at rev, which is known
// only at the end: both sums are kept.
func (h *revHash) key(k *keyIndex, from int) {
	seen := k.at(h.rev)
	if seen >= 0 && k.vers[seen].ver != 0 {
		h.anyLive = true
	}
	for i := from; i < len(k.vers); i++ {
		v := &k.vers[i]
		if v.mod.main > h.rev || (v.mod.main == h.c && v.ver == 0) {
			continue
		}
		if v.mod.main > h.c || (i == seen && v.ver != 0) {
			h.live.add(k.key, v)
		}
		h.dead.add(k.key, v)
	}
}

func (h *revHash) sum() KVHash {
	s := h.dead
	if h.anyLive {
		s = h.live
	}
	return KVHash{Hash: s.sum, CompactRevision: h.c, Revision: h.rev}
}

// CompactHash is the hash the last compaction took: HashKV of its
// revision with the compaction revision before it, as etcd computes while
// it compacts.
func (s *Store) CompactHash() KVHash { return s.compactHash }

// pendingCompact is the compaction a checkpoint copy holds back, whose
// versions are gone for every reader but the copy, or 0.
func (s *Store) pendingCompact() int64 {
	if s.frozen != nil {
		return s.frozen.compact
	}
	return 0
}

// HashAll hashes every version in the index, as HashKV does, without its
// rules for compacted versions.
func (s *Store) HashAll() uint32 {
	h := newKVHasher()
	pending := s.pendingCompact()
	s.keys.Ascend(func(k *keyIndex) bool {
		for i := k.dropped(pending); i < len(k.vers); i++ {
			h.add(k.key, &k.vers[i])
		}
		return true
	})
	return h.sum
}

// kvHasher sums versions: per version, etcd's revision key (main and sub
// revision, and a tombstone mark), then the key and the version's fields,
// and the length and checksum of its value.
type kvHasher struct {
	sum uint32
	buf []byte
}

func newKVHasher() *kvHasher {
	// etcd starts with the name of its key bucket.
	return &kvHasher{sum: crc32.Update(0, castagnoli, []byte("key"))}
}

func (h *kvHasher) add(key []byte, v *version) {
	b := binary.BigEndian.AppendUint64(h.buf[:0], uint64(v.mod.main))
	b = append(b, '_')
	b = binary.BigEndian.AppendUint64(b, uint64(v.mod.sub))
	if v.ver == 0 {
		b = append(b, 't')
	}
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = append(b, key...)
	b = binary.AppendVarint(b, v.create)
	b = binary.AppendVarint(b, v.ver)
	b = binary.AppendVarint(b, v.lease)
	b = binary.BigEndian.AppendUint32(b, v.value.Len)
	b = binary.BigEndian.AppendUint32(b, v.value.Sum)
	h.sum = crc32.Update(h.sum, castagnoli, b)
	h.buf = b
}
