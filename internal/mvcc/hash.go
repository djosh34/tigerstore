package mvcc

import (
	"encoding/binary"
	"hash/crc32"
)

// KVHash is the answer to etcd's Maintenance.HashKV: a CRC-32C of every
// version up to Revision, with the compaction revision it was taken at.
type KVHash struct{}

// revHash sums the versions up to rev with HashKV's rules, a key at a
// time, with c as the compaction revision.
type revHash struct {
	rev, c     int64
	live, dead *kvHasher
}

func newRevHash(rev, c int64) *revHash {
	return &revHash{rev: rev, c: c, live: newKVHasher(), dead: newKVHasher()}
}

// key adds k's versions from its from-th on. etcd's rule for versions at
// or below c depends on whether any key exists at rev, which is known
// only at the end: both sums are kept.
func (h *revHash) key(key []byte, k *keyIndex, from int) {
	seen := k.at(h.rev)
	for i := from; i < len(k.vers()); i++ {
		v := &k.vers()[i]
		if v.mod.main > h.rev || (v.mod.main == h.c && v.ver == 0) {
			continue
		}
		if v.mod.main > h.c || (i == seen && v.ver != 0) {
			h.live.add(key, v)
		}
		h.dead.add(key, v)
	}
}

func (h *revHash) sum() KVHash {
	return KVHash{}
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
