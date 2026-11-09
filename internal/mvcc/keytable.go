package mvcc

import (
	"bytes"
	"hash/maphash"
)

// keyTable finds the slot of a key by its bytes (Store.rec): open
// addressing with linear probing over 8-byte entries, the upper 32 bits
// of the key's hash and the slot, 0 for an empty entry (slots 0 and 1 are
// the probes, never in the table). It holds no pointer, so the garbage
// collector does not walk it, and takes about 10 bytes a key where a Go
// map of the key's string took about 40 (#506). A removal shifts the
// entries after it back, so the table keeps no tombstones.
type keyTable struct {
	entries []uint64
	n       int
	seed    maphash.Seed
}

// minEntries is the smallest table.
const minEntries = 64

func newKeyTable() keyTable {
	return keyTable{entries: make([]uint64, minEntries), seed: maphash.MakeSeed()}
}

func (t *keyTable) hash(key []byte) uint32 { return uint32(maphash.Bytes(t.seed, key) >> 32) }

// find returns the position of key's entry, or of the empty entry where
// it would go, and whether it is there.
func (t *keyTable) find(s *Store, key []byte, h uint32) (int, bool) {
	mask := len(t.entries) - 1
	for i := int(h) & mask; ; i = (i + 1) & mask {
		e := t.entries[i]
		if uint32(e) == 0 {
			return i, false
		}
		if uint32(e>>32) == h && bytes.Equal(s.key(s.rec(uint32(e))), key) {
			return i, true
		}
	}
}

// get returns the slot of key.
func (t *keyTable) get(s *Store, key []byte) (uint32, bool) {
	i, ok := t.find(s, key, t.hash(key))
	return uint32(t.entries[i]), ok
}

// add adds the key of slot id, which the table does not hold.
func (t *keyTable) add(s *Store, id uint32) {
	if (t.n+1)*5 > len(t.entries)*4 {
		t.resize(2 * len(t.entries))
	}
	key := s.key(s.rec(id))
	h := t.hash(key)
	i, _ := t.find(s, key, h)
	t.entries[i] = uint64(h)<<32 | uint64(id)
	t.n++
}

// remove takes key out of the table.
func (t *keyTable) remove(s *Store, key []byte) {
	i, ok := t.find(s, key, t.hash(key))
	if !ok {
		return
	}
	mask := len(t.entries) - 1
	// Each entry after the hole up to the next empty one moves into it
	// if the hole lies between the entry's home and its place.
	for j := (i + 1) & mask; t.entries[j] != 0; j = (j + 1) & mask {
		home := int(uint32(t.entries[j]>>32)) & mask
		if (j-home)&mask >= (j-i)&mask {
			t.entries[i] = t.entries[j]
			i = j
		}
	}
	t.entries[i] = 0
	t.n--
	if len(t.entries) > minEntries && t.n*8 < len(t.entries) {
		t.resize(len(t.entries) / 2)
	}
}

// resize moves the entries to a table of size entries, a power of two.
func (t *keyTable) resize(size int) {
	old := t.entries
	t.entries = make([]uint64, size)
	mask := size - 1
	for _, e := range old {
		if e == 0 {
			continue
		}
		i := int(uint32(e>>32)) & mask
		for t.entries[i] != 0 {
			i = (i + 1) & mask
		}
		t.entries[i] = e
	}
}
