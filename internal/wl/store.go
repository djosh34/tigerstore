package wl

import (
	"bytes"
	"hash/maphash"
	"unsafe"
)

// The store keeps every key's live revision as its encoded
// mvccpb.KeyValue, in a slab outside Go's heap: a read copies the record
// into the reply as it is. Earlier revisions are history entries whose
// values stay in the WAL. A hash index finds keys; a tree of directories
// orders them for ranges.

// keyEnt is one key, live or not. It holds no pointers.
type keyEnt struct {
	koff   uint64 // key bytes in the key arena
	klen   uint32
	dir    uint32
	rec    uint64 // live record in the slab, 0 if the key has none
	rlen   uint32
	hist   uint32 // newest earlier state, 0 for none
	create int64
	mod    int64 // the revision of the state; of the deletion if not live
	ver    int64
	lease  int64
	voff   int64 // the WAL offset of the live value
	vlen   uint32
	_      uint32
}

// histEnt is a state a key had before its current one.
type histEnt struct {
	create, mod, ver, lease int64
	voff                    int64
	vlen                    uint32
	prev                    uint32
	deleted                 bool
}

type store struct {
	rev        int64
	compactRev int64

	keys  []keyEnt
	nkeys uint32
	hash  []uint64 // tag << 32 | key id + 1
	mask  uint64
	seed  maphash.Seed

	karena arena
	slab   *slab
	hist   []histEnt
	nhist  uint32
	tree   tree

	// The budget: the slab's records may take limit bytes; past it, the
	// end of an apply lets go of records by a clock hand, and a read of a
	// key whose record was let go builds it again from the log.
	limit     int64
	fixed     int64 // rings and stages, mapped at startup
	budget    int64
	hand      uint32
	evictions uint64
	loads     uint64

	leases map[int64]int64
	wal    *wal
	// segRefs lists, by logical log segment, the keys (keyRef set) and
	// history entries whose values lay there when they were recorded; the
	// cleaner checks each still does before it copies the value forward.
	segRefs map[int64][]uint32
	cleanAt int // the cleaner's place in segRefs[wal.cleaned]
	moved   uint64
	scratch []byte

	// Records replaced or deleted during an apply, freed once its
	// replies and watch events are written.
	freeLater []uint64
}

const (
	maxKeys    = 1 << 24
	hashSlots  = 1 << 23
	maxHist    = 1 << 26
	keyArena   = 4 << 30
	slabBytes  = 48 << 30
	dirSlotLog = 20
)

func newStore() *store {
	st := &store{
		rev:     1, // etcd's empty store is at revision 1
		keys:    mmapSlice[keyEnt](maxKeys),
		hash:    mmapSlice[uint64](hashSlots),
		mask:    hashSlots - 1,
		seed:    maphash.MakeSeed(),
		karena:  arena{mem: mmapBytes(keyArena), top: 1},
		slab:    newSlab(slabBytes),
		hist:    mmapSlice[histEnt](maxHist),
		nhist:   1,
		leases:  map[int64]int64{},
		segRefs: map[int64][]uint32{},
	}
	st.tree.init(st)
	return st
}

func (st *store) key(id uint32) []byte {
	k := &st.keys[id]
	return st.karena.get(k.koff, k.klen)
}

// find returns the id of key, if the store has ever had it.
func (st *store) find(key []byte) (uint32, bool) {
	h := maphash.Bytes(st.seed, key)
	tag := h >> 32
	for i := h & st.mask; ; i = (i + 1) & st.mask {
		e := st.hash[i]
		if e == 0 {
			return 0, false
		}
		if e>>32 == tag {
			id := uint32(e) - 1
			k := &st.keys[id]
			if int(k.klen) == len(key) && bytes.Equal(st.karena.get(k.koff, k.klen), key) {
				return id, true
			}
		}
	}
}

// intern returns the id of key, adding it if it is new.
func (st *store) intern(key []byte) uint32 {
	h := maphash.Bytes(st.seed, key)
	tag := h >> 32
	i := h & st.mask
	for ; ; i = (i + 1) & st.mask {
		e := st.hash[i]
		if e == 0 {
			break
		}
		if e>>32 == tag {
			id := uint32(e) - 1
			k := &st.keys[id]
			if int(k.klen) == len(key) && bytes.Equal(st.karena.get(k.koff, k.klen), key) {
				return id
			}
		}
	}
	id := st.nkeys
	st.nkeys++
	if st.nkeys >= maxKeys {
		panic("too many keys")
	}
	st.keys[id] = keyEnt{koff: st.karena.add(key), klen: uint32(len(key)), dir: noDir}
	st.hash[i] = tag<<32 | uint64(id+1)
	return id
}

// exists reports whether key id is live.
func (st *store) exists(id uint32) bool { return st.keys[id].create != 0 }

// live returns key id's record, or nil. A record the budget let go is
// built again from the log and held again.
func (st *store) live(id uint32) []byte {
	k := &st.keys[id]
	if k.create == 0 {
		return nil
	}
	if k.rec == 0 {
		st.load(id)
	}
	return st.slab.bytes(k.rec, int(k.rlen))
}

// load builds key id's record from its value in the log.
func (st *store) load(id uint32) {
	k := &st.keys[id]
	st.loads++
	val := st.readLog(k.voff, int(k.vlen))
	key := st.karena.get(k.koff, k.klen)
	n := kvSize(len(key), len(val), k.create, k.mod, k.ver, k.lease)
	off := st.slab.alloc(n)
	rec := appendKV(st.slab.mem[off:off:int(off)+n], key, val, k.create, k.mod, k.ver, k.lease)
	if len(rec) != n {
		panic("record size")
	}
	k.rec, k.rlen = off, uint32(n)
}

// readLog reads n bytes of the log at off.
func (st *store) readLog(off int64, n int) []byte {
	val := st.scratch[:0]
	if cap(val) < n {
		val = make([]byte, 0, n)
		st.scratch = val
	}
	val = val[:n]
	if st.wal != nil {
		st.wal.read(val, off)
	} else {
		clear(val)
	}
	return val
}

// setBudget sizes the slab's limit from the node's memory budget: an
// eighth for Go's heap and the runtime, the mapped rings, and the
// tables as they grow.
func (st *store) setBudget(budget, fixed int64) {
	st.budget, st.fixed = budget, fixed
	st.relimit()
}

func (st *store) relimit() {
	tables := int64(st.nkeys)*int64(unsafe.Sizeof(keyEnt{})) + int64(st.nhist)*int64(unsafe.Sizeof(histEnt{})) +
		int64(len(st.hash))*8 + int64(st.karena.top) + int64(st.tree.bytes())
	st.limit = st.budget - st.budget/8 - st.fixed - tables
}

// fit lets go of records past the limit, at most a step's worth.
func (st *store) fit() {
	if st.budget == 0 {
		return
	}
	st.relimit()
	for i := 0; i < 1<<13 && st.slab.used > st.limit && st.nkeys > 0; i++ {
		if st.hand >= st.nkeys {
			st.hand = 0
		}
		k := &st.keys[st.hand]
		st.hand++
		if k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
			st.evictions++
		}
	}
}

func kvSize(klen, vlen int, create, mod, ver, lease int64) int {
	n := sizeBytesField(1, klen) + sizeVarintField(2, uint64(create)) + sizeVarintField(3, uint64(mod)) +
		sizeVarintField(4, uint64(ver)) + sizeVarintField(6, uint64(lease))
	if vlen > 0 {
		n += sizeBytesField(5, vlen)
	}
	return n
}

func appendKV(b []byte, key, value []byte, create, mod, ver, lease int64) []byte {
	b = appendBytesField(b, 1, key)
	if create != 0 {
		b = appendVarintField(b, 2, uint64(create))
	}
	if mod != 0 {
		b = appendVarintField(b, 3, uint64(mod))
	}
	if ver != 0 {
		b = appendVarintField(b, 4, uint64(ver))
	}
	if len(value) > 0 {
		b = appendBytesField(b, 5, value)
	}
	if lease != 0 {
		b = appendVarintField(b, 6, uint64(lease))
	}
	return b
}

// pushHist keeps key id's current state as history.
func (st *store) pushHist(k *keyEnt) {
	if k.mod == 0 {
		return
	}
	h := st.nhist
	st.nhist++
	if st.nhist >= maxHist {
		panic("history is full")
	}
	st.hist[h] = histEnt{create: k.create, mod: k.mod, ver: k.ver, lease: k.lease,
		voff: k.voff, vlen: k.vlen, prev: k.hist, deleted: k.create == 0}
	k.hist = h
	if k.vlen > 0 && k.create != 0 {
		st.ref(k.voff, h)
	}
}

// put writes value under key id at revision rev; voff is where the value
// lies in the WAL. It returns the old record, if the key was live, which
// stays valid until the apply ends.
func (st *store) put(id uint32, value []byte, lease int64, rev int64, voff int64) (prev []byte) {
	k := &st.keys[id]
	create, ver := rev, int64(1)
	if k.create != 0 {
		prev = st.live(id)
		st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
		create, ver = k.create, k.ver+1
	} else {
		st.tree.insert(id)
	}
	st.pushHist(k)
	key := st.karena.get(k.koff, k.klen)
	n := kvSize(len(key), len(value), create, rev, ver, lease)
	off := st.slab.alloc(n)
	rec := appendKV(st.slab.mem[off:off:int(off)+n], key, value, create, rev, ver, lease)
	if len(rec) != n {
		panic("record size")
	}
	k.rec, k.rlen = off, uint32(n)
	k.create, k.mod, k.ver, k.lease = create, rev, ver, lease
	k.voff, k.vlen = voff, uint32(len(value))
	if len(value) > 0 {
		st.ref(voff, id|keyRef)
	}
	return prev
}

// del deletes key id at revision rev and returns its record, which stays
// valid until the apply ends, or nil if it was not live.
func (st *store) del(id uint32, rev int64) []byte {
	k := &st.keys[id]
	if k.create == 0 {
		return nil
	}
	prev := st.live(id)
	st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
	st.pushHist(k)
	st.tree.remove(id)
	k.rec, k.rlen = 0, 0
	k.create, k.mod, k.ver, k.lease = 0, rev, 0, 0
	k.voff, k.vlen = 0, 0
	return prev
}

// endApply frees the records an apply replaced.
func (st *store) endApply() {
	for i := 0; i < len(st.freeLater); i += 2 {
		st.slab.free(st.freeLater[i], int(st.freeLater[i+1]))
	}
	st.freeLater = st.freeLater[:0]
	st.fit()
}

// at returns key id's record as it was at revision rev, built from the
// history and the WAL if it changed since, or nil if it was not live.
func (st *store) at(id uint32, rev int64) []byte {
	k := &st.keys[id]
	if rev == 0 || k.mod <= rev {
		return st.live(id)
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if e.mod > rev {
			continue
		}
		if e.deleted {
			return nil
		}
		val := st.readLog(e.voff, int(e.vlen))
		return appendKV(nil, st.key(id), val, e.create, e.mod, e.ver, e.lease)
	}
	return nil
}

// modAt returns the mod revision, create revision, version and lease of key
// id now, all 0 if it is not live.
func (st *store) state(id uint32) (mod, create, ver, lease int64) {
	k := &st.keys[id]
	if k.create == 0 {
		return 0, 0, 0, 0
	}
	return k.mod, k.create, k.ver, k.lease
}

// recValue returns the value bytes of an encoded KeyValue record.
func recValue(rec []byte) []byte {
	for m := rec; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return nil
		}
		if num == 5 {
			return p
		}
		m = rest
	}
	return nil
}

var _ = unsafe.Sizeof(keyEnt{})

const keyRef = 1 << 31

func (st *store) ref(voff int64, r uint32) {
	g := voff / segSize
	st.segRefs[g] = append(st.segRefs[g], r)
}

// cleanStep copies forward, up to budget bytes, the values that still lie
// in logical segment seg, and reports whether the segment is clean. room
// is what the log's stage may take.
func (st *store) cleanStep(seg int64, budget, room int) bool {
	refs := st.segRefs[seg]
	w := st.wal
	for ; st.cleanAt < len(refs); st.cleanAt++ {
		r := refs[st.cleanAt]
		var voff *int64
		var vlen int
		var val []byte
		if r&keyRef != 0 {
			k := &st.keys[r&^keyRef]
			if k.create == 0 || k.vlen == 0 || k.voff/segSize != seg {
				continue
			}
			voff, vlen = &k.voff, int(k.vlen)
			if k.rec != 0 {
				val = recValue(st.slab.bytes(k.rec, int(k.rlen)))
			}
		} else {
			e := &st.hist[r]
			if e.deleted || e.vlen == 0 || e.voff/segSize != seg {
				continue
			}
			voff, vlen = &e.voff, int(e.vlen)
		}
		if vlen+12 > budget || vlen+12 > room || !w.covered(w.off+int64(vlen)+12) {
			return false
		}
		if val == nil {
			val = st.readLog(*voff, vlen)
		}
		*voff = w.appendRaw(val)
		st.ref(*voff, r)
		st.moved += uint64(vlen)
		budget -= vlen + 12
		room -= vlen + 12
	}
	delete(st.segRefs, seg)
	st.cleanAt = 0
	return true
}
