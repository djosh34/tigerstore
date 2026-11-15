package wl

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/maphash"
	"slices"
	"unsafe"
)

// The store keeps a key's live revision as its encoded mvccpb.KeyValue,
// in a slab outside Go's heap: a read copies the record into the reply
// as it is. Every value also stays in the log, named by its offset there:
// earlier revisions are history entries that keep only that offset, and
// a live record the memory budget let go is built again from the log
// when it is read. A hash index finds keys; a tree of directories orders
// them for ranges.

// keyEnt is one key, live or not. It holds no pointers.
type keyEnt struct {
	koff   uint64 // key bytes in the key arena
	klen   uint32
	dir    uint32
	rec    uint64 // live record in the slab; 0 if the key is not live or the budget let it go
	rlen   uint32
	hist   uint32 // newest earlier state, 0 for none
	create int64  // 0 if the key is not live
	mod    int64  // the revision of the state; of the deletion if not live
	ver    int64
	lease  int64
	voff   int64 // the log offset of the live value
	vlen   uint32
	_      uint32
}

// change is one key's change at a revision.
type change struct {
	rev int64
	key uint32
	del bool
}

// histEnt is a state a key had before its current one.
type histEnt struct {
	create, mod, ver, lease int64
	voff                    int64
	vlen                    uint32
	prev                    uint32
	deleted                 bool
}

// logReader reads values back from the log.
type logReader interface {
	read(p []byte, off int64) error
}

type store struct {
	rev        int64
	compactRev int64

	keys  []keyEnt
	nkeys uint32
	hash  []uint64 // tag << 32 | key id + 1
	mask  uint64
	seed  maphash.Seed

	karena   arena
	slab     *slab
	hist     []histEnt
	nhist    uint32
	histFree uint32 // a list of free history slots, through prev
	// trimKey is the next key whose history compaction trims, while
	// trimming.
	trimKey  uint32
	trimming bool
	tree     tree

	leases  map[int64]*lease
	log     logReader
	scratch []byte // a value read from the log

	// The budget: the slab's records may take limit bytes; past it, the
	// end of an apply lets go of records by a clock hand, and a read of a
	// key whose record was let go builds it again from the log.
	budget    int64 // the node's memory, 0 for no limit
	mapped    int64 // the rings and stages mapped outside Go's heap at startup
	limit     int64
	hand      uint32
	evictions uint64
	loads     uint64

	// changes is every change by revision, the index of watches from a
	// past revision.
	changes []change

	// Records replaced or deleted during an apply, freed once its
	// replies and watch events are written.
	freeLater []uint64
}

const (
	hashSlots  = 1 << 23
	maxKeys    = hashSlots / 2 // the hash index is half full at most
	maxHist    = 1 << 26
	keyArena   = 4 << 30
	slabBytes  = 48 << 30
	dirSlotLog = 20
)

func newStore() (*store, error) {
	st := &store{
		rev:    1, // etcd's empty store is at revision 1
		mask:   hashSlots - 1,
		seed:   maphash.MakeSeed(),
		karena: arena{top: 1},
		nhist:  1,
		leases: map[int64]*lease{},
	}
	var err error
	if st.keys, err = mmapSlice[keyEnt](maxKeys); err != nil {
		return nil, err
	}
	if st.hash, err = mmapSlice[uint64](hashSlots); err != nil {
		return nil, err
	}
	if st.karena.mem, err = mmapBytes(keyArena); err != nil {
		return nil, err
	}
	if st.slab, err = newSlab(slabBytes); err != nil {
		return nil, err
	}
	if st.hist, err = mmapSlice[histEnt](maxHist); err != nil {
		return nil, err
	}
	st.tree.init(st)
	return st, nil
}

var (
	errTooManyKeys = errors.New("the store holds as many keys as it can")
	errHistoryFull = errors.New("the store's history is full")
)

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
func (st *store) intern(key []byte) (uint32, error) {
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
				return id, nil
			}
		}
	}
	id := st.nkeys
	if id+1 >= maxKeys {
		return 0, errTooManyKeys
	}
	koff, err := st.karena.add(key)
	if err != nil {
		return 0, err
	}
	st.nkeys++
	st.keys[id] = keyEnt{koff: koff, klen: uint32(len(key)), dir: noDir}
	st.hash[i] = tag<<32 | uint64(id+1)
	return id, nil
}

// exists reports whether key id is live.
func (st *store) exists(id uint32) bool { return st.keys[id].create != 0 }

// live returns key id's record, or nil if it is not live. A record the
// budget let go is built again from the log and held again. The record
// stays valid until the end of the apply after the call.
func (st *store) live(id uint32) ([]byte, error) {
	k := &st.keys[id]
	if k.create == 0 {
		return nil, nil
	}
	if k.rec == 0 {
		if err := st.load(id); err != nil {
			return nil, err
		}
	}
	return st.slab.bytes(k.rec, int(k.rlen)), nil
}

// load builds key id's record from its value in the log.
func (st *store) load(id uint32) error {
	k := &st.keys[id]
	val, err := st.readLog(k.voff, int(k.vlen))
	if err != nil {
		return err
	}
	key := st.karena.get(k.koff, k.klen)
	n := kvSize(len(key), len(val), k.create, k.mod, k.ver, k.lease)
	off, err := st.slab.alloc(n)
	if err != nil {
		return err
	}
	appendKV(st.slab.mem[off:off:int(off)+n], key, val, k.create, k.mod, k.ver, k.lease)
	k.rec, k.rlen = off, uint32(n)
	st.loads++
	return nil
}

// readLog reads n bytes of the log at off into the store's scratch,
// which the next read overwrites.
func (st *store) readLog(off int64, n int) ([]byte, error) {
	if cap(st.scratch) < n {
		st.scratch = make([]byte, n)
	}
	val := st.scratch[:n]
	if n == 0 {
		return val, nil
	}
	if st.log == nil {
		return nil, errors.New("the store has no log to read values from")
	}
	if err := st.log.read(val, off); err != nil {
		return nil, fmt.Errorf("read a value from the log: %w", err)
	}
	return val, nil
}

// setBudget sizes the slab's limit from the node's memory budget, of
// which mapped bytes are the rings and stages mapped outside Go's heap.
func (st *store) setBudget(budget, mapped int64) {
	st.budget, st.mapped = budget, mapped
	st.relimit()
}

// relimit leaves the slab what the budget does not take otherwise: Go's
// share, the mapped rings and stages, and the tables as they grow.
func (st *store) relimit() {
	goTables := st.tree.bytes() + int64(cap(st.changes))*int64(unsafe.Sizeof(change{}))
	st.limit = st.budget - goShare(st.budget) - goTables - st.outside() + int64(st.slab.top)
}

// goShare is the part of the budget kept for Go's heap and runtime,
// besides the tables the store keeps there: an eighth, and the entries
// Raft's storage holds.
func goShare(budget int64) int64 { return budget/8 + storageHeld }

// outside is the memory the node maps outside Go's heap, which Go's memory
// limit does not count: the rings and stages, the store's tables and its
// slab up to the highest slot handed out.
func (st *store) outside() int64 {
	return st.mapped + int64(st.nkeys)*int64(unsafe.Sizeof(keyEnt{})) + int64(st.nhist)*int64(unsafe.Sizeof(histEnt{})) +
		int64(len(st.hash))*8 + int64(st.karena.top) + int64(st.slab.top)
}

// goLimit is the memory limit Go's runtime gets: what the budget leaves
// once the memory outside Go's heap is counted, and never less than Go's
// share. Go's limit counts only its own memory; at the whole budget, Go's
// heap grew on top of the store's tables, and the nodes swapped (#602).
func (st *store) goLimit() int64 {
	return max(st.budget-st.outside(), goShare(st.budget))
}

// fitSteps is the most records one apply lets go of.
const fitSteps = 1 << 13

// fit lets go of records past the limit, at most fitSteps of them.
func (st *store) fit() {
	if st.budget == 0 {
		return
	}
	st.relimit()
	for i := 0; i < fitSteps && st.slab.used > st.limit && st.nkeys > 0; i++ {
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
func (st *store) pushHist(k *keyEnt) error {
	if k.mod == 0 {
		return nil
	}
	h := st.histFree
	if h != 0 {
		st.histFree = st.hist[h].prev
	} else {
		h = st.nhist
		if h+1 >= maxHist {
			return errHistoryFull
		}
		st.nhist++
	}
	st.hist[h] = histEnt{
		create: k.create, mod: k.mod, ver: k.ver, lease: k.lease,
		voff: k.voff, vlen: k.vlen, prev: k.hist, deleted: k.create == 0,
	}
	k.hist = h
	return nil
}

// put writes value under key id at revision rev; voff is where the value
// lies in the log. It returns the old record, if the key was live, which
// stays valid until the apply ends.
func (st *store) put(id uint32, value []byte, lease int64, rev int64, voff int64) (prev []byte, err error) {
	k := &st.keys[id]
	if prev, err = st.live(id); err != nil {
		return nil, err
	}
	key := st.karena.get(k.koff, k.klen)
	create, ver := rev, int64(1)
	if prev != nil {
		create, ver = k.create, k.ver+1
	}
	n := kvSize(len(key), len(value), create, rev, ver, lease)
	off, err := st.slab.alloc(n)
	if err != nil {
		return nil, err
	}
	if prev == nil {
		if err := st.tree.insert(id); err != nil {
			st.slab.free(off, n)
			return nil, err
		}
	}
	if err := st.pushHist(k); err != nil {
		return nil, err
	}
	if prev != nil {
		st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
	}
	appendKV(st.slab.mem[off:off:int(off)+n], key, value, create, rev, ver, lease)
	old := int64(0)
	if prev != nil {
		old = k.lease
	}
	st.setLease(id, old, lease)
	k.rec, k.rlen = off, uint32(n)
	k.create, k.mod, k.ver, k.lease = create, rev, ver, lease
	k.voff, k.vlen = voff, uint32(len(value))
	st.changes = append(st.changes, change{rev: rev, key: id})
	return prev, nil
}

// del deletes key id at revision rev and returns its record, which stays
// valid until the apply ends, or nil if it was not live.
func (st *store) del(id uint32, rev int64) ([]byte, error) {
	k := &st.keys[id]
	prev, err := st.live(id)
	if err != nil || prev == nil {
		return nil, err
	}
	if err := st.pushHist(k); err != nil {
		return nil, err
	}
	st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
	st.tree.remove(id)
	st.setLease(id, k.lease, 0)
	k.rec, k.rlen = 0, 0
	k.create, k.mod, k.ver, k.lease = 0, rev, 0, 0
	k.voff, k.vlen = 0, 0
	st.changes = append(st.changes, change{rev: rev, key: id, del: true})
	return prev, nil
}

// endApply frees the records an apply replaced, keeps the rest within
// the budget, and goes on with a compaction's trim.
func (st *store) endApply() {
	for i := 0; i < len(st.freeLater); i += 2 {
		st.slab.free(st.freeLater[i], int(st.freeLater[i+1]))
	}
	st.freeLater = st.freeLater[:0]
	st.fit()
	st.trimSteps(trimStep)
}

// trimStep is the most keys one apply trims the history of.
const trimStep = 4096

// compact drops what no read at rev or later needs: the change index
// before it at once, and the key histories a few keys at a time after
// each apply.
func (st *store) compact(rev int64) {
	st.compactRev = rev
	i, _ := slices.BinarySearchFunc(st.changes, rev, func(ch change, r int64) int { return cmp.Compare(ch.rev, r) })
	if i > len(st.changes)/2 {
		st.changes = append([]change(nil), st.changes[i:]...)
	} else {
		st.changes = st.changes[i:]
	}
	st.trimKey, st.trimming = 0, true
}

// trimSteps trims the histories of up to k keys.
func (st *store) trimSteps(k int) {
	for ; k > 0 && st.trimming; k-- {
		if st.trimKey >= st.nkeys {
			st.trimming = false
			return
		}
		st.trimHistory(st.trimKey)
		st.trimKey++
	}
}

// trimHistory frees key id's states before the newest one at or before
// the compacted revision, and that one too if it is a deletion: a read at
// the compacted revision or later needs neither.
func (st *store) trimHistory(id uint32) {
	k := &st.keys[id]
	c := st.compactRev
	var cut uint32
	if k.mod <= c {
		cut, k.hist = k.hist, 0
	} else {
		link := &k.hist
		for h := k.hist; h != 0; h = st.hist[h].prev {
			e := &st.hist[h]
			if e.mod > c {
				link = &e.prev
				continue
			}
			if e.deleted {
				cut, *link = h, 0
			} else {
				cut, e.prev = e.prev, 0
			}
			break
		}
	}
	for cut != 0 {
		next := st.hist[cut].prev
		st.hist[cut] = histEnt{prev: st.histFree}
		st.histFree = cut
		cut = next
	}
}

// at returns key id's record as it was at revision rev, built from the
// history and the log if it changed since, or nil if it was not live.
func (st *store) at(id uint32, rev int64) ([]byte, error) {
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
			return nil, nil
		}
		val, err := st.readLog(e.voff, int(e.vlen))
		if err != nil {
			return nil, err
		}
		return appendKV(nil, st.key(id), val, e.create, e.mod, e.ver, e.lease), nil
	}
	return nil, nil
}

// state returns the mod revision, create revision, version and lease of
// key id now, all 0 if it is not live.
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
