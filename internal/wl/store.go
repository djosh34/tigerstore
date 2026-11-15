package wl

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
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

// keyEnt is one key, live or not. It holds no pointers. A free id has
// klen 0: no key is empty.
type keyEnt struct {
	koff   uint64 // key bytes in the key slab
	klen   uint32
	dir    uint32
	rec    uint64 // live record in the slab; 0 if the key is not live or the budget let it go
	rlen   uint32
	hist   uint32 // newest earlier state, 0 for none
	create int64  // 0 if the key is not live
	mod    int64  // the revision of the state; of the deletion if not live
	ver    int64
	lease  int64
	voff   int64 // where the live value lies: in the log, or in a checkpoint (ckptOff)
	vlen   uint32
	// inTree: the tree holds the key. It holds every key a read at the
	// compacted revision or later may see, deleted ones too: a range at a
	// past revision finds a key deleted since.
	inTree bool
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
	// keyFree are ids freed for reuse: keys deleted before the compacted
	// revision, which no read or watch can name any more.
	keyFree []uint32
	hash    []uint64 // a hash index (hashEntry) of the keys
	mask    uint64
	seed    maphash.Seed

	kmem     *slab // key bytes
	karena   arena // directory paths
	slab     *slab
	hist     []histEnt
	nhist    uint32
	histFree uint32 // a list of free history slots, through prev
	// trimKey is the next key whose history compaction trims, while
	// trimming.
	trimKey  uint32
	trimming bool
	// While a checkpoint is written, trimming and the change index keep
	// everything from the revision it was compacted to, holdRev.
	holding bool
	holdRev int64
	tree    tree

	leases map[int64]*lease
	log    logReader
	// ckpts are the checkpoint files values may lie in, by the generation
	// bit of their offsets: the newest one, and the one being written.
	ckpts   [2]*os.File
	scratch []byte // a value read from the log

	// The budget: the slab's records may take limit bytes; past it, the
	// end of an apply lets go of records by a clock hand, and a read of a
	// key whose record was let go builds it again from the log.
	budget    int64 // the node's memory, 0 for no limit
	mapped    int64 // the rings and stages mapped outside Go's heap at startup
	goLive    int64 // Go's live heap, as the collector last found it
	limit     int64
	hand      uint32
	evictions uint64
	loads     uint64
	// liveBytes and histBytes are the size of the live values and of the
	// earlier ones the history keeps: what a checkpoint holds.
	liveBytes, histBytes int64

	// changes is every change by revision, the index of watches from a
	// past revision.
	changes []change

	// Records replaced or deleted during an apply, freed once its
	// replies and watch events are written.
	freeLater []uint64
}

const (
	hashStart = 1 << 20 // the hash index's first size, which doubles
	maxKeys   = 1 << 27
	maxHist   = 1 << 26
	keyArena  = 4 << 30
	dirArena  = 1 << 30
	slabBytes = 48 << 30
	dirStart  = 1 << 14 // the directory index's first size, which doubles
)

func newStore() (*store, error) {
	st := &store{
		rev:    1, // etcd's empty store is at revision 1
		mask:   hashStart - 1,
		seed:   maphash.MakeSeed(),
		karena: arena{top: 1},
		nhist:  1,
		leases: map[int64]*lease{},
	}
	var err error
	if st.keys, err = mmapSlice[keyEnt](maxKeys); err != nil {
		return nil, err
	}
	if st.hash, err = mmapSlice[uint64](hashStart); err != nil {
		return nil, err
	}
	if st.kmem, err = newSlab(keyArena); err != nil {
		return nil, err
	}
	if st.karena.mem, err = mmapBytes(dirArena); err != nil {
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
	return st.kmem.bytes(k.koff, int(k.klen))
}

// find returns the id of key, if the store has ever had it.
func (st *store) find(key []byte) (uint32, bool) {
	h := maphash.Bytes(st.seed, key)
	for i := h & st.mask; ; i = (i + 1) & st.mask {
		e := st.hash[i]
		if e == 0 {
			return 0, false
		}
		if entryHash(e) == uint32(h) {
			id := uint32(e) - 1
			k := &st.keys[id]
			if int(k.klen) == len(key) && bytes.Equal(st.kmem.bytes(k.koff, int(k.klen)), key) {
				return id, true
			}
		}
	}
}

// intern returns the id of key, adding it if it is new.
func (st *store) intern(key []byte) (uint32, error) {
	if id, ok := st.find(key); ok {
		return id, nil
	}
	used := st.nkeys - uint32(len(st.keyFree))
	if 2*uint64(used+1) > uint64(len(st.hash)) {
		hash, err := mmapSlice[uint64](2 * len(st.hash))
		if err != nil {
			return 0, err
		}
		rehash(st.hash, hash)
		if err := munmapSlice(st.hash); err != nil {
			return 0, err
		}
		st.hash, st.mask = hash, uint64(len(hash)-1)
	}
	id := st.nkeys
	if k := len(st.keyFree); k > 0 {
		id = st.keyFree[k-1]
	} else if id+1 >= maxKeys {
		return 0, errTooManyKeys
	}
	koff, err := st.kmem.alloc(len(key))
	if err != nil {
		return 0, err
	}
	if id == st.nkeys {
		st.nkeys++
	} else {
		st.keyFree = st.keyFree[:len(st.keyFree)-1]
	}
	copy(st.kmem.mem[koff:], key)
	st.keys[id] = keyEnt{koff: koff, klen: uint32(len(key)), dir: noDir}
	putEntry(st.hash, maphash.Bytes(st.seed, key), id)
	return id, nil
}

// freeKey gives back key id, which is not live and has no history: its
// index entry, its bytes and its id.
func (st *store) freeKey(id uint32) {
	k := &st.keys[id]
	h := maphash.Bytes(st.seed, st.key(id))
	for i := h & st.mask; st.hash[i] != 0; i = (i + 1) & st.mask {
		if uint32(st.hash[i])-1 == id {
			removeEntry(st.hash, i)
			break
		}
	}
	st.kmem.free(k.koff, int(k.klen))
	*k = keyEnt{dir: noDir}
	st.keyFree = append(st.keyFree, id)
}

// A hash index is open addressing over a power of two of slots, each 0 or
// a hashEntry: the low 32 bits of a name's hash, then its id + 1. It
// doubles when half full; an entry's bits say its slot in the new size,
// so growing it reads no name.

func hashEntry(h uint64, id uint32) uint64 { return h<<32 | uint64(id+1) }

func entryHash(e uint64) uint32 { return uint32(e >> 32) }

// putEntry puts the entry of hash h and id into idx, which has room.
func putEntry(idx []uint64, h uint64, id uint32) {
	mask := uint64(len(idx) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		if idx[i] == 0 {
			idx[i] = hashEntry(h, id)
			return
		}
	}
}

// removeEntry empties slot i of idx and moves the entries after it in
// its run back, so that each stays where a probe from its home slot finds
// it.
func removeEntry(idx []uint64, i uint64) {
	mask := uint64(len(idx) - 1)
	for j := (i + 1) & mask; idx[j] != 0; j = (j + 1) & mask {
		home := uint64(entryHash(idx[j])) & mask
		// The entry at j may move to i unless its home lies after i, up
		// to j.
		if (j-home)&mask >= (j-i)&mask {
			idx[i] = idx[j]
			i = j
		}
	}
	idx[i] = 0
}

// rehash puts the entries of old into to, a larger index.
func rehash(old, to []uint64) {
	for _, e := range old {
		if e != 0 {
			putEntry(to, uint64(entryHash(e)), uint32(e)-1)
		}
	}
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
	key := st.key(id)
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

// readLog reads the n bytes of a value at off, in the log or a
// checkpoint, into the store's scratch, which the next read overwrites.
func (st *store) readLog(off int64, n int) ([]byte, error) {
	if cap(st.scratch) < n {
		st.scratch = make([]byte, n)
	}
	val := st.scratch[:n]
	if n == 0 {
		return val, nil
	}
	if inCkpt(off) {
		f := st.ckpts[ckptGenOf(off)]
		if f == nil {
			return nil, fmt.Errorf("a value at %#x names a checkpoint the store does not have", off)
		}
		if _, err := f.ReadAt(val, off&ckptOffMask); err != nil {
			return nil, fmt.Errorf("read a value from the checkpoint: %w", err)
		}
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

// relimit leaves the slab's records what the budget does not take
// otherwise: what Go needs, the mapped rings and stages, and the tables
// outside Go's heap as they grow. Its freed slots stay mapped, but letting
// records go never unmaps them: the limit is on the records alone, and
// the freed slots are taken again before the slab grows.
func (st *store) relimit() {
	st.limit = st.budget - st.goNeed() - (st.outside() - int64(st.slab.top))
}

// goShare is the least of the budget kept for Go's heap and runtime: an
// eighth, and the entries Raft's storage holds.
func goShare(budget int64) int64 { return budget/8 + storageHeld }

// goNeed is what Go's heap and runtime need: the live heap the collector
// last found and a quarter more for it to collect in, and at least Go's
// share. Below its live heap, Go would collect without end.
func (st *store) goNeed() int64 { return max(goShare(st.budget), st.goLive+st.goLive/4) }

// outside is the memory the node maps outside Go's heap, which Go's memory
// limit does not count: the rings and stages, the store's tables and its
// slab up to the highest slot handed out.
func (st *store) outside() int64 {
	return st.mapped + int64(st.nkeys)*int64(unsafe.Sizeof(keyEnt{})) + int64(st.nhist)*int64(unsafe.Sizeof(histEnt{})) +
		int64(len(st.hash))*8 + int64(st.kmem.top) + int64(st.karena.top) + int64(st.slab.top)
}

// goLimit is the memory limit Go's runtime gets: what the budget leaves
// once the memory outside Go's heap is counted, and never less than Go
// needs. Go's limit counts only its own memory; at the whole budget, Go's
// heap grew on top of the store's tables, and the nodes swapped (#602).
func (st *store) goLimit() int64 {
	return max(st.budget-st.outside(), st.goNeed())
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
	st.histBytes += int64(k.vlen)
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
	key := st.key(id)
	create, ver := rev, int64(1)
	if prev != nil {
		create, ver = k.create, k.ver+1
	}
	n := kvSize(len(key), len(value), create, rev, ver, lease)
	off, err := st.slab.alloc(n)
	if err != nil {
		return nil, err
	}
	if !k.inTree {
		if err := st.tree.insert(id); err != nil {
			st.slab.free(off, n)
			return nil, err
		}
		k.inTree = true
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
	if prev != nil {
		st.liveBytes -= int64(k.vlen)
	}
	st.liveBytes += int64(len(value))
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
	st.setLease(id, k.lease, 0)
	st.liveBytes -= int64(k.vlen)
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
	st.retrim()
}

// trimRev is the revision whose reads trimming keeps what they need: the
// compacted one, or the one a checkpoint being written holds.
func (st *store) trimRev() int64 {
	if st.holding {
		return min(st.compactRev, st.holdRev)
	}
	return st.compactRev
}

// retrim drops the change index before trimRev and starts trimming the
// histories to it.
func (st *store) retrim() {
	i, _ := slices.BinarySearchFunc(st.changes, st.trimRev(), func(ch change, r int64) int { return cmp.Compare(ch.rev, r) })
	if i > len(st.changes)/2 {
		st.changes = append([]change(nil), st.changes[i:]...)
	} else {
		st.changes = st.changes[i:]
	}
	st.trimKey, st.trimming = 0, true
}

// hold keeps everything a read at the compacted revision needs, while a
// checkpoint taken now is written; release lets trimming go on.
func (st *store) hold() { st.holding, st.holdRev = true, st.compactRev }

func (st *store) release() {
	st.holding = false
	if st.compactRev > st.holdRev {
		st.retrim()
	}
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
// the compacted revision or later needs neither. A key deleted at or
// before the compacted revision leaves the tree, and one deleted before it
// is freed: no read, watch or change names it.
func (st *store) trimHistory(id uint32) {
	k := &st.keys[id]
	if k.klen == 0 {
		return
	}
	c := st.trimRev()
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
		st.histBytes -= int64(st.hist[cut].vlen)
		st.hist[cut] = histEnt{prev: st.histFree}
		st.histFree = cut
		cut = next
	}
	if k.create == 0 && k.hist == 0 && k.mod <= c {
		if k.inTree {
			st.tree.remove(id)
			k.inTree = false
		}
		if k.mod < c {
			st.freeKey(id)
		}
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

// searchChanges returns the index of the first change at revision rev or
// later.
func searchChanges(chs []change, rev int64) int {
	i, _ := slices.BinarySearchFunc(chs, rev, func(ch change, r int64) int { return cmp.Compare(ch.rev, r) })
	return i
}

// ckptStates appends to out key id's states a checkpoint at revision rev
// holds, newest first: those up to rev that a read at the revision
// trimming keeps (trimRev) or later can see. A key such a read cannot see
// has none.
func (st *store) ckptStates(id uint32, rev int64, out []ckptState) []ckptState {
	k := &st.keys[id]
	if k.klen == 0 {
		return out
	}
	c := st.trimRev()
	add := func(s ckptState, cur bool) bool {
		if s.mod > rev {
			return true
		}
		// As trimming leaves it: the newest state at or before c, unless a
		// deletion; a key deleted at c keeps that deletion.
		if s.mod <= c {
			if !s.deleted || (cur && s.mod == c) {
				out = append(out, s)
			}
			return false
		}
		out = append(out, s)
		return true
	}
	cur := ckptState{deleted: k.create == 0, create: k.create, mod: k.mod, ver: k.ver, lease: k.lease, voff: k.voff, vlen: k.vlen}
	if !cur.deleted && k.rec != 0 {
		cur.rec = st.slab.bytes(k.rec, int(k.rlen))
	}
	if k.mod == 0 || !add(cur, true) {
		return out
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if !add(ckptState{deleted: e.deleted, create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen}, false) {
			break
		}
	}
	return out
}

// restoreKey adds key with its states, newest first, as a checkpoint holds
// them, to a store being loaded.
func (st *store) restoreKey(key []byte, states []ckptState) (uint32, error) {
	id, err := st.intern(key)
	if err != nil {
		return 0, err
	}
	k := &st.keys[id]
	for i := len(states) - 1; i >= 0; i-- {
		s := &states[i]
		if err := st.pushHist(k); err != nil {
			return 0, err
		}
		k.create, k.mod, k.ver, k.lease = s.create, s.mod, s.ver, s.lease
		if s.deleted {
			k.create, k.ver, k.lease = 0, 0, 0
		}
		k.voff, k.vlen = s.voff, s.vlen
	}
	if len(states) > 0 {
		if err := st.tree.insert(id); err != nil {
			return 0, err
		}
		k.inTree = true
	}
	if k.create != 0 {
		st.setLease(id, 0, k.lease)
		st.liveBytes += int64(k.vlen)
	}
	return id, nil
}

// repoint names the values a checkpoint now holds by their place in it.
func (st *store) repoint(moves []ckptMove) {
	for _, m := range moves {
		k := &st.keys[m.id]
		if k.voff == m.from {
			k.voff = m.to
		}
		for h := k.hist; h != 0; h = st.hist[h].prev {
			if e := &st.hist[h]; e.voff == m.from {
				e.voff = m.to
			}
		}
	}
}

// close gives back the store's memory outside Go's heap and closes its
// checkpoint files.
func (st *store) close() error {
	for i, f := range st.ckpts {
		if f != nil {
			_ = f.Close()
			st.ckpts[i] = nil
		}
	}
	var errs []error
	for _, b := range [][]byte{st.kmem.mem, st.karena.mem, st.slab.mem} {
		errs = append(errs, unixMunmap(b))
	}
	errs = append(errs, munmapSlice(st.keys), munmapSlice(st.hash), munmapSlice(st.hist))
	return errors.Join(errs...)
}
