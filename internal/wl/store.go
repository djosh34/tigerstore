package wl

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"maps"
	"math"
	"slices"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/planted"
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
	vsum   uint32 // the value's CRC-32C, which a read back checks
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
	vlen, vsum              uint32
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
	// compacted is set once a compaction applied, and kept in
	// checkpoints. etcd's compacted revision starts at -1, so its first
	// Compact may name revision 0; this store's starts at 0 and tells the
	// two apart by it.
	compacted bool
	// prevCompactRev is the compacted revision before the last
	// compaction, which HashKV at the compacted one names.
	prevCompactRev int64

	keys  []keyEnt
	nkeys uint32
	// keyFree are ids freed for reuse: keys deleted before the compacted
	// revision, which no read or watch can name any more.
	keyFree []uint32
	hash    []uint64 // a hash index (hashEntry) of the keys
	mask    uint64
	seed    maphash.Seed

	kmem     *slab // key bytes
	slab     *slab
	hist     []histEnt
	nhist    uint32
	histFree uint32 // a list of free history slots, through prev
	nfree    uint32 // the slots in histFree
	// trimKey is the next key whose history compaction trims, while
	// trimming.
	trimKey  uint32
	trimming bool
	// While a checkpoint is written, trimming and the change index keep
	// everything from the revision it was compacted to, holdRev.
	holding bool
	holdRev int64
	// readHolds are the revisions reads hold while they are sent: large
	// ranges and watches in pieces, replies that wait for the reserve.
	// Each counts how many hold it. Trimming keeps what a read at each
	// needs, and readHoldMin is the lowest of them.
	readHolds   map[int64]int
	readHoldMin int64
	// trimHolds are revisions only trimming keeps (holdTrim), each with
	// how many hold it, and trimHoldMin the lowest of them.
	trimHolds   map[int64]int
	trimHoldMin int64
	// heldRevs is readHolds' revisions sorted, or empty until takeStates
	// sorts them again after one came or went.
	heldRevs []int64
	// heldStates are the states held replies read (reserve.go), by key
	// and revision, each with how many hold it: a checkpoint takes in
	// their values (takeStates).
	heldStates map[heldState]int
	// heldStatesPeak is the most heldStates held since it was last made:
	// a map never shrinks, so one mostly emptied is made again.
	heldStatesPeak int
	tree           tree

	leases     map[int64]*lease
	log        logReader
	fetchOwner *node
	legacyView *legacyBatch
	awayBytes  int64
	// ckpts are the checkpoint files values may lie in, by the generation
	// bit of their offsets: the newest one, and the one being written.
	ckpts   [2]ckptFile
	scratch []byte // a value read from the log
	// While noWait is set, as a read served on the loop sets it, a value
	// the loop would wait on the disk for is not read: it goes to away,
	// and the read fails with errAway, to be served again once the
	// value was fetched off the loop into fetched (fetch.go).
	noWait  bool
	away    []logValue
	fetched map[int64]fetchedValue
	// fetchedBytes is what fetched holds, and fetchedOrder its values by
	// when they came, oldest first (keepFetched, trimFetched).
	fetchedBytes int64
	fetchedOrder []fetchedAt
	// bad is the first value read back that did not match its checksum:
	// the loop stops on it, and no read serves it (#708).
	bad error

	// The budget: the slab's records may take limit bytes; past it, the
	// end of an apply lets go of records by a clock hand, and a read of a
	// key whose record was let go builds it again from the log.
	budget int64 // the node's memory, 0 for no limit
	mapped int64 // the rings and stages mapped outside Go's heap at startup
	goLive int64 // Go's live heap, as the collector last found it
	closed bool  // close let go of its mappings
	// grew, if set, is called as loading a checkpoint grows the tables
	// outside Go's heap, every loadGrowKeys keys and at its end, so that
	// Go's limit follows them (node.limitGo).
	grew      func()
	limit     int64
	hand      uint32
	evictions uint64
	shrinks   uint64 // the times shrinkSlab gave pages back
	loads     uint64
	// unkept counts the records load built without keeping them: the
	// slab had no room within its limit (#805).
	unkept uint64
	// liveBytes and histBytes are the size of the live values and of the
	// earlier ones the history keeps: what a checkpoint holds.
	liveBytes, histBytes int64
	// liveKeys is how many keys are live, which the metrics read.
	liveKeys int64

	// changes is every change by revision, the index of watches from a
	// past revision.
	changes []change

	// Records replaced or deleted during an apply, freed once its
	// replies and watch events are written.
	freeLater []uint64

	// tag lets reply-room waiters check identity without keeping this
	// store and its tables alive after a snapshot replaces it.
	tag *byte
}

// The store's tables reserve their room once (maxKeys, maxHist, keyArena,
// slabBytes: sys_os.go, sys_sim.go); the indexes double.
const (
	hashStart = 1 << 20 // the hash index's first size, which doubles
	dirStart  = 1 << 14 // the directory index's first size, which doubles
)

func newStore() (*store, error) {
	st := &store{
		tag:    new(byte),
		rev:    1, // etcd's empty store is at revision 1
		mask:   hashStart - 1,
		seed:   maphash.MakeSeed(),
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
// budget let go is built again from the log and held again, if the slab
// has room for it within its limit; if not, it is built for the caller
// alone (load). The record stays valid until the end of the apply after
// the call.
func (st *store) live(id uint32) ([]byte, error) {
	k := &st.keys[id]
	if k.create == 0 {
		return nil, nil
	}
	if k.rec == 0 {
		return st.load(id)
	}
	return st.slab.bytes(k.rec, int(k.rlen)), nil
}

// load builds key id's record from its value in the log, and keeps it in
// the slab while the slab stays within topMax, the room past its limit an
// apply then takes back (fit). A read kept every record it loaded before
// (#805): nothing lets records go during a read, only after an apply, so
// a range over more values than the limit kept every one it read, past
// the budget, and under the simulator past its fixed slab, where it
// failed with errSlabFull and the node closed the client. Past topMax,
// or with the slab full, the record is built in Go's heap for its caller,
// and a read of it again builds it again; a reply holds at most a piece
// of them (bigRange). Records are not let go during a read: the reply
// holds slab bytes until it is written.
func (st *store) load(id uint32) ([]byte, error) {
	k := &st.keys[id]
	val, err := st.readValueRef(id, k.mod, k.voff, k.vlen, k.vsum)
	if err != nil {
		return nil, err
	}
	key := st.key(id)
	n := kvSize(len(key), len(val), k.create, k.mod, k.ver, k.lease)
	st.loads++
	// Up to topMax, the room the slab has past its limit until an apply
	// lets records go (fit), as the writes do: kept only up to the limit
	// itself, a read kept almost nothing once writes filled the slab, and
	// read a key read again and again from the disk each time (the review
	// of #807). Its top counts too, as its pages stay resident once handed
	// out: a slot past it must come from the free slots.
	if _, size := classOf(n); st.budget != 0 && (st.slab.used+int64(size) > st.topMax() ||
		!st.slab.reuses(n) && int64(st.slab.top+size) > st.topMax()) {
		st.unkept++
		return appendKV(make([]byte, 0, n), key, val, k.create, k.mod, k.ver, k.lease), nil
	}
	off, err := st.slab.alloc(n)
	if errors.Is(err, errSlabFull) {
		st.unkept++
		return appendKV(make([]byte, 0, n), key, val, k.create, k.mod, k.ver, k.lease), nil
	}
	if err != nil {
		return nil, err
	}
	appendKV(st.slab.mem[off:off:int(off)+n], key, val, k.create, k.mod, k.ver, k.lease)
	k.rec, k.rlen = off, uint32(n)
	return st.slab.bytes(off, n), nil
}

// ErrBadValue is a value read back from the log or a checkpoint that does
// not match the checksum the store took when it was written: the disk
// damaged it at rest. The node stops instead of serving it.
var ErrBadValue = errors.New("wl: a value does not match its checksum")

// valueSum is the checksum the store keeps of a value.
func valueSum(v []byte) uint32 { return crc32.Checksum(v, castagnoli) }

// ReadBackError is a value (ErrBadValue) or an entry's record
// (ErrBadEntry) read back from the disk that does not check: the Len
// bytes at Off, in the log, or in a checkpoint if Off has ckptBit.
type ReadBackError struct {
	Err error
	Off int64
	Len int
}

func (e *ReadBackError) Error() string {
	return fmt.Sprintf("%v: the %d bytes at %#x", e.Err, e.Len, e.Off)
}

func (e *ReadBackError) Unwrap() error { return e.Err }

// InCheckpoint reports whether the bytes lie in a checkpoint.
func (e *ReadBackError) InCheckpoint() bool { return inCkpt(e.Off) }

// checkValue checks a value read back at off against sum, its checksum.
func checkValue(v []byte, off int64, sum uint32) error {
	if valueSum(v) != sum {
		return &ReadBackError{Err: ErrBadValue, Off: off, Len: len(v)}
	}
	return nil
}

// readValue reads the n bytes of a value at off, as readLog, and checks
// them against sum. The first value that does not check stops the node
// (st.bad).
func (st *store) readValue(off int64, n, sum uint32) ([]byte, error) {
	var val []byte
	var err error
	if f, ok := st.fetched[off]; ok && len(f.val) == int(n) {
		val, err = f.val, f.err
	} else {
		val, err = st.readLog(off, int(n))
	}
	if errors.Is(err, errAway) {
		st.awayBytes += int64(n)
		st.away = append(st.away, logValue{off: off, len: n, sum: sum})
	}
	if err != nil {
		return nil, err
	}
	if err := checkValue(val, off, sum); err != nil {
		if st.bad == nil {
			st.bad = err
		}
		return nil, err
	}
	return val, nil
}

func (st *store) readValueRef(id uint32, mod, off int64, n, sum uint32) ([]byte, error) {
	if b := st.legacyView; b != nil {
		if val, err, ok := b.lookup(id, mod); ok {
			if err != nil {
				return nil, err
			}
			if err := checkValue(val, off, sum); err != nil {
				if st.bad == nil {
					st.bad = err
				}
				return nil, err
			}
			return val, nil
		}
	}
	if st.noWait && n > 0 && st.fetchOwner != nil && st.fetchOwner.cursorFetch {
		st.awayBytes += int64(n)
		st.fetchOwner.collectLegacy(id, mod, n, sum)
		return nil, errAway
	}
	return st.readValue(off, n, sum)
}

// logValue is a value the store names in the log.
type logValue struct {
	off      int64
	len, sum uint32
}

// logValues appends to out the values the store names in the log before
// offset before, in the order of their place, each once.
func (st *store) logValues(before int64, out []logValue) []logValue {
	add := func(off int64, n, sum uint32) {
		if n > 0 && !inCkpt(off) && off < before {
			out = append(out, logValue{off: off, len: n, sum: sum})
		}
	}
	for i := range st.keys[:st.nkeys] {
		k := &st.keys[i]
		if k.klen == 0 {
			continue
		}
		add(k.voff, k.vlen, k.vsum)
		for h := k.hist; h != 0; h = st.hist[h].prev {
			e := &st.hist[h]
			add(e.voff, e.vlen, e.vsum)
		}
	}
	slices.SortFunc(out, func(a, b logValue) int { return cmp.Compare(a.off, b.off) })
	return slices.CompactFunc(out, func(a, b logValue) bool { return a.off == b.off })
}

// checkLogValues checks the values a store loaded from a checkpoint names
// in the log before from, where the start's scan of the log begins. The
// start reads every other byte the store names: the checkpoint's, whose
// CRC it checks, and the log's records from from on. A value that does
// not check refuses the start, as damage to the log does (#708). Near
// values share a read.
func (st *store) checkLogValues(from int64) error {
	vs := st.logValues(from, nil)
	var span []byte
	for i := 0; i < len(vs); {
		start, end := vs[i].off, vs[i].off+int64(vs[i].len)
		j := i + 1
		for ; j < len(vs) && vs[j].off-end <= ckptGap && vs[j].off+int64(vs[j].len)-start <= ckptSpan; j++ {
			end = max(end, vs[j].off+int64(vs[j].len))
		}
		if int64(cap(span)) < end-start {
			span = make([]byte, end-start)
		}
		p := span[:end-start]
		if err := st.log.read(p, start); err != nil {
			return fmt.Errorf("check the values the checkpoint names in the log: %w", err)
		}
		for _, v := range vs[i:j] {
			if err := checkValue(p[v.off-start:v.off-start+int64(v.len)], v.off, v.sum); err != nil {
				return fmt.Errorf("%w: %w", ErrCorruptLog, err)
			}
		}
		i = j
	}
	return nil
}

// readLog reads the n bytes of a value at off, in the log or a
// checkpoint, into the store's scratch, which the next read overwrites.
// Nothing checks them: readValue does.
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
		if st.noWait {
			return val, readFileNoWait(f, val, off&ckptOffMask)
		}
		if _, err := f.ReadAt(val, off&ckptOffMask); err != nil {
			return nil, fmt.Errorf("read a value from the checkpoint: %w", err)
		}
		return val, nil
	}
	if st.log == nil {
		return nil, errors.New("the store has no log to read values from")
	}
	if r, ok := st.log.(noWaitReader); ok && st.noWait {
		return val, r.readNoWait(val, off)
	}
	if err := st.log.read(val, off); err != nil {
		return nil, fmt.Errorf("read a value from the log: %w", err)
	}
	return val, nil
}

// noWaitReader is a log that reads without waiting on the disk (the
// node's log, wal.readNoWait).
type noWaitReader interface {
	readNoWait(p []byte, off int64) error
}

// notReadNow reports whether a read of want bytes with RWF_NOWAIT that
// read n and failed with err must be made off the loop: the bytes are not
// in the page cache (EAGAIN, or a short read), or the file system takes
// no RWF_NOWAIT, as tmpfs refuses it with EOPNOTSUPP (#689's area-1
// review).
func notReadNow(n, want int, err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EOPNOTSUPP) || err == nil && n < want
}

// readFileNoWait reads p at off of checkpoint f from the page cache, or
// fails with errAway. A checkpoint in memory reads as ever.
func readFileNoWait(f ckptFile, p []byte, off int64) error {
	if leased, ok := f.(*leasedCkpt); ok {
		f = leased.ckptFile
	}
	d, ok := f.(interface{ Fd() uintptr })
	if !ok {
		_, err := f.ReadAt(p, off)
		return err
	}
	n, err := sysPreadNoWait(int(d.Fd()), p, off)
	switch {
	case notReadNow(n, len(p), err):
		return errAway
	case err != nil:
		return fmt.Errorf("read a value from the checkpoint: %w", err)
	}
	return nil
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
// the freed slots are taken again before the slab grows. The limit is at
// most half the slab, whose size is fixed (slabBytes): past it the slab
// filled before the records let go, and the node stopped (#776). The
// slab's top then has room to grow past the limit with freed slots of
// other sizes before shrinkSlab brings it down (topMax).
func (st *store) relimit() {
	st.limit = min(st.budget-st.goNeed()-(st.outside()-int64(st.slab.top)), int64(len(st.slab.mem))/2)
}

// goShare is the least of the budget kept for Go's heap and runtime: an
// eighth, and the entries Raft's storage holds.
func goShare(budget int64) int64 { return budget/8 + heldFor(budget) }

// goNeed is what Go's heap and runtime need: the live heap the collector
// last found and a quarter more for it to collect in, and at least Go's
// share. Below its live heap, Go would collect without end.
func (st *store) goNeed() int64 { return max(goShare(st.budget), st.goLive+st.goLive/4) }

// outside is the memory the node maps outside Go's heap, which Go's memory
// limit does not count: the rings and stages, the store's tables and its
// slab up to the highest slot handed out.
func (st *store) outside() int64 {
	return st.mapped + int64(st.nkeys)*int64(unsafe.Sizeof(keyEnt{})) + int64(st.nhist)*int64(unsafe.Sizeof(histEnt{})) +
		int64(len(st.hash))*8 + int64(st.kmem.top) + int64(st.slab.top)
}

// goLimit is the memory limit Go's runtime gets: what the budget leaves
// once the memory outside Go's heap is counted, and never less than Go
// needs. Go's limit counts only its own memory; at the whole budget, Go's
// heap grew on top of the store's tables, and the nodes swapped (#602).
func (st *store) goLimit() int64 {
	return max(st.budget-st.outside(), st.goNeed())
}

// shrinkSlab gives the slab's memory past its limit back to the system,
// once its top grew past the limit by a 32nd of the budget. A freed slot
// stays mapped and resident, so the slab kept the most it ever held while
// the tables, which grow with the keys, lowered its limit: a node with a
// 560 MiB budget, past-ram's, reached 658 MB and its cgroup killed it.
// The records whose slots end past the page the limit falls in are let
// go, as fit lets records go, and built again from the log when read; the
// slab's top comes down to that page, and the pages past it go back.
func (st *store) shrinkSlab() {
	if st.budget == 0 || int64(st.slab.top) <= st.topMax() {
		return
	}
	to := (max(int(st.limit), minClass) + pageSize - 1) &^ (pageSize - 1)
	for i := range st.keys[:st.nkeys] {
		k := &st.keys[i]
		if k.rec != 0 && slotEnd(k.rec, int(k.rlen)) > to {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
			st.evictions++
		}
	}
	st.slab.shrink(to)
	st.shrinks++
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
	if b == nil {
		b = make([]byte, 0, kvSize(len(key), len(value), create, mod, ver, lease))
	}
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
		st.nfree--
	} else {
		h = st.nhist
		if h+1 >= maxHist {
			return errHistoryFull
		}
		st.nhist++
	}
	st.hist[h] = histEnt{
		create: k.create, mod: k.mod, ver: k.ver, lease: k.lease,
		voff: k.voff, vlen: k.vlen, vsum: k.vsum, prev: k.hist, deleted: k.create == 0,
	}
	st.histBytes += int64(k.vlen)
	k.hist = h
	return nil
}

// put writes value under key id at revision rev; voff is where the value
// lies in the log. It returns the old record, if the key was live, which
// stays valid until the apply ends.
//
// With wantPrev it returns the key's record before, valid until the
// apply ends; without, it reads none: a put over a record the budget let
// go of read its value back from the disk on every node, which nothing
// but prevKV, ignore_value, ignore_lease or a watcher asks for (#689's
// area-3 review).
func (st *store) put(id uint32, value []byte, lease int64, rev int64, voff int64, wantPrev bool) (prev []byte, err error) {
	k := &st.keys[id]
	if wantPrev {
		if prev, err = st.live(id); err != nil {
			return nil, err
		}
	}
	live := k.create != 0
	key := st.key(id)
	create, ver := rev, int64(1)
	if live {
		create, ver = k.create, k.ver+1
	} else if planted.On("B09b") && k.hist != 0 && !st.hist[k.hist].deleted {
		// Planted: a key created again after a delete keeps its old
		// version and create revision.
		create, ver = st.hist[k.hist].create, st.hist[k.hist].ver+1
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
	if k.rec != 0 {
		st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
	}
	appendKV(st.slab.mem[off:off:int(off)+n], key, value, create, rev, ver, lease)
	old := int64(0)
	if live {
		old = k.lease
	}
	st.setLease(id, old, lease)
	if live {
		st.liveBytes -= int64(k.vlen)
	} else {
		st.liveKeys++
	}
	st.liveBytes += int64(len(value))
	k.rec, k.rlen = off, uint32(n)
	k.create, k.mod, k.ver, k.lease = create, rev, ver, lease
	k.voff, k.vlen, k.vsum = voff, uint32(len(value)), valueSum(value)
	st.changes = append(st.changes, change{rev: rev, key: id})
	return prev, nil
}

// del deletes key id at revision rev and reports whether it was live;
// with wantPrev, it returns its record too, which stays valid until the
// apply ends. Without, it reads nothing: a delete of a range or a lease's
// keys that nothing asks the values of read every value the budget had
// let go of back into the slab, held to the apply's end, past the budget
// on every node (#689's review); etcd reads none either.
func (st *store) del(id uint32, rev int64, wantPrev bool) ([]byte, bool, error) {
	k := &st.keys[id]
	if k.create == 0 {
		return nil, false, nil
	}
	var prev []byte
	if wantPrev {
		var err error
		if prev, err = st.live(id); err != nil {
			return nil, false, err
		}
	}
	if err := st.pushHist(k); err != nil {
		return nil, false, err
	}
	if k.rec != 0 {
		st.freeLater = append(st.freeLater, k.rec, uint64(k.rlen))
	}
	st.setLease(id, k.lease, 0)
	st.liveBytes -= int64(k.vlen)
	st.liveKeys--
	k.rec, k.rlen = 0, 0
	k.create, k.mod, k.ver, k.lease = 0, rev, 0, 0
	k.voff, k.vlen, k.vsum = 0, 0, 0
	st.changes = append(st.changes, change{rev: rev, key: id, del: true})
	return prev, true, nil
}

// endApply frees the records an apply replaced, keeps the rest within
// the budget, and goes on with a compaction's trim.
func (st *store) endApply() {
	for i := 0; i < len(st.freeLater); i += 2 {
		st.slab.free(st.freeLater[i], int(st.freeLater[i+1]))
	}
	st.freeLater = st.freeLater[:0]
	st.keepWithin()
	st.trimSteps(trimStep)
}

// keepWithin lets go of records past the slab's limit and gives back the
// memory past it. An apply ends with it, and so does each turn of the
// loop: a read builds the records it finds let go again (load), and a node
// that serves reads alone grew its slab past its budget (past-ram's cache
// mode). Neither holds a record's bytes past its end: a reply copies them.
func (st *store) keepWithin() {
	if st.budget == 0 {
		return
	}
	st.relimit()
	if st.slab.used <= st.limit && int64(st.slab.top) <= st.topMax() {
		return
	}
	st.fit()
	st.shrinkSlab()
}

// topMax is how far the slab's top may grow past its records' limit
// before shrinkSlab brings it down: a 32nd of the budget, and at most
// three quarters of the slab. A freed slot is taken again only for a
// record of its own size, so records of changing sizes grow the top
// while the records stay within the limit, and without the bound filled
// a slab whose limit the budget let pass it (#776).
func (st *store) topMax() int64 {
	return min(st.limit+st.budget/32, int64(len(st.slab.mem))*3/4)
}

// trimStep is the most keys one apply trims the history of.
const trimStep = 4096

// compact drops what no read at rev or later needs: the change index
// before it at once, and the key histories a few keys at a time after
// each apply.
func (st *store) compact(rev int64) {
	st.prevCompactRev, st.compactRev, st.compacted = st.compactRev, rev, true
	st.retrim()
}

// trimRev is the revision whose reads trimming keeps what they need: the
// compacted one, or the one a checkpoint being written holds, or the
// lowest a range reply sent in pieces reads at.
func (st *store) trimRev() int64 {
	c := st.ckptRev()
	// From the revision before the lowest held, so that the states written
	// at it stay, a put its own Txn deleted too (the re-check of
	// e710ebb9).
	if len(st.readHolds) > 0 {
		c = min(c, st.readHoldMin-1)
	}
	if len(st.trimHolds) > 0 {
		c = min(c, st.trimHoldMin-1)
	}
	return c
}

// ckptRev is the revision a checkpoint keeps the states of: the
// compacted one, or the one a checkpoint being written holds. Range
// replies' holds keep more in the store, but not in a checkpoint's keys:
// it only takes in their values (takeStates).
func (st *store) ckptRev() int64 {
	if st.holding {
		return min(st.compactRev, st.holdRev)
	}
	return st.compactRev
}

// holdRead keeps what a read at rev needs until releaseRead(rev): its
// keys' states, the tree's keys live at rev, and their ids. A checkpoint
// takes in the values of the states a held read sees (takeStates).
func (st *store) holdRead(rev int64) {
	if len(st.readHolds) == 0 || rev < st.readHoldMin {
		st.readHoldMin = rev
	}
	if st.readHolds == nil {
		st.readHolds = map[int64]int{}
	}
	st.readHolds[rev]++
	if st.readHolds[rev] == 1 {
		st.heldRevs = st.heldRevs[:0]
	}
}

func (st *store) releaseRead(rev int64) { st.releaseReads(rev) }

// releaseReads releases a hold of each of revs (holdRead).
func (st *store) releaseReads(revs ...int64) {
	before := st.trimRev()
	for _, rev := range revs {
		c := st.readHolds[rev]
		switch c {
		case 0:
		case 1:
			delete(st.readHolds, rev)
			st.heldRevs = st.heldRevs[:0]
		default:
			st.readHolds[rev] = c - 1
		}
	}
	if len(st.readHolds) == 0 {
		st.readHolds, st.heldRevs = nil, nil // a map never shrinks: an empty one lets go
	}
	_, ok := st.readHolds[st.readHoldMin]
	if !ok && len(st.readHolds) > 0 {
		st.readHoldMin = math.MaxInt64
		for rev := range st.readHolds {
			st.readHoldMin = min(st.readHoldMin, rev)
		}
	}
	if st.trimRev() > before {
		st.retrim()
	}
}

// holdTrim keeps trimming from rev until releaseTrim(rev), as holdRead
// does, but a checkpoint takes in nothing for it: a reply waiting for
// the reserve holds its records' least revision so, and names the states
// it reads (holdState). Held as a read, each such reply made every key
// give up its state at that revision (the re-check of 9b538554).
func (st *store) holdTrim(rev int64) {
	if len(st.trimHolds) == 0 || rev < st.trimHoldMin {
		st.trimHoldMin = rev
	}
	if st.trimHolds == nil {
		st.trimHolds = map[int64]int{}
	}
	st.trimHolds[rev]++
}

func (st *store) releaseTrim(rev int64) {
	before := st.trimRev()
	c := st.trimHolds[rev]
	if c > 1 {
		st.trimHolds[rev] = c - 1
	} else {
		delete(st.trimHolds, rev)
	}
	if len(st.trimHolds) == 0 {
		st.trimHolds = nil // a map never shrinks: an empty one lets go
	}
	_, ok := st.trimHolds[st.trimHoldMin]
	if !ok && len(st.trimHolds) > 0 {
		st.trimHoldMin = math.MaxInt64
		for r := range st.trimHolds {
			st.trimHoldMin = min(st.trimHoldMin, r)
		}
	}
	if st.trimRev() > before {
		st.retrim()
	}
}

// heldState is a key's state by the revision of its put.
type heldState struct {
	id  uint32
	mod int64
}

// heldStateSize is what a held state takes, about: its entry in
// heldStates, and in a checkpoint's list of the values it takes in.
const heldStateSize = 88

// holdState keeps key id's state put at mod for a checkpoint to take in
// until releaseState: trimming keeps it by a hold of a revision it is
// read at (holdRead).
func (st *store) holdState(id uint32, mod int64) {
	if st.heldStates == nil {
		st.heldStates = map[heldState]int{}
	}
	st.heldStates[heldState{id, mod}]++
	st.heldStatesPeak = max(st.heldStatesPeak, len(st.heldStates))
}

func (st *store) releaseState(id uint32, mod int64) {
	k := heldState{id, mod}
	c := st.heldStates[k]
	if c > 1 {
		st.heldStates[k] = c - 1
	} else {
		delete(st.heldStates, k)
	}
	switch {
	case len(st.heldStates) == 0:
		st.heldStates, st.heldStatesPeak = nil, 0 // a map never shrinks: an empty one lets go
	case st.heldStatesPeak > 1024 && len(st.heldStates) < st.heldStatesPeak/4:
		// Mostly emptied, it is made again at its size (the invariant
		// re-check, Astra: one state left of 200,000 kept 7 MB).
		m := make(map[heldState]int, len(st.heldStates))
		maps.Copy(m, st.heldStates)
		st.heldStates, st.heldStatesPeak = m, len(m)
	}
}

// stateHeld reports whether key id's state put at mod is held
// (holdState).
func (st *store) stateHeld(id uint32, mod int64) bool {
	c := st.heldStates[heldState{id, mod}]
	return c > 0
}

// heldReads is how many holds there are (holdRead, holdTrim).
func (st *store) heldReads() int {
	n := 0
	for _, c := range st.readHolds {
		n += c
	}
	for _, c := range st.trimHolds {
		n += c
	}
	return n
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
// the compacted revision or later needs neither. A checkpoint being
// written keeps a deletion at the revision it holds. A key deleted at or
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
			if e.deleted && (!st.holding || e.mod < c) {
				cut, *link = h, 0
			} else {
				// A checkpoint being written holds a deletion at the
				// revision it holds trimming to: it may be the key's newest
				// state at the checkpoint's revision, which a put since
				// made history (ckptStates, #712).
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
		st.nfree++
		cut = next
	}
	if k.create == 0 && k.hist == 0 && k.mod <= c {
		if k.inTree {
			st.tree.remove(id)
			k.inTree = false
		}
		if k.mod < c || planted.On("B08") {
			// Planted (B08): the tombstone at the compacted revision
			// goes too, and a watch from there misses its delete.
			st.freeKey(id)
		}
	}
}

// at returns key id's record as it was at revision rev, built from the
// history and the log if it changed since, or nil if it was not live.
func (st *store) at(id uint32, rev int64) ([]byte, error) {
	k := &st.keys[id]
	if rev == 0 || k.mod <= rev || planted.On("B31") {
		// Planted (B31): a lookup that ignores the revision returns the
		// value that overwrote the one asked for.
		if b := st.legacyView; b != nil && b.material {
			if rec, err, ok := b.canonicalRecord(id, k.mod); ok {
				return rec, err
			}
		}
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
		if b := st.legacyView; b != nil {
			if rec, err, ok := b.canonicalRecord(id, e.mod); ok {
				return rec, err
			}
		}
		val, err := st.readValueRef(id, e.mod, e.voff, e.vlen, e.vsum)
		if err != nil {
			return nil, err
		}
		return appendKV(nil, st.key(id), val, e.create, e.mod, e.ver, e.lease), nil
	}
	return nil, nil
}

// keyAt is at without the value, for a range of keys only: a record in
// memory is taken as it is, and one the budget let go of, or a value of
// the history, is built from the key's state without its value, which
// may lie only on disk (#689's area-3 review).
func (st *store) keyAt(id uint32, rev int64) []byte {
	k := &st.keys[id]
	if rev == 0 || k.mod <= rev || planted.On("B31") {
		switch {
		case k.create == 0:
			return nil
		case k.rec != 0:
			return st.slab.bytes(k.rec, int(k.rlen))
		}
		return appendKV(nil, st.key(id), nil, k.create, k.mod, k.ver, k.lease)
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if e.mod > rev {
			continue
		}
		if e.deleted {
			return nil
		}
		return appendKV(nil, st.key(id), nil, e.create, e.mod, e.ver, e.lease)
	}
	return nil
}

// stateAt returns key id's state at revision rev, as at finds it, without
// its value; ok is false if the key was not live then.
func (st *store) stateAt(id uint32, rev int64) (s ckptState, ok bool) {
	k := &st.keys[id]
	if rev == 0 || k.mod <= rev || planted.On("B31") {
		if k.create == 0 {
			return s, false
		}
		return ckptState{create: k.create, mod: k.mod, ver: k.ver, lease: k.lease, voff: k.voff, vlen: k.vlen, vsum: k.vsum}, true
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if e.mod > rev {
			continue
		}
		if e.deleted {
			return s, false
		}
		return ckptState{create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen, vsum: e.vsum}, true
	}
	return s, false
}

// liveAt reports whether key id was live at revision rev, as at finds
// it, but from its state alone: a range counts keys it does not return
// without reading their values, which may lie only on disk.
func (st *store) liveAt(id uint32, rev int64) bool {
	k := &st.keys[id]
	if rev == 0 || k.mod <= rev || planted.On("B31") {
		return k.create != 0
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		if e := &st.hist[h]; e.mod <= rev {
			return !e.deleted
		}
	}
	return false
}

// putAt returns the record key id was put as at revision rev: the newest
// live state written at rev. A Txn can delete the key later in the same
// revision, which leaves its newest state at rev a tombstone.
func (st *store) putAt(id uint32, rev int64) ([]byte, error) {
	k := &st.keys[id]
	if k.mod == rev && k.create != 0 {
		return st.live(id)
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if e.mod > rev || e.deleted {
			continue
		}
		if e.mod < rev {
			break
		}
		if b := st.legacyView; b != nil {
			if rec, err, ok := b.canonicalRecord(id, e.mod); ok {
				return rec, err
			}
		}
		val, err := st.readValueRef(id, e.mod, e.voff, e.vlen, e.vsum)
		if err != nil {
			return nil, err
		}
		return appendKV(nil, st.key(id), val, e.create, e.mod, e.ver, e.lease), nil
	}
	return nil, nil
}

// putStateAt is putAt's state without its value: that of the put of
// key id at revision rev, also where the same revision deleted the key
// after it.
func (st *store) putStateAt(id uint32, rev int64) (ckptState, bool) {
	s, _, ok := st.putStateAtSlot(id, rev)
	return s, ok
}

// putStateAtSlot also names the history slot it just resolved. That hint is
// valid only within this metadata turn, before any store mutation; it is not
// a retained source or a continuation cursor. Zero means the current key.
func (st *store) putStateAtSlot(id uint32, rev int64) (ckptState, uint32, bool) {
	k := &st.keys[id]
	if k.mod == rev && k.create != 0 {
		return ckptState{create: k.create, mod: k.mod, ver: k.ver, lease: k.lease, voff: k.voff, vlen: k.vlen, vsum: k.vsum}, 0, true
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if e.mod > rev || e.deleted {
			continue
		}
		if e.mod < rev {
			break
		}
		return ckptState{create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen, vsum: e.vsum}, h, true
	}
	return ckptState{}, 0, false
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
// recVersion is a record's version.
func recVersion(rec []byte) int64 {
	for m := rec; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return 0
		}
		if num == 4 {
			return int64(v)
		}
		m = rest
	}
	return 0
}

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
// holds, newest first: those up to rev that a read at revision c or later
// can see. A key such a read cannot see has none. A checkpoint's writer
// keeps the states from ckptRev, which a checkpoint being written may
// hold below the compacted revision, and takes in the values of a few
// older ones that held reads see (takeStates); stateHash takes what a
// read can see, from the compacted revision on.
func (st *store) ckptStates(id uint32, rev, c int64, out []ckptState) []ckptState {
	k := &st.keys[id]
	if k.klen == 0 {
		return out
	}
	// newest is set until a state at or before rev is taken: the key's
	// newest at rev. A put after rev, while a checkpoint at rev is being
	// written, makes a newer one in the store, and a deletion at c that is
	// the newest at rev must stay (#712).
	newest := true
	add := func(s ckptState) bool {
		if s.mod > rev {
			return true
		}
		atRev := newest
		newest = false
		// As trimming leaves it: the newest state at or before c, unless a
		// deletion; a key deleted at c keeps that deletion.
		if s.mod <= c {
			if !s.deleted || (atRev && s.mod == c) {
				out = append(out, s)
			}
			return false
		}
		out = append(out, s)
		return true
	}
	cur := ckptState{deleted: k.create == 0, create: k.create, mod: k.mod, ver: k.ver, lease: k.lease, voff: k.voff, vlen: k.vlen, vsum: k.vsum}
	if k.mod == 0 || !add(cur) {
		return out
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if !add(ckptState{deleted: e.deleted, create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen, vsum: e.vsum}) {
			break
		}
	}
	return out
}

// takeStates appends to out key id's states whose values a checkpoint at
// revision rev, keeping the states from c (ckptStates), takes in: those
// ckptStates gives, and each older one a read held below c sees
// (holdRead) or that a held reply reads (holdState), so that a held read's values outlive the checkpoint file and
// the log the checkpoint replaces (#689's re-check, D1 and D2). Only the
// states a hold sees: the history between, which trimming keeps for the
// oldest hold, is not taken in (the re-check of 7ab6166c, area A). A read
// at c-1 counts as held too: a watch sent in pieces holds the revision
// before its first event's (bigwatch.go), which may be the compacted one,
// and may come once the checkpoint scanned the key.
func (st *store) takeStates(id uint32, rev, c int64, out []ckptState) ([]ckptState, int) {
	n0, walked := len(out), 0
	out = st.ckptStates(id, rev, c, out)
	k := &st.keys[id]
	if k.klen == 0 || k.mod == 0 {
		return out, walked
	}
	if len(st.heldRevs) == 0 && len(st.readHolds) > 0 {
		for rev := range st.readHolds {
			st.heldRevs = append(st.heldRevs, rev)
		}
		slices.Sort(st.heldRevs)
	}
	// The states older than the newest at or before c, ckptStates's
	// last, newest first: each one is what a read sees from its revision
	// to the next state's.
	floor := c
	if len(out) > n0 {
		floor = out[len(out)-1].mod
	}
	kept := out[n0:]
	held := func(from, to int64) bool {
		// A state its own revision overwrote, a Txn's put of a key it
		// then deleted, is read at that revision (putAt): a watch's event,
		// a delete's prev_kv in a held reply (the re-check of e710ebb9).
		to = max(to, from+1)
		if from <= c-1 && c-1 < to || from == c {
			// A watch may come to hold c-1 and c once the key is scanned:
			// the state it sees at c-1, and a put at c its own Txn
			// deleted, which no read sees but its watch event (the
			// re-check of 9b538554).
			return true
		}
		i, _ := slices.BinarySearch(st.heldRevs, from)
		return i < len(st.heldRevs) && st.heldRevs[i] < to
	}
	least := c - 1
	if len(st.heldRevs) > 0 {
		least = min(least, st.heldRevs[0])
	}
	if len(st.trimHolds) > 0 {
		// The states held replies name lie from their least revision on.
		least = min(least, st.trimHoldMin)
	}
	next := int64(math.MaxInt64)
	visit := func(s ckptState) bool {
		walked++
		to := next
		next = s.mod
		if s.mod > floor || s.mod == floor && slices.ContainsFunc(kept, func(o ckptState) bool { return o.voff == s.voff && o.deleted == s.deleted }) {
			return true
		}
		if !s.deleted && s.vlen > 0 && (held(s.mod, to) || st.stateHeld(id, s.mod)) {
			out = append(out, s)
		}
		// Every state at the least held revision, a put its own Txn deleted
		// beside its deletion too (the re-check of 6c8b0fcc).
		return s.mod >= least
	}
	if !visit(ckptState{deleted: k.create == 0, create: k.create, mod: k.mod, ver: k.ver, lease: k.lease, voff: k.voff, vlen: k.vlen, vsum: k.vsum}) {
		return out, walked
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		e := &st.hist[h]
		if !visit(ckptState{deleted: e.deleted, create: e.create, mod: e.mod, ver: e.ver, lease: e.lease, voff: e.voff, vlen: e.vlen, vsum: e.vsum}) {
			break
		}
	}
	return out, walked
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
		k.voff, k.vlen, k.vsum = s.voff, s.vlen, s.vsum
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
		st.liveKeys++
	}
	return id, nil
}

// repoint names the values a checkpoint now holds by their place in it.
func (st *store) repoint(id uint32, from, to int64) {
	k := &st.keys[id]
	if k.voff == from {
		k.voff = to
	}
	for h := k.hist; h != 0; h = st.hist[h].prev {
		if e := &st.hist[h]; e.voff == from {
			e.voff = to
		}
	}
}

// close gives back the store's memory outside Go's heap and closes its
// checkpoint files.
func (st *store) close() error {
	// A closed store's mappings may be another's since: they are unmapped
	// once.
	if st.closed {
		return nil
	}
	st.closed = true
	for i, f := range st.ckpts {
		if f != nil {
			_ = f.Close()
			st.ckpts[i] = nil
		}
	}
	var errs []error
	for _, b := range [][]byte{st.kmem.mem, st.slab.mem} {
		errs = append(errs, unixMunmap(b))
	}
	errs = append(errs, munmapSlice(st.keys), munmapSlice(st.hash), munmapSlice(st.hist))
	return errors.Join(errs...)
}
