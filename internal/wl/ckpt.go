package wl

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Checkpoints. A checkpoint is the store as it was after an applied index
// I, at revision R, in one file: the leases, every key a read at the
// compacted revision or later can see with its states up to R and their
// values, and the changes after the compacted revision. It is written a
// few keys at a time on the loop while writes go on; a helper goroutine
// reads the values that lie on disk and writes the file. Each value it
// holds is then named by its place in the file, so the log before I is
// read no more: Raft's storage may drop those entries, and the log's
// segments before them are removed. A node starts from its newest
// checkpoint and the log after it, and a follower that lags further than
// Raft's storage keeps gets the leader's checkpoint as a Raft snapshot.
//
// The file is the magic, then the header: index, term, revision,
// compacted revision, the ConfState and the leases. Then records, each a
// kind byte: a key (its id in the writer's store, its name, and its
// states newest first), a change, or the end. A CRC-32C of everything
// before it closes the file. The pointer file, CURRENT, names the newest
// checkpoint, with what is the node's own: where its log goes on and the
// newest durable hard state.

// A value's offset with ckptBit set lies in a checkpoint, at the offset
// in its low bits, in the store's checkpoint of generation ckptGenOf.
const (
	ckptBit     = int64(1) << 62
	ckptGenBit  = int64(1) << 61
	ckptOffMask = ckptGenBit - 1
)

func inCkpt(off int64) bool { return off&ckptBit != 0 }

func ckptGenOf(off int64) int { return int(off>>61) & 1 }

func ckptOff(gen int, at int64) int64 { return ckptBit | int64(gen)<<61 | at }

var (
	ckptMagic = []byte("tsckpt\x00\x04")
	ptrMagic  = []byte("tsptr\x00\x00\x02")
)

const (
	ckptEnd    = 0
	ckptKey    = 1
	ckptChange = 2

	// A state's flags: the key was deleted; its value lies in the log at
	// the offset that follows, or in the file at the offset that follows.
	// A value with neither follows the state, in a checkpoint sent to a
	// follower.
	ckptDeleted = 1
	ckptInLog   = 2
	ckptInFile  = 4

	// What one turn encodes at most, and how many chunks wait for the
	// writer at most.
	ckptChunkBytes = 4 << 20
	ckptChunkKeys  = 4096
	ckptInflight   = 2

	ptrName = "CURRENT"
)

// ckptScanStates is the most states of keys a turn's key scan reads
// (scanKeys), past the key it is on: a key's states are all its history,
// and 4,096 keys of a store that never compacts took 220-300 ms of the
// loop (#704).
var ckptScanStates = 1 << 16

// ckptMin is the log written after the newest checkpoint before the next
// is taken. A checkpoint writes the keys and takes in only the values of
// the log it cuts, so it bounds what a start replays at a small cost.
var ckptMin int64 = 1 << 30

// ckptLoopShare bounds the loop's time on checkpoints: the next starts no
// sooner than this many times the loop's time encoding the last after
// it started. A checkpoint encodes every key and the history a read may
// see, so its time grows with the store, not with the writes since.
const ckptLoopShare = 40

// ckptMeta is a checkpoint's header.
type ckptMeta struct {
	index, term     uint64
	cs              *pb.ConfState
	members         ckptMembers
	rev, compactRev int64
	// compacted says whether a compaction applied, which tells one at
	// revision 0 from none (store.compacted).
	compacted bool
}

func ckptName(index uint64) string { return fmt.Sprintf("%016x.ckpt", index) }

// ckptState is one state of a key, as a checkpoint holds it.
type ckptState struct {
	deleted                 bool
	create, mod, ver, lease int64
	voff                    int64
	vlen, vsum              uint32
}

// ckptChunk is what one turn hands the writer: bytes encoded, then the
// values it copies after them, in order.
type ckptChunk struct {
	buf  []byte
	vals []ckptRel
	last bool
	ptr  *ckptPointer // with the last chunk: the pointer to write once the file is durable
	err  error
}

// ckptRel is a value the checkpoint takes in: the n bytes at src, in the
// log or in the checkpoint before, go to the file at dst. Key id's states
// that name src name dst once the checkpoint is done.
type ckptRel struct {
	src, dst int64
	n, sum   uint32
	id       uint32
}

// durableLog is a log whose durable part other goroutines may read.
type durableLog interface {
	durableEnd() int64
	readDurable(p []byte, off int64) error
}

// The writer reads the values it takes in, in the order of their place,
// a span at a time: values less than ckptGap apart share a read of at
// most ckptSpan bytes.
const (
	ckptSpan = 4 << 20
	ckptGap  = 256 << 10
)

// ckptWriter writes a checkpoint's chunks to its file in order. With the
// last chunk it adds the CRC, makes the file durable, gives it its name
// and points CURRENT at it.
type ckptWriter struct {
	sys            sysEnv
	dir, tmp, name string
	// f is the file it writes, which it makes, off the loop (#704); the
	// store takes it once the last chunk is back. unsynced counts the bytes
	// written since its last sync.
	f        file
	unsynced int64
	prev     ckptFile // the checkpoint before, which values may lie in
	crc      uint32
	log      durableLog
	in, out  chan *ckptChunk
	quit     chan struct{}
	done     chan struct{} // closed when run returns
	wake     func()
	span     []byte

	// For the stats line: time spent reading values and writing the
	// file, and their bytes.
	readNs, readBytes, writeNs, writeBytes atomic.Int64
}

func (cw *ckptWriter) run() {
	defer close(cw.done)
	for {
		var c *ckptChunk
		select {
		case c = <-cw.in:
		case <-cw.quit:
			cw.closeFile()
			return
		}
		c.err = cw.write(c)
		select {
		case cw.out <- c:
		case <-cw.quit:
			cw.closeFile()
			return
		}
		if cw.wake != nil {
			cw.wake()
		}
		if c.err != nil {
			cw.closeFile()
			return
		}
		if c.last {
			return
		}
	}
}

// closeFile closes the file of a checkpoint that the store will not take,
// once.
func (cw *ckptWriter) closeFile() {
	if cw.f != nil {
		_ = cw.f.Close()
		cw.f = nil
	}
}

// ckptSyncEvery is how much a checkpoint writes between syncs, so that it
// never leaves the kernel hundreds of MB to write back at once, on a disk
// the log's syncs share (#704).
const ckptSyncEvery = 32 << 20

func (cw *ckptWriter) write(c *ckptChunk) error {
	if cw.f == nil {
		f, err := cw.sys.openFile(filepath.Join(cw.dir, cw.tmp), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		cw.f = f
	}
	t0 := time.Now()
	if err := cw.readValues(c); err != nil {
		return err
	}
	t1 := time.Now()
	cw.readNs.Add(int64(t1.Sub(t0)))
	cw.crc = crc32.Update(cw.crc, castagnoli, c.buf)
	if c.last {
		c.buf = binary.LittleEndian.AppendUint32(c.buf, cw.crc)
	}
	if _, err := cw.f.Write(c.buf); err != nil {
		return fmt.Errorf("write a checkpoint: %w", err)
	}
	if cw.unsynced += int64(len(c.buf)); cw.unsynced >= ckptSyncEvery && !c.last {
		if err := cw.f.Sync(); err != nil {
			return fmt.Errorf("sync a checkpoint: %w", err)
		}
		cw.unsynced = 0
	}
	cw.writeNs.Add(int64(time.Since(t1)))
	cw.writeBytes.Add(int64(len(c.buf)))
	if !c.last {
		return nil
	}
	if err := cw.f.Sync(); err != nil {
		return fmt.Errorf("sync a checkpoint: %w", err)
	}
	select {
	case <-cw.quit:
		return errCkptAborted // the loop installs a snapshot or stops
	default:
	}
	if err := cw.sys.rename(filepath.Join(cw.dir, cw.tmp), filepath.Join(cw.dir, cw.name)); err != nil {
		return err
	}
	return writePointer(cw.sys, cw.dir, c.ptr)
}

// readValues appends the chunk's values to its bytes. They come sorted by
// their place, so near ones share one read.
func (cw *ckptWriter) readValues(c *ckptChunk) error {
	vs := c.vals
	for i := 0; i < len(vs); {
		start, end := vs[i].src, vs[i].src+int64(vs[i].n)
		j := i + 1
		for ; j < len(vs); j++ {
			v := vs[j]
			if inCkpt(v.src) != inCkpt(start) || v.src-end > ckptGap || v.src+int64(v.n)-start > ckptSpan {
				break
			}
			end = max(end, v.src+int64(v.n))
		}
		if int64(cap(cw.span)) < end-start {
			cw.span = make([]byte, end-start)
		}
		p := cw.span[:end-start]
		if inCkpt(start) {
			if cw.prev == nil {
				return fmt.Errorf("a value at %#x names a checkpoint the writer does not have", start)
			}
			if _, err := cw.prev.ReadAt(p, start&ckptOffMask); err != nil {
				return fmt.Errorf("read a value from a checkpoint: %w", err)
			}
		} else if err := cw.log.readDurable(p, start); err != nil {
			return err
		}
		cw.readBytes.Add(end - start)
		for _, v := range vs[i:j] {
			val := p[v.src-start : v.src-start+int64(v.n)]
			if err := checkValue(val, v.src, v.sum); err != nil {
				return err
			}
			c.buf = append(c.buf, val...)
		}
		i = j
	}
	return nil
}

// A checkpoint goes through phases: it scans the keys for the values it
// takes in, hands the writer its header and those values, then encodes
// the keys and the changes.
const (
	ckptScanning = iota
	ckptValues
	ckptKeys
)

// ckptRun is a checkpoint being written, the loop's side.
// ckptTurn is where a turn's checkpoint step spent its time, for the
// slow-turn line: the key scan, the cut and placing of the values the
// checkpoint takes in (with their count), and the encoding.
type ckptTurn struct {
	scan, place, encode time.Duration
	rels                int
}

type ckptRun struct {
	meta    ckptMeta
	gen     int
	logFrom int64 // where the log after I starts
	members []byte
	leases  []byte
	w       *ckptWriter
	phase   int
	at      int64  // the file offset of the next byte encoded
	next    uint32 // the next key id to scan or encode
	end     uint32
	chRev   int64 // the next change: the first of revision chRev, past chSkip of them
	chSkip  int
	sent    int // chunks handed to the writer and not back yet
	lastOut bool
	states  []ckptState

	// The values it takes in: those in the checkpoint before, and those
	// in the log before cut, sorted by their place. Values in the log
	// before prevFrom are candidates while it scans, with their bytes
	// counted by segment; the log before cut is removed once it is done.
	prevFrom int64
	prevCut  int64
	// snapWaiters are the clients' Snapshots that asked before it started.
	snapWaiters []handle
	cut         int64
	rels        []ckptRel
	segLive     map[int64]int64
	relAt       int // the next value to hand the writer

	// For the stats line: when it started, the loop's time encoding, and
	// the bytes of values it takes in and of those it leaves in the log.
	started          time.Time
	encodeNs         int64
	taken, leftInLog int64
}

// startCkpt starts a checkpoint of the store as it is now, after applied
// index index of term term; logFrom is where that entry's record starts
// in the log. The store keeps what it needs until it is done.
func (n *node) startCkpt(index, term uint64, logFrom int64) error {
	st := n.st
	gen := 1 - n.ckptGen
	n.simCheckpoint(index)
	if st.ckpts[gen] != nil {
		_ = st.ckpts[gen].Close()
		st.setCkpt(gen, nil)
	}
	name := ckptName(index)
	tmp := name + ".tmp"
	st.hold()
	r := &ckptRun{
		meta:     ckptMeta{index: index, term: term, cs: proto.Clone(n.confState).(*pb.ConfState), rev: st.rev, compactRev: st.compactRev, compacted: st.compacted},
		gen:      gen,
		logFrom:  logFrom,
		end:      st.nkeys,
		chRev:    st.compactRev,
		prevFrom: n.ckptFrom,
		prevCut:  n.ckptCut,
		segLive:  map[int64]int64{},
		started:  time.Now(),
	}
	r.leases = st.encodeLeases(nil)
	r.members = n.encodeMembers(nil)
	r.snapWaiters, n.snapWaiters = n.snapWaiters, nil
	dl, _ := st.log.(durableLog)
	r.w = &ckptWriter{
		sys: n.sys,
		dir: n.ckptDir, tmp: tmp, name: name, prev: st.ckpts[1-gen], log: dl,
		in: make(chan *ckptChunk, ckptInflight), out: make(chan *ckptChunk, ckptInflight),
		quit: make(chan struct{}), done: make(chan struct{}),
	}
	if n.shell != nil {
		r.w.wake = n.shell.wakeLoop
	}
	go r.w.run()
	n.ckpt = r
	n.stats.ckpts++
	n.logf("checkpoint started at index %d, revision %d: %d key ids, values %d MB live and %d MB history, the log from %d",
		index, st.rev, st.nkeys, st.liveBytes>>20, st.histBytes>>20, logFrom)
	return nil
}

// ckptStep takes back the chunks written and hands the writer the next;
// with block, it waits for one chunk to come back. It finishes the
// checkpoint once the last chunk is written.
func (n *node) ckptStep(block bool) error {
	r := n.ckpt
	if r == nil {
		return nil
	}
	for r.sent > 0 {
		var c *ckptChunk
		if block {
			c = <-r.w.out
			block = false
		} else {
			select {
			case c = <-r.w.out:
			default:
			}
		}
		if c == nil {
			break
		}
		r.sent--
		if c.err != nil {
			return c.err
		}
		if c.last {
			return n.ckptDone()
		}
	}
	t := time.Now()
	defer func() { r.encodeNs += int64(time.Since(t)) }()
	if r.phase == ckptScanning {
		n.scanKeys(r)
		n.turnCkpt.scan = time.Since(t)
		if r.phase == ckptScanning {
			return nil
		}
	}
	te := time.Now()
	defer func() { n.turnCkpt.encode = time.Since(te) }()
	for r.sent < ckptInflight && !r.lastOut {
		c, err := n.encodeChunk(r)
		if err != nil {
			return err
		}
		r.w.in <- c
		r.sent++
	}
	return nil
}

// The invariant of reads held across checkpoints (#689). A read may be
// held at a revision while a reply is sent in pieces or waits for the
// reserve: a large range at its revision, a large watch at its revision
// and the one before, a waiting reply at its records' least revision for
// trimming and at each record's own state (store.holdRead, holdTrim,
// holdState). Whatever the holds, at every moment:
//
//   - every read a holder makes, at every held revision, answers exactly
//     as the live store answered it when the hold was taken: each key's
//     state there, a watch's put events at its revision (a put its own Txn
//     deleted there included) and the values before them, each record of
//     a waiting reply;
//   - so does every read at the compacted revision or later;
//   - the newest checkpoint, with the log from its cut on, holds every
//     value those reads need: none lies only in a replaced checkpoint or
//     in the log before the cut;
//   - a store loaded from a checkpoint has the state hash of the store it
//     was taken of at its revision (stateHash): the values only holds
//     need lie in the file unnamed.
//
// TestHeldReadsSurviveCheckpointsAsAModelSays checks it against a model
// of the store's revisions, over random puts, deletes, Txns that put and
// delete one key, compactions, holds taken and released, checkpoints,
// which writes and holds may come in the middle of, and loads.

// scanKeys scans the next keys for the values the checkpoint takes in.
// Once all are scanned, it picks the log it cuts and places the values.
func (n *node) scanKeys(r *ckptRun) {
	st := n.st
	stop := min(r.end, r.next+ckptChunkKeys)
	for states := 0; r.next < stop && states < ckptScanStates; r.next++ {
		var walked int
		r.states, walked = st.takeStates(r.next, r.meta.rev, st.ckptRev(), r.states[:0])
		// What it walked counts, behind a hold a key's long history.
		states += max(len(r.states), walked)
		for i := range r.states {
			s := &r.states[i]
			switch {
			case s.vlen == 0:
			case inCkpt(s.voff):
				r.rels = append(r.rels, ckptRel{src: s.voff, n: s.vlen, sum: s.vsum, id: r.next})
			case s.voff < r.prevFrom:
				r.rels = append(r.rels, ckptRel{src: s.voff, n: s.vlen, sum: s.vsum, id: r.next})
				r.segLive[s.voff/ckptCutUnit] += int64(s.vlen)
			default:
				r.leftInLog += int64(s.vlen)
			}
		}
	}
	if r.next < r.end {
		return
	}
	t := time.Now()
	defer func() { n.turnCkpt.place, n.turnCkpt.rels = time.Since(t), len(r.rels) }()
	r.cut = cutPoint(r.segLive, r.prevCut, r.prevFrom, ckptCutUnit)
	k := 0
	for _, v := range r.rels {
		if inCkpt(v.src) || v.src < r.cut {
			r.rels[k] = v
			k++
		} else {
			r.leftInLog += int64(v.n)
		}
	}
	r.rels = r.rels[:k]
	slices.SortFunc(r.rels, func(a, b ckptRel) int { return cmp.Compare(a.src, b.src) })
	// A Put that keeps its value names the same bytes again, of the same
	// key: one copy serves both.
	r.rels = slices.CompactFunc(r.rels, func(a, b ckptRel) bool { return a.src == b.src })
	r.segLive = nil
	r.phase, r.next = ckptValues, 0
}

// cutPoint picks where the log is cut after a checkpoint, at a multiple
// of unit from from, where the log is cut already, up to prevFrom, the
// checkpoint before's place in it: the furthest place with at most a
// 1/ckptCutShare of the log before it still named by the store, which the
// checkpoint then takes in. segLive holds the bytes named, by unit. A log
// that the store still names more than that is kept: taking it in would
// copy more than the cut frees.
func cutPoint(segLive map[int64]int64, from, prevFrom, unit int64) int64 {
	first, c := from/unit, prevFrom/unit
	if c <= first {
		return from
	}
	segs := slices.Sorted(maps.Keys(segLive))
	live := int64(0)
	for _, s := range segs {
		if s < c {
			live += segLive[s]
		}
	}
	for i := len(segs) - 1; ; i-- {
		if live*ckptCutShare <= (c-first)*unit {
			return c * unit
		}
		for i >= 0 && segs[i] >= c {
			i--
		}
		if i < 0 {
			return from
		}
		c = segs[i]
		live -= segLive[c]
	}
}

// The log before the cut that the store still names is at most
// 1/ckptCutShare of it, counted in units of ckptCutUnit.
var (
	ckptCutShare int64 = 4
	ckptCutUnit  int64 = segSize
)

// ckptDone makes the checkpoint written the newest: the store names the
// values it took in there, Raft's storage hands it to followers that lag
// and may drop the entries before it, and the checkpoint before it is
// removed.
func (n *node) ckptDone() error {
	r := n.ckpt
	n.ckpt = nil
	old := n.ckptGen
	oldName := n.ckptName
	// The writer is done with the file: the store reads values from it now.
	n.st.setCkpt(r.gen, r.w.f)
	for _, v := range r.rels {
		n.st.repoint(v.id, v.src, ckptOff(r.gen, v.dst))
	}
	n.ckptGen, n.ckptFrom, n.ckptCut, n.ckptName = r.gen, r.logFrom, r.cut, r.w.name
	n.ckptNotBefore = r.started.Add(ckptLoopShare * time.Duration(r.encodeNs))
	n.storage.snap = &pb.Snapshot{
		Data:     []byte(r.w.name),
		Metadata: &pb.SnapshotMetadata{Index: new(r.meta.index), Term: new(r.meta.term), ConfState: r.meta.cs},
	}
	n.st.release()
	n.startClientSnaps(r.snapWaiters, r.w.name, r.meta)
	n.logf("checkpoint at index %d, revision %d: %d bytes, the log from %d and cut at %d, in %v; %d MB of values taken in, %d MB left in the log; the loop encoded for %v, the writer read for %v and wrote for %v",
		r.meta.index, r.meta.rev, r.at+4, r.logFrom, r.cut, time.Since(r.started).Round(time.Millisecond), r.taken>>20, r.leftInLog>>20,
		time.Duration(r.encodeNs).Round(time.Millisecond), time.Duration(r.w.readNs.Load()).Round(time.Millisecond), time.Duration(r.w.writeNs.Load()).Round(time.Millisecond))
	// A held read's values were taken in too (scanKeys), so nothing is
	// read from the replaced checkpoint now.
	if f := n.st.ckpts[old]; f != nil {
		_ = f.Close()
		n.st.setCkpt(old, nil)
	}
	if oldName != "" && oldName != n.ckptName {
		n.rm.remove(filepath.Join(n.ckptDir, oldName))
	}
	n.simCheckpointDone(r.meta.index, r.w.name)
	return nil
}

// abortCkpt stops a checkpoint being written, waits for its writer, and
// removes its file. Only a store that is replaced, or a node that stops,
// aborts one. A writer past its last check of quit may still name the
// checkpoint and point CURRENT at it: it is waited for, so that it never
// writes CURRENT alongside, or after, an install's pointer (#689's
// area-1 review, finding 1). That checkpoint is then whole, and stays
// while CURRENT may name it. Its file is closed here: a writer that wrote
// its last chunk leaves the file to ckptDone, which an abort never runs.
func (n *node) abortCkpt() {
	r := n.ckpt
	if r == nil {
		return
	}
	n.ckpt = nil
	close(r.w.quit)
	<-r.w.done
	r.w.closeFile()
	_ = n.sys.remove(filepath.Join(n.ckptDir, r.w.tmp))
	n.st.release()
}

// errCkptAborted is a checkpoint's write that stopped at its abort.
var errCkptAborted = errors.New("the checkpoint was aborted")

// encodeChunk encodes the checkpoint's next part: the header with the
// first values, the values, then the keys and the changes.
func (n *node) encodeChunk(r *ckptRun) (*ckptChunk, error) {
	st := n.st
	c := &ckptChunk{}
	if r.phase == ckptValues {
		if r.relAt == 0 {
			b, err := r.header()
			if err != nil {
				return nil, err
			}
			c.buf = b
			r.at = int64(len(b))
		}
		size := 0
		i := r.relAt
		for ; i < len(r.rels) && size < ckptChunkBytes; i++ {
			v := &r.rels[i]
			v.dst = r.at
			r.at += int64(v.n)
			r.taken += int64(v.n)
			size += int(v.n)
		}
		c.vals = r.rels[r.relAt:i]
		r.relAt = i
		if i == len(r.rels) {
			r.phase = ckptKeys
		}
		return c, nil
	}
	c.buf = make([]byte, 0, 1<<20)
	for keys, most := 0, cmp.Or(n.ckptKeys, ckptChunkKeys); r.next < r.end && keys < most && len(c.buf) < ckptChunkBytes; keys++ {
		n.encodeKey(r, c, r.next)
		r.next++
	}
	if r.next == r.end {
		r.encodeChanges(st, c)
	}
	if r.next == r.end && r.chRev > r.meta.rev {
		c.buf = append(c.buf, ckptEnd)
		c.last = true
		r.lastOut = true
		c.ptr = &ckptPointer{gen: r.gen, name: r.w.name, logFrom: r.logFrom, cut: r.cut}
		if n.wal != nil {
			c.ptr.hs = n.wal.durableHS
		}
	}
	r.at += int64(len(c.buf))
	return c, nil
}

// header is the file's start: the magic, index, term, revision,
// compacted revision, whether a compaction applied, the ConfState, the members, the leases, and the
// length of the values that follow.
func (r *ckptRun) header() ([]byte, error) {
	b := append([]byte(nil), ckptMagic...)
	b = binary.AppendUvarint(b, r.meta.index)
	b = binary.AppendUvarint(b, r.meta.term)
	b = binary.AppendUvarint(b, uint64(r.meta.rev))
	b = binary.AppendUvarint(b, uint64(r.meta.compactRev))
	b = binary.AppendUvarint(b, boolUvarint(r.meta.compacted))
	cs, err := proto.Marshal(r.meta.cs)
	if err != nil {
		return nil, err
	}
	b = binary.AppendUvarint(b, uint64(len(cs)))
	b = append(b, cs...)
	b = append(b, r.members...)
	b = append(b, r.leases...)
	vals := int64(0)
	for _, v := range r.rels {
		vals += int64(v.n)
	}
	return binary.AppendUvarint(b, uint64(vals)), nil
}

// encodeChanges encodes the changes from the compacted revision up to the
// checkpoint's, as many as the chunk takes.
func (r *ckptRun) encodeChanges(st *store, c *ckptChunk) {
	j := searchChanges(st.changes, r.chRev) + r.chSkip
	for ; j < len(st.changes) && len(c.buf) < ckptChunkBytes; j++ {
		ch := st.changes[j]
		if ch.rev > r.meta.rev {
			break
		}
		c.buf = append(c.buf, ckptChange)
		c.buf = binary.AppendUvarint(c.buf, uint64(ch.rev))
		c.buf = binary.AppendUvarint(c.buf, uint64(ch.key))
		del := byte(0)
		if ch.del {
			del = 1
		}
		c.buf = append(c.buf, del)
		// A change at the compacted revision may be of a key no read
		// sees, which the file does not hold: it names the key.
		var key []byte
		if ch.rev == r.meta.compactRev {
			key = st.key(ch.key)
		}
		c.buf = binary.AppendUvarint(c.buf, uint64(len(key)))
		c.buf = append(c.buf, key...)
		if ch.rev == r.chRev {
			r.chSkip++
		} else {
			r.chRev, r.chSkip = ch.rev, 1
		}
	}
	if j == len(st.changes) || st.changes[j].rev > r.meta.rev {
		r.chRev, r.chSkip = r.meta.rev+1, 0
	}
}

// encodeKey encodes key id with the states a read at the compacted
// revision or later, up to the checkpoint's, can see; a key with none is
// left out. Each state's value is named where it lies: in the file, as
// the checkpoint took it in, or in the log.
func (n *node) encodeKey(r *ckptRun, c *ckptChunk, id uint32) {
	st := n.st
	states := st.ckptStates(id, r.meta.rev, st.ckptRev(), r.states[:0])
	r.states = states
	if len(states) == 0 {
		return
	}
	key := st.key(id)
	c.buf = append(c.buf, ckptKey)
	c.buf = binary.AppendUvarint(c.buf, uint64(id))
	c.buf = binary.AppendUvarint(c.buf, uint64(len(key)))
	c.buf = append(c.buf, key...)
	c.buf = binary.AppendUvarint(c.buf, uint64(len(states)))
	for i := range states {
		s := &states[i]
		flags, at := uint64(0), s.voff
		if s.deleted {
			flags |= ckptDeleted
		}
		if s.vlen > 0 {
			if j, ok := slices.BinarySearchFunc(r.rels, s.voff, func(v ckptRel, off int64) int { return cmp.Compare(v.src, off) }); ok {
				flags, at = flags|ckptInFile, r.rels[j].dst
			} else {
				flags |= ckptInLog
			}
		}
		c.buf = binary.AppendUvarint(c.buf, flags)
		c.buf = binary.AppendUvarint(c.buf, uint64(s.create))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.mod))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.ver))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.lease))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.vlen))
		if s.vlen > 0 {
			c.buf = binary.AppendUvarint(c.buf, uint64(s.vsum))
			c.buf = binary.AppendUvarint(c.buf, uint64(at))
		}
	}
}

// encodeLeases appends the leases: a count, then each one's ID and TTL.
func (st *store) encodeLeases(b []byte) []byte {
	ids := slices.Sorted(maps.Keys(st.leases))
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = binary.AppendUvarint(b, uint64(id))
		b = binary.AppendUvarint(b, uint64(st.leases[id].ttl))
	}
	return b
}

// ---- loading

// ckptReader reads a checkpoint in order, with its offset and CRC.
type ckptReader struct {
	r   *bufio.Reader
	off int64
	crc uint32
	buf []byte
	// src, if set, is the file r reads, which keeps its read errors: a
	// read of the checkpoint that fails otherwise, as one past its end or
	// of a field too long, is damage (ErrCorruptCheckpoint).
	src *fileReader
}

// fileReader reads a file and keeps the first error of a read but the end.
type fileReader struct {
	r   io.Reader
	err error
}

func (fr *fileReader) Read(p []byte) (int, error) {
	n, err := fr.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && fr.err == nil {
		fr.err = err
	}
	return n, err
}

// readFailed reports whether err, from reading a checkpoint, is the
// disk's failure, not the file's end or damage.
func readFailed(err error) bool {
	return err != nil && !errors.Is(err, ErrCorruptCheckpoint) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)
}

// damage is err, an error of a read, as damage to the checkpoint, unless
// reading its file failed.
func (cr *ckptReader) damage(err error) error {
	if err == nil || cr.src == nil || cr.src.err != nil || errors.Is(err, ErrCorruptCheckpoint) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrCorruptCheckpoint, err)
}

func (cr *ckptReader) ReadByte() (byte, error) {
	b, err := cr.r.ReadByte()
	if err != nil {
		return 0, cr.damage(err)
	}
	cr.off++
	cr.crc = crc32.Update(cr.crc, castagnoli, []byte{b})
	return b, nil
}

func (cr *ckptReader) uvarint() (uint64, error) {
	v, err := binary.ReadUvarint(cr)
	return v, cr.damage(err)
}

// bytes reads n bytes, valid until the next call.
func (cr *ckptReader) bytes(n uint64) ([]byte, error) {
	if n > 1<<31 {
		return nil, cr.damage(fmt.Errorf("a checkpoint field of %d bytes", n))
	}
	if uint64(cap(cr.buf)) < n {
		cr.buf = make([]byte, n)
	}
	b := cr.buf[:n]
	if _, err := io.ReadFull(cr.r, b); err != nil {
		return nil, cr.damage(err)
	}
	cr.off += int64(n)
	cr.crc = crc32.Update(cr.crc, castagnoli, b)
	return b, nil
}

// skip reads past n bytes.
func (cr *ckptReader) skip(n uint64) error {
	for n > 0 {
		k := min(n, 1<<20)
		if _, err := cr.bytes(k); err != nil {
			return err
		}
		n -= k
	}
	return nil
}

// valueAt reads where a state's value of n bytes lies, by its flags: the
// log offset or the file offset that follows, or the bytes that follow,
// in the file of generation gen.
func (cr *ckptReader) valueAt(flags, n uint64, gen int) (int64, error) {
	if n == 0 {
		return 0, nil
	}
	if flags&(ckptInLog|ckptInFile) == 0 {
		at := ckptOff(gen, cr.off)
		_, err := cr.bytes(n)
		return at, err
	}
	at, err := cr.uvarint()
	if err != nil {
		return 0, err
	}
	if flags&ckptInFile != 0 {
		return ckptOff(gen, int64(at)), nil
	}
	return int64(at), nil
}

// ckptFile is a checkpoint file as a store reads it: a node's own file,
// or its bytes in the simulator (CheckpointStateHash).
type ckptFile interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	Close() error
	Name() string
}

// ErrCorruptCheckpoint is a checkpoint, or the pointer to it, that does
// not check: a node refuses to start on it.
var ErrCorruptCheckpoint = errors.New("wl: corrupt checkpoint")

// loadCkpt fills st, an empty store, from the checkpoint in f, whose
// values the store then reads from f as generation gen.
func loadCkpt(f ckptFile, gen int, st *store) (ckptMeta, error) {
	return loadCkptChecked(f, gen, st, true)
}

// loadCkptChecked is loadCkpt, which checks the file's CRC if check.
func loadCkptChecked(f ckptFile, gen int, st *store, check bool) (ckptMeta, error) {
	var m ckptMeta
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return m, err
	}
	src := &fileReader{r: f}
	cr := &ckptReader{r: bufio.NewReaderSize(src, 1<<20), src: src}
	magic, err := cr.bytes(uint64(len(ckptMagic)))
	if readFailed(err) {
		// The disk failed the read: not a corrupt checkpoint (#689, seed
		// 32 of TestMembershipFaults took an EIO for "no magic").
		return m, fmt.Errorf("%s: read the checkpoint: %w", f.Name(), err)
	}
	if err != nil || string(magic) != string(ckptMagic) {
		return m, fmt.Errorf("%s: %w: no magic", f.Name(), ErrCorruptCheckpoint)
	}
	var v [5]uint64
	for i := range v {
		if v[i], err = cr.uvarint(); err != nil {
			return m, err
		}
	}
	m.index, m.term, m.rev, m.compactRev, m.compacted = v[0], v[1], int64(v[2]), int64(v[3]), v[4] != 0
	csLen, err := cr.uvarint()
	if err != nil {
		return m, err
	}
	csb, err := cr.bytes(csLen)
	if err != nil {
		return m, err
	}
	m.cs = &pb.ConfState{}
	if err := proto.Unmarshal(csb, m.cs); err != nil {
		return m, cr.damage(err)
	}
	if m.members, err = cr.readMembers(); err != nil {
		return m, cr.damage(err)
	}
	nl, err := cr.uvarint()
	if err != nil {
		return m, err
	}
	for range nl {
		id, err := cr.uvarint()
		if err != nil {
			return m, err
		}
		ttl, err := cr.uvarint()
		if err != nil {
			return m, err
		}
		st.leases[int64(id)] = &lease{ttl: int64(ttl), keys: map[uint32]struct{}{}}
	}
	vals, err := cr.uvarint()
	if err != nil {
		return m, err
	}
	if err := cr.skip(vals); err != nil {
		return m, err
	}
	st.ckpts[gen] = f
	ids := []uint32{} // the store's id of each id in the file
	var states []ckptState
	// key holds a key's bytes while its states are read, which may read
	// more fields into cr's buffer; the store copies them (intern).
	var key []byte
	for {
		kind, err := cr.ReadByte()
		if err != nil {
			return m, err
		}
		switch kind {
		case ckptEnd:
			want := cr.crc
			sum, err := cr.bytes(4)
			if err != nil {
				return m, err
			}
			if binary.LittleEndian.Uint32(sum) != want && check {
				return m, fmt.Errorf("%s: %w: its CRC", f.Name(), ErrCorruptCheckpoint)
			}
			st.rev, st.compactRev, st.compacted = m.rev, m.compactRev, m.compacted
			if st.grew != nil {
				st.grew()
			}
			return m, nil
		case ckptKey:
			fid, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			kl, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			kb, err := cr.bytes(kl)
			if err != nil {
				return m, err
			}
			key = append(key[:0], kb...)
			ns, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			states = states[:0]
			for range ns {
				var s ckptState
				var f [6]uint64
				for i := range f {
					if f[i], err = cr.uvarint(); err != nil {
						return m, err
					}
				}
				s.deleted = f[0]&ckptDeleted != 0
				s.create, s.mod, s.ver, s.lease, s.vlen = int64(f[1]), int64(f[2]), int64(f[3]), int64(f[4]), uint32(f[5])
				if f[5] > 0 {
					sum, err := cr.uvarint()
					if err != nil {
						return m, err
					}
					s.vsum = uint32(sum)
				}
				if s.voff, err = cr.valueAt(f[0], f[5], gen); err != nil {
					return m, err
				}
				states = append(states, s)
			}
			id, err := st.restoreKey(key, states)
			if err != nil {
				return m, err
			}
			if st.grew != nil && id%loadGrowKeys == 0 {
				st.grew()
			}
			for uint64(len(ids)) <= fid {
				ids = append(ids, noKey)
			}
			ids[fid] = id
		case ckptChange:
			rev, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			fid, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			del, err := cr.ReadByte()
			if err != nil {
				return m, err
			}
			kl, err := cr.uvarint()
			if err != nil {
				return m, err
			}
			kb, err := cr.bytes(kl)
			if err != nil {
				return m, err
			}
			for uint64(len(ids)) <= fid {
				ids = append(ids, noKey)
			}
			if ids[fid] == noKey {
				if kl == 0 {
					return m, fmt.Errorf("%s: %w: a change of a key it does not hold", f.Name(), ErrCorruptCheckpoint)
				}
				if ids[fid], err = st.intern(kb); err != nil {
					return m, err
				}
			}
			st.changes = append(st.changes, change{rev: int64(rev), key: ids[fid], del: del != 0})
		default:
			return m, fmt.Errorf("%s: %w: a record of kind %d", f.Name(), ErrCorruptCheckpoint, kind)
		}
	}
}

const noKey = ^uint32(0)

// loadGrowKeys is how many keys a checkpoint's load takes between the
// calls of its store's grew.
const loadGrowKeys = 1 << 16

// ---- the pointer

// ckptPointer is CURRENT: the newest checkpoint, by its generation and
// name, where the log goes on after it, and the newest durable hard state
// when it was written.
type ckptPointer struct {
	gen     int
	name    string
	logFrom int64
	cut     int64 // the log before it is cut: the checkpoint holds what the store names there
	hs      *pb.HardState
}

// writePointer makes p dir's CURRENT. It syncs dir first: a crash keeps
// each directory operation since the last sync or not on its own, and
// one that kept CURRENT's rename but lost the rename that gave the
// checkpoint it names its name left a node unable to start (#711).
func writePointer(sys sysEnv, dir string, p *ckptPointer) error {
	if err := syncDir(sys, dir); err != nil {
		return err
	}
	hs := p.hs
	if hs == nil {
		hs = &pb.HardState{}
	}
	hb, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	b := append([]byte(nil), ptrMagic...)
	b = binary.AppendUvarint(b, uint64(p.gen))
	b = binary.AppendUvarint(b, uint64(len(p.name)))
	b = append(b, p.name...)
	b = binary.AppendUvarint(b, uint64(p.logFrom))
	b = binary.AppendUvarint(b, uint64(p.cut))
	b = binary.AppendUvarint(b, uint64(len(hb)))
	b = append(b, hb...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	tmp := filepath.Join(dir, ptrName+".tmp")
	if err := writeFileSynced(sys, tmp, b); err != nil {
		return err
	}
	if err := sys.rename(tmp, filepath.Join(dir, ptrName)); err != nil {
		return err
	}
	return syncDir(sys, dir)
}

func writeFileSynced(sys sysEnv, path string, b []byte) error {
	f, err := sys.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// mkdirSynced makes directory dir, if it is not there, and syncs the
// directory that holds it, so that a power loss keeps its entry: a log or
// checkpoint directory whose entry was lost loses everything in it,
// whatever was synced inside (#689's area-1 review).
func mkdirSynced(sys sysEnv, dir string) error {
	if err := sys.mkdirAll(dir); err != nil {
		return err
	}
	return syncDir(sys, filepath.Dir(dir))
}

func syncDir(sys sysEnv, dir string) error {
	d, err := sys.openFile(dir, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// readPointer reads dir's CURRENT, or returns nil if there is none.
func readPointer(sys sysEnv, dir string) (*ckptPointer, error) {
	b, err := sys.readFile(filepath.Join(dir, ptrName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parsePointer(b, filepath.Join(dir, ptrName))
}

// parsePointer reads b, the pointer file at path.
func parsePointer(b []byte, path string) (*ckptPointer, error) {
	bad := fmt.Errorf("%s: %w", path, ErrCorruptCheckpoint)
	if len(b) < len(ptrMagic)+4 || string(b[:len(ptrMagic)]) != string(ptrMagic) {
		return nil, bad
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, bad
	}
	p := &ckptPointer{hs: &pb.HardState{}}
	m := body[len(ptrMagic):]
	next := func() uint64 {
		v, k := binary.Uvarint(m)
		if k <= 0 {
			m = nil
			return 0
		}
		m = m[k:]
		return v
	}
	take := func(n uint64) []byte {
		if n > uint64(len(m)) {
			m = nil
			return nil
		}
		t := m[:n]
		m = m[n:]
		return t
	}
	p.gen = int(next() & 1)
	p.name = string(take(next()))
	p.logFrom = int64(next())
	p.cut = int64(next())
	hb := take(next())
	if m == nil || len(m) != 0 {
		return nil, bad
	}
	if err := proto.Unmarshal(hb, p.hs); err != nil {
		return nil, fmt.Errorf("%w: %w", bad, err)
	}
	return p, nil
}

// Pointer is what the simulator reads of a node's checkpoint pointer, the
// file ckpt/CURRENT under its data dir: the checkpoint it names, under
// ckpt/, and the first segment of the log, wal/<FirstSegment>.seg in %016x,
// that a start reads. A start reads no segment before it, though it keeps
// them for the values they hold.
type Pointer struct {
	Checkpoint   string
	FirstSegment int64
	LogFrom      int64 // where the log starts: the checkpoint left it there
}

// ReadPointer reads b, a node's ckpt/CURRENT.
func ReadPointer(b []byte) (Pointer, error) {
	p, err := parsePointer(b, ptrName)
	if err != nil {
		return Pointer{}, err
	}
	return Pointer{Checkpoint: p.name, FirstSegment: p.logFrom / segSize, LogFrom: p.logFrom}, nil
}

// cleanCkptDir removes what a node that stopped left half done in dir:
// checkpoints being written or received, and any but keep.
func cleanCkptDir(sys sysEnv, dir, keep string) error { return removeCkpts(sys, dir, keep, true) }

// removeCkpts removes dir's checkpoints but keep, and with tmp the files
// being written or received too.
func removeCkpts(sys sysEnv, dir, keep string, tmp bool) error {
	names, err := sys.readDir(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == ptrName || name == keep {
			continue
		}
		if strings.HasSuffix(name, ".ckpt") || (tmp && strings.HasSuffix(name, ".tmp")) {
			if err := sys.remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadNewest loads the node's newest checkpoint into its store and Raft's
// storage, if it has one, and returns where the log goes on after it;
// the log from n.ckptCut on holds values it names. It removes what a node
// that stopped left half done.
func (n *node) loadNewest() (int64, error) {
	if err := mkdirSynced(n.sys, n.ckptDir); err != nil {
		return 0, err
	}
	p, err := readPointer(n.sys, n.ckptDir)
	if err != nil {
		return 0, err
	}
	keep := ""
	if p != nil {
		keep = p.name
	}
	// CURRENT's rename may not be durable yet, if the node stopped right
	// after it: synced first, a power loss can no longer bring back the
	// pointer before, whose checkpoint the cleanup removes.
	if p != nil {
		if err := syncDir(n.sys, n.ckptDir); err != nil {
			return 0, err
		}
	}
	if err := cleanCkptDir(n.sys, n.ckptDir, keep); err != nil {
		return 0, err
	}
	if p == nil {
		return 0, nil
	}
	f, err := n.sys.openFile(filepath.Join(n.ckptDir, p.name), os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	m, err := loadCkpt(f, p.gen, n.st)
	if err != nil {
		return 0, err
	}
	n.storage.applySnapshot(&pb.SnapshotMetadata{Index: new(m.index), Term: new(m.term), ConfState: m.cs}, p.name)
	n.storage.hs = p.hs
	n.applied = m.index
	n.confState = m.cs
	if err := n.takeMembers(&m.members); err != nil {
		return 0, err
	}
	n.ckptGen, n.ckptFrom, n.ckptCut, n.ckptName = p.gen, p.logFrom, p.cut, p.name
	return p.logFrom, nil
}

// ---- a checkpoint a follower takes

// ckptHeader is what rewriteCkpt puts in place of a checkpoint's
// membership: a restored node's new cluster.
type ckptHeader struct {
	cs      *pb.ConfState
	members ckptMembers
}

// selfContained writes to dst the checkpoint in src with every value in
// it: a follower has neither this node's log nor its checkpoint before.
// Values the file names in the log are read from log.
func selfContained(src file, log durableLog, dst io.Writer) error {
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return copyCkpt(src, func(p []byte, inFile bool, at int64) error {
		if inFile {
			_, err := src.ReadAt(p, at)
			return err
		}
		return log.readDurable(p, at)
	}, dst, nil)
}

// rewriteCkpt copies the checkpoint r holds, every value in it, to dst
// with hdr's membership.
func rewriteCkpt(r io.Reader, dst io.Writer, hdr *ckptHeader) error {
	return copyCkpt(r, func([]byte, bool, int64) error {
		return fmt.Errorf("%w: a value it does not hold", ErrCorruptCheckpoint)
	}, dst, hdr)
}

// copyCkpt copies the checkpoint r holds to dst with every value in it:
// read reads a value it names in itself or in the log. With hdr, the
// ConfState and the members are hdr's.
func copyCkpt(r io.Reader, read func(p []byte, inFile bool, at int64) error, dst io.Writer, hdr *ckptHeader) error {
	cr := &ckptReader{r: bufio.NewReaderSize(r, 1<<20)}
	bw := bufio.NewWriterSize(dst, 1<<20)
	crc := uint32(0)
	out := func(b []byte) {
		crc = crc32.Update(crc, castagnoli, b)
		_, _ = bw.Write(b)
	}
	var tmp []byte
	uv := func() (uint64, error) {
		v, err := cr.uvarint()
		tmp = binary.AppendUvarint(tmp[:0], v)
		out(tmp)
		return v, err
	}
	copyBytes := func(n uint64) error {
		b, err := cr.bytes(n)
		out(b)
		return err
	}
	if err := copyBytes(uint64(len(ckptMagic))); err != nil {
		return err
	}
	for range 5 { // index, term, revision, compacted revision, whether compacted
		if _, err := uv(); err != nil {
			return err
		}
	}
	if hdr == nil {
		csLen, err := uv()
		if err != nil {
			return err
		}
		if err := copyBytes(csLen); err != nil {
			return err
		}
		cm, err := cr.readMembers()
		if err != nil {
			return err
		}
		n := &node{members: cm.members, removed: cm.removed, alarms: cm.alarms, downgrade: cm.downgrade, format: cm.format, formatEpoch: cm.formatEpoch, auth: cm.auth}
		out(n.encodeMembers(nil))
	} else {
		csLen, err := cr.uvarint()
		if err != nil {
			return err
		}
		if _, err := cr.bytes(csLen); err != nil {
			return err
		}
		cm, err := cr.readMembers()
		if err != nil {
			return err
		}
		cs, err := proto.Marshal(hdr.cs)
		if err != nil {
			return err
		}
		out(binary.AppendUvarint(nil, uint64(len(cs))))
		out(cs)
		// A new cluster: its members announce afresh. As etcd's restore,
		// it keeps the alarms, users and roles.
		n := &node{members: hdr.members.members, removed: hdr.members.removed, format: baseFormat, alarms: cm.alarms, auth: cm.auth}
		out(n.encodeMembers(nil))
	}
	nl, err := uv()
	if err != nil {
		return err
	}
	for range 2 * nl {
		if _, err := uv(); err != nil {
			return err
		}
	}
	vals, err := cr.uvarint()
	if err != nil {
		return err
	}
	out([]byte{0})
	if err := cr.skip(vals); err != nil {
		return err
	}
	var val []byte
	for {
		kind, err := cr.ReadByte()
		if err != nil {
			return err
		}
		if kind == ckptEnd {
			want := cr.crc
			sum, err := cr.bytes(4)
			if err != nil {
				return err
			}
			if binary.LittleEndian.Uint32(sum) != want {
				return fmt.Errorf("%w: its CRC", ErrCorruptCheckpoint)
			}
			out([]byte{ckptEnd})
			_, _ = bw.Write(binary.LittleEndian.AppendUint32(nil, crc))
			return bw.Flush()
		}
		out([]byte{kind})
		switch kind {
		case ckptKey:
			if _, err := uv(); err != nil {
				return err
			}
			kl, err := uv()
			if err != nil {
				return err
			}
			if err := copyBytes(kl); err != nil {
				return err
			}
			ns, err := uv()
			if err != nil {
				return err
			}
			for range ns {
				var f [6]uint64
				for i := range f {
					if f[i], err = cr.uvarint(); err != nil {
						return err
					}
				}
				at, sum := uint64(0), uint64(0)
				if f[5] > 0 {
					if sum, err = cr.uvarint(); err != nil {
						return err
					}
				}
				if f[5] > 0 && f[0]&(ckptInLog|ckptInFile) != 0 {
					if at, err = cr.uvarint(); err != nil {
						return err
					}
				}
				inLog, inFile := f[0]&ckptInLog != 0, f[0]&ckptInFile != 0
				f[0] &^= ckptInLog | ckptInFile
				for _, v := range f {
					tmp = binary.AppendUvarint(tmp[:0], v)
					out(tmp)
				}
				if f[5] == 0 {
					continue
				}
				tmp = binary.AppendUvarint(tmp[:0], sum)
				out(tmp)
				if !inLog && !inFile {
					if err := copyBytes(f[5]); err != nil {
						return err
					}
					continue
				}
				if uint64(cap(val)) < f[5] {
					val = make([]byte, f[5])
				}
				v := val[:f[5]]
				if err := read(v, inFile, int64(at)); err != nil {
					return fmt.Errorf("read a value for a follower's checkpoint: %w", err)
				}
				if err := checkValue(v, int64(at), uint32(sum)); err != nil {
					return err
				}
				out(v)
			}
		case ckptChange:
			for range 2 {
				if _, err := uv(); err != nil {
					return err
				}
			}
			if err := copyBytes(1); err != nil {
				return err
			}
			kl, err := uv()
			if err != nil {
				return err
			}
			if err := copyBytes(kl); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: a record of kind %d", ErrCorruptCheckpoint, kind)
		}
	}
}
