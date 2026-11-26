package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/planted"
)

// The log ring (#602, ruled 2026-11-18). The log's space is to be written
// once, when the data directory is made, and reused in place, so that
// a synced write never meets space the file system must first convert
// (on ext4, 308 MiB/s at three writers of 256 KiB, against 343 MiB/s
// into space written before, the disk the ceilings measure). Its rules,
// which the changes towards it keep at every step:
//
//  1. No stale record is read as current. A record's checksum is seeded
//     by its offset in the log, which grows without end: the ring's lap
//     and the record's place in it. Bytes that an earlier lap, or another
//     place, left behind check where the log reads them no more often
//     than damage does, one time in 2^32, so they end the log as the
//     zeros past its end do, which never check: a record's kind is never
//     0.
//  2. No acknowledged write is lost across reuse and crash. Space is
//     written over only once the newest durable checkpoint holds all
//     that the store still names before it: the cut takes those values
//     in.
//  3. The ring's space stays within the disk budget.
//
// This part is rule 1's format.
//
// The log. Raft's appends are written as records into a page-aligned
// stage on the loop; one helper thread writes the stage's blocks to the
// log's segments with O_DIRECT|O_DSYNC and wakes the loop through an
// eventfd, and the append's responses are stepped once it is durable.
//
// Offsets in the log grow without end. Offset o lies in segment
// o/segSize, a file of its own. Without a ring each write grows it and
// nothing in the log is overwritten; with one, the segment is a file of
// the ring's reused in place (logring.go). The store names values by
// their log offset, and reads an old or let-go value back from the stage
// while it may not be on disk, else from its segment. Once a checkpoint
// holds what the segments before it held, they are cut: removed, or a
// ring's given back to its pool.
//
// A record is [4 crc32c][4 body length][1 kind], then the body; the crc
// covers everything after it, seeded by the record's offset in the log
// (recSeed), so a record checks only where it was written. An entry's
// body is [8 index][8 term][1 entry type] and the entry's data; a hard
// state's is the state's protobuf. A record that does not check ends the log: a crash tore it,
// or it is the zeros past the end. It is damage, and the log is refused
// (ErrCorruptLog), where a crash cannot have torn it: before a later
// segment that holds data, as each write's part in a segment is durable
// before the next segment's part is written; or before a record that
// checks, as a crash leaves none after the one it tore. But a write torn
// at sectors keeps some sectors past one it lost, which hold zeros (the
// stage clears the bytes past the log's end), so in the last segment a
// record with a sector of zeros, as etcd's isTornEntry says, ends the log
// whatever follows it. Damage in the last segment that no record after it
// shows, such as a flipped bit of a length, is not found.

const (
	block   = 4096
	syncGap = 800 * time.Microsecond
	// A sync that wrote loadBytes or more is followed by the next no
	// sooner than loadGap after it started (wal.gap).
	loadGap   = 4 * time.Millisecond
	loadBytes = 64 << 10

	recHead      = 4 + 4 + 1
	entHead      = 8 + 8 + 1
	walEntHdr    = recHead + entHead
	recEntry     = 1
	recHardState = 2
	// recSynced, a ring's, says the log is durable before its offset,
	// its body's 8 bytes (logring.go).
	recSynced     = 3
	maxWALRecBody = 1 << 30
	sector        = 512
)

// ErrCorruptLog is a log with a record that does not check where no crash
// can have torn it (wal.go's head).
var ErrCorruptLog = errors.New("wl: corrupt log")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type wal struct {
	sys sysEnv
	// prom, once set, takes each sync's time: a write is durable when
	// it returns, so it is etcd's WAL fsync and backend commit.
	prom atomic.Pointer[metrics.Set]
	dir  string
	off  int64 // the log's end, where the next record goes

	iov     [][]byte
	hdrs    []byte
	pending []walJob // appends written to the stage, by end offset
	stable  int64    // the end of the appends whose responses are stepped
	// durableHS is the newest hard state the log holds durably.
	durableHS *pb.HardState
	waiting   []*pb.Message // appends kept while the stage is full
	// progressAt is when the disk last made an append durable.
	progressAt time.Time
	offs       []int64 // where the last write's entries have their data

	// stage mirrors the newest part of the log in page-aligned memory:
	// the loop copies each append there, and the helper writes its blocks
	// to the segments, so the kernel neither copies the log into its page
	// cache nor writes it back.
	stage    []byte
	stageLen int64 // len(stage), a power of two
	inflight bool
	want     atomic.Int64 // the loop's: the helper writes up to here
	done     atomic.Int64 // the helper's: the log is durable up to here
	kickFD   int          // the helper waits on it
	wakeFD   int          // the loop waits on it
	// lastKick is when the last sync started, and lastBytes what it
	// wrote; deferred, that appends wait for the next one at lastKick +
	// gap().
	lastKick  time.Time
	lastBytes int64
	deferred  bool
	// skippedAt is when the first record no sync took was written, a
	// leader's commit skipped (appendNow): one syncs it commitHold later
	// at most (kick).
	skippedAt time.Time
	// failed is the helper's error; it stops writing then.
	failed atomic.Pointer[error]

	// Segments: the helper writes through wfds, which it alone uses; rfds
	// read them, and grows as segments are made.
	made       chan segment
	stop       chan struct{} // closed by close
	madeDone   chan struct{}
	helperDone chan struct{}
	closing    atomic.Bool
	closeOnce  sync.Once
	wfds       []int // from segment wbase on
	wbase      int64
	mu         sync.Mutex
	rfds       []int // from segment rbase on
	rbase      int64
	// readers counts the reads of rfds under way off the loop; while
	// any is, cut leaves the descriptors it lets go in unused, and the
	// last reader closes them: a descriptor closed under a read could
	// be taken by another file before the read.
	readers int
	unused  []int

	stats walStats
	// The helper's time in its writes, for the stats line: how long the
	// disk takes the log's blocks.
	writeNs atomic.Int64
	// lastSyncNs is how long the helper's last sync took in its own
	// writes, which the loop logs when it is slow (node.fsyncDone).
	lastSyncNs atomic.Int64

	// The ring (logring.go), nil for a log with none: spares are its files
	// that hold no segment, freed the segments a cut let go of, which the
	// maker renames to spares (freedCh wakes it). How many segments are
	// free the loop counts itself (ringFree).
	ring    *ringInfo
	spares  []string
	nextSp  int64 // the next spare's number (spareSeq)
	freed   []int64
	freedCh chan struct{}
	zeroBuf []byte
	// formatted is how long this start took to format the ring, 0 if the
	// log had one: the node logs it.
	formatted time.Duration
	// dataEnd is where the log's last record but a sync's mark, or a hard
	// state that only moved the commit (commitOnly), ends, and
	// claimed what the newest mark says is durable (appendSynced): a ring
	// syncs a mark alone once the log goes idle with its last records
	// unclaimed (markIdle).
	dataEnd, claimed int64
	// wanted is the room a write or an append refused for want of it
	// asked for (ringRefuses, leaderDrops, followerDrops), 0 once one
	// fits: the ring is low while it lacks it.
	wanted int64
}

type walStats struct {
	waits  uint64 // appends that waited for room in the stage
	syncs  uint64
	writes uint64
	bytes  uint64
	// marks are the syncs of a mark alone (markIdle), stateOnly the syncs
	// for appends that carried no entry, a hard state only, skipped the
	// appends that only moved the commit and owed no response, which took
	// no sync of their own (appendNow), and flushes the syncs that took
	// them once commitHold passed with no other (kick).
	marks, stateOnly, skipped, flushes uint64
}

type walJob struct {
	end   int64
	resps []*pb.Message
	hs    *pb.HardState // the hard state the append wrote, if any
	at    time.Time     // when the loop handed it the log
	ents  bool          // whether the append carried entries
	// held is set for an append that only moved the commit and owes
	// responses: its sync waits, at most commitHold, for the next
	// append's (appendNow, kick).
	held bool
}

type segment struct {
	wfd, rfd int
	err      error
}

func segPath(dir string, k int64) string { return filepath.Join(dir, fmt.Sprintf("%016x.seg", k)) }

// openWAL opens the log in dir, or starts an empty one. It calls fn with
// each record the log holds from offset from on, in order, up to the
// first that does not check; the log goes on after that one, and what lay
// from it on is cut. from is where a checkpoint left the log, 0 for none;
// the log before keep is removed, and from keep on it is kept to read.
// stage is the size of the stage, a power of two (sizes.stage).
func openWAL(sys sysEnv, dir string, stage int, keep, from int64, fn func(r *walRecord) error) (*wal, error) {
	return openWALRing(sys, dir, stage, 0, keep, from, fn)
}

// openWALRing is openWAL with a ring of ring bytes (logring.go), 0 for
// none: a log with no segment formats it. A log formatted before keeps
// its ring, and one made without a ring keeps none.
func openWALRing(sys sysEnv, dir string, stage int, ring, keep, from int64, fn func(r *walRecord) error) (*wal, error) {
	if err := mkdirSynced(sys, dir); err != nil {
		return nil, err
	}
	info, err := readRing(sys, dir)
	if err != nil {
		return nil, err
	}
	var formatted time.Duration
	if info == nil && ring > 0 {
		has, err := hasSegments(sys, dir)
		if err != nil {
			return nil, err
		}
		if !has {
			start := time.Now()
			if err := formatRing(sys, dir, ringSegs(ring)); err != nil {
				return nil, fmt.Errorf("format the log's ring: %w", err)
			}
			info = &ringInfo{segs: ringSegs(ring), segSize: segSize}
			formatted = max(time.Since(start), 1)
		}
	}
	if info != nil && info.segSize != segSize {
		return nil, fmt.Errorf("the log's ring has segments of %d bytes, this build %d", info.segSize, segSize)
	}
	end, err := scanWAL(sys, dir, from, fn)
	if err != nil {
		return nil, err
	}

	kick, err := sys.eventfd(false)
	if err != nil {
		return nil, err
	}
	wake, err := sys.eventfd(true)
	if err != nil {
		return nil, err
	}
	w := &wal{
		sys: sys, dir: dir, kickFD: kick, wakeFD: wake, made: make(chan segment, 1),
		stop: make(chan struct{}), madeDone: make(chan struct{}), helperDone: make(chan struct{}),
		formatted: formatted,
	}
	if w.stage, err = mmapBytes(stage); err != nil {
		return nil, err
	}
	w.stageLen = int64(stage)
	if info != nil {
		w.ring, w.freedCh = info, make(chan struct{}, 1)
		if w.spares, err = listSpares(sys, dir); err != nil {
			return nil, err
		}
		w.nextSp = spareSeq(w.spares)
	}
	next, err := w.reopen(keep, end)
	if err != nil {
		return nil, err
	}
	go w.makeSegments(next)
	go w.helper(end)
	return w, nil
}

// reopen readies the log to go on at end: it opens the segments that hold
// it from offset from to read, and the last one to write from end on, cut
// there; it lets go of any segment before from's or after end's, syncs
// what is left (syncSegments), and puts the last block's bytes before end
// into the stage, which the helper writes again. It returns the first
// segment still to make.
//
// A log without a ring removes what it lets go of, and cuts its last
// segment by truncating it. A ring's space stays written: the segments
// before from's go back to the pool as they are, as a cut gives them; the
// last segment gets zeros from end on, and the segments after it zeros in
// full before they go back, as the records a crash left there lie at
// offsets the log reaches again (logring.go).
func (w *wal) reopen(from, end int64) (int64, error) {
	first, last := from/segSize, end/segSize
	if err := w.letGo(0, first, false); err != nil {
		return 0, err
	}
	w.rbase = first
	for k := first; k <= last; k++ {
		fd, err := w.sys.open(segPath(w.dir, k), unix.O_RDONLY, 0)
		if errors.Is(err, unix.ENOENT) && k == last {
			// The log ends where a segment would start.
			break
		}
		if err != nil {
			return 0, err
		}
		w.rfds = append(w.rfds, fd)
	}
	// A crash may have kept some removes of an earlier start and lost
	// others, so the segments past the end may have gaps (#702).
	if err := w.letGo(last+1, math.MaxInt64, true); err != nil {
		return 0, err
	}
	w.off, w.stable, w.wbase = end, end, last
	w.done.Store(end)
	w.want.Store(end)
	path := segPath(w.dir, last)
	cut := w.rbase+int64(len(w.rfds)) > last
	if cut && w.ring == nil {
		if err := w.sys.truncate(path, end%segSize); err != nil {
			return 0, err
		}
	}
	if w.ring != nil {
		if err := w.rewriteTail(from, end); err != nil {
			return 0, err
		}
	}
	if err := w.syncSegments(); err != nil {
		return 0, err
	}
	if !cut {
		return last, nil
	}
	wfd, err := w.sys.openDirect(path)
	if err != nil {
		return 0, err
	}
	w.wfds = append(w.wfds, wfd)
	blk := end &^ (block - 1)
	pos := blk & (w.stageLen - 1)
	n, err := sysPread(w.rfds[last-w.rbase], w.stage[pos:pos+end-blk], blk%segSize)
	if err != nil {
		return 0, fmt.Errorf("read the log's last block back from %s: %w", path, err)
	}
	if int64(n) != end-blk {
		return 0, fmt.Errorf("read the log's last block back from %s: a short read, %d of its %d bytes at %d", path, n, end-blk, blk%segSize)
	}
	if w.ring != nil {
		// The last block again, with zeros past end, and zeros past it to
		// the segment's end. Offsets here are the segment's own: one past
		// its last block is its end, not its start.
		at := blk - last*segSize
		if end > blk {
			if _, err := sysPwrite(wfd, w.stage[pos:pos+block], at); err != nil {
				return 0, fmt.Errorf("write the log's last block again to %s: %w", path, err)
			}
			at += block
		}
		if at < segSize {
			if err := w.zeroFrom(path, at); err != nil {
				return 0, err
			}
		}
	}
	return last + 1, nil
}

// letGo lets go of the log's segments from k to before to, those that
// exist: without a ring it removes them; a ring's go back to its pool,
// with zeros written over them first if zero.
func (w *wal) letGo(k, to int64, zero bool) error {
	if w.ring == nil {
		return w.removeSegments(k, to)
	}
	names, err := w.sys.readDir(w.dir)
	if err != nil {
		return err
	}
	moved := false
	for _, name := range names {
		var i int64
		if _, err := fmt.Sscanf(name, "%016x.seg", &i); err != nil || i < k || i >= to {
			continue
		}
		path := filepath.Join(w.dir, name)
		if zero {
			if err := w.zeroFrom(path, 0); err != nil {
				return err
			}
		}
		spare := sparePath(w.dir, w.nextSp)
		w.nextSp++
		if err := w.sys.rename(path, spare); err != nil {
			return err
		}
		w.spares = append(w.spares, spare)
		moved = true
	}
	if moved {
		return syncDir(w.sys, w.dir)
	}
	return nil
}

// rewriteTail writes again, directly, the whole blocks of the log from
// maxStage before end, which the scan read, so that the start's sync makes
// them durable. A sync that failed may have left the bytes of its write in
// the page cache only: a failed writeback marks them clean, so they read
// back but never reach the disk, and a later sync does not retry them. A
// log without a ring cut its last segment to end, after which the sync
// wrote them; a ring keeps its segments whole, and its start read them,
// went on past them, and a power loss later left zeros where later marks
// said the log was durable (TestMembershipFaults seed 4 on #752,
// TestARingRecoversFromAFailedSyncWarmThenCold). The node stops at its
// first failed sync, and one write of the helper's covers at most its
// stage, so the bytes a failed sync left are within the failed process's
// stage of the end: maxStage, the largest any process has, as a restart
// with a smaller memory budget has a smaller stage (#752's re-check). The
// last block, past the blocks this writes, reopen writes again itself.
// The rewrite has a buffer of its own: the stage, which reopen fills with
// the last block next, must hold only zeros past the end.
func (w *wal) rewriteTail(from, end int64) error {
	lo, hi := max(from, end-max(w.stageLen, maxStage()))&^(block-1), end&^(block-1)
	if lo >= hi {
		return nil
	}
	buf, err := mmapBytes(int(min(segSize, 4<<20)))
	if err != nil {
		return err
	}
	defer func() { _ = unixMunmap(buf) }()
	for off := lo; off < hi; {
		k := off / segSize
		n := min(hi-off, (k+1)*segSize-off, int64(len(buf)))
		got, err := sysPread(w.rfds[k-w.rbase], buf[:n], off%segSize)
		if err == nil && int64(got) != n {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return fmt.Errorf("read the log back from %s at %d: %d of %d bytes: %w", segPath(w.dir, k), off%segSize, got, n, err)
		}
		fd, err := w.sys.openDirect(segPath(w.dir, k))
		if err != nil {
			return err
		}
		for at := int64(0); at < n; {
			// A short direct write writes the rest again, as the helper's
			// do (writeLoop).
			m, err := sysPwrite(fd, buf[at:n], off%segSize+at)
			if err == nil && m <= 0 {
				err = io.ErrShortWrite
			}
			if err != nil {
				_ = sysClose(fd)
				return fmt.Errorf("write the log again to %s at %d: %w", segPath(w.dir, k), off%segSize+at, err)
			}
			at += int64(m)
		}
		_ = sysClose(fd)
		off += n
	}
	return nil
}

// maxStage is the largest stage a process has (sizeFor), whatever its
// memory budget.
func maxStage() int64 { return int64(sizeFor(1 << 62).stage) }

// zeroFrom writes zeros durably over the segment at path from offset off
// on (logring.go).
func (w *wal) zeroFrom(path string, off int64) error {
	if w.zeroBuf == nil {
		b, err := mmapBytes(int(min(segSize, 4<<20)))
		if err != nil {
			return err
		}
		w.zeroBuf = b
	}
	return writeZeros(w.sys, path, off, w.zeroBuf, false)
}

// syncSegments makes durable what the scan read and reopen cut, before
// the node counts the log as recovered: a write that failed, or a process
// that died before its write was durable, can leave records in the page
// cache that a scan reads and a power loss takes (#701). It syncs every
// segment the log holds, and the directory, whose removes of segments
// past the end go with them.
func (w *wal) syncSegments() error {
	for k, fd := range w.rfds {
		if err := sysFsync(fd); err != nil {
			return fmt.Errorf("sync %s: %w", segPath(w.dir, w.rbase+int64(k)), err)
		}
	}
	dfd, err := w.sys.open(w.dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = sysClose(dfd) }()
	if err := sysFsync(dfd); err != nil {
		return fmt.Errorf("sync %s: %w", w.dir, err)
	}
	return nil
}

// removeSegments removes the log's segments from k to before to, those
// that exist.
func (w *wal) removeSegments(k, to int64) error {
	names, err := w.sys.readDir(w.dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		var i int64
		if _, err := fmt.Sscanf(name, "%016x.seg", &i); err != nil || i < k || i >= to {
			continue
		}
		if err := w.sys.remove(filepath.Join(w.dir, name)); err != nil {
			return err
		}
	}
	return nil
}

// cut lets go of the segments wholly before off, which nothing reads any
// more: the store's values and Raft's entries before it lie in a
// checkpoint. It returns their paths, for the caller to remove off the
// loop (remover). Only the loop calls it.
func (w *wal) cut(off int64) []string {
	k := min(off, w.stable) / segSize
	if planted.On("B19") && k < w.stable/segSize {
		// Planted: the segment that holds off goes too, though recovery
		// from the checkpoint still reads it.
		k++
	}
	if k <= w.rbase {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var paths []string
	for w.rbase < k && len(w.rfds) > 0 {
		if w.readers > 0 {
			w.unused = append(w.unused, w.rfds[0])
		} else {
			_ = sysClose(w.rfds[0])
		}
		if w.ring != nil {
			w.freed = append(w.freed, w.rbase)
		} else {
			paths = append(paths, segPath(w.dir, w.rbase))
		}
		w.rfds = w.rfds[1:]
		w.rbase++
	}
	if len(w.freed) > 0 {
		// A ring's segments go back to its pool, off the loop (maker).
		select {
		case w.freedCh <- struct{}{}:
		default:
		}
	}
	return paths
}

// makeSegments makes the segments from k on, one ahead of the helper.
func (w *wal) makeSegments(k int64) {
	defer close(w.madeDone)
	dfd, err := w.sys.open(w.dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		w.hand(segment{err: err})
		return
	}
	defer func() { _ = sysClose(dfd) }()
	for ; ; k++ {
		var sg segment
		var err error
		if w.ring != nil {
			sg, err = w.takeSpare(dfd, k)
		} else {
			sg, err = w.makeSegment(dfd, segPath(w.dir, k))
		}
		if errors.Is(err, errWALClosed) {
			return
		}
		if err != nil {
			w.hand(segment{err: err})
			return
		}
		w.mu.Lock()
		w.rfds = append(w.rfds, sg.rfd)
		w.mu.Unlock()
		if !w.hand(sg) {
			_ = sysClose(sg.wfd)
			return
		}
	}
}

// hand gives the helper a segment, unless the log closes first; a ring's
// maker meanwhile gives the segments cut back to the pool.
func (w *wal) hand(sg segment) bool {
	for {
		select {
		case w.made <- sg:
			return true
		case <-w.freedCh:
			if err := w.recycle(); err != nil {
				w.fail(err)
			}
		case <-w.stop:
			return false
		}
	}
}

// takeSpare makes segment k of a ring from a spare, waiting for one while
// the ring is full: a cut gives some back (logring.go). Only the maker
// calls it.
func (w *wal) takeSpare(dfd int, k int64) (segment, error) {
	for {
		if err := w.recycle(); err != nil {
			return segment{}, err
		}
		w.mu.Lock()
		n := len(w.spares)
		w.mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-w.freedCh:
		case <-w.stop:
			return segment{}, errWALClosed
		}
	}
	w.mu.Lock()
	spare := w.spares[len(w.spares)-1]
	w.spares = w.spares[:len(w.spares)-1]
	w.mu.Unlock()
	path := segPath(w.dir, k)
	if err := w.sys.rename(spare, path); err != nil {
		return segment{}, err
	}
	if planted.On("B05") {
		// Planted: the segment takes records before its directory
		// entry is synced, as makeSegment's.
	} else if err := sysFsync(dfd); err != nil {
		return segment{}, err
	}
	var sg segment
	var err error
	if sg.wfd, err = w.sys.openDirect(path); err != nil {
		return segment{}, err
	}
	if sg.rfd, err = w.sys.open(path, unix.O_RDONLY, 0); err != nil {
		_ = sysClose(sg.wfd)
		return segment{}, err
	}
	return sg, nil
}

// recycle renames the segments a cut freed to spares, and syncs the
// directory once for them. Only the maker calls it.
func (w *wal) recycle() error {
	w.mu.Lock()
	freed := w.freed
	w.freed = nil
	w.mu.Unlock()
	if len(freed) == 0 {
		return nil
	}
	spares := make([]string, 0, len(freed))
	for _, k := range freed {
		spare := sparePath(w.dir, w.nextSp)
		w.nextSp++
		if err := w.sys.rename(segPath(w.dir, k), spare); err != nil {
			return err
		}
		spares = append(spares, spare)
	}
	if err := syncDir(w.sys, w.dir); err != nil {
		return err
	}
	w.mu.Lock()
	w.spares = append(w.spares, spares...)
	w.mu.Unlock()
	return nil
}

// fail stops the log with err, as the helper does.
func (w *wal) fail(err error) {
	err = fmt.Errorf("the log's ring: %w", err)
	w.failed.CompareAndSwap(nil, &err)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.wakeFD, b[:])
}

var errWALClosed = errors.New("the log is closed")

// close stops the log's goroutines and closes its files. Nothing writes
// to it after.
func (w *wal) close() { w.closeOnce.Do(w.shut) }

func (w *wal) shut() {
	close(w.stop)
	<-w.madeDone
	w.closing.Store(true)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.kickFD, b[:])
	<-w.helperDone
	// A segment made and never taken.
	select {
	case sg := <-w.made:
		if sg.err == nil {
			_ = sysClose(sg.wfd)
		}
	default:
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, fd := range slices.Concat(w.rfds, w.unused, w.wfds) {
		_ = sysClose(fd)
	}
	_ = sysClose(w.kickFD)
	_ = sysClose(w.wakeFD)
}

// allocFree is the room a disk must have left for a segment's space to be
// allocated ahead (makeSegment): eight segments.
func allocFree() int64 { return 8 * segSize }

func (w *wal) makeSegment(dfd int, path string) (segment, error) {
	fd, err := w.sys.open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		return segment{}, err
	}
	_ = sysClose(fd)
	if planted.On("B05") {
		// Planted: the segment takes records before its directory
		// entry is synced.
	} else if err := sysFsync(dfd); err != nil {
		return segment{}, err
	}
	var sg segment
	if sg.wfd, err = w.sys.openDirect(path); err != nil {
		return segment{}, err
	}
	// The segment's space is allocated ahead, its size kept, so that an
	// append allocates no blocks: on ext4 an O_DSYNC write that grows a
	// file into new blocks journals them, which doubled the helper's CPU
	// per write against prototype 6's ring (0.51 to 0.99 us an op of the
	// leader on crd-heavy, perf of 43860972055), and the allocated
	// segments brought crd-heavy at 0 ms from x1.97 to x2.56 on the same
	// seeds (43866870299, 43866865595). The size stays as each append
	// leaves it, so recovery reads what it read before; blocks past it are
	// never read. Allocated space is taken from the disk, so it is
	// allocated only while allocFree is left: on a fuller disk the segment
	// grows as before, and leaves the room to the checkpoints that cut the
	// log (in campaign 43870771307 nodes whose small disks held segments
	// allocated ahead stopped writing checkpoints with ENOSPC). A file
	// system without fallocate leaves the segment to grow as before too.
	if free, err := w.sys.freeSpace(w.dir); err == nil && free >= allocFree() {
		if err := w.sys.allocate(sg.wfd, segSize); err == nil {
			_ = sysFsync(sg.wfd)
		}
	}
	if sg.rfd, err = w.sys.open(path, unix.O_RDONLY, 0); err != nil {
		_ = sysClose(sg.wfd)
		return segment{}, err
	}
	return sg, nil
}

// segFD returns the write file of segment k, waiting until it is made,
// and closes those before it, which the helper writes no more. Only the
// helper calls it.
func (w *wal) segFD(k int64) (int, error) {
	for w.wbase+int64(len(w.wfds)) <= k {
		var sg segment
		select {
		case sg = <-w.made:
		case <-w.stop:
			// The log closes, and its maker stopped: the segment may
			// never come (#689's area-1 review).
			return -1, errWALClosed
		}
		if sg.err != nil {
			return -1, sg.err
		}
		w.wfds = append(w.wfds, sg.wfd)
	}
	for w.wbase < k {
		_ = sysClose(w.wfds[0])
		w.wfds = w.wfds[1:]
		w.wbase++
	}
	return w.wfds[k-w.wbase], nil
}

func (w *wal) helper(flushed int64) {
	w.sys.lockOSThread()
	defer close(w.helperDone)
	err := w.writeLoop(flushed)
	if errors.Is(err, errWALClosed) {
		return
	}
	err = fmt.Errorf("write the log: %w", err)
	w.failed.Store(&err)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.wakeFD, b[:])
}

// writeLoop writes what each kick asks for: the blocks from the one that
// holds the durable end to the one that holds the target. The last block
// is written again the next time.
//
// The loop goes on writing the stage while the helper writes it, and both
// touch the last block: a data race, left on purpose, as a lock or a copy
// would cost the loop's hot path. It is sound because of one invariant:
// nothing before the target (want) is written while a write is under way.
// The loop only appends at w.off, which is at or past the target, and
// clears the rest of its own last block; the helper writes the stage up
// to the target's block end. So every byte before the target reaches the
// disk as the loop wrote it, and the bytes after it, which the helper may
// catch half written, lie past the end it reports durable (done): the
// next write puts that block down again whole, and after a crash a scan
// stops at the first record whose CRC does not check (scanWAL). The race
// detector does not see it in the real build or under simio, where the
// stage is mapped memory (mem_mmap.go).
func (w *wal) writeLoop(flushed int64) error {
	var b [8]byte
	for {
		if _, err := sysRead(w.kickFD, b[:]); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if w.closing.Load() {
			return errWALClosed
		}
		target := w.want.Load()
		t := time.Now()
		from := flushed &^ (block - 1)
		to := (target + block - 1) &^ (block - 1)
		for from < to {
			pos := from & (w.stageLen - 1)
			n := min(to-from, w.stageLen-pos, segSize-from%segSize)
			fd, err := w.segFD(from / segSize)
			if err != nil {
				return err
			}
			if planted.On("B16") && (from+n)%segSize == 0 && from+n < to {
				// Planted: the log moves to the next segment without the
				// last write of this one made durable.
				if err := w.plantedUnsyncedWrite(from, n, pos); err != nil {
					return err
				}
				from += n
				continue
			}
			k, err := sysPwrite(fd, w.stage[pos:pos+n], from%segSize)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil && k > 0 && planted.On("B12") {
				// Planted: the log goes on past a short write, and what
				// it left out is never written.
				k, err = int(n), nil
			}
			if err != nil && planted.On("B11") {
				// Planted: the failed write counts as done.
				k, err = int(n), nil
			}
			if err != nil {
				return err
			}
			from += int64(k)
		}
		d := time.Since(t)
		w.writeNs.Add(int64(d))
		w.lastSyncNs.Store(int64(d))
		if m := w.prom.Load(); m != nil {
			m.WALWrite.Observe(d.Seconds())
			m.WALFsync.Observe(d.Seconds())
			m.BackendCommit.Observe(d.Seconds())
			m.WALWriteBytes.Add(float64(target - flushed))
		}
		flushed = target
		w.done.Store(target)
		binary.LittleEndian.PutUint64(b[:], 1)
		_, _ = sysWrite(w.wakeFD, b[:])
	}
}

// plantedUnsyncedWrite is planted bug B16: it writes the n stage bytes at
// pos, the log's at from, to their segment through the page cache, with
// no sync.
func (w *wal) plantedUnsyncedWrite(from, n, pos int64) error {
	fd, err := w.sys.open(segPath(w.dir, from/segSize), unix.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = sysPwrite(fd, w.stage[pos:pos+n], from%segSize)
	return errors.Join(err, sysClose(fd))
}

// kick starts a sync if none runs and appends wait, at most one every
// gap(): under load each sync takes more appends, and the loop wakes for
// fewer completions, so its turns and the acks they send grow.
//
// Appends that wait for room in the stage start one too, with no job
// pending: the records written and not synced, as a leader's commits
// (appendNow), hold the stage's room until a sync takes them, and no
// sync's end would ever let the waiting appends in (#821's review).
// While every job pending is a commit held (walJob.held), none starts
// before the first's commitHold ends: the next append's sync covers them.
// With no job pending and records written that no sync took, as a
// leader's commits skipped (appendNow), one starts once commitHold passed
// since the first: a commit stays unsynced that long at most, past which
// a clean stop, or a crash, kept it (#821's re-review). The hold's end
// comes before the gap between syncs: it bounds the wait, the gap only
// gathers more appends to a sync.
func (w *wal) kick() {
	if w.inflight {
		return
	}
	if len(w.pending) == 0 && w.want.Load() >= w.off {
		// Nothing is left for a sync deferred: another, the idle mark's
		// (markIdle), took it. Kept, the loop would wake for it at once,
		// on every turn, and never block (#821's hang under gosim).
		w.deferred = false
		return
	}
	now := time.Now()
	if until := w.holdUntil(); !until.IsZero() {
		if now.Before(until) {
			w.deferred = true
			return
		}
	} else if d := now.Sub(w.lastKick); d < w.gap() {
		w.deferred = true
		return
	}
	w.deferred = false
	w.skippedAt = time.Time{}
	w.lastKick = now
	w.stats.syncs++
	switch {
	case len(w.pending) == 0:
		w.stats.flushes++
	case !slices.ContainsFunc(w.pending, func(j walJob) bool { return j.ents }):
		w.stats.stateOnly++
	}
	if w.ring != nil {
		w.appendSynced()
	}
	w.lastBytes = w.off - w.want.Load()
	w.want.Store(w.off)
	w.inflight = true
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.kickFD, b[:])
}

// commitHold is the longest a commit held waits for the next append's sync
// (walJob.held).
const commitHold = 3 * time.Millisecond

// holdUntil is when the sync of commits that wait for the next append's
// ends its wait, commitHold past the first: the commits held, when every
// job pending is one (walJob.held), or the records written and taken by
// no sync, when no job is pending (wal.skippedAt). It is the zero time
// when a job pending is not held, or appends wait for the stage, whose
// sync starts at once.
func (w *wal) holdUntil() time.Time {
	if len(w.waiting) > 0 {
		return time.Time{}
	}
	if len(w.pending) == 0 {
		if w.skippedAt.IsZero() {
			return time.Time{}
		}
		return w.skippedAt.Add(commitHold)
	}
	for _, j := range w.pending {
		if !j.held {
			return time.Time{}
		}
	}
	return w.pending[0].at.Add(commitHold)
}

// deferUntil is when a deferred sync may start (kick): the hold's end, or
// else gap() past the last.
func (w *wal) deferUntil() time.Time {
	if h := w.holdUntil(); !h.IsZero() {
		return h
	}
	return w.lastKick.Add(w.gap())
}

// gap is the least time between the starts of two syncs: syncGap, or
// loadGap after a sync that wrote loadBytes or more.
//
// Under the bench's Kubernetes load at 0 ms a leader ran in one of two
// modes, decided per run (#689, "0 ms: the bisect"): syncs of about
// 770 KB every 3.6-5.7 ms, at 3.1-3.5 µs of the loop per op, or of about
// 156 KB every 1.1-2.9 ms, with four to five times the turns, proposals,
// ReadIndex rounds and peer messages per op, at 5.2-6.0 µs, about a
// quarter less throughput. The helper wrote about 90% of the time in
// both, so syncGap never held the small ones back. With loadGap, every
// run takes the large batches; a node whose syncs write little, as under
// a light load, still syncs syncGap apart, and a lone write waits for
// none.
func (w *wal) gap() time.Duration {
	if w.lastBytes >= loadBytes {
		return loadGap
	}
	return syncGap
}

// fits reports whether the stage has room for the records of ents and a
// hard state.
func (w *wal) fits(ents []*pb.Entry) bool {
	need := int64(recHead + 64)
	for _, e := range ents {
		need += walEntHdr + int64(len(e.GetData()))
	}
	return w.off+need-w.done.Load() <= w.stageLen-block
}

var errStageFull = errors.New("the log ran a whole stage ahead of the disk")

// write appends entries and a hard state to the stage, without sync.
func (w *wal) write(ents []*pb.Entry, hs []byte) error {
	w.iov = w.iov[:0]
	w.offs = w.offs[:0]
	need := len(ents)*walEntHdr + recHead + len(hs)
	if cap(w.hdrs) < need {
		w.hdrs = make([]byte, 0, 2*need)
	}
	h := w.hdrs[:need]
	at := 0
	off := w.off
	for _, e := range ents {
		d := e.GetData()
		b := h[at : at+walEntHdr]
		putEntryHead(b, e, off)
		at += walEntHdr
		w.iov = append(w.iov, b)
		off += walEntHdr
		w.offs = append(w.offs, off)
		if len(d) > 0 {
			w.iov = append(w.iov, d)
			off += int64(len(d))
		}
	}
	if len(hs) > 0 {
		b := h[at : at+recHead+len(hs)]
		binary.LittleEndian.PutUint32(b[4:], uint32(len(hs)))
		b[8] = recHardState
		copy(b[recHead:], hs)
		binary.LittleEndian.PutUint32(b, crc32.Update(recSeed(off), castagnoli, b[4:]))
		w.iov = append(w.iov, b)
		off += int64(len(b))
	}
	if off-w.done.Load() > w.stageLen-block {
		return errStageFull
	}
	for _, v := range w.iov {
		for len(v) > 0 {
			pos := w.off & (w.stageLen - 1)
			k := copy(w.stage[pos:], v)
			v = v[k:]
			w.off += int64(k)
			w.stats.bytes += uint64(k)
		}
	}
	w.dataEnd = w.off
	// The helper writes whole blocks: the rest of the last one is zeros,
	// never what the stage held a lap ago, which a scan would take for
	// records.
	if tail := w.off % block; tail != 0 {
		pos := w.off & (w.stageLen - 1)
		clear(w.stage[pos : pos+block-tail])
	}
	w.stats.writes++
	return nil
}

// read copies the log's bytes at off into p: from the stage where they
// may not be on disk yet, else from the segments.
func (w *wal) read(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if off+int64(len(p)) > w.off || off < 0 {
		return fmt.Errorf("read %d bytes of the log at %d, past its end %d", len(p), off, w.off)
	}
	if off >= w.durableEnd() {
		for got := 0; got < len(p); {
			pos := (off + int64(got)) & (w.stageLen - 1)
			got += copy(p[got:], w.stage[pos:])
		}
		return nil
	}
	return w.readDurable(p, off)
}

// errAway is a value the loop would wait on the disk for: it is read off
// the loop (fetch.go).
var errAway = errors.New("the value is not in memory")

// readNoWait is read of the bytes in the stage or the page cache: it
// fails with errAway where it would wait for the disk.
func (w *wal) readNoWait(p []byte, off int64) error {
	if len(p) == 0 || off >= w.durableEnd() {
		return w.read(p, off)
	}
	return w.readSegments(p, off, func(fd int, p []byte, off int64) (int, error) {
		n, err := sysPreadNoWait(fd, p, off)
		if notReadNow(n, len(p), err) {
			return 0, errAway
		}
		return n, err
	})
}

// durableEnd is where the log's bytes on disk end, those the helper writes
// again aside: other goroutines may read the log before it.
func (w *wal) durableEnd() int64 { return w.done.Load() &^ (block - 1) }

// readDurable copies the log's bytes at off, before durableEnd, from its
// segments into p. Any goroutine may call it.
func (w *wal) readDurable(p []byte, off int64) error { return w.readSegments(p, off, sysPread) }

// readSegments is readDurable through pread.
func (w *wal) readSegments(p []byte, off int64, pread func(fd int, p []byte, off int64) (int, error)) error {
	w.mu.Lock()
	w.readers++
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.readers--
		if w.readers == 0 {
			for _, fd := range w.unused {
				_ = sysClose(fd)
			}
			w.unused = w.unused[:0]
		}
		w.mu.Unlock()
	}()
	for got := 0; got < len(p); {
		o := off + int64(got)
		k := min(len(p)-got, int(segSize-o%segSize))
		w.mu.Lock()
		i := o/segSize - w.rbase
		if i < 0 || i >= int64(len(w.rfds)) {
			w.mu.Unlock()
			return fmt.Errorf("read the log at %d, in segment %d, which it does not hold", o, o/segSize)
		}
		fd := w.rfds[i]
		w.mu.Unlock()
		n, err := pread(fd, p[got:got+k], o%segSize)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("the log's segment %d ends before %d", o/segSize, o)
		}
		got += n
	}
	return nil
}

// walRecord is one record read back from the log.
type walRecord struct {
	off   int64 // where its body starts
	kind  byte
	index uint64
	term  uint64
	typ   pb.EntryType
	data  []byte // an entry's data or a hard state, valid until the next record
}

// scanWAL reads dir's records in order from offset from, where a record
// starts, and calls fn, if any, with each, until the first that does not
// check. It returns the log's end: the offset after the last good record.
func scanWAL(sys sysEnv, dir string, from int64, fn func(r *walRecord) error) (int64, error) {
	var buf []byte
	var r walRecord
	off := from
	ring, err := readRing(sys, dir)
	if err != nil {
		return 0, err
	}
	if ring != nil && ring.segSize != segSize {
		// A reader of another process's log, such as chaos's, would read
		// its segments at the wrong offsets (#751's CI).
		return 0, fmt.Errorf("the log's ring has segments of %d bytes, this process %d", ring.segSize, segSize)
	}
	// Planted bug B04b holds each record back until the next one checks,
	// and drops the last before a torn tail; lastIndex is for B04c.
	var held walRecord
	heldOff, lastIndex := int64(-1), uint64(0)
	emit := func(r *walRecord) error {
		if fn == nil {
			return nil
		}
		if planted.On("B04b") {
			if heldOff >= 0 {
				if err := fn(&held); err != nil {
					return err
				}
			}
			held, heldOff = *r, off
			return nil
		}
		return fn(r)
	}
	for k := from / segSize; ; k++ {
		b, err := sys.readFile(segPath(dir, k))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return 0, err
		}
		if int64(len(b)) > segSize {
			return 0, fmt.Errorf("segment %d is longer than a segment", k)
		}
		short := int64(len(b)) < segSize
		if k == from/segSize {
			if int64(len(b)) < from%segSize {
				// The log ends before where its checkpoint left it: what
				// lay between was older than the checkpoint and not yet
				// durable.
				return from, nil
			}
			b = b[from%segSize:]
		}
		buf = append(buf, b...)
		for len(buf) >= recHead {
			l := int64(binary.LittleEndian.Uint32(buf[4:]))
			if l <= maxWALRecBody && int64(len(buf)) < recHead+l {
				break
			}
			if l > maxWALRecBody || !checks(buf[:recHead+l], off) && !plantedAccepts(buf[:recHead+l], off, lastIndex) {
				var err error
				if ring != nil {
					err = ringDamaged(sys, dir, k, off, buf)
				} else {
					err = damaged(sys, dir, k, short, off, buf)
				}
				if err == nil && heldOff >= 0 && tornRecord(buf) {
					// Planted (B04b): the record before a torn tail is
					// cut too.
					return heldOff, nil
				}
				return off, err
			}
			rec := buf[:recHead+l]
			if rec[8] == recSynced {
				// A ring's mark of what was durable: recovery's, not the
				// log's callers' (ringDamaged).
				off += recHead + l
				buf = buf[recHead+l:]
				continue
			}
			r = walRecord{off: off + recHead, kind: rec[8], data: rec[recHead:]}
			if r.kind == recEntry {
				r.index = binary.LittleEndian.Uint64(rec[9:])
				r.term = binary.LittleEndian.Uint64(rec[17:])
				r.typ = pb.EntryType(rec[25])
				r.off += entHead
				r.data = rec[walEntHdr:]
			}
			if r.kind == recEntry {
				lastIndex = r.index
			}
			if err := emit(&r); err != nil {
				return 0, err
			}
			off += recHead + l
			buf = buf[recHead+l:]
		}
		if short {
			// A segment that ends short is the last: the log ends in it.
			break
		}
	}
	if ring != nil {
		if err := ringDamaged(sys, dir, math.MaxInt64-1, off, buf); err != nil {
			return off, err
		}
	} else if chainAfter(buf, off) {
		return off, fmt.Errorf("%w: the log ends in a record at offset %d that runs past its end, but records that check follow its start", ErrCorruptLog, off)
	}
	if heldOff >= 0 {
		if tornRecord(buf) {
			// Planted (B04b): a record torn at the log's end cuts the
			// one before it too.
			return heldOff, nil
		}
		if err := fn(&held); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// chainAfter reports whether b, the log's bytes at off from a record that
// runs past the log's end, holds two records in a row that check, or one
// that ends the log, past its start. A crash leaves the rest of such a record
// unwritten, zeros or bytes of its own; records that check after its
// start mean its header was read wrong, as a damaged length that claims
// more than the log holds, and the log is not to be cut there (#700).
func chainAfter(b []byte, off int64) bool {
	for i := 1; i+recHead <= len(b); i++ {
		r := b[i:]
		at := off + int64(i)
		if !checks(r, at) {
			continue
		}
		n := recHead + int(binary.LittleEndian.Uint32(r[4:]))
		if n == len(r) || checks(r[n:], at+int64(n)) {
			return true
		}
	}
	return false
}

// plantedAccepts reports whether planted bug B04a takes rec, a record at
// log offset off whose CRC does not verify, as good: one whose kind and
// length are a record's and that no crash tore. B04c takes one that
// decodes: an entry after the last, lastIndex, or a hard state.
func plantedAccepts(rec []byte, off int64, lastIndex uint64) bool {
	l := int64(len(rec) - recHead)
	switch {
	case planted.On("B04a"):
		return !torn(rec, off) && (rec[8] == recEntry && l >= entHead || rec[8] == recHardState)
	case planted.On("B04c"):
		if rec[8] == recHardState {
			return l > 0 && l < 64
		}
		if rec[8] != recEntry || l < entHead {
			return false
		}
		i := binary.LittleEndian.Uint64(rec[9:])
		return i >= 1 && i <= lastIndex+1
	}
	return false
}

// tornRecord reports whether b, the log's bytes past its last record that
// checks, start a record a crash tore rather than the zeros past the end.
func tornRecord(b []byte) bool {
	return slices.ContainsFunc(b[:min(len(b), recHead)], func(c byte) bool { return c != 0 })
}

// putEntryHead puts into b, walEntHdr bytes, the head of entry e's
// record at log offset off, whose data follows it.
func putEntryHead(b []byte, e *pb.Entry, off int64) {
	d := e.GetData()
	binary.LittleEndian.PutUint32(b[4:], uint32(entHead+len(d)))
	b[8] = recEntry
	binary.LittleEndian.PutUint64(b[9:], e.GetIndex())
	binary.LittleEndian.PutUint64(b[17:], e.GetTerm())
	b[25] = byte(e.GetType())
	binary.LittleEndian.PutUint32(b, crc32.Update(crc32.Update(recSeed(off), castagnoli, b[4:walEntHdr]), castagnoli, d))
}

// recSeed is where the checksum of a record at log offset off starts:
// the CRC of the offset, so that the same bytes at another offset do not
// check (rule 1 of the ring).
func recSeed(off int64) uint32 {
	var o [8]byte
	binary.LittleEndian.PutUint64(o[:], uint64(off))
	return crc32.Update(0, castagnoli, o[:])
}

// checks reports whether b starts with a whole record that checks at log
// offset off: one of a record's kinds, whose CRC verifies. The kind keeps
// the zeros past the log's end from checking: at some offsets, the first
// 3,141,446,827, the seeded CRC of zeros is itself zero (#749's review).
func checks(b []byte, off int64) bool {
	if len(b) < recHead || b[8] != recEntry && b[8] != recHardState && b[8] != recSynced {
		return false
	}
	l := int64(binary.LittleEndian.Uint32(b[4:]))
	if l > maxWALRecBody || int64(len(b)) < recHead+l || b[8] == recEntry && l < entHead || b[8] == recSynced && l != 8 {
		return false
	}
	return crc32.Update(recSeed(off), castagnoli, b[4:recHead+l]) == binary.LittleEndian.Uint32(b)
}

// torn reports whether rec, a record that does not check at log offset
// off, has a part within one sector that is all zeros, as a write a crash
// tore at sectors leaves.
func torn(rec []byte, off int64) bool {
	for len(rec) > 0 {
		k := min(int64(len(rec)), sector-off%sector)
		if !slices.ContainsFunc(rec[:k], func(c byte) bool { return c != 0 }) {
			return true
		}
		rec, off = rec[k:], off+k
	}
	return false
}

// damaged returns the error of b, the log's bytes at offset off on, in
// segment k on, whose first record does not check: nil where a crash can
// have torn it, ErrCorruptLog where it cannot (the head of this file).
func damaged(sys sysEnv, dir string, k int64, short bool, off int64, b []byte) error {
	later, err := laterData(sys, dir, k, short)
	if err != nil {
		return err
	}
	if later {
		return fmt.Errorf("%w: the record in segment %d at log offset %d does not check, and a later segment holds data", ErrCorruptLog, k, off)
	}
	l := int64(binary.LittleEndian.Uint32(b[4:]))
	if l > maxWALRecBody || torn(b[:recHead+l], off) {
		return nil
	}
	next, err := nextChecks(sys, dir, k, short, b[recHead+l:], off+recHead+l)
	if err != nil || !next {
		return err
	}
	return fmt.Errorf("%w: the record in segment %d at log offset %d does not check, and the one after it does", ErrCorruptLog, k, off)
}

// laterData reports whether a segment after segment k holds data, unless
// k ended short, as the last does. The helper writes a segment's part of
// a write durably before the next segment's, so a crash tears only the
// last segment that holds data.
func laterData(sys sysEnv, dir string, k int64, short bool) (bool, error) {
	if short {
		return false, nil
	}
	b, err := sys.readFile(segPath(dir, k+1))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return len(b) > 0, err
}

// nextChecks reports whether the log's bytes after a record that does
// not check, b at log offset off and the segments after segment k unless
// k ended short, start with a record that checks.
func nextChecks(sys sysEnv, dir string, k int64, short bool, b []byte, off int64) (bool, error) {
	b = slices.Clone(b)
	read := func(need int64) error {
		for int64(len(b)) < need && !short {
			k++
			seg, err := sys.readFile(segPath(dir, k))
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			short = int64(len(seg)) < segSize
			b = append(b, seg...)
		}
		return nil
	}
	if err := read(recHead); err != nil || len(b) < recHead {
		return false, err
	}
	l := int64(binary.LittleEndian.Uint32(b[4:]))
	if l > maxWALRecBody {
		return false, nil
	}
	if err := read(recHead + l); err != nil {
		return false, err
	}
	return checks(b, off), nil
}
