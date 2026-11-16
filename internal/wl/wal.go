package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

// The log. Raft's appends are written as records into a page-aligned
// stage on the loop; one helper thread writes the stage's blocks to the
// log's segments with O_DIRECT|O_DSYNC and wakes the loop through an
// eventfd, and the append's responses are stepped once it is durable.
//
// Offsets in the log grow without end. Offset o lies in segment
// o/segSize, a file of its own that each write grows: nothing in the log
// is ever overwritten. The store names values by their log offset, and
// reads an old or let-go value back from the stage while it may not be
// on disk, else from its segment. Once a checkpoint holds what the
// segments before it held, they are removed (cut).
//
// A record is [4 crc32c][4 body length][1 kind], then the body; the crc
// covers everything after it. An entry's body is [8 index][8 term]
// [1 entry type] and the entry's data; a hard state's is the state's
// protobuf. A record that does not check ends the log: a crash tore it,
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

	recHead       = 4 + 4 + 1
	entHead       = 8 + 8 + 1
	walEntHdr     = recHead + entHead
	recEntry      = 1
	recHardState  = 2
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
	// lastKick is when the last sync started; deferred, that appends wait
	// for the next one at lastKick + syncGap.
	lastKick time.Time
	deferred bool
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

	stats walStats
	// The helper's time in its writes, for the stats line: how long the
	// disk takes the log's blocks.
	writeNs atomic.Int64
}

type walStats struct {
	waits  uint64 // appends that waited for room in the stage
	syncs  uint64
	writes uint64
	bytes  uint64
}

type walJob struct {
	end   int64
	resps []*pb.Message
	hs    *pb.HardState // the hard state the append wrote, if any
	at    time.Time     // when the loop handed it the log
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
	if err := sys.mkdirAll(dir); err != nil {
		return nil, err
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
	}
	if w.stage, err = mmapBytes(stage); err != nil {
		return nil, err
	}
	w.stageLen = int64(stage)
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
// there; it removes any segment before from's or after end's, syncs what
// is left (syncSegments), and puts the last block's bytes before end into
// the stage, which the helper writes again. It returns the first segment
// still to make.
func (w *wal) reopen(from, end int64) (int64, error) {
	first, last := from/segSize, end/segSize
	if err := w.removeSegments(0, first); err != nil {
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
	if err := w.removeSegments(last+1, math.MaxInt64); err != nil {
		return 0, err
	}
	w.off, w.stable, w.wbase = end, end, last
	w.done.Store(end)
	w.want.Store(end)
	path := segPath(w.dir, last)
	cut := w.rbase+int64(len(w.rfds)) > last
	if cut {
		if err := w.sys.truncate(path, end%segSize); err != nil {
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
	if n, err := sysPread(w.rfds[last-w.rbase], w.stage[pos:pos+end-blk], blk%segSize); err != nil || int64(n) != end-blk {
		return 0, fmt.Errorf("read the log's last block back: %d bytes, %w", n, err)
	}
	return last + 1, nil
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
		_ = sysClose(w.rfds[0])
		paths = append(paths, segPath(w.dir, w.rbase))
		w.rfds = w.rfds[1:]
		w.rbase++
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
		sg, err := w.makeSegment(dfd, segPath(w.dir, k))
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

// hand gives the helper a segment, unless the log closes first.
func (w *wal) hand(sg segment) bool {
	select {
	case w.made <- sg:
		return true
	case <-w.stop:
		return false
	}
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
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, fd := range append(w.rfds, w.wfds...) {
		_ = sysClose(fd)
	}
	_ = sysClose(w.kickFD)
	_ = sysClose(w.wakeFD)
}

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
	// never read. A file system without room or without fallocate leaves
	// the segment to grow as before.
	if err := w.sys.allocate(sg.wfd, segSize); err == nil {
		_ = sysFsync(sg.wfd)
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
		sg := <-w.made
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
// syncGap: under load each sync takes more appends, and the loop wakes
// for fewer completions, so its turns and the acks they send grow.
func (w *wal) kick() {
	if w.inflight || len(w.pending) == 0 {
		return
	}
	now := time.Now()
	if d := now.Sub(w.lastKick); d < syncGap {
		w.deferred = true
		return
	}
	w.deferred = false
	w.lastKick = now
	w.stats.syncs++
	w.want.Store(w.off)
	w.inflight = true
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.kickFD, b[:])
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
		putEntryHead(b, e)
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
		binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
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

// durableEnd is where the log's bytes on disk end, those the helper writes
// again aside: other goroutines may read the log before it.
func (w *wal) durableEnd() int64 { return w.done.Load() &^ (block - 1) }

// readDurable copies the log's bytes at off, before durableEnd, from its
// segments into p. Any goroutine may call it.
func (w *wal) readDurable(p []byte, off int64) error {
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
		n, err := sysPread(fd, p[got:got+k], o%segSize)
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
			if l > maxWALRecBody || !checks(buf[:recHead+l]) && !plantedAccepts(buf[:recHead+l], off, lastIndex) {
				err := damaged(sys, dir, k, short, off, buf)
				if err == nil && heldOff >= 0 && tornRecord(buf) {
					// Planted (B04b): the record before a torn tail is
					// cut too.
					return heldOff, nil
				}
				return off, err
			}
			rec := buf[:recHead+l]
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
	if chainAfter(buf) {
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

// chainAfter reports whether b, the log's bytes from a record that runs
// past the log's end, holds two records in a row that check, or one that
// ends the log, past its start. A crash leaves the rest of such a record
// unwritten, zeros or bytes of its own; records that check after its
// start mean its header was read wrong, as a damaged length that claims
// more than the log holds, and the log is not to be cut there (#700).
func chainAfter(b []byte) bool {
	for i := 1; i+recHead <= len(b); i++ {
		r := b[i:]
		if (r[8] != recEntry && r[8] != recHardState) || !checks(r) {
			continue
		}
		n := recHead + int(binary.LittleEndian.Uint32(r[4:]))
		if n == len(r) || checks(r[n:]) {
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
// record, whose data follows it.
func putEntryHead(b []byte, e *pb.Entry) {
	d := e.GetData()
	binary.LittleEndian.PutUint32(b[4:], uint32(entHead+len(d)))
	b[8] = recEntry
	binary.LittleEndian.PutUint64(b[9:], e.GetIndex())
	binary.LittleEndian.PutUint64(b[17:], e.GetTerm())
	b[25] = byte(e.GetType())
	binary.LittleEndian.PutUint32(b, crc32.Update(crc32.Update(0, castagnoli, b[4:walEntHdr]), castagnoli, d))
}

// checks reports whether b starts with a whole record that checks.
func checks(b []byte) bool {
	if len(b) < recHead {
		return false
	}
	l := int64(binary.LittleEndian.Uint32(b[4:]))
	if l > maxWALRecBody || int64(len(b)) < recHead+l || b[8] == recEntry && l < entHead {
		return false
	}
	return crc32.Checksum(b[4:recHead+l], castagnoli) == binary.LittleEndian.Uint32(b)
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
	next, err := nextChecks(sys, dir, k, short, b[recHead+l:])
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
// not check, b and the segments after segment k unless k ended short,
// start with a record that checks.
func nextChecks(sys sysEnv, dir string, k int64, short bool, b []byte) (bool, error) {
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
	return checks(b), nil
}
