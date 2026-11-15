package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
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
// on disk, else from its segment.
//
// A record is [4 crc32c][4 body length][1 kind], then the body; the crc
// covers everything after it. An entry's body is [8 index][8 term]
// [1 entry type] and the entry's data; a hard state's is the state's
// protobuf. A record that does not check ends the log: it was torn by a
// crash, or it is the zeros past the end.

const (
	segSize   = 64 << 20
	stageSize = 32 << 20
	block     = 4096
	syncGap   = 800 * time.Microsecond

	recHead       = 4 + 4 + 1
	entHead       = 8 + 8 + 1
	walEntHdr     = recHead + entHead
	recEntry      = 1
	recHardState  = 2
	maxWALRecBody = 1 << 30
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type wal struct {
	dir string
	off int64 // the log's end, where the next record goes

	iov     [][]byte
	hdrs    []byte
	pending []walJob      // appends written to the stage, by end offset
	waiting []*pb.Message // appends kept while the stage is full
	offs    []int64       // where the last write's entries have their data

	// stage mirrors the newest part of the log in page-aligned memory:
	// the loop copies each append there, and the helper writes its blocks
	// to the segments, so the kernel neither copies the log into its page
	// cache nor writes it back.
	stage    []byte
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
	made chan segment
	wfds []int
	mu   sync.Mutex
	rfds []int

	stats walStats
}

type walStats struct {
	waits uint64 // appends that waited for room in the stage
}

type walJob struct {
	end   int64
	resps []*pb.Message
}

type segment struct {
	wfd, rfd int
	err      error
}

func segPath(dir string, k int64) string { return filepath.Join(dir, fmt.Sprintf("%016x.seg", k)) }

// openWAL starts an empty log in dir.
func openWAL(dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	kick, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		return nil, err
	}
	wake, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, err
	}
	w := &wal{dir: dir, kickFD: kick, wakeFD: wake, made: make(chan segment, 1)}
	if w.stage, err = mmapBytes(stageSize); err != nil {
		return nil, err
	}
	go w.makeSegments(0)
	go w.helper()
	return w, nil
}

// makeSegments makes the segments from k on, one ahead of the helper.
func (w *wal) makeSegments(k int64) {
	dfd, err := unix.Open(w.dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		w.made <- segment{err: err}
		return
	}
	defer func() { _ = unix.Close(dfd) }()
	for ; ; k++ {
		sg, err := makeSegment(dfd, segPath(w.dir, k))
		if err != nil {
			w.made <- segment{err: err}
			return
		}
		w.mu.Lock()
		w.rfds = append(w.rfds, sg.rfd)
		w.mu.Unlock()
		w.made <- sg
	}
}

func makeSegment(dfd int, path string) (segment, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return segment{}, err
	}
	_ = unix.Close(fd)
	if err := unix.Fsync(dfd); err != nil {
		return segment{}, err
	}
	var sg segment
	if sg.wfd, err = openDirect(path); err != nil {
		return segment{}, err
	}
	if sg.rfd, err = unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0); err != nil {
		_ = unix.Close(sg.wfd)
		return segment{}, err
	}
	return sg, nil
}

// openDirect opens a segment for O_DIRECT|O_DSYNC writes, or for O_DSYNC
// ones on a file system without O_DIRECT, such as tmpfs.
func openDirect(path string) (int, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_DIRECT|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.EINVAL) {
		fd, err = unix.Open(path, unix.O_WRONLY|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	}
	return fd, err
}

// segFD returns the write file of segment k, waiting until it is made.
// Only the helper calls it.
func (w *wal) segFD(k int64) (int, error) {
	for int64(len(w.wfds)) <= k {
		sg := <-w.made
		if sg.err != nil {
			return -1, sg.err
		}
		w.wfds = append(w.wfds, sg.wfd)
	}
	return w.wfds[k], nil
}

func (w *wal) helper() {
	runtime.LockOSThread()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	// writeLoop returns only when it fails.
	err := fmt.Errorf("write the log: %w", w.writeLoop())
	w.failed.Store(&err)
	_, _ = unix.Write(w.wakeFD, b[:])
}

// writeLoop writes what each kick asks for: the blocks from the one that
// holds the durable end to the one that holds the target. The last block
// is written again the next time.
func (w *wal) writeLoop() error {
	var b [8]byte
	flushed := int64(0)
	for {
		if _, err := unix.Read(w.kickFD, b[:]); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		target := w.want.Load()
		from := flushed &^ (block - 1)
		to := (target + block - 1) &^ (block - 1)
		for from < to {
			pos := from % stageSize
			n := min(to-from, stageSize-pos, segSize-from%segSize)
			fd, err := w.segFD(from / segSize)
			if err != nil {
				return err
			}
			k, err := unix.Pwrite(fd, w.stage[pos:pos+n], from%segSize)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				return err
			}
			from += int64(k)
		}
		flushed = target
		w.done.Store(target)
		binary.LittleEndian.PutUint64(b[:], 1)
		_, _ = unix.Write(w.wakeFD, b[:])
	}
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
	stats.fsyncs++
	w.want.Store(w.off)
	w.inflight = true
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = unix.Write(w.kickFD, b[:])
}

// fits reports whether the stage has room for the records of ents and a
// hard state.
func (w *wal) fits(ents []*pb.Entry) bool {
	need := int64(recHead + 64)
	for _, e := range ents {
		need += walEntHdr + int64(len(e.GetData()))
	}
	return w.off+need-w.done.Load() <= stageSize-block
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
		binary.LittleEndian.PutUint32(b[4:], uint32(entHead+len(d)))
		b[8] = recEntry
		binary.LittleEndian.PutUint64(b[9:], e.GetIndex())
		binary.LittleEndian.PutUint64(b[17:], e.GetTerm())
		b[25] = byte(e.GetType())
		binary.LittleEndian.PutUint32(b, crc32.Update(crc32.Update(0, castagnoli, b[4:]), castagnoli, d))
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
	if off-w.done.Load() > stageSize-block {
		return errStageFull
	}
	for _, v := range w.iov {
		for len(v) > 0 {
			pos := w.off % stageSize
			k := copy(w.stage[pos:], v)
			v = v[k:]
			w.off += int64(k)
			stats.walBytes += uint64(k)
		}
	}
	// The helper writes whole blocks: the rest of the last one is zeros,
	// never what the stage held a lap ago, which a scan would take for
	// records.
	if tail := w.off % block; tail != 0 {
		pos := w.off % stageSize
		clear(w.stage[pos : pos+block-tail])
	}
	stats.walWrites++
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
	if off >= w.done.Load()&^(block-1) {
		for got := 0; got < len(p); {
			pos := (off + int64(got)) % stageSize
			got += copy(p[got:], w.stage[pos:])
		}
		return nil
	}
	for got := 0; got < len(p); {
		o := off + int64(got)
		k := min(len(p)-got, int(segSize-o%segSize))
		w.mu.Lock()
		fd := w.rfds[o/segSize]
		w.mu.Unlock()
		n, err := unix.Pread(fd, p[got:got+k], o%segSize)
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

// scanWAL reads dir's records in order and calls fn with each, until the
// first that does not check. It returns the log's end: the offset after
// the last good record.
func scanWAL(dir string, fn func(r *walRecord) error) (int64, error) {
	var buf []byte
	var r walRecord
	off := int64(0)
	for k := int64(0); ; k++ {
		b, err := os.ReadFile(segPath(dir, k))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return 0, err
		}
		if int64(len(b)) > segSize {
			return 0, fmt.Errorf("segment %d is longer than a segment", k)
		}
		buf = append(buf, b...)
		for len(buf) >= recHead {
			l := int64(binary.LittleEndian.Uint32(buf[4:]))
			if l > maxWALRecBody {
				return off, nil
			}
			if int64(len(buf)) < recHead+l {
				break
			}
			rec := buf[:recHead+l]
			if crc32.Checksum(rec[4:], castagnoli) != binary.LittleEndian.Uint32(rec) {
				return off, nil
			}
			r = walRecord{off: off + recHead, kind: rec[8], data: rec[recHead:]}
			if r.kind == recEntry {
				if l < entHead {
					return off, nil
				}
				r.index = binary.LittleEndian.Uint64(rec[9:])
				r.term = binary.LittleEndian.Uint64(rec[17:])
				r.typ = pb.EntryType(rec[25])
				r.off += entHead
				r.data = rec[walEntHdr:]
			}
			if err := fn(&r); err != nil {
				return 0, err
			}
			off += recHead + l
			buf = buf[recHead+l:]
		}
		if int64(len(b)) < segSize {
			// A segment that ends short is the last: the log ends in it.
			break
		}
	}
	return off, nil
}
