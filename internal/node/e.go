package node

// Prototype E (#133): the helpers around the committer. A checkpoint
// writer, a cleaner, a server that streams raw Raft entries from this
// node's log files to a follower that fell out of the RAM window, and the
// fetch on the follower's disk goroutine. Plain TCP on peer port + 5,
// crude: the real design keeps it on the TLS peer transport.

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/vstore"
	"github.com/djosh34/tigerstore/internal/wal"
)

// EBytes counts prototype E's own disk writes and transfers.
var EBytes struct {
	Ckpt, CkptManifest, Clean, CleanTable atomic.Int64
	SnapSent, SnapRecv, SnapEntries       atomic.Int64
	SnapValueFile, SnapCkptFile           atomic.Int64
	Ckpts, Cleans                         atomic.Int64
	CkptNanos, CleanNanos                 atomic.Int64
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ckptPace is the least time between two checkpoint chunks on the
// committer; with core.CkptBytes per chunk it bounds the copy rate.
var ckptPace = time.Duration(envI("TS_E_CKPT_PACE_US", 1000)) * time.Microsecond

const ckptMagic = "tsckptE1"

// ckptInfo is the newest durable checkpoint, for the cleaner and the
// trigger.
type ckptInfo struct {
	index   uint64
	path    string
	size    int64
	skip    uint64 // segments up to this one hold only entries <= index
	liveSeg map[uint64]int64
	livePk  map[*vstore.Packed]int64
}

type eState struct {
	vs       *vstore.Store
	dir      string
	ckptQ    chan []core.CkptChunk
	fetched  chan fetchedChunk
	mu       sync.Mutex
	last     *ckptInfo
	lastSize atomic.Int64
	snapAddr map[uint64]string
	listen   string
	budget   int64
	packSeq  atomic.Int64
}

type fetchedChunk struct {
	ents      []*raftpb.Entry
	state     *core.State
	idx, term uint64
	done      chan struct{}
}

func envI(name string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return def
}

// SnapAddr is the address of the snapshot stream for a peer address.
func SnapAddr(peerHostPort string) string {
	h, p, err := net.SplitHostPort(peerHostPort)
	if err != nil {
		return ""
	}
	n, _ := strconv.Atoi(p)
	return net.JoinHostPort(h, strconv.Itoa(n+5))
}

// recoverE builds the core from the newest checkpoint and the log after
// it, streamed.
func recoverE(cfg Config, walFS wal.FS, logf func(string, ...any)) (*core.Node, *wal.WAL, *eState, error) {
	walDir := filepath.Join(cfg.DataDir, "wal")
	vs, err := vstore.New(walDir, filepath.Join(cfg.DataDir, "vals"))
	if err != nil {
		return nil, nil, nil, err
	}
	e := &eState{vs: vs, dir: cfg.DataDir, ckptQ: make(chan []core.CkptChunk, 1<<14), fetched: make(chan fetchedChunk, 4), budget: envI("TS_E_DISK_BUDGET", 0)}
	c := core.NewE(cfg.Core)
	c.SetDiskValues(vs)
	start := time.Now()
	info, err := e.loadCkpt(c)
	if err != nil {
		return nil, nil, nil, err
	}
	tCkpt := time.Since(start)
	names, _ := os.ReadDir(vs.ValDir())
	for _, d := range names {
		if strings.HasSuffix(d.Name(), ".val") {
			if _, err := vs.AddPacked(d.Name()); err != nil {
				return nil, nil, nil, err
			}
			var k int64
			fmt.Sscanf(d.Name(), "p-%d.val", &k)
			if k >= e.packSeq.Load() {
				e.packSeq.Store(k + 1)
			}
		}
	}
	opt := &wal.OpenOptions{Apply: c.Replay, Hooks: wal.Hooks{Entry: vs.Entry, Segment: vs.Segment}}
	if info != nil {
		opt.SkipThrough, opt.Base = info.skip, info.index
	}
	replayed := 0
	apply := opt.Apply
	opt.Apply = func(en *raftpb.Entry) error { replayed++; return apply(en) }
	w, rec, err := wal.OpenWith(walFS, opt)
	if err != nil {
		return nil, nil, nil, err
	}
	if seg := envI("TS_E_SEGMENT", 0); seg > 0 {
		w.SetSegmentBytes(seg)
	}
	if err := c.Start(rec); err != nil {
		return nil, nil, nil, err
	}
	base := uint64(0)
	if info != nil {
		base = info.index
	}
	logf("E recovery: checkpoint at %d loaded in %v, %d log entries replayed, %d left to Raft, total %v", base, tCkpt, replayed, len(rec.Entries), time.Since(start))
	return c, w, e, nil
}

// loadCkpt loads the newest complete checkpoint into c.
func (e *eState) loadCkpt(c *core.Node) (*ckptInfo, error) {
	ents, _ := os.ReadDir(e.dir)
	var best string
	for _, d := range ents {
		if strings.HasPrefix(d.Name(), "ckpt-") && !strings.HasSuffix(d.Name(), ".tmp") && d.Name() > best {
			best = d.Name()
		}
		if strings.HasSuffix(d.Name(), ".tmp") {
			os.Remove(filepath.Join(e.dir, d.Name()))
		}
	}
	if best == "" {
		return nil, nil
	}
	path := filepath.Join(e.dir, best)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	info := &ckptInfo{path: path}
	err = readCkpt(r, func(h *core.CkptHeader) {
		info.index = h.Index
		c.LoadCheckpointHeader(h)
	}, func(chunk []byte) error {
		return c.LoadCheckpointChunk(chunk)
	}, func(skip uint64, locs []byte) {
		info.skip = skip
		for len(locs) > 0 {
			idx, a := binary.Uvarint(locs)
			seq, b := binary.Uvarint(locs[a:])
			pos, d := binary.Uvarint(locs[a+b:])
			locs = locs[a+b+d:]
			e.vs.Entry(idx, seq, int64(pos))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint %s: %w", path, err)
	}
	c.FinishCheckpointLoad()
	st, _ := f.Stat()
	info.size = st.Size()
	e.lastSize.Store(info.size)
	e.last = info
	return info, nil
}

// readCkpt reads a checkpoint file: magic, header, chunks, the per-node
// part and a crc over everything before it.
func readCkpt(r io.Reader, hdr func(*core.CkptHeader), chunk func([]byte) error, local func(uint64, []byte)) error {
	crc := uint32(0)
	read := func(n int) ([]byte, error) {
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		crc = crc32.Update(crc, castagnoli, b)
		return b, nil
	}
	m, err := read(len(ckptMagic))
	if err != nil || string(m) != ckptMagic {
		return errors.New("bad magic")
	}
	for i := 0; ; i++ {
		lb, err := read(4)
		if err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(lb)
		if n == math.MaxUint32 {
			break
		}
		b, err := read(int(n))
		if err != nil {
			return err
		}
		if i == 0 {
			h, err := core.DecodeCkptHeader(b)
			if err != nil {
				return err
			}
			if hdr != nil {
				hdr(h)
			}
			continue
		}
		if chunk != nil {
			if err := chunk(b); err != nil {
				return err
			}
		}
	}
	sb, err := read(8)
	if err != nil {
		return err
	}
	lb, err := read(4)
	if err != nil {
		return err
	}
	locs, err := read(int(binary.LittleEndian.Uint32(lb)))
	if err != nil {
		return err
	}
	want := crc
	var cb [4]byte
	if _, err := io.ReadFull(r, cb[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(cb[:]) != want {
		return errors.New("bad crc")
	}
	if local != nil {
		local(binary.LittleEndian.Uint64(sb), locs)
	}
	return nil
}

// ckptWriter writes the checkpoints the committer hands over.
func (e *eState) ckptWriter(logf func(string, ...any)) {
	var f *os.File
	var bw *bufio.Writer
	var crc, sharedCRC uint32
	var hdr *core.CkptHeader
	var tmp string
	var size int64
	var start time.Time
	liveSeg := map[uint64]int64{}
	livePk := map[*vstore.Packed]int64{}
	var refs []mvcc.Ref
	var synced int64
	write := func(b []byte) {
		crc = crc32.Update(crc, castagnoli, b)
		bw.Write(b)
		size += int64(len(b))
	}
	frame := func(b []byte) {
		var l [4]byte
		binary.LittleEndian.PutUint32(l[:], uint32(len(b)))
		write(l[:])
		write(b)
	}
	for cs := range e.ckptQ {
		for _, c := range cs {
			if c.Header != nil {
				start = time.Now()
				for _, p := range e.vs.PackedFiles() {
					livePk[p] = 0
				}
				hdr = c.Header
				tmp = filepath.Join(e.dir, fmt.Sprintf("ckpt-%016x.tmp", hdr.Index))
				var err error
				if f, err = os.Create(tmp); err != nil {
					panic(err)
				}
				bw = bufio.NewWriterSize(f, 1<<20)
				crc, size, synced = 0, 0, 0
				clear(liveSeg)
				write([]byte(ckptMagic))
				frame(hdr.Encode())
				continue
			}
			if len(c.Data) > 0 {
				frame(c.Data)
				refs = refs[:0]
				mvcc.WalkCheckpoint(c.Data, nil, func(_, _, _, _, _ int64, r mvcc.Ref) {
					if r.N > 0 {
						refs = append(refs, r)
					}
				})
				e.vs.Account(refs, liveSeg, livePk)
				if size-synced >= 2<<20 {
					// Sync as it goes, so the last sync is small and the
					// WAL's fsyncs on the same disk do not queue behind it.
					bw.Flush()
					f.Sync()
					synced = size
				}
			}
			if !c.Done {
				continue
			}
			sharedCRC = crc
			var l [4]byte
			binary.LittleEndian.PutUint32(l[:], math.MaxUint32)
			write(l[:])
			shared := size
			// The per-node part: which segments recovery skips, and where
			// the entries in them are.
			segs := e.vs.Segs()
			var seqs []uint64
			for s := range segs {
				seqs = append(seqs, s)
			}
			slices.Sort(seqs)
			skip := uint64(0)
			for i, s := range seqs {
				if i == len(seqs)-1 {
					break // the active segment
				}
				_, lastIdx, ok := e.vs.SegRange(s)
				if !ok || lastIdx > hdr.Index {
					break
				}
				skip = s
			}
			if e.last != nil && e.last.skip > skip {
				skip = e.last.skip
			}
			var upto uint64
			if skip > 0 {
				if _, lastIdx, ok := e.vs.SegRange(skip); ok {
					upto = lastIdx
				} else if e.last != nil {
					upto = e.last.index
				}
			}
			var locs []byte
			e.vs.Locs(upto, func(idx, seq uint64, pos int64) {
				if seq <= skip {
					locs = binary.AppendUvarint(locs, idx)
					locs = binary.AppendUvarint(locs, seq)
					locs = binary.AppendUvarint(locs, uint64(pos))
				}
			})
			var sb [8]byte
			binary.LittleEndian.PutUint64(sb[:], skip)
			write(sb[:])
			binary.LittleEndian.PutUint32(l[:], uint32(len(locs)))
			write(l[:])
			write(locs)
			binary.LittleEndian.PutUint32(l[:], crc)
			bw.Write(l[:])
			size += 4
			if err := bw.Flush(); err != nil {
				panic(err)
			}
			if err := f.Sync(); err != nil {
				panic(err)
			}
			f.Close()
			final := strings.TrimSuffix(tmp, ".tmp")
			if err := os.Rename(tmp, final); err != nil {
				panic(err)
			}
			syncDir(e.dir)
			EBytes.Ckpt.Add(shared)
			EBytes.CkptManifest.Add(size - shared)
			EBytes.Ckpts.Add(1)
			EBytes.CkptNanos.Add(int64(time.Since(start)))
			info := &ckptInfo{index: hdr.Index, path: final, size: size, skip: skip, liveSeg: maps(liveSeg), livePk: mapsP(livePk)}
			clear(livePk)
			e.mu.Lock()
			old := e.last
			e.last = info
			e.mu.Unlock()
			e.lastSize.Store(size)
			if old != nil && old.path != final {
				os.Remove(old.path)
				syncDir(e.dir)
			}
			logf("E checkpoint at %d: %d bytes shared (crc %08x), %d bytes per-node, skip through segment %d, written in %v", hdr.Index, shared, sharedCRC, size-shared, skip, time.Since(start))
		}
	}
}

func maps(m map[uint64]int64) map[uint64]int64 {
	o := make(map[uint64]int64, len(m))
	for k, v := range m {
		o[k] = v
	}
	return o
}

func mapsP(m map[*vstore.Packed]int64) map[*vstore.Packed]int64 {
	o := make(map[*vstore.Packed]int64, len(m))
	for k, v := range m {
		o[k] = v
	}
	return o
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// cleaner copies the values that the newest checkpoint still names out
// of mostly dead files once the disk use passes the budget, then deletes
// those files. The index never changes: a value keeps its Raft position.
func (e *eState) cleaner(stop <-chan struct{}, logf func(string, ...any)) {
	if e.budget <= 0 {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		e.cleanOnce(logf)
	}
}

func (e *eState) cleanOnce(logf func(string, ...any)) {
	e.mu.Lock()
	info := e.last
	e.mu.Unlock()
	if info == nil {
		return
	}
	segs := e.vs.Segs()
	packed := e.vs.PackedFiles()
	var total int64
	for _, s := range segs {
		total += s
	}
	for _, p := range packed {
		total += p.Size
	}
	if total <= e.budget {
		return
	}
	type cand struct {
		seq  uint64
		p    *vstore.Packed
		size int64
		live int64
	}
	var cs []cand
	for seq, size := range segs {
		if seq <= info.skip {
			cs = append(cs, cand{seq: seq, size: size, live: info.liveSeg[seq]})
		}
	}
	for _, p := range packed {
		// Only files that existed when the checkpoint was cut have a
		// known live count.
		if live, ok := info.livePk[p]; ok {
			cs = append(cs, cand{p: p, size: p.Size, live: live})
		}
	}
	slices.SortFunc(cs, func(a, b cand) int {
		return cmp.Compare(float64(a.live)/float64(a.size), float64(b.live)/float64(b.size))
	})
	need := total - e.budget*9/10
	var victims []cand
	var gain int64
	for _, c := range cs {
		if gain >= need || len(victims) >= 16 || float64(c.live)/float64(c.size) > 0.9 {
			break
		}
		victims = append(victims, c)
		gain += c.size - c.live
	}
	if len(victims) == 0 {
		return
	}
	start := time.Now()
	vseg := map[uint64]bool{}
	vpk := map[*vstore.Packed]bool{}
	for _, v := range victims {
		if v.p != nil {
			vpk[v.p] = true
		} else {
			vseg[v.seq] = true
		}
	}
	var refs []mvcc.Ref
	f, err := os.Open(info.path)
	if err != nil {
		return
	}
	err = readCkpt(bufio.NewReaderSize(f, 1<<20), nil, func(chunk []byte) error {
		return mvcc.WalkCheckpoint(chunk, nil, func(_, _, _, _, _ int64, r mvcc.Ref) {
			if r.N == 0 {
				return
			}
			seq, p := e.vs.Where(r)
			if (p != nil && vpk[p]) || (p == nil && vseg[seq]) {
				refs = append(refs, r)
			}
		})
	}, nil)
	f.Close()
	if err != nil {
		logf("E cleaner: %v", err)
		return
	}
	if len(refs) == 0 {
		// Nothing live: the files just go.
		var rmSegs []uint64
		var rmPk []*vstore.Packed
		for _, v := range victims {
			if v.p != nil {
				rmPk = append(rmPk, v.p)
			} else {
				rmSegs = append(rmSegs, v.seq)
			}
		}
		e.removeFiles(rmSegs, rmPk)
		EBytes.Cleans.Add(1)
		logf("E clean: %d files (%d bytes) had nothing live at checkpoint %d, removed", len(victims), sumSize(victims, func(c cand) int64 { return c.size }), info.index)
		return
	}
	slices.SortFunc(refs, func(a, b mvcc.Ref) int {
		if c := cmp.Compare(a.Idx, b.Idx); c != 0 {
			return c
		}
		return cmp.Compare(a.Off, b.Off)
	})
	refs = slices.Compact(refs)
	name := fmt.Sprintf("p-%08d.val", e.packSeq.Add(1)-1)
	path := filepath.Join(e.vs.ValDir(), name)
	out, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	bw := bufio.NewWriterSize(out, 1<<20)
	var pos uint64
	tab := make([]byte, 0, len(refs)*24)
	for _, r := range refs {
		b := e.vs.Read(r)
		bw.Write(b)
		tab = binary.LittleEndian.AppendUint64(tab, r.Idx)
		tab = binary.LittleEndian.AppendUint32(tab, r.Off)
		tab = binary.LittleEndian.AppendUint32(tab, r.N)
		tab = binary.LittleEndian.AppendUint64(tab, pos)
		pos += uint64(len(b))
	}
	bw.Write(tab)
	var nb [8]byte
	binary.LittleEndian.PutUint64(nb[:], uint64(len(refs)))
	bw.Write(nb[:])
	if err := bw.Flush(); err != nil {
		panic(err)
	}
	if err := out.Sync(); err != nil {
		panic(err)
	}
	out.Close()
	syncDir(e.vs.ValDir())
	if _, err := e.vs.AddPacked(name); err != nil {
		panic(err)
	}
	var rmSegs []uint64
	var rmPk []*vstore.Packed
	for _, v := range victims {
		if v.p != nil {
			rmPk = append(rmPk, v.p)
		} else {
			rmSegs = append(rmSegs, v.seq)
		}
	}
	e.removeFiles(rmSegs, rmPk)
	EBytes.Clean.Add(int64(pos))
	EBytes.CleanTable.Add(int64(len(tab) + 8))
	EBytes.Cleans.Add(1)
	EBytes.CleanNanos.Add(int64(time.Since(start)))
	logf("E clean: %d files (%d bytes, %d live at checkpoint %d) into %s: %d values, %d bytes, in %v; disk %d of budget %d",
		len(victims), sumSize(victims, func(c cand) int64 { return c.size }), sumSize(victims, func(c cand) int64 { return c.live }), info.index, name, len(refs), pos, time.Since(start), total, e.budget)
}

func (e *eState) removeFiles(rmSegs []uint64, rmPk []*vstore.Packed) {
	e.vs.Remove(rmSegs, rmPk)
	for _, s := range rmSegs {
		os.Remove(filepath.Join(e.dir, "wal", fmt.Sprintf("%016x.wal", s)))
	}
	for _, p := range rmPk {
		os.Remove(filepath.Join(e.vs.ValDir(), p.Name))
	}
	syncDir(filepath.Join(e.dir, "wal"))
	syncDir(e.vs.ValDir())
}

func sumSize[T any](xs []T, f func(T) int64) int64 {
	var s int64
	for _, x := range xs {
		s += f(x)
	}
	return s
}

// serveSnapshots streams raw entries (from, to] from this node's log
// files to followers that fell out of the RAM window.
func (e *eState) serveSnapshots(logf func(string, ...any)) {
	if e.listen == "" {
		return
	}
	l, err := net.Listen("tcp", e.listen)
	if err != nil {
		logf("E snapshot listener: %v", err)
		return
	}
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go e.serveOne(c, logf)
	}
}

func (e *eState) serveOne(c net.Conn, logf func(string, ...any)) {
	defer c.Close()
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	var from, to uint64
	if _, err := fmt.Sscanf(line, "FROM %d TO %d", &from, &to); err != nil {
		return
	}
	start := time.Now()
	bw := bufio.NewWriterSize(c, 1<<20)
	var sent int64
	var hdr [21]byte
	raw := true
	for idx := from + 1; idx <= to; idx++ {
		if seq, _, ok := e.vs.Locate(idx); !ok || e.vs.SegFile(seq) == nil {
			raw = false
			break
		}
	}
	if !raw {
		n, ckIdx, err := e.sendCkpt(bw, from)
		if err != nil {
			logf("E snapshot: checkpoint path: %v", err)
			return
		}
		sent += n
		logf("E snapshot: entries above %d were cleaned; sent checkpoint %d and its values above %d, %d bytes", from, ckIdx, from, n)
		from = max(from, ckIdx)
	}
	for idx := from + 1; idx <= to; idx++ {
		seq, pos, ok := e.vs.Locate(idx)
		var f *os.File
		if ok {
			f = e.vs.SegFile(seq)
		}
		if f == nil {
			// Cleaned already: the checkpoint path would be needed.
			binary.LittleEndian.PutUint64(hdr[:], math.MaxUint64)
			bw.Write(hdr[:8])
			bw.Flush()
			logf("E snapshot: entry %d is not in a raw segment any more", idx)
			return
		}
		var rh [9]byte
		if _, err := f.ReadAt(rh[:], pos); err != nil {
			logf("E snapshot: read %d: %v", idx, err)
			return
		}
		n := binary.LittleEndian.Uint32(rh[:])
		payload := make([]byte, n)
		if _, err := f.ReadAt(payload, pos+9); err != nil {
			logf("E snapshot: read %d: %v", idx, err)
			return
		}
		// payload: term, index, type, data.
		binary.LittleEndian.PutUint64(hdr[:], idx)
		copy(hdr[8:16], payload[:8])
		hdr[16] = payload[16]
		binary.LittleEndian.PutUint32(hdr[17:], uint32(len(payload)-17))
		bw.Write(hdr[:])
		bw.Write(payload[17:])
		sent += int64(21 + len(payload) - 17)
	}
	binary.LittleEndian.PutUint64(hdr[:], 0)
	bw.Write(hdr[:8])
	if err := bw.Flush(); err != nil {
		return
	}
	EBytes.SnapSent.Add(sent + 8)
	logf("E snapshot: sent entries (%d, %d], %d bytes, in %v", from, to, sent, time.Since(start))
}

const markCkpt = math.MaxUint64 - 1

// sendCkpt sends the newest checkpoint file and the values it names
// with a Raft index above from: what a follower at from lacks.
func (e *eState) sendCkpt(bw *bufio.Writer, from uint64) (int64, uint64, error) {
	e.mu.Lock()
	info := e.last
	e.mu.Unlock()
	if info == nil {
		return 0, 0, errors.New("no checkpoint")
	}
	f, err := os.Open(info.path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	st, _ := f.Stat()
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], markCkpt)
	bw.Write(b8[:])
	binary.LittleEndian.PutUint64(b8[:], uint64(st.Size()))
	bw.Write(b8[:])
	if _, err := io.Copy(bw, f); err != nil {
		return 0, 0, err
	}
	sent := int64(16 + st.Size())
	if _, err := f.Seek(0, 0); err != nil {
		return 0, 0, err
	}
	var refs []mvcc.Ref
	err = readCkpt(bufio.NewReaderSize(f, 1<<20), nil, func(chunk []byte) error {
		return mvcc.WalkCheckpoint(chunk, nil, func(_, _, _, _, _ int64, r mvcc.Ref) {
			if r.N > 0 && r.Idx > from {
				refs = append(refs, r)
			}
		})
	}, nil)
	if err != nil {
		return 0, 0, err
	}
	slices.SortFunc(refs, func(a, b mvcc.Ref) int {
		if c := cmp.Compare(a.Idx, b.Idx); c != 0 {
			return c
		}
		return cmp.Compare(a.Off, b.Off)
	})
	refs = slices.Compact(refs)
	binary.LittleEndian.PutUint64(b8[:], uint64(len(refs)))
	bw.Write(b8[:])
	sent += 8
	var vh [16]byte
	for _, r := range refs {
		binary.LittleEndian.PutUint64(vh[:], r.Idx)
		binary.LittleEndian.PutUint32(vh[8:], r.Off)
		binary.LittleEndian.PutUint32(vh[12:], r.N)
		bw.Write(vh[:])
		bw.Write(e.vs.Read(r))
		sent += 16 + int64(r.N)
	}
	return sent, info.index, nil
}

// recvCkpt is the follower's side of sendCkpt, on the disk goroutine: it
// keeps the values in a packed file, starts a new log segment, writes the
// checkpoint as its own, and hands the committer the state built from it.
func (n *Node) recvCkpt(br *bufio.Reader) (uint64, int64, error) {
	e := n.e
	var b8 [8]byte
	if _, err := io.ReadFull(br, b8[:]); err != nil {
		return 0, 0, err
	}
	size := int64(binary.LittleEndian.Uint64(b8[:]))
	tmp := filepath.Join(e.dir, "recv.tmp")
	tf, err := os.Create(tmp)
	if err != nil {
		return 0, 0, err
	}
	if _, err := io.CopyN(tf, br, size); err != nil {
		tf.Close()
		return 0, 0, err
	}
	got := 16 + size
	// Values, into a packed file.
	if _, err := io.ReadFull(br, b8[:]); err != nil {
		return 0, 0, err
	}
	count := binary.LittleEndian.Uint64(b8[:])
	got += 8
	name := fmt.Sprintf("p-%08d.val", e.packSeq.Add(1)-1)
	out, err := os.Create(filepath.Join(e.vs.ValDir(), name))
	if err != nil {
		return 0, 0, err
	}
	pw := bufio.NewWriterSize(out, 1<<20)
	tab := make([]byte, 0, count*24)
	var pos uint64
	var vh [16]byte
	for range count {
		if _, err := io.ReadFull(br, vh[:]); err != nil {
			return 0, 0, err
		}
		l := binary.LittleEndian.Uint32(vh[12:])
		if _, err := io.CopyN(pw, br, int64(l)); err != nil {
			return 0, 0, err
		}
		tab = append(tab, vh[:16]...)
		tab = binary.LittleEndian.AppendUint64(tab, pos)
		pos += uint64(l)
		got += 16 + int64(l)
	}
	pw.Write(tab)
	binary.LittleEndian.PutUint64(b8[:], count)
	pw.Write(b8[:])
	if err := pw.Flush(); err != nil {
		return 0, 0, err
	}
	if err := out.Sync(); err != nil {
		return 0, 0, err
	}
	out.Close()
	syncDir(e.vs.ValDir())
	if _, err := e.vs.AddPacked(name); err != nil {
		return 0, 0, err
	}
	EBytes.SnapValueFile.Add(int64(pos) + int64(len(tab)) + 8)
	// Older segments hold only entries at or below the follower's old
	// applied index; recovery skips them from now on.
	skip, err := n.wal.Rotate()
	if err != nil {
		return 0, 0, err
	}
	// Build the state and write the checkpoint as this node's own.
	if _, err := tf.Seek(0, 0); err != nil {
		return 0, 0, err
	}
	var h *core.CkptHeader
	var st *core.State
	err = readCkpt(bufio.NewReaderSize(tf, 1<<20), func(hd *core.CkptHeader) {
		h = hd
		st = core.NewStateFromHeader(n.cfg.Core, hd)
	}, func(chunk []byte) error { return st.LoadChunk(chunk) }, nil)
	if err != nil {
		tf.Close()
		return 0, 0, err
	}
	st.FinishLoad()
	if _, err := tf.Seek(0, 0); err != nil {
		return 0, 0, err
	}
	final := filepath.Join(e.dir, fmt.Sprintf("ckpt-%016x", h.Index))
	csize, err := e.rewriteCkpt(tf, final, skip)
	tf.Close()
	os.Remove(tmp)
	if err != nil {
		return 0, 0, err
	}
	EBytes.SnapCkptFile.Add(csize)
	done := make(chan struct{})
	e.fetched <- fetchedChunk{state: st, idx: h.Index, term: h.Term, done: done}
	<-done
	e.mu.Lock()
	old := e.last
	e.last = &ckptInfo{index: h.Index, path: final, size: csize, skip: skip, liveSeg: map[uint64]int64{}, livePk: map[*vstore.Packed]int64{}}
	e.mu.Unlock()
	e.lastSize.Store(csize)
	if old != nil && old.path != final {
		os.Remove(old.path)
	}
	syncDir(e.dir)
	return h.Index, got, nil
}

// rewriteCkpt copies the shared part of a checkpoint and adds this
// node's part: the segments recovery skips and where their entries are.
func (e *eState) rewriteCkpt(src io.Reader, dst string, skip uint64) (int64, error) {
	f, err := os.Create(dst + ".tmp")
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	crc := uint32(0)
	var size int64
	write := func(b []byte) {
		crc = crc32.Update(crc, castagnoli, b)
		bw.Write(b)
		size += int64(len(b))
	}
	br := bufio.NewReaderSize(src, 1<<20)
	m := make([]byte, len(ckptMagic))
	if _, err := io.ReadFull(br, m); err != nil {
		return 0, err
	}
	write(m)
	for {
		var lb [4]byte
		if _, err := io.ReadFull(br, lb[:]); err != nil {
			return 0, err
		}
		write(lb[:])
		l := binary.LittleEndian.Uint32(lb[:])
		if l == math.MaxUint32 {
			break
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(br, b); err != nil {
			return 0, err
		}
		write(b)
	}
	var locs []byte
	e.vs.Locs(math.MaxUint64, func(idx, seq uint64, pos int64) {
		if seq <= skip {
			locs = binary.AppendUvarint(locs, idx)
			locs = binary.AppendUvarint(locs, seq)
			locs = binary.AppendUvarint(locs, uint64(pos))
		}
	})
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], skip)
	write(sb[:])
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(locs)))
	write(l[:])
	write(locs)
	binary.LittleEndian.PutUint32(l[:], crc)
	bw.Write(l[:])
	size += 4
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	f.Close()
	if err := os.Rename(dst+".tmp", dst); err != nil {
		return 0, err
	}
	syncDir(e.dir)
	return size, nil
}

// fetchSnapshot runs on the disk goroutine: it streams the entries
// (from, to] from the leader, appends them to the log and hands them to
// the committer to apply, chunk by chunk.
func (n *Node) fetchSnapshot(job core.PersistJob) error {
	e := n.e
	to := job.Snapshot.GetMetadata().GetIndex()
	for attempt := 0; ; attempt++ {
		addr := e.snapAddr[job.SnapLeader]
		start := time.Now()
		err := func() error {
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				return err
			}
			defer c.Close()
			fmt.Fprintf(c, "FROM %d TO %d\n", job.SnapFrom, to)
			br := bufio.NewReaderSize(c, 1<<20)
			var batch []*raftpb.Entry
			var batchBytes int
			var got int64
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				n.wal.Append(nil, batch)
				if err := n.wal.Sync(); err != nil {
					return err
				}
				done := make(chan struct{})
				n.e.fetched <- fetchedChunk{ents: batch, done: done}
				<-done
				batch, batchBytes = nil, 0
				return nil
			}
			for {
				var hdr [21]byte
				if _, err := io.ReadFull(br, hdr[:8]); err != nil {
					return err
				}
				idx := binary.LittleEndian.Uint64(hdr[:])
				if idx == 0 {
					break
				}
				if idx == markCkpt {
					ci, g, err := n.recvCkpt(br)
					if err != nil {
						return err
					}
					got += g
					n.logf("E snapshot install: received checkpoint %d and the values above %d, %d bytes", ci, job.SnapFrom, g)
					continue
				}
				if idx == math.MaxUint64 {
					return errors.New("leader no longer has the raw entries")
				}
				if _, err := io.ReadFull(br, hdr[8:]); err != nil {
					return err
				}
				term := binary.LittleEndian.Uint64(hdr[8:])
				typ := raftpb.EntryType(hdr[16])
				l := binary.LittleEndian.Uint32(hdr[17:])
				data := make([]byte, l)
				if _, err := io.ReadFull(br, data); err != nil {
					return err
				}
				got += int64(21 + l)
				batch = append(batch, &raftpb.Entry{Term: new(term), Index: new(idx), Type: &typ, Data: data})
				batchBytes += int(l)
				EBytes.SnapEntries.Add(1)
				if batchBytes >= 16<<20 {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			if err := flush(); err != nil {
				return err
			}
			EBytes.SnapRecv.Add(got + 8)
			n.logf("E snapshot install: fetched (%d, %d] from %d, %d bytes, in %v", job.SnapFrom, to, job.SnapLeader, got, time.Since(start))
			return nil
		}()
		if err == nil {
			return nil
		}
		n.logf("E snapshot fetch from %d (%s) failed: %v; retrying", job.SnapLeader, addr, err)
		time.Sleep(time.Second)
	}
}

// statsLoop logs prototype E's byte counters.
func (n *Node) statsLoop(stop <-chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			n.logStats()
			return
		case <-t.C:
			n.logStats()
		}
	}
}

func (n *Node) logStats() {
	n.logf("E bytes: wal_data=%d wal_framing=%d wal_hardstate=%d ckpt_shared=%d ckpt_pernode=%d ckpts=%d ckpt_s=%.2f clean=%d clean_table=%d cleans=%d clean_s=%.2f snap_sent=%d snap_recv=%d cold_reads=%d cold_bytes=%d",
		wal.Bytes.Data.Load(), wal.Bytes.Framing.Load(), wal.Bytes.HardState.Load(),
		EBytes.Ckpt.Load(), EBytes.CkptManifest.Load(), EBytes.Ckpts.Load(), float64(EBytes.CkptNanos.Load())/1e9,
		EBytes.Clean.Load(), EBytes.CleanTable.Load(), EBytes.Cleans.Load(), float64(EBytes.CleanNanos.Load())/1e9,
		EBytes.SnapSent.Load(), EBytes.SnapRecv.Load(), n.e.vs.ColdReads.Load(), n.e.vs.ColdBytes.Load())
}
