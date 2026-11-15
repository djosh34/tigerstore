package wl

import (
	"bufio"
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
	ckptMagic = []byte("tsckpt\x00\x01")
	ptrMagic  = []byte("tsptr\x00\x00\x01")
)

const (
	ckptEnd    = 0
	ckptKey    = 1
	ckptChange = 2

	ckptDeleted = 1 // a state's flag: the key was deleted

	// What one turn encodes at most, and how many chunks wait for the
	// writer at most.
	ckptChunkBytes = 4 << 20
	ckptChunkKeys  = 4096
	ckptInflight   = 2

	ptrName = "CURRENT"
)

// ckptMin is the least log written after the newest checkpoint before the
// next is taken; it is at least twice the values a checkpoint holds, so
// that writing checkpoints takes at most half of what the log takes.
var ckptMin int64 = 1 << 30

// ckptMeta is a checkpoint's header.
type ckptMeta struct {
	index, term     uint64
	cs              *pb.ConfState
	rev, compactRev int64
}

func ckptName(index uint64) string { return fmt.Sprintf("%016x.ckpt", index) }

// ckptState is one state of a key, as a checkpoint holds it.
type ckptState struct {
	deleted                 bool
	create, mod, ver, lease int64
	voff                    int64
	vlen                    uint32
	rec                     []byte // the live record in the slab, if the store holds it
}

// ckptChunk is what one turn encodes: bytes with holes the writer fills
// with values that lie on disk.
type ckptChunk struct {
	buf   []byte
	refs  []ckptRef
	moves []ckptMove
	last  bool
	ptr   *ckptPointer // with the last chunk: the pointer to write once the file is durable
	err   error
}

// ckptRef is a value the writer reads into buf[at:at+n]: from the log at
// off, or from a checkpoint file.
type ckptRef struct {
	at, n int
	off   int64
	f     *os.File
}

// ckptMove names a value the checkpoint now holds: key id's states with
// the value at from have it at to, once the chunk is written.
type ckptMove struct {
	id       uint32
	from, to int64
}

// durableLog is a log whose durable part other goroutines may read.
type durableLog interface {
	durableEnd() int64
	readDurable(p []byte, off int64) error
}

// ckptWriter writes a checkpoint's chunks to its file in order. With the
// last chunk it adds the CRC, makes the file durable, gives it its name
// and points CURRENT at it.
type ckptWriter struct {
	dir, tmp, name string
	f              *os.File
	crc            uint32
	log            durableLog
	in, out        chan *ckptChunk
	quit           chan struct{}
	wake           func()

	// For the stats line: time spent reading values from disk and
	// writing the file, and their bytes.
	readNs, readBytes, writeNs, writeBytes atomic.Int64
}

func (cw *ckptWriter) run() {
	for {
		var c *ckptChunk
		select {
		case c = <-cw.in:
		case <-cw.quit:
			return
		}
		c.err = cw.write(c)
		select {
		case cw.out <- c:
		case <-cw.quit:
			return
		}
		if cw.wake != nil {
			cw.wake()
		}
		if c.err != nil || c.last {
			return
		}
	}
}

func (cw *ckptWriter) write(c *ckptChunk) error {
	t0 := time.Now()
	read := int64(0)
	for _, r := range c.refs {
		p := c.buf[r.at : r.at+r.n]
		if r.f != nil {
			if _, err := r.f.ReadAt(p, r.off); err != nil {
				return fmt.Errorf("read a value from a checkpoint: %w", err)
			}
		} else if err := cw.log.readDurable(p, r.off); err != nil {
			return err
		}
		read += int64(r.n)
	}
	t1 := time.Now()
	cw.readNs.Add(int64(t1.Sub(t0)))
	cw.readBytes.Add(read)
	cw.crc = crc32.Update(cw.crc, castagnoli, c.buf)
	if c.last {
		c.buf = binary.LittleEndian.AppendUint32(c.buf, cw.crc)
	}
	if _, err := cw.f.Write(c.buf); err != nil {
		return fmt.Errorf("write a checkpoint: %w", err)
	}
	cw.writeNs.Add(int64(time.Since(t1)))
	cw.writeBytes.Add(int64(len(c.buf)))
	if !c.last {
		return nil
	}
	if err := cw.f.Sync(); err != nil {
		return fmt.Errorf("sync a checkpoint: %w", err)
	}
	if err := os.Rename(filepath.Join(cw.dir, cw.tmp), filepath.Join(cw.dir, cw.name)); err != nil {
		return err
	}
	return writePointer(cw.dir, c.ptr)
}

// ckptRun is a checkpoint being written, the loop's side.
type ckptRun struct {
	meta    ckptMeta
	gen     int
	logFrom int64 // where the log after I starts
	leases  []byte
	w       *ckptWriter
	at      int64  // the file offset of the next byte encoded
	next    uint32 // the next key id to encode
	end     uint32
	chRev   int64 // the next change: the first of revision chRev, past chSkip of them
	chSkip  int
	header  bool
	sent    int // chunks handed to the writer and not back yet
	lastOut bool
	states  []ckptState

	// For the stats line: when it started, the loop's time encoding, and
	// the value bytes it copied from memory and left to the writer.
	started          time.Time
	encodeNs         int64
	fromMem, fromLog int64
}

// startCkpt starts a checkpoint of the store as it is now, after applied
// index index of term term; logFrom is where that entry's record starts
// in the log. The store keeps what it needs until it is done.
func (n *node) startCkpt(index, term uint64, logFrom int64) error {
	st := n.st
	gen := 1 - n.ckptGen
	if st.ckpts[gen] != nil {
		_ = st.ckpts[gen].Close()
		st.ckpts[gen] = nil
	}
	name := ckptName(index)
	tmp := name + ".tmp"
	f, err := os.OpenFile(filepath.Join(n.ckptDir, tmp), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	st.ckpts[gen] = f
	st.hold()
	r := &ckptRun{
		meta:    ckptMeta{index: index, term: term, cs: proto.Clone(n.confState).(*pb.ConfState), rev: st.rev, compactRev: st.compactRev},
		gen:     gen,
		logFrom: logFrom,
		end:     st.nkeys,
		chRev:   st.compactRev,
		started: time.Now(),
	}
	r.leases = st.encodeLeases(nil)
	dl, _ := st.log.(durableLog)
	r.w = &ckptWriter{
		dir: n.ckptDir, tmp: tmp, name: name, f: f, log: dl,
		in: make(chan *ckptChunk, ckptInflight), out: make(chan *ckptChunk, ckptInflight),
		quit: make(chan struct{}),
	}
	if n.shell != nil {
		r.w.wake = n.shell.wakeLoop
	}
	go r.w.run()
	n.ckpt = r
	n.stats.ckpts++
	n.logf("checkpoint started at index %d, revision %d: %d key ids, values %d MB live and %d MB history, the log %d MB past the last checkpoint",
		index, st.rev, st.nkeys, st.liveBytes>>20, st.histBytes>>20, (n.wal.off-n.ckptFrom)>>20)
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
		n.st.repoint(c.moves)
		if c.last {
			return n.ckptDone()
		}
	}
	for r.sent < ckptInflight && !r.lastOut {
		t := time.Now()
		c, err := n.encodeChunk(r)
		r.encodeNs += int64(time.Since(t))
		if err != nil {
			return err
		}
		r.w.in <- c
		r.sent++
	}
	return nil
}

// ckptDone makes the checkpoint written the newest: Raft's storage hands
// it to followers that lag and may drop the entries before it, and the
// checkpoint before it is removed.
func (n *node) ckptDone() error {
	r := n.ckpt
	n.ckpt = nil
	old := n.ckptGen
	oldName := n.ckptName
	n.ckptGen, n.ckptFrom, n.ckptName = r.gen, r.logFrom, r.w.name
	n.storage.snap = &pb.Snapshot{
		Data:     []byte(r.w.name),
		Metadata: &pb.SnapshotMetadata{Index: new(r.meta.index), Term: new(r.meta.term), ConfState: r.meta.cs},
	}
	n.st.release()
	n.logf("checkpoint at index %d, revision %d: %d bytes, the log from %d, in %v; the loop encoded for %v, the writer read %d MB in %v and wrote in %v",
		r.meta.index, r.meta.rev, r.at+4, r.logFrom, time.Since(r.started).Round(time.Millisecond), time.Duration(r.encodeNs).Round(time.Millisecond),
		r.w.readBytes.Load()>>20, time.Duration(r.w.readNs.Load()).Round(time.Millisecond), time.Duration(r.w.writeNs.Load()).Round(time.Millisecond))
	if f := n.st.ckpts[old]; f != nil {
		_ = f.Close()
		n.st.ckpts[old] = nil
	}
	if oldName != "" && oldName != n.ckptName {
		if err := os.Remove(filepath.Join(n.ckptDir, oldName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// abortCkpt stops a checkpoint being written and removes its file. The
// store's values it re-pointed lie in that file: only a store that is
// replaced aborts one.
func (n *node) abortCkpt() {
	r := n.ckpt
	if r == nil {
		return
	}
	n.ckpt = nil
	close(r.w.quit)
	_ = os.Remove(filepath.Join(n.ckptDir, r.w.tmp))
	n.st.release()
}

// encodeChunk encodes the checkpoint's next part.
func (n *node) encodeChunk(r *ckptRun) (*ckptChunk, error) {
	st := n.st
	c := &ckptChunk{buf: make([]byte, 0, ckptChunkBytes+64<<10)}
	if !r.header {
		r.header = true
		c.buf = append(c.buf, ckptMagic...)
		c.buf = binary.AppendUvarint(c.buf, r.meta.index)
		c.buf = binary.AppendUvarint(c.buf, r.meta.term)
		c.buf = binary.AppendUvarint(c.buf, uint64(r.meta.rev))
		c.buf = binary.AppendUvarint(c.buf, uint64(r.meta.compactRev))
		cs, err := proto.Marshal(r.meta.cs)
		if err != nil {
			return nil, err
		}
		c.buf = binary.AppendUvarint(c.buf, uint64(len(cs)))
		c.buf = append(c.buf, cs...)
		c.buf = append(c.buf, r.leases...)
	}
	for keys := 0; r.next < r.end && keys < ckptChunkKeys && len(c.buf) < ckptChunkBytes; keys++ {
		if err := n.encodeKey(r, c, r.next); err != nil {
			return nil, err
		}
		r.next++
	}
	if r.next == r.end {
		r.encodeChanges(st, c)
	}
	if r.next == r.end && r.chRev > r.meta.rev {
		c.buf = append(c.buf, ckptEnd)
		c.last = true
		r.lastOut = true
		c.ptr = &ckptPointer{gen: r.gen, name: r.w.name, logFrom: r.logFrom}
		if n.wal != nil {
			c.ptr.hs = n.wal.durableHS
		}
	}
	r.at += int64(len(c.buf))
	return c, nil
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
// left out.
func (n *node) encodeKey(r *ckptRun, c *ckptChunk, id uint32) error {
	st := n.st
	states := st.ckptStates(id, r.meta.rev, r.states[:0])
	r.states = states
	if len(states) == 0 {
		return nil
	}
	key := st.key(id)
	c.buf = append(c.buf, ckptKey)
	c.buf = binary.AppendUvarint(c.buf, uint64(id))
	c.buf = binary.AppendUvarint(c.buf, uint64(len(key)))
	c.buf = append(c.buf, key...)
	c.buf = binary.AppendUvarint(c.buf, uint64(len(states)))
	for i := range states {
		s := &states[i]
		flags := uint64(0)
		if s.deleted {
			flags = ckptDeleted
		}
		c.buf = binary.AppendUvarint(c.buf, flags)
		c.buf = binary.AppendUvarint(c.buf, uint64(s.create))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.mod))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.ver))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.lease))
		c.buf = binary.AppendUvarint(c.buf, uint64(s.vlen))
		if s.vlen == 0 {
			continue
		}
		at := len(c.buf)
		c.moves = append(c.moves, ckptMove{id: id, from: s.voff, to: ckptOff(r.gen, r.at+int64(at))})
		if err := n.placeValue(r, c, s); err != nil {
			return err
		}
	}
	return nil
}

// placeValue puts a state's value into the chunk: copied from its live
// record, or from the log's stage, or as a hole the writer fills from
// disk.
func (n *node) placeValue(r *ckptRun, c *ckptChunk, s *ckptState) error {
	st := n.st
	at, l := len(c.buf), int(s.vlen)
	if s.rec != nil {
		c.buf = append(c.buf, recValue(s.rec)...)
		r.fromMem += int64(l)
		return nil
	}
	if inCkpt(s.voff) {
		f := st.ckpts[ckptGenOf(s.voff)]
		if f == nil {
			return fmt.Errorf("a value at %#x names a checkpoint the store does not have", s.voff)
		}
		c.buf = append(c.buf, make([]byte, l)...)
		c.refs = append(c.refs, ckptRef{at: at, n: l, off: s.voff & ckptOffMask, f: f})
		r.fromLog += int64(l)
		return nil
	}
	if dl, ok := st.log.(durableLog); ok && s.voff+int64(l) <= dl.durableEnd() {
		c.buf = append(c.buf, make([]byte, l)...)
		c.refs = append(c.refs, ckptRef{at: at, n: l, off: s.voff})
		r.fromLog += int64(l)
		return nil
	}
	val, err := st.readLog(s.voff, l)
	if err != nil {
		return err
	}
	c.buf = append(c.buf, val...)
	r.fromMem += int64(l)
	return nil
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
}

func (cr *ckptReader) ReadByte() (byte, error) {
	b, err := cr.r.ReadByte()
	if err != nil {
		return 0, err
	}
	cr.off++
	cr.crc = crc32.Update(cr.crc, castagnoli, []byte{b})
	return b, nil
}

func (cr *ckptReader) uvarint() (uint64, error) { return binary.ReadUvarint(cr) }

// bytes reads n bytes, valid until the next call.
func (cr *ckptReader) bytes(n uint64) ([]byte, error) {
	if n > 1<<31 {
		return nil, fmt.Errorf("a checkpoint field of %d bytes", n)
	}
	if uint64(cap(cr.buf)) < n {
		cr.buf = make([]byte, n)
	}
	b := cr.buf[:n]
	if _, err := io.ReadFull(cr.r, b); err != nil {
		return nil, err
	}
	cr.off += int64(n)
	cr.crc = crc32.Update(cr.crc, castagnoli, b)
	return b, nil
}

var errCkptCorrupt = errors.New("the checkpoint does not check")

// loadCkpt fills st, an empty store, from the checkpoint in f, whose
// values the store then reads from f as generation gen.
func loadCkpt(f *os.File, gen int, st *store) (ckptMeta, error) {
	var m ckptMeta
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return m, err
	}
	cr := &ckptReader{r: bufio.NewReaderSize(f, 1<<20)}
	magic, err := cr.bytes(uint64(len(ckptMagic)))
	if err != nil || string(magic) != string(ckptMagic) {
		return m, fmt.Errorf("%s: %w: no magic", f.Name(), errCkptCorrupt)
	}
	var v [4]uint64
	for i := range v {
		if v[i], err = cr.uvarint(); err != nil {
			return m, err
		}
	}
	m.index, m.term, m.rev, m.compactRev = v[0], v[1], int64(v[2]), int64(v[3])
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
		return m, err
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
	st.ckpts[gen] = f
	ids := []uint32{} // the store's id of each id in the file
	var states []ckptState
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
			if binary.LittleEndian.Uint32(sum) != want {
				return m, fmt.Errorf("%s: %w: its CRC", f.Name(), errCkptCorrupt)
			}
			st.rev, st.compactRev = m.rev, m.compactRev
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
			key := []byte(string(kb))
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
				s.voff = ckptOff(gen, cr.off)
				if _, err := cr.bytes(f[5]); err != nil {
					return m, err
				}
				states = append(states, s)
			}
			id, err := st.restoreKey(key, states)
			if err != nil {
				return m, err
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
					return m, fmt.Errorf("%s: %w: a change of a key it does not hold", f.Name(), errCkptCorrupt)
				}
				if ids[fid], err = st.intern(kb); err != nil {
					return m, err
				}
			}
			st.changes = append(st.changes, change{rev: int64(rev), key: ids[fid], del: del != 0})
		default:
			return m, fmt.Errorf("%s: %w: a record of kind %d", f.Name(), errCkptCorrupt, kind)
		}
	}
}

const noKey = ^uint32(0)

// ---- the pointer

// ckptPointer is CURRENT: the newest checkpoint, by its generation and
// name, where the log goes on after it, and the newest durable hard state
// when it was written.
type ckptPointer struct {
	gen     int
	name    string
	logFrom int64
	hs      *pb.HardState
}

func writePointer(dir string, p *ckptPointer) error {
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
	b = binary.AppendUvarint(b, uint64(len(hb)))
	b = append(b, hb...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	tmp := filepath.Join(dir, ptrName+".tmp")
	if err := writeFileSynced(tmp, b); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, ptrName)); err != nil {
		return err
	}
	return syncDir(dir)
}

func writeFileSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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

func syncDir(dir string) error {
	d, err := os.Open(dir)
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
func readPointer(dir string) (*ckptPointer, error) {
	b, err := os.ReadFile(filepath.Join(dir, ptrName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	bad := fmt.Errorf("%s: %w", filepath.Join(dir, ptrName), errCkptCorrupt)
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
	hb := take(next())
	if m == nil || len(m) != 0 {
		return nil, bad
	}
	if err := proto.Unmarshal(hb, p.hs); err != nil {
		return nil, err
	}
	return p, nil
}

// cleanCkptDir removes what a node that stopped left half done in dir:
// checkpoints being written or received, and any but keep.
func cleanCkptDir(dir, keep string) error { return removeCkpts(dir, keep, true) }

// removeCkpts removes dir's checkpoints but keep, and with tmp the files
// being written or received too.
func removeCkpts(dir, keep string, tmp bool) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if name == ptrName || name == keep {
			continue
		}
		if strings.HasSuffix(name, ".ckpt") || (tmp && strings.HasSuffix(name, ".tmp")) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadNewest loads the node's newest checkpoint into its store and Raft's
// storage, if it has one, and returns where the log goes on after it. It
// removes what a node that stopped left half done.
func (n *node) loadNewest() (int64, error) {
	if err := os.MkdirAll(n.ckptDir, 0o700); err != nil {
		return 0, err
	}
	p, err := readPointer(n.ckptDir)
	if err != nil {
		return 0, err
	}
	keep := ""
	if p != nil {
		keep = p.name
	}
	if err := cleanCkptDir(n.ckptDir, keep); err != nil {
		return 0, err
	}
	if p == nil {
		return 0, nil
	}
	f, err := os.Open(filepath.Join(n.ckptDir, p.name))
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
	n.ckptGen, n.ckptFrom, n.ckptName = p.gen, p.logFrom, p.name
	return p.logFrom, nil
}
