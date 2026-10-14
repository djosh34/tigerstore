package node

// A+ prototype (#133): the helpers that do the checkpoint, cleaning and
// snapshot I/O, and the restart from a checkpoint.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/aplus"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type ref struct {
	lo  []byte
	fid uint64
	off int64
	n   int64
}

// ckptr writes checkpoints and builds snapshots. One goroutine owns it.
type ckptr struct {
	meta     *os.File
	metaFid  uint64
	metaSize int64
	live     map[uint64]int64
	size     map[uint64]int64
	leaves   []ref
	groups   [][]ref
	groupRef []ref
	root     ref
	gen      uint64
	super    *os.File
	// the newest written checkpoint
	index, term  uint64
	rev, compRev int64
	peers        []uint64
}

var nextFile atomic.Uint64 // next value or metadata file number

func syncDir() error {
	f, err := os.Open(aplus.Dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (c *ckptr) newMeta() error {
	if c.meta != nil {
		c.meta.Close()
	}
	c.metaFid = aplus.MetaFileBase + nextFile.Add(1)
	f, err := os.OpenFile(filepath.Join(aplus.Dir, aplus.FileName(c.metaFid)), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	c.meta, c.metaSize = f, 0
	c.size[c.metaFid] = 0
	return syncDir()
}

func encodeRefs(rs []ref) []byte {
	b := make([]byte, 8)
	for _, r := range rs {
		b = binary.AppendUvarint(b, uint64(len(r.lo)))
		b = append(b, r.lo...)
		b = binary.AppendUvarint(b, r.fid)
		b = binary.AppendUvarint(b, uint64(r.off))
		b = binary.AppendUvarint(b, uint64(r.n))
	}
	binary.LittleEndian.PutUint32(b[4:], uint32(len(rs)))
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
	return b
}

func decodeRefs(b []byte) ([]ref, error) {
	if len(b) < 8 || crc32.Checksum(b[4:], castagnoli) != binary.LittleEndian.Uint32(b) {
		return nil, errors.New("aplus: bad ref page")
	}
	n := int(binary.LittleEndian.Uint32(b[4:]))
	b = b[8:]
	uv := func() uint64 {
		x, k := binary.Uvarint(b)
		b = b[k:]
		return x
	}
	rs := make([]ref, n)
	for i := range rs {
		l := uv()
		rs[i].lo = bytes.Clone(b[:l])
		b = b[l:]
		rs[i].fid, rs[i].off, rs[i].n = uv(), int64(uv()), int64(uv())
	}
	return rs, nil
}

func pageBytes(r ref) []byte {
	m := aplus.Get(r.fid)
	return m[r.off : r.off+r.n : r.off+r.n]
}

func sameRefs(a, b []ref) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].fid != b[i].fid || a[i].off != b[i].off || !bytes.Equal(a[i].lo, b[i].lo) {
			return false
		}
	}
	return true
}

const groupFanout = 64

// write writes one checkpoint: the dirty leaf pages, the group pages
// whose leaves changed, the root, then the superblock.
func (c *ckptr) write(j *core.CkptJob) (core.CkptResult, error) {
	var buf []byte
	base := c.metaSize
	add := func(b []byte) ref {
		r := ref{fid: c.metaFid, off: base + int64(len(buf)), n: int64(len(b))}
		buf = append(buf, b...)
		c.live[c.metaFid] += r.n
		return r
	}
	drop := func(r ref) {
		if r.n > 0 {
			c.live[r.fid] -= r.n
		}
	}
	res := core.CkptResult{Job: j, Written: map[int][]mvcc.NewPage{}}
	var leaves []ref
	for i, fp := range j.Capture.Pages {
		if !fp.Dirty {
			leaves = append(leaves, ref{fp.Lo, fp.Fid, fp.Off, fp.N})
			continue
		}
		drop(ref{fid: fp.Fid, n: fp.N})
		mvcc.EncodePage(fp, func(lo, b []byte) {
			r := add(b)
			r.lo = bytes.Clone(lo)
			leaves = append(leaves, r)
			res.Written[i] = append(res.Written[i], mvcc.NewPage{Lo: r.lo, Fid: r.fid, Off: r.off, N: r.n})
			if fp.ForMetaMov {
				aplus.CkptMetaMove.Add(r.n)
			} else {
				aplus.CkptLeaf.Add(r.n)
			}
		})
	}
	var groups [][]ref
	var groupRef []ref
	for i := 0; i < len(leaves); i += groupFanout {
		g := leaves[i:min(i+groupFanout, len(leaves))]
		gi := len(groups)
		groups = append(groups, g)
		if gi < len(c.groups) && sameRefs(c.groups[gi], g) {
			groupRef = append(groupRef, c.groupRef[gi])
			continue
		}
		if gi < len(c.groupRef) {
			drop(c.groupRef[gi])
		}
		b := encodeRefs(g)
		r := add(b)
		r.lo = g[0].lo
		aplus.CkptInterior.Add(r.n)
		groupRef = append(groupRef, r)
	}
	for gi := len(groups); gi < len(c.groupRef); gi++ {
		drop(c.groupRef[gi])
	}
	drop(c.root)
	rb := encodeRefs(groupRef)
	root := add(rb)
	aplus.CkptRoot.Add(root.n + 4096)
	if _, err := c.meta.Write(buf); err != nil {
		return res, err
	}
	if err := c.meta.Sync(); err != nil {
		return res, err
	}
	c.metaSize += int64(len(buf))
	c.size[c.metaFid] = c.metaSize
	c.leaves, c.groups, c.groupRef, c.root = leaves, groups, groupRef, root
	c.index, c.term, c.rev, c.compRev = j.Index, j.Term, j.Capture.Rev, j.Capture.CompactRev
	if err := c.writeSuper(j); err != nil {
		return res, err
	}
	for f, l := range c.live {
		if f == c.metaFid {
			continue
		}
		if l <= 0 {
			aplus.Remove(f)
			delete(c.live, f)
			delete(c.size, f)
		} else if float64(l) < 0.5*float64(c.size[f]) {
			res.Move = append(res.Move, f)
		}
	}
	if c.metaSize >= 64<<20 {
		if err := c.newMeta(); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (c *ckptr) writeSuper(j *core.CkptJob) error {
	c.gen++
	b := make([]byte, 4096)
	p := b[4:4]
	for _, x := range []uint64{c.gen, j.Index, j.Term, uint64(j.Capture.Rev), uint64(j.Capture.CompactRev), j.HSTerm, j.Vote, j.ReplaySeq, c.root.fid, uint64(c.root.off), uint64(c.root.n), nextFile.Load()} {
		p = binary.LittleEndian.AppendUint64(p, x)
	}
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
	if _, err := c.super.WriteAt(b, int64(c.gen%2)*4096); err != nil {
		return err
	}
	return c.super.Sync()
}

type super struct {
	gen, index, term uint64
	rev, compRev     int64
	hsTerm, hsVote   uint64
	replaySeq        uint64
	root             ref
	next             uint64
}

func readSuper() (*super, error) {
	b, err := os.ReadFile(filepath.Join(aplus.Dir, "super"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || len(b) < 8192 {
		return nil, err
	}
	var best *super
	for slot := range 2 {
		s := b[slot*4096 : (slot+1)*4096]
		if crc32.Checksum(s[4:], castagnoli) != binary.LittleEndian.Uint32(s) {
			continue
		}
		var x [12]uint64
		for i := range x {
			x[i] = binary.LittleEndian.Uint64(s[4+8*i:])
		}
		sp := &super{gen: x[0], index: x[1], term: x[2], rev: int64(x[3]), compRev: int64(x[4]), hsTerm: x[5], hsVote: x[6], replaySeq: x[7], root: ref{fid: x[8], off: int64(x[9]), n: int64(x[10])}, next: x[11]}
		if best == nil || sp.gen > best.gen {
			best = sp
		}
	}
	return best, nil
}

// load rebuilds the store from the newest checkpoint, or returns nil.
func (c *ckptr) load() (*super, *mvcc.Store, error) {
	sp, err := readSuper()
	if err != nil || sp == nil {
		return nil, nil, err
	}
	nextFile.Store(sp.next)
	c.gen = sp.gen
	c.root = sp.root
	c.live[sp.root.fid] += sp.root.n
	groupRef, err := decodeRefs(pageBytes(sp.root))
	if err != nil {
		return nil, nil, err
	}
	kv := mvcc.New()
	value := func(fid uint64, off, n int64) []byte {
		m := aplus.Get(fid)
		return m[off : off+n : off+n]
	}
	for _, g := range groupRef {
		c.live[g.fid] += g.n
		leaves, err := decodeRefs(pageBytes(g))
		if err != nil {
			return nil, nil, err
		}
		c.groups = append(c.groups, leaves)
		for _, l := range leaves {
			c.live[l.fid] += l.n
			if err := kv.LoadPage(l.lo, l.fid, l.off, l.n, pageBytes(l), value); err != nil {
				return nil, nil, fmt.Errorf("aplus: leaf page at %d:%d: %w", l.fid, l.off, err)
			}
			c.leaves = append(c.leaves, l)
		}
	}
	c.groupRef = groupRef
	kv.SetRevs(sp.rev, sp.compRev)
	for f := range c.live {
		if st, err := os.Stat(filepath.Join(aplus.Dir, aplus.FileName(f))); err == nil {
			c.size[f] = st.Size()
		}
	}
	c.index, c.term, c.rev, c.compRev = sp.index, sp.term, sp.rev, sp.compRev
	return sp, kv, nil
}

// buildSnap packs the newest written checkpoint and every value it
// references into one blob: header, leaf pages, values.
func (c *ckptr) buildSnap() *raftpb.Snapshot {
	var vals []byte
	moved := map[[2]uint64]int64{}
	b := []byte("APSNAP1\x00")
	for _, x := range []uint64{c.index, c.term, uint64(c.rev), uint64(c.compRev), uint64(len(c.leaves))} {
		b = binary.LittleEndian.AppendUint64(b, x)
	}
	var pages []byte
	for _, l := range c.leaves {
		pb, err := mvcc.ReencodePage(pageBytes(l), func(fid uint64, off, n int64) (uint64, int64) {
			k := [2]uint64{fid, uint64(off)}
			if o, ok := moved[k]; ok {
				return 1, o
			}
			o := int64(len(vals))
			m := aplus.Get(fid)
			vals = append(vals, m[off:off+n]...)
			moved[k] = o
			return 1, o
		})
		if err != nil {
			panic(err)
		}
		pages = binary.AppendUvarint(pages, uint64(len(l.lo)))
		pages = append(pages, l.lo...)
		pages = binary.AppendUvarint(pages, uint64(len(pb)))
		pages = append(pages, pb...)
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(len(pages)))
	b = append(b, pages...)
	b = append(b, vals...)
	aplus.SnapBuilt.Add(int64(len(b)))
	return &raftpb.Snapshot{Data: b, Metadata: &raftpb.SnapshotMetadata{Index: new(c.index), Term: new(c.term), ConfState: &raftpb.ConfState{Voters: c.peers}}}
}

// installSnap writes a snapshot's values to a new value file and builds
// the store from its pages. It runs on the persist goroutine.
func installSnap(s *raftpb.Snapshot) (*mvcc.Store, error) {
	b := s.GetData()
	if len(b) < 56 || string(b[:8]) != "APSNAP1\x00" {
		return nil, errors.New("aplus: bad snapshot")
	}
	rev := int64(binary.LittleEndian.Uint64(b[24:]))
	compRev := int64(binary.LittleEndian.Uint64(b[32:]))
	plen := binary.LittleEndian.Uint64(b[48:])
	pages := b[56 : 56+plen]
	vals := b[56+plen:]
	fid := aplus.ValueFileBase + nextFile.Add(1)
	f, err := os.OpenFile(filepath.Join(aplus.Dir, aplus.FileName(fid)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(vals); err != nil {
		return nil, err
	}
	if err := errors.Join(f.Sync(), f.Close(), syncDir()); err != nil {
		return nil, err
	}
	m := aplus.Map(fid)
	kv := mvcc.New()
	for len(pages) > 0 {
		l, k := binary.Uvarint(pages)
		pages = pages[k:]
		lo := pages[:l]
		pages = pages[l:]
		n, k := binary.Uvarint(pages)
		pages = pages[k:]
		pg := pages[:n]
		pages = pages[n:]
		if err := kv.LoadPage(lo, 0, 0, 0, pg, func(_ uint64, off, n int64) []byte { return m[off : off+n : off+n] }); err != nil {
			return nil, err
		}
	}
	kv.SetRevs(rev, compRev)
	return kv, nil
}

// clean copies the live values of one file into a new value file.
func clean(j *core.CleanJob) ([][]byte, uint64, error) {
	var buf []byte
	offs := make([]int, len(j.Relocs))
	for i, r := range j.Relocs {
		offs[i] = len(buf)
		buf = append(buf, r.Old...)
	}
	fid := aplus.ValueFileBase + nextFile.Add(1)
	f, err := os.OpenFile(filepath.Join(aplus.Dir, aplus.FileName(fid)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, 0, err
	}
	if _, err := f.Write(buf); err != nil {
		return nil, 0, err
	}
	if err := errors.Join(f.Sync(), f.Close(), syncDir()); err != nil {
		return nil, 0, err
	}
	aplus.CleanBytes.Add(int64(len(buf)))
	m := aplus.Map(fid)
	news := make([][]byte, len(j.Relocs))
	for i, r := range j.Relocs {
		news[i] = m[offs[i] : offs[i]+len(r.Old) : offs[i]+len(r.Old)]
	}
	return news, fid, nil
}

func fileSizes() map[uint64]int64 {
	ents, err := os.ReadDir(aplus.Dir)
	if err != nil {
		return nil
	}
	out := map[uint64]int64{}
	for _, e := range ents {
		name := e.Name()
		var n uint64
		switch {
		case strings.HasSuffix(name, ".wal"):
			fmt.Sscanf(name, "%016x.wal", &n)
		case strings.HasSuffix(name, ".val"):
			fmt.Sscanf(name, "%016x.val", &n)
			n += aplus.ValueFileBase
		default:
			continue
		}
		if info, err := e.Info(); err == nil {
			out[n] = info.Size()
		}
	}
	return out
}

type cleanDone struct {
	job  *core.CleanJob
	news [][]byte
	fid  uint64
}

// runAplus starts the helpers. Results come back on the inbox.
func (n *Node) runAplus(done <-chan struct{}) {
	go func() {
		for {
			select {
			case x := <-n.ckptQ:
				switch x := x.(type) {
				case *core.CkptJob:
					res, err := n.ck.write(x)
					if err != nil {
						panic(err)
					}
					n.inbox <- input{apCkpt: &res}
				case *core.Ckpt:
					s := n.ck.buildSnap()
					n.inbox <- input{apSnap: s}
				}
			case <-done:
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case x := <-n.cleanQ:
				switch x := x.(type) {
				case *core.CleanJob:
					news, fid, err := clean(x)
					if err != nil {
						panic(err)
					}
					n.inbox <- input{apClean: &cleanDone{x, news, fid}}
				case []uint64:
					for _, f := range x {
						aplus.Remove(f)
					}
				}
			case <-done:
				return
			}
		}
	}()
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				n.logf("%s", aplus.Stats())
				if s := fileSizes(); s != nil {
					select {
					case n.inbox <- input{apSizes: s}:
					case <-done:
						return
					}
				}
			case <-done:
				n.logf("%s", aplus.Stats())
				return
			}
		}
	}()
}

func (n *Node) dispatchAplus() {
	e := n.core.TakeAplus()
	if e.Ckpt != nil {
		go func() { n.ckptQ <- e.Ckpt }()
	}
	if e.BuildSnap != nil {
		go func() { n.ckptQ <- e.BuildSnap }()
	}
	if e.Clean != nil {
		go func() { n.cleanQ <- e.Clean }()
	}
	if len(e.Delete) > 0 {
		go func() { n.cleanQ <- e.Delete }()
	}
}

// openAplus opens the log from the newest checkpoint if there is one.
func openAplus(cfg Config, dir wal.FS) (*wal.WAL, *core.Node, *ckptr, error) {
	ck := &ckptr{live: map[uint64]int64{}, size: map[uint64]int64{}, peers: cfg.Core.Peers}
	start := time.Now()
	sp, kv, err := ck.load()
	if err != nil {
		return nil, nil, nil, err
	}
	// File numbers continue after every file on disk.
	if ents, err := os.ReadDir(aplus.Dir); err == nil {
		for _, e := range ents {
			var x uint64
			if strings.HasSuffix(e.Name(), ".val") || strings.HasSuffix(e.Name(), ".meta") {
				fmt.Sscanf(e.Name()[:16], "%016x", &x)
				if x >= nextFile.Load() {
					nextFile.Store(x + 1)
				}
			}
		}
	}
	var seq uint64
	if sp != nil {
		seq = sp.replaySeq
	}
	w, rec, err := wal.OpenFrom(dir, seq)
	if err != nil {
		return nil, nil, nil, err
	}
	var c *core.Node
	if sp != nil {
		ck2 := &core.Ckpt{Index: sp.index, Term: sp.term, ReplaySeq: sp.replaySeq, Refs: map[uint64]bool{}}
		for f, l := range kv.LiveBytes() {
			if l > 0 {
				ck2.Refs[f] = true
			}
		}
		c, err = core.NewFrom(cfg.Core, rec, ck2, kv, wal.HardState{Term: sp.hsTerm, Vote: sp.hsVote, Commit: sp.index})
		if err == nil {
			c.SetDurable(ck2)
		}
	} else {
		c, err = core.New(cfg.Core, rec)
	}
	if err != nil {
		return nil, nil, nil, errors.Join(err, w.Close())
	}
	f, err := os.OpenFile(filepath.Join(aplus.Dir, "super"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, nil, err
	}
	if st, _ := f.Stat(); st.Size() < 8192 {
		if err := errors.Join(f.Truncate(8192), f.Sync(), syncDir()); err != nil {
			return nil, nil, nil, err
		}
	}
	ck.super = f
	if err := ck.newMeta(); err != nil {
		return nil, nil, nil, err
	}
	var ents int
	if rec != nil {
		ents = len(rec.Entries)
	}
	if cfg.Logf != nil {
		cfg.Logf("aplus-open checkpoint=%v index=%d replayed_entries=%d took=%v", sp != nil, ck.index, ents, time.Since(start))
	}
	return w, c, ck, nil
}
