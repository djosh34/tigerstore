package mvcc

// A+ prototype (#133): value descriptors, a page directory with dirty
// flags, checkpoint capture, page encoding, and the cleaner's swap.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"time"
	"unsafe"

	"github.com/google/btree"

	"github.com/djosh34/tigerstore/internal/aplus"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// PageBytes is the size at which a leaf page is cut.
const PageBytes = 4096

type page struct {
	lo    []byte
	fid   uint64
	off   int64
	n     int64
	dirty bool
}

type aplusState struct {
	// The entry being applied: its bytes in RAM and the same bytes in
	// the mapped log file.
	heapBase uintptr
	heapLen  int
	dst      []byte
	fid      uint64

	pages *btree.BTreeG[*page]
	epoch uint64
	// live is the value bytes per file that some version references.
	live map[uint64]int64
	// loading is set once LoadPage replaced the empty directory.
	loading bool
}

func (a *aplusState) init() {
	a.pages = btree.NewG(32, func(x, y *page) bool { return bytes.Compare(x.lo, y.lo) < 0 })
	a.pages.ReplaceOrInsert(&page{lo: []byte{}, dirty: true})
	a.live = map[uint64]int64{}
}

// SetEntry names the entry being applied: heap is its data in RAM, dst the
// same bytes in file fid's mapping. Values that are slices of heap are
// stored as slices of dst.
func (s *Store) SetEntry(heap, dst []byte, fid uint64) {
	s.ap.heapBase = uintptr(unsafe.Pointer(unsafe.SliceData(heap)))
	s.ap.heapLen = len(heap)
	s.ap.dst = dst
	s.ap.fid = fid
}

// ClearEntry ends SetEntry.
func (s *Store) ClearEntry() { s.ap.heapLen, s.ap.dst = 0, nil }

func (a *aplusState) durable(v *version) {
	if len(v.value) == 0 {
		return
	}
	p := uintptr(unsafe.Pointer(unsafe.SliceData(v.value)))
	if a.heapLen > 0 && p >= a.heapBase && p < a.heapBase+uintptr(a.heapLen) {
		d := int(p - a.heapBase)
		n := len(v.value)
		v.crc = crc32.Checksum(v.value, castagnoli)
		v.value = a.dst[d : d+n : d+n]
		a.live[a.fid] += int64(n)
		aplus.LogValueBytes.Add(int64(n))
		aplus.Puts.Add(1)
		return
	}
	if fid, _, ok := aplus.Locate(v.value); ok {
		a.live[fid] += int64(len(v.value))
	}
}

func (a *aplusState) unref(v []byte) {
	if fid, _, ok := aplus.Locate(v); ok {
		a.live[fid] -= int64(len(v))
	}
}

// touch marks the page of k dirty, once per key per checkpoint epoch.
func (s *Store) touch(k *keyIndex) {
	if k.epoch == s.ap.epoch+1 {
		return
	}
	k.epoch = s.ap.epoch + 1
	s.pageFor(k.key).dirty = true
}

func (s *Store) pageFor(key []byte) *page {
	var p *page
	s.ap.pages.DescendLessOrEqual(&page{lo: key}, func(x *page) bool { p = x; return false })
	return p
}

// LiveBytes returns the live value bytes per file.
func (s *Store) LiveBytes() map[uint64]int64 { return s.ap.live }

// FrozenKey is a key and its versions as of a capture. Neither changes.
type FrozenKey struct {
	key  []byte
	vers []version
}

// FrozenPage is one leaf page of a capture. Keys is set only if Dirty.
type FrozenPage struct {
	Lo         []byte
	Fid        uint64
	Off, N     int64
	Dirty      bool
	Keys       []FrozenKey
	ForMetaMov bool
}

// Capture is a frozen view of the dirty pages.
type Capture struct {
	Rev, CompactRev int64
	Pages           []FrozenPage
	Keys            int
	Dur             time.Duration
}

// Capture freezes every dirty page, and every page stored in a file of
// move, on the committer. The versions slices are shared, not copied: the
// store never changes a version in place (copy on write).
func (s *Store) Capture(move map[uint64]bool) *Capture {
	start := time.Now()
	c := &Capture{Rev: s.rev, CompactRev: s.compactRev}
	var all []*page
	s.ap.pages.Ascend(func(p *page) bool { all = append(all, p); return true })
	c.Pages = make([]FrozenPage, len(all))
	for i, p := range all {
		fp := FrozenPage{Lo: p.lo, Fid: p.fid, Off: p.off, N: p.n}
		if p.dirty || p.n == 0 || move[p.fid] {
			fp.Dirty = true
			fp.ForMetaMov = !p.dirty && move[p.fid]
			fn := func(k *keyIndex) bool {
				fp.Keys = append(fp.Keys, FrozenKey{k.key, k.vers[:len(k.vers):len(k.vers)]})
				return true
			}
			if i+1 < len(all) {
				s.keys.AscendRange(&keyIndex{key: p.lo}, &keyIndex{key: all[i+1].lo}, fn)
			} else {
				s.keys.AscendGreaterOrEqual(&keyIndex{key: p.lo}, fn)
			}
			c.Keys += len(fp.Keys)
			p.dirty = false
		}
		c.Pages[i] = fp
	}
	s.ap.epoch++
	c.Dur = time.Since(start)
	aplus.MaxPause(&aplus.PauseCaptureNs, &aplus.PauseCaptureMax, c.Dur)
	return c
}

// NewPage is where a written leaf page went.
type NewPage struct {
	Lo     []byte
	Fid    uint64
	Off, N int64
}

// CheckpointDone replaces each captured dirty page by the pages it was
// written as. A page dirtied again since the capture stays dirty.
func (s *Store) CheckpointDone(c *Capture, written map[int][]NewPage) {
	for i, nps := range written {
		old, ok := s.ap.pages.Get(&page{lo: c.Pages[i].Lo})
		if !ok {
			panic("aplus: captured page vanished")
		}
		s.ap.pages.Delete(old)
		for j, np := range nps {
			lo := np.Lo
			if j == 0 {
				lo = old.lo
			}
			s.ap.pages.ReplaceOrInsert(&page{lo: lo, fid: np.Fid, off: np.Off, n: np.N, dirty: old.dirty})
		}
	}
}

// EncodePage writes the keys of fp as leaf pages of about PageBytes and
// calls emit with each page's first key and bytes.
func EncodePage(fp FrozenPage, emit func(lo, b []byte)) {
	buf := make([]byte, 8, PageBytes+512)
	lo := fp.Lo
	nkeys := 0
	flush := func() {
		binary.LittleEndian.PutUint32(buf[4:], uint32(nkeys))
		binary.LittleEndian.PutUint32(buf, crc32.Checksum(buf[4:], castagnoli))
		emit(lo, buf)
		buf = make([]byte, 8, PageBytes+512)
		nkeys = 0
	}
	for i, k := range fp.Keys {
		if len(buf) >= PageBytes {
			flush()
			lo = fp.Keys[i].key
		}
		buf = appendKey(buf, k)
		nkeys++
	}
	flush()
}

func appendKey(b []byte, k FrozenKey) []byte {
	b = binary.AppendUvarint(b, uint64(len(k.key)))
	b = append(b, k.key...)
	b = binary.AppendUvarint(b, uint64(len(k.vers)))
	vs := len(b)
	for i, v := range k.vers {
		if i == len(k.vers)-1 {
			// The older versions: history written again.
			aplus.CkptLeafHist.Add(int64(len(b) - vs))
		}
		b = binary.AppendVarint(b, v.mod.main)
		b = binary.AppendUvarint(b, uint64(v.mod.sub))
		b = binary.AppendVarint(b, v.create)
		b = binary.AppendVarint(b, v.ver)
		b = binary.AppendVarint(b, v.lease)
		if v.ver == 0 {
			continue
		}
		fid, off, ok := aplus.Locate(v.value)
		if !ok {
			fid, off = 0, 0
		}
		b = binary.AppendUvarint(b, fid)
		b = binary.AppendUvarint(b, uint64(off))
		b = binary.AppendUvarint(b, uint64(len(v.value)))
		b = binary.LittleEndian.AppendUint32(b, v.crc)
	}
	return b
}

var errBadPage = errors.New("aplus: bad page")

// DecodePage calls fn for each key of a leaf page. Value descriptors come
// as file, offset and length; mapFid may rename files (snapshots).
func DecodePage(b []byte, fn func(key []byte, vers []version) error, value func(fid uint64, off, n int64) []byte) error {
	if len(b) < 8 || crc32.Checksum(b[4:], castagnoli) != binary.LittleEndian.Uint32(b) {
		return errBadPage
	}
	nkeys := int(binary.LittleEndian.Uint32(b[4:]))
	b = b[8:]
	uv := func() uint64 {
		x, n := binary.Uvarint(b)
		if n <= 0 {
			panic(errBadPage)
		}
		b = b[n:]
		return x
	}
	sv := func() int64 {
		x, n := binary.Varint(b)
		if n <= 0 {
			panic(errBadPage)
		}
		b = b[n:]
		return x
	}
	for range nkeys {
		kl := uv()
		key := bytes.Clone(b[:kl])
		b = b[kl:]
		nv := uv()
		vers := make([]version, nv)
		for j := range vers {
			v := &vers[j]
			v.mod.main = sv()
			v.mod.sub = int64(uv())
			v.create = sv()
			v.ver = sv()
			v.lease = sv()
			if v.ver == 0 {
				continue
			}
			fid, off, n := uv(), uv(), uv()
			v.crc = binary.LittleEndian.Uint32(b)
			b = b[4:]
			if n > 0 {
				v.value = value(fid, int64(off), int64(n))
			}
		}
		if err := fn(key, vers); err != nil {
			return err
		}
	}
	return nil
}

// LoadPage adds the keys of a leaf page to a store being rebuilt, and the
// page to its directory at (fid, off, n). Pages must come in key order.
func (s *Store) LoadPage(lo []byte, fid uint64, off, n int64, b []byte, value func(fid uint64, off, n int64) []byte) error {
	if !s.ap.loading {
		s.ap.loading = true
		s.ap.pages.Clear(false)
	}
	s.ap.pages.ReplaceOrInsert(&page{lo: bytes.Clone(lo), fid: fid, off: off, n: n, dirty: fid == 0})
	return DecodePage(b, func(key []byte, vers []version) error {
		for _, v := range vers {
			if fid, _, ok := aplus.Locate(v.value); ok {
				s.ap.live[fid] += int64(len(v.value))
			}
		}
		s.keys.ReplaceOrInsert(&keyIndex{key: key, vers: vers})
		return nil
	}, value)
}

// SetRevs sets the revisions of a store rebuilt from a checkpoint.
func (s *Store) SetRevs(rev, compactRev int64) { s.rev, s.compactRev = rev, compactRev }

// ReencodePage rewrites a leaf page with each value moved by move, which
// returns the new file and offset. Snapshots use it to pack values.
func ReencodePage(b []byte, move func(fid uint64, off, n int64) (uint64, int64)) ([]byte, error) {
	var keys []FrozenKey
	type ref struct {
		fid uint64
		off int64
	}
	// Decode with fake values that carry the new location in their
	// pointer-free form: collect locations in order instead.
	var locs []ref
	err := DecodePage(b, func(key []byte, vers []version) error {
		keys = append(keys, FrozenKey{key, vers})
		return nil
	}, func(fid uint64, off, n int64) []byte {
		nf, no := move(fid, off, n)
		locs = append(locs, ref{nf, no})
		return make([]byte, n) // placeholder; only its length is used
	})
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8, len(b))
	li := 0
	for _, k := range keys {
		out = binary.AppendUvarint(out, uint64(len(k.key)))
		out = append(out, k.key...)
		out = binary.AppendUvarint(out, uint64(len(k.vers)))
		for _, v := range k.vers {
			out = binary.AppendVarint(out, v.mod.main)
			out = binary.AppendUvarint(out, uint64(v.mod.sub))
			out = binary.AppendVarint(out, v.create)
			out = binary.AppendVarint(out, v.ver)
			out = binary.AppendVarint(out, v.lease)
			if v.ver == 0 {
				continue
			}
			var fid uint64
			var off int64
			if len(v.value) > 0 {
				fid, off = locs[li].fid, locs[li].off
				li++
			}
			out = binary.AppendUvarint(out, fid)
			out = binary.AppendUvarint(out, uint64(off))
			out = binary.AppendUvarint(out, uint64(len(v.value)))
			out = binary.LittleEndian.AppendUint32(out, v.crc)
		}
	}
	binary.LittleEndian.PutUint32(out[4:], uint32(len(keys)))
	binary.LittleEndian.PutUint32(out, crc32.Checksum(out[4:], castagnoli))
	return out, nil
}

// Reloc is one value the cleaner moves.
type Reloc struct {
	k   *keyIndex
	mod rev
	Old []byte
}

// FindValues returns every version whose value is in file fid. It scans
// the whole index on the committer (crude).
func (s *Store) FindValues(fid uint64) []Reloc {
	var out []Reloc
	s.keys.Ascend(func(k *keyIndex) bool {
		for _, v := range k.vers {
			if f, _, ok := aplus.Locate(v.value); ok && f == fid {
				out = append(out, Reloc{k: k, mod: v.mod, Old: v.value})
			}
		}
		return true
	})
	return out
}

// Swap points each relocated version at its new bytes, copy on write.
// A version that is gone or changed since FindValues is skipped.
func (s *Store) Swap(rs []Reloc, news [][]byte) (swapped int) {
	for i, r := range rs {
		j, ok := slices.BinarySearchFunc(r.k.vers, r.mod, func(v version, m rev) int {
			if v.mod.main != m.main {
				return int(v.mod.main - m.main)
			}
			return int(v.mod.sub - m.sub)
		})
		if !ok || unsafe.SliceData(r.k.vers[j].value) != unsafe.SliceData(r.Old) {
			continue
		}
		if cur, ok := s.keys.Get(r.k); !ok || cur != r.k {
			continue
		}
		nv := slices.Clone(r.k.vers)
		s.ap.unref(nv[j].value)
		nv[j].value = news[i]
		r.k.vers = nv
		if fid, _, ok := aplus.Locate(news[i]); ok {
			s.ap.live[fid] += int64(len(news[i]))
		}
		s.touch(r.k)
		swapped++
	}
	return swapped
}
