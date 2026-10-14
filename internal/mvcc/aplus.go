package mvcc

// A+ prototype (#133): value descriptors, dirty-key tracking, checkpoint
// capture, page encoding, and the cleaner's swap. The page tree itself is
// owned by the checkpoint writer (internal/node); the committer only lists
// the keys that changed since the last capture.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"time"
	"unsafe"

	"github.com/djosh34/tigerstore/internal/aplus"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// PageBytes is the size at which a leaf page is cut.
const PageBytes = 4096

type aplusState struct {
	// The entry being applied: its bytes in RAM and the same bytes in
	// the mapped log file.
	heapBase uintptr
	heapLen  int
	dst      []byte
	fid      uint64

	epoch uint64
	dirty []*keyIndex
	// live is the value bytes per file that some version references.
	live map[uint64]int64
}

func (a *aplusState) init() { a.live = map[uint64]int64{} }

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

// touch lists k as changed, once per checkpoint epoch.
func (s *Store) touch(k *keyIndex) {
	if k.epoch == s.ap.epoch+1 {
		return
	}
	k.epoch = s.ap.epoch + 1
	s.ap.dirty = append(s.ap.dirty, k)
}

// LiveBytes returns the live value bytes per file.
func (s *Store) LiveBytes() map[uint64]int64 { return s.ap.live }

// FrozenKey is a key and its versions as of a capture. Neither changes:
// the store never changes a version in place (copy on write). No versions
// means the key is gone.
type FrozenKey struct {
	Key  []byte
	vers []version
}

// Capture is the changed keys since the last capture.
type Capture struct {
	Rev, CompactRev int64
	Keys            []FrozenKey
	Dur             time.Duration
}

// Capture freezes the changed keys on the committer: one slice header
// copy per key.
func (s *Store) Capture() *Capture {
	start := time.Now()
	c := &Capture{Rev: s.rev, CompactRev: s.compactRev, Keys: make([]FrozenKey, len(s.ap.dirty))}
	for i, k := range s.ap.dirty {
		c.Keys[i] = FrozenKey{k.key, k.vers[:len(k.vers):len(k.vers)]}
	}
	clear(s.ap.dirty)
	s.ap.dirty = s.ap.dirty[:0]
	s.ap.epoch++
	c.Dur = time.Since(start)
	aplus.MaxPause(&aplus.PauseCaptureNs, &aplus.PauseCaptureMax, c.Dur)
	return c
}

// Gone reports whether the key no longer exists in the index.
func (k FrozenKey) Gone() bool { return len(k.vers) == 0 }

// AppendKey encodes one key with all its versions.
func AppendKey(b []byte, k FrozenKey) []byte {
	b = binary.AppendUvarint(b, uint64(len(k.Key)))
	b = append(b, k.Key...)
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

// EncodePage makes a leaf page from encoded keys.
func EncodePage(entries [][]byte) []byte {
	n := 8
	for _, e := range entries {
		n += len(e)
	}
	b := make([]byte, 8, n)
	for _, e := range entries {
		b = append(b, e...)
	}
	binary.LittleEndian.PutUint32(b[4:], uint32(len(entries)))
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
	return b
}

var errBadPage = errors.New("aplus: bad page")

// Desc is a value descriptor in a page.
type Desc struct {
	Fid       uint64
	Off, N    int64
	Main, Sub int64
}

// DecodePage calls fn for each key of a leaf page with its encoded bytes
// and its value descriptors. If value is not nil, it builds versions with
// values from it.
func DecodePage(b []byte, fn func(key, raw []byte, vers []version, descs []Desc) error, value func(fid uint64, off, n int64) []byte) (err error) {
	if len(b) < 8 || crc32.Checksum(b[4:], castagnoli) != binary.LittleEndian.Uint32(b) {
		return errBadPage
	}
	defer func() {
		if r := recover(); r != nil {
			err = errBadPage
		}
	}()
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
	var descs []Desc
	for range nkeys {
		start := b
		kl := uv()
		key := b[:kl:kl]
		b = b[kl:]
		nv := uv()
		var vers []version
		if value != nil {
			vers = make([]version, nv)
		}
		descs = descs[:0]
		for j := range int(nv) {
			var v version
			v.mod.main = sv()
			v.mod.sub = int64(uv())
			v.create = sv()
			v.ver = sv()
			v.lease = sv()
			if v.ver != 0 {
				fid, off, n := uv(), uv(), uv()
				v.crc = binary.LittleEndian.Uint32(b)
				b = b[4:]
				descs = append(descs, Desc{fid, int64(off), int64(n), v.mod.main, v.mod.sub})
				if n > 0 && value != nil {
					v.value = value(fid, int64(off), int64(n))
				}
			}
			if value != nil {
				vers[j] = v
			}
		}
		if err := fn(key, start[:len(start)-len(b)], vers, descs); err != nil {
			return err
		}
	}
	return nil
}

// ScanPage calls fn for each key of a leaf page with its encoded bytes and
// value descriptors.
func ScanPage(b []byte, fn func(key, raw []byte, descs []Desc) error) error {
	return DecodePage(b, func(key, raw []byte, _ []version, descs []Desc) error { return fn(key, raw, descs) }, nil)
}

// LoadPage adds the keys of a leaf page to a store being rebuilt. With
// dirty set, the next checkpoint writes them.
func (s *Store) LoadPage(b []byte, value func(fid uint64, off, n int64) []byte, dirty bool) error {
	return DecodePage(b, func(key, _ []byte, vers []version, _ []Desc) error {
		for _, v := range vers {
			if fid, _, ok := aplus.Locate(v.value); ok {
				s.ap.live[fid] += int64(len(v.value))
			}
		}
		k := &keyIndex{key: bytes.Clone(key), vers: vers}
		s.keys.ReplaceOrInsert(k)
		if dirty {
			s.touch(k)
		}
		return nil
	}, value)
}

// SetRevs sets the revisions of a store rebuilt from a checkpoint.
func (s *Store) SetRevs(rev, compactRev int64) { s.rev, s.compactRev = rev, compactRev }

// ReencodePage rewrites a leaf page with each value moved by move, which
// returns the new file and offset. Snapshots use it to pack values.
func ReencodePage(b []byte, move func(fid uint64, off, n int64) (uint64, int64)) ([]byte, error) {
	var entries [][]byte
	err := DecodePage(b, func(key, _ []byte, vers []version, _ []Desc) error {
		e := binary.AppendUvarint(nil, uint64(len(key)))
		e = append(e, key...)
		e = binary.AppendUvarint(e, uint64(len(vers)))
		for _, v := range vers {
			e = binary.AppendVarint(e, v.mod.main)
			e = binary.AppendUvarint(e, uint64(v.mod.sub))
			e = binary.AppendVarint(e, v.create)
			e = binary.AppendVarint(e, v.ver)
			e = binary.AppendVarint(e, v.lease)
			if v.ver == 0 {
				continue
			}
			// value holds the new location: file, offset, length.
			var fid, off, n uint64
			if len(v.value) == 24 {
				fid = binary.LittleEndian.Uint64(v.value)
				off = binary.LittleEndian.Uint64(v.value[8:])
				n = binary.LittleEndian.Uint64(v.value[16:])
			}
			e = binary.AppendUvarint(e, fid)
			e = binary.AppendUvarint(e, off)
			e = binary.AppendUvarint(e, n)
			e = binary.LittleEndian.AppendUint32(e, v.crc)
		}
		entries = append(entries, e)
		return nil
	}, func(fid uint64, off, n int64) []byte {
		nf, no := move(fid, off, n)
		loc := make([]byte, 24)
		binary.LittleEndian.PutUint64(loc, nf)
		binary.LittleEndian.PutUint64(loc[8:], uint64(no))
		binary.LittleEndian.PutUint64(loc[16:], uint64(n))
		return loc
	})
	if err != nil {
		return nil, err
	}
	return EncodePage(entries), nil
}

// Reloc is one value the cleaner moves: the version (key, revision) and
// the bytes it should still reference.
type Reloc struct {
	Key       []byte
	Main, Sub int64
	Old       []byte
}

// Swap points each relocated version at its new bytes, copy on write.
// A version that is gone or changed since the checkpoint is skipped.
func (s *Store) Swap(rs []Reloc, news [][]byte) (swapped int) {
	for i, r := range rs {
		k, ok := s.keys.Get(&keyIndex{key: r.Key})
		if !ok {
			continue
		}
		j, ok := slices.BinarySearchFunc(k.vers, rev{r.Main, r.Sub}, func(v version, m rev) int {
			if v.mod.main != m.main {
				return int(v.mod.main - m.main)
			}
			return int(v.mod.sub - m.sub)
		})
		if !ok || unsafe.SliceData(k.vers[j].value) != unsafe.SliceData(r.Old) || len(k.vers[j].value) != len(r.Old) {
			continue
		}
		nv := slices.Clone(k.vers)
		s.ap.unref(nv[j].value)
		nv[j].value = news[i]
		k.vers = nv
		if fid, _, ok := aplus.Locate(news[i]); ok {
			s.ap.live[fid] += int64(len(news[i]))
		}
		s.touch(k)
		swapped++
	}
	return swapped
}
