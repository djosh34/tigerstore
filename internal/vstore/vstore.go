// Package vstore is prototype E's (#133) per-node manifest: where on this
// node's disk the value at a Raft position lives. Values stay in the Raft
// log segments they were written to, or in packed files that cleaning
// made; a value keeps its Raft position either way, so the index never
// changes. Reads run on the caller's goroutine, never the committer's.
package vstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// EntryDataOff is where an entry's data starts in its record: the record
// header (9 bytes) and term, index and type (17 bytes).
const EntryDataOff = 26

type loc struct {
	seq uint32
	pos uint32
}

// Packed is one file made by cleaning: values back to back, then a table
// of (idx u64, off u32, n u32, pos u64) per value in address order, then
// the table's length (u64).
type Packed struct {
	Name string
	f    *os.File
	tab  []PackedEnt
	Size int64
}

// PackedEnt is one value of a packed file.
type PackedEnt struct {
	Idx    uint64
	Off, N uint32
	Pos    uint64
}

type seg struct {
	f       *os.File
	removed bool
}

// Store is the manifest.
type Store struct {
	walDir, valDir string

	mu     sync.RWMutex
	base   uint64
	locs   []loc
	segs   map[uint64]*seg
	packed []*Packed

	ColdReads, ColdBytes atomic.Int64
}

// New returns a manifest over the segments in walDir and packed files
// in valDir.
func New(walDir, valDir string) (*Store, error) {
	if err := os.MkdirAll(valDir, 0o755); err != nil {
		return nil, err
	}
	return &Store{walDir: walDir, valDir: valDir, segs: map[uint64]*seg{}}, nil
}

// ValDir is where packed files go.
func (s *Store) ValDir() string { return s.valDir }

// Segment registers a log segment.
func (s *Store) Segment(seq uint64) {
	f, err := os.Open(filepath.Join(s.walDir, fmt.Sprintf("%016x.wal", seq)))
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.segs[seq] = &seg{removed: true}
		return
	}
	if old := s.segs[seq]; old != nil && old.f != nil {
		old.f.Close()
	}
	s.segs[seq] = &seg{f: f}
}

// Entry records where entry idx's record starts. A later record for the
// same index replaces it and everything after it, as in the log.
func (s *Store) Entry(idx, seq uint64, pos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case len(s.locs) == 0 || idx < s.base:
		s.base, s.locs = idx, s.locs[:0]
	case idx < s.base+uint64(len(s.locs)):
		s.locs = s.locs[:idx-s.base]
	case idx > s.base+uint64(len(s.locs)):
		// A gap: an install that skipped entries. Positions below are
		// kept as missing.
		for s.base+uint64(len(s.locs)) < idx {
			s.locs = append(s.locs, loc{seq: 0})
		}
	}
	s.locs = append(s.locs, loc{seq: uint32(seq), pos: uint32(pos)})
}

// Locate returns the segment and record position of entry idx.
func (s *Store) Locate(idx uint64) (seq uint64, pos int64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if idx < s.base || idx >= s.base+uint64(len(s.locs)) {
		return 0, 0, false
	}
	l := s.locs[idx-s.base]
	if l.seq == 0 {
		return 0, 0, false
	}
	sg := s.segs[uint64(l.seq)]
	if sg == nil || sg.removed {
		return 0, 0, false
	}
	return uint64(l.seq), int64(l.pos), true
}

// SegFile returns an open segment, for reads by a helper.
func (s *Store) SegFile(seq uint64) *os.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sg := s.segs[seq]; sg != nil && !sg.removed {
		return sg.f
	}
	return nil
}

// Read returns the bytes of r from disk.
func (s *Store) Read(r mvcc.Ref) []byte {
	b := make([]byte, r.N)
	s.ColdReads.Add(1)
	s.ColdBytes.Add(int64(r.N))
	s.mu.RLock()
	if r.Idx >= s.base && r.Idx < s.base+uint64(len(s.locs)) {
		l := s.locs[r.Idx-s.base]
		if sg := s.segs[uint64(l.seq)]; l.seq != 0 && sg != nil && !sg.removed {
			f := sg.f
			s.mu.RUnlock()
			if _, err := f.ReadAt(b, int64(l.pos)+EntryDataOff+int64(r.Off)); err != nil {
				panic(fmt.Sprintf("vstore: read %v: %v", r, err))
			}
			return b
		}
	}
	for i := len(s.packed) - 1; i >= 0; i-- {
		p := s.packed[i]
		j := sort.Search(len(p.tab), func(j int) bool {
			e := &p.tab[j]
			return e.Idx > r.Idx || (e.Idx == r.Idx && e.Off >= r.Off)
		})
		if j < len(p.tab) && p.tab[j].Idx == r.Idx && p.tab[j].Off == r.Off {
			f := p.f
			pos := int64(p.tab[j].Pos)
			s.mu.RUnlock()
			if _, err := f.ReadAt(b, pos); err != nil {
				panic(fmt.Sprintf("vstore: read %v from %s: %v", r, p.Name, err))
			}
			return b
		}
	}
	s.mu.RUnlock()
	panic(fmt.Sprintf("vstore: value %v is nowhere on this node", r))
}

// Where names the file that holds r: a segment seq, or a packed file.
func (s *Store) Where(r mvcc.Ref) (seq uint64, packed *Packed) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r.Idx >= s.base && r.Idx < s.base+uint64(len(s.locs)) {
		l := s.locs[r.Idx-s.base]
		if sg := s.segs[uint64(l.seq)]; l.seq != 0 && sg != nil && !sg.removed {
			return uint64(l.seq), nil
		}
	}
	for i := len(s.packed) - 1; i >= 0; i-- {
		p := s.packed[i]
		j := sort.Search(len(p.tab), func(j int) bool {
			e := &p.tab[j]
			return e.Idx > r.Idx || (e.Idx == r.Idx && e.Off >= r.Off)
		})
		if j < len(p.tab) && p.tab[j].Idx == r.Idx && p.tab[j].Off == r.Off {
			return 0, p
		}
	}
	return 0, nil
}

// Locs returns every known entry position below or at upto, for the
// checkpoint's per-node part.
func (s *Store) Locs(upto uint64, fn func(idx, seq uint64, pos int64)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, l := range s.locs {
		idx := s.base + uint64(i)
		if idx > upto {
			break
		}
		if sg := s.segs[uint64(l.seq)]; l.seq != 0 && sg != nil && !sg.removed {
			fn(idx, uint64(l.seq), int64(l.pos))
		}
	}
}

// Segs returns the live segments and their sizes.
func (s *Store) Segs() map[uint64]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[uint64]int64{}
	for seq, sg := range s.segs {
		if !sg.removed && sg.f != nil {
			if st, err := sg.f.Stat(); err == nil {
				out[seq] = st.Size()
			}
		}
	}
	return out
}

// PackedFiles returns the packed files.
func (s *Store) PackedFiles() []*Packed {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.packed)
}

// SegRange returns the first and last entry in segment seq.
func (s *Store) SegRange(seq uint64) (first, last uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, l := range s.locs {
		if uint64(l.seq) == seq {
			if !ok {
				first, ok = s.base+uint64(i), true
			}
			last = s.base + uint64(i)
		}
	}
	return
}

// AddPacked opens a packed file and adds it to the manifest.
func (s *Store) AddPacked(name string) (*Packed, error) {
	f, err := os.Open(filepath.Join(s.valDir, name))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	var nb [8]byte
	if _, err := f.ReadAt(nb[:], st.Size()-8); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint64(nb[:])
	raw := make([]byte, n*24)
	if _, err := f.ReadAt(raw, st.Size()-8-int64(len(raw))); err != nil {
		return nil, err
	}
	p := &Packed{Name: name, f: f, Size: st.Size(), tab: make([]PackedEnt, n)}
	for i := range p.tab {
		b := raw[i*24:]
		p.tab[i] = PackedEnt{Idx: binary.LittleEndian.Uint64(b), Off: binary.LittleEndian.Uint32(b[8:]), N: binary.LittleEndian.Uint32(b[12:]), Pos: binary.LittleEndian.Uint64(b[16:])}
	}
	s.mu.Lock()
	s.packed = append(s.packed, p)
	s.mu.Unlock()
	return p, nil
}

// Remove drops segments and packed files from the manifest. Their file
// descriptors stay open a while for readers that already looked them up.
func (s *Store) Remove(segs []uint64, packed []*Packed) {
	s.mu.Lock()
	var files []*os.File
	for _, seq := range segs {
		if sg := s.segs[seq]; sg != nil {
			sg.removed = true
			if sg.f != nil {
				files = append(files, sg.f)
			}
		}
	}
	s.packed = slices.DeleteFunc(s.packed, func(p *Packed) bool {
		if slices.Contains(packed, p) {
			files = append(files, p.f)
			return true
		}
		return false
	})
	s.mu.Unlock()
	time.AfterFunc(30*time.Second, func() {
		for _, f := range files {
			f.Close()
		}
	})
}
