package wal

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
)

// A pack is a file that cleaning makes. It holds values that a checkpoint
// still names, copied out of older segments and packs, each with its Raft
// position: the index of the entry that wrote it and its offset in the
// entry's data. A value keeps its position when its bytes move, so the
// index never changes. The file is
//
//	header | value records | end record
//
// The header is a magic string, the format version and the pack's number.
// The records are framed and chained like those of a segment of version
// 1, starting from crc 0, and the end record holds the number of values. A pack counts once a
// synced root names it. Open removes the packs that the newest root does
// not name, which a crash left behind, and a named pack that is missing or
// does not verify is ErrCorrupt.

const (
	// packVersion is the format version of packs.
	packVersion    = 1
	packMagic      = "tigerpck"
	packPrefix     = "pack-"
	packHeaderSize = len(packMagic) + 4 + 8
)

func packName(id uint64) string { return fmt.Sprintf("%s%016x", packPrefix, id) }

// Pack is a pack file that is open for reading, with where each of its
// values is.
type Pack struct {
	id   uint64
	size int64
	// values is how many bytes its values take, without their records.
	values int64
	f      File
	// vals are sorted by position.
	vals []packVal
}

// packVal is one value of a pack: its position and the offset of its
// bytes in the file.
type packVal struct {
	index    uint64
	off, len uint32
	at       uint32
}

// Close closes the pack's file. Only a pack that no root names, and that
// is not installed, is closed by its maker.
func (p *Pack) Close() error { return p.f.Close() }

// find returns the offset in the file of the n bytes at offset off of the
// data of entry index, if the pack holds them.
func (p *Pack) find(index uint64, off, n uint32) (int64, bool) {
	if len(p.vals) == 0 || index < p.vals[0].index || index > p.vals[len(p.vals)-1].index {
		return 0, false
	}
	// The last value at or before the position.
	i := sort.Search(len(p.vals), func(i int) bool {
		v := &p.vals[i]
		return v.index > index || (v.index == index && v.off > off)
	})
	if i == 0 {
		return 0, false
	}
	v := &p.vals[i-1]
	if v.index != index || uint64(off)+uint64(n) > uint64(v.off)+uint64(v.len) {
		return 0, false
	}
	return int64(v.at) + int64(off-v.off), true
}

func cmpPackVal(a, b packVal) int {
	return cmp.Or(cmp.Compare(a.index, b.index), cmp.Compare(a.off, b.off))
}

// PackWriter writes a new pack.
type PackWriter struct{}

// sortPackVals sorts a pack's values by position and drops repeats: two
// versions may name the same value.
func sortPackVals(p *Pack) {
	slices.SortFunc(p.vals, cmpPackVal)
	p.vals = slices.CompactFunc(p.vals, func(a, b packVal) bool { return a.index == b.index && a.off == b.off })
	p.vals = slices.Clip(p.vals)
	p.values = 0
	for _, v := range p.vals {
		p.values += int64(v.len)
	}
}

// openPacks opens the packs the newest root names, among the files
// names, and removes the others.
func (w *WAL) openPacks(names []string) error {
	removed := false
	for _, name := range names {
		var id uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, packPrefix), "%016x", &id); err != nil || name != packName(id) {
			return fmt.Errorf("%w: pack name %s", ErrCorrupt, name)
		}
		w.nextPack = max(w.nextPack, id+1)
		if slices.Contains(w.named, id) {
			continue
		}
		if err := w.fs.Remove(name); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		if err := w.fs.SyncDir(); err != nil {
			return err
		}
	}
	for _, id := range w.named {
		if !slices.Contains(names, packName(id)) {
			return fmt.Errorf("%w: pack %d that the root names is missing", ErrCorrupt, id)
		}
		p, err := openPack(w.fs, id)
		if err != nil {
			return err
		}
		w.packs = append(w.packs, p)
	}
	slices.SortFunc(w.packs, func(a, b *Pack) int { return cmp.Compare(a.id, b.id) })
	return nil
}

// openPack reads pack id and checks every record of it.
func openPack(fs FS, id uint64) (*Pack, error) {
	f, err := fs.Open(packName(id))
	if err != nil {
		return nil, err
	}
	p := &Pack{id: id, f: f}
	if p.size, err = p.read(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	sortPackVals(p)
	return p, nil
}

// packReadBytes is the piece of a pack that read holds at a time. A pack
// can be larger than the memory a restart has, so read never holds the
// whole file (#292).
const packReadBytes = 64 << 10

// read takes a pack's values from f, which a synced root names, so
// anything that does not verify is damage. It returns the pack's size.
func (p *Pack) read(f File) (int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, math.MaxInt64), packReadBytes)
	corrupt := fmt.Errorf("%w: pack %d does not verify", ErrCorrupt, p.id)
	// A read that ends inside the pack is damage.
	ended := func(err error) bool { return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) }
	short := func(err error) error {
		if ended(err) {
			return corrupt
		}
		return err
	}
	var hdr [max(packHeaderSize, recHdrSize+valueHdrSize)]byte
	_, err := io.ReadFull(r, hdr[:packHeaderSize])
	if err != nil && !ended(err) {
		return 0, err
	}
	if err != nil || string(hdr[:len(packMagic)]) != packMagic || binary.LittleEndian.Uint64(hdr[len(packMagic)+4:]) != p.id {
		return 0, fmt.Errorf("%w: bad header of pack %d", ErrCorrupt, p.id)
	}
	if v := binary.LittleEndian.Uint32(hdr[len(packMagic):]); v != packVersion {
		return 0, fmt.Errorf("%w: pack %d has format version %d; this build reads version %d", ErrFormat, p.id, v, packVersion)
	}
	var crc uint32
	for off := int64(packHeaderSize); ; {
		// The record's header, and as much of its payload as says what
		// it holds.
		if _, err := io.ReadFull(r, hdr[:recHdrSize]); err != nil {
			return 0, short(err)
		}
		n := int64(binary.LittleEndian.Uint32(hdr[:]))
		want := binary.LittleEndian.Uint32(hdr[4:])
		head := hdr[recHdrSize : recHdrSize+min(n, valueHdrSize)]
		if _, err := io.ReadFull(r, head); err != nil {
			return 0, short(err)
		}
		crc = crc32.Update(crc, castagnoli, hdr[recHdrSize-1:recHdrSize+len(head)])
		for left := n - int64(len(head)); left > 0; {
			b, err := r.Peek(int(min(left, packReadBytes)))
			if err != nil {
				return 0, short(err)
			}
			crc = crc32.Update(crc, castagnoli, b)
			if _, err := r.Discard(len(b)); err != nil {
				return 0, err
			}
			left -= int64(len(b))
		}
		if crc != want {
			return 0, corrupt
		}
		end := off + recHdrSize + n
		switch hdr[recHdrSize-1] {
		case recValue:
			if n < valueHdrSize {
				return 0, fmt.Errorf("%w: short value record in pack %d", ErrCorrupt, p.id)
			}
			p.vals = append(p.vals, packVal{
				index: binary.LittleEndian.Uint64(head),
				off:   binary.LittleEndian.Uint32(head[8:]),
				len:   uint32(n - valueHdrSize),
				at:    uint32(off + recHdrSize + valueHdrSize),
			})
			off = end
			continue
		case recEnd:
			// The end record holds the count of values and ends the file.
			_, err := r.ReadByte()
			if err != nil && !errors.Is(err, io.EOF) {
				return 0, err
			}
			if err != nil && n == 8 && binary.LittleEndian.Uint64(head) == uint64(len(p.vals)) {
				return end, nil
			}
		}
		return 0, corrupt
	}
}

// closePacks closes the files of the named packs.
func (w *WAL) closePacks() error {
	var err error
	for _, p := range slices.Concat(w.packs, w.dropped) {
		err = errors.Join(err, p.f.Close())
	}
	w.packs, w.dropped = nil, nil
	return err
}

// packPlace returns the pack, and the offset in its file, of the n bytes
// at offset off of the data of entry index, if a pack holds them.
func (w *WAL) packPlace(index uint64, off, n uint32) (*Pack, int64, bool) {
	for _, p := range slices.Backward(w.packs) {
		if at, ok := p.find(index, off, n); ok {
			return p, at, true
		}
	}
	return nil, 0, false
}

// FileID names a file of the log: segment N, or pack N.
type FileID struct {
	Pack bool
	N    uint64
}

// CleanFile is a file that cleaning may replace, and its size. Values is
// how many bytes the values of a pack take, without their records.
type CleanFile struct {
	FileID
	Size, Values int64
}
