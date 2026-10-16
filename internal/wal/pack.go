package wal

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	// packFlush is how much of a pack the writer buffers before a write.
	packFlush = 1 << 20
)

func packName(id uint64) string { return fmt.Sprintf("%s%016x", packPrefix, id) }

// Pack is a pack file that is open for reading, with where each of its
// values is.
type Pack struct {
	id   uint64
	size int64
	f    File
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

// ID is the pack's number.
func (p *Pack) ID() uint64 { return p.id }

// Size is the size of the pack's file.
func (p *Pack) Size() int64 { return p.size }

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

func (p *Pack) readAt(b []byte, at int64) error {
	n, err := p.f.ReadAt(b, at)
	if n == len(b) {
		return nil
	}
	return fmt.Errorf("wal: read %d of %d bytes at %d of pack %d: %w", n, len(b), at, p.id, err)
}

// mapBytes is about what the pack's map takes in memory.
func (p *Pack) mapBytes() int64 { return 64 + int64(cap(p.vals))*24 }

// PackWriter writes a new pack.
type PackWriter struct {
	fs  FS
	p   *Pack
	buf []byte
	crc uint32
}

// NewPack creates the file of the next pack.
func (w *WAL) NewPack() (*PackWriter, error) {
	id := w.nextPack
	w.nextPack++
	f, err := w.fs.Create(packName(id))
	if err != nil {
		return nil, err
	}
	buf := append(make([]byte, 0, packFlush+packFlush/4), packMagic...)
	buf = binary.LittleEndian.AppendUint32(buf, packVersion)
	buf = binary.LittleEndian.AppendUint64(buf, id)
	return &PackWriter{fs: w.fs, p: &Pack{id: id, f: f}, buf: buf}, nil
}

// Add appends the value at offset off of the data of entry index.
func (pw *PackWriter) Add(index uint64, off uint32, value []byte) error {
	var h [valueHdrSize]byte
	binary.LittleEndian.PutUint64(h[:], index)
	binary.LittleEndian.PutUint32(h[8:], off)
	at := pw.p.size + int64(len(pw.buf)) + recHdrSize + valueHdrSize
	if at+int64(len(value)) > 1<<32-1 {
		return fmt.Errorf("wal: pack %d would pass 4 GiB", pw.p.id)
	}
	pw.buf, pw.crc = appendRecord(pw.buf, pw.crc, recValue, h[:], value)
	pw.p.vals = append(pw.p.vals, packVal{index: index, off: off, len: uint32(len(value)), at: uint32(at)})
	if len(pw.buf) >= packFlush {
		return pw.flush()
	}
	return nil
}

// Bytes is the size of the pack so far.
func (pw *PackWriter) Bytes() int64 { return pw.p.size + int64(len(pw.buf)) }

func (pw *PackWriter) flush() error {
	n, err := pw.p.f.Write(pw.buf)
	pw.p.size += int64(n)
	pw.buf = pw.buf[:0]
	return err
}

// Finish writes the end record, syncs the file and the directory, and
// returns the pack, open for reading. It counts once InstallPacks names
// it and that root is synced. After an error the caller closes the pack's
// file with Abort.
func (pw *PackWriter) Finish() (*Pack, error) {
	var h [8]byte
	binary.LittleEndian.PutUint64(h[:], uint64(len(pw.p.vals)))
	pw.buf, pw.crc = appendRecord(pw.buf, pw.crc, recEnd, h[:])
	if err := pw.flush(); err != nil {
		return nil, err
	}
	if err := pw.p.f.Sync(); err != nil {
		return nil, err
	}
	if err := pw.fs.SyncDir(); err != nil {
		return nil, err
	}
	sortPackVals(pw.p)
	return pw.p, nil
}

// Abort closes and removes the file of a pack that is not finished.
func (pw *PackWriter) Abort() error {
	return errors.Join(pw.p.f.Close(), pw.fs.Remove(packName(pw.p.id)))
}

// sortPackVals sorts a pack's values by position and drops repeats: two
// versions may name the same value.
func sortPackVals(p *Pack) {
	slices.SortFunc(p.vals, cmpPackVal)
	p.vals = slices.CompactFunc(p.vals, func(a, b packVal) bool { return a.index == b.index && a.off == b.off })
	p.vals = slices.Clip(p.vals)
}

// openPacks opens the packs the newest root names, among the files
// names, and removes the others.
func (w *WAL) openPacks(names []string, rec *Recovered) error {
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
		rec.Bytes += p.size
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
	data, err := f.ReadAll()
	if err == nil {
		err = p.read(data)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	p.size = int64(len(data))
	sortPackVals(p)
	return p, nil
}

// read takes a pack's values from its bytes, which a synced root names, so
// anything that does not verify is damage.
func (p *Pack) read(data []byte) error {
	if len(data) < packHeaderSize || string(data[:len(packMagic)]) != packMagic ||
		binary.LittleEndian.Uint64(data[len(packMagic)+4:]) != p.id {
		return fmt.Errorf("%w: bad header of pack %d", ErrCorrupt, p.id)
	}
	if v := binary.LittleEndian.Uint32(data[len(packMagic):]); v != packVersion {
		return fmt.Errorf("wal: pack %d has format version %d; this build reads version %d", p.id, v, packVersion)
	}
	var crc uint32
	for off := packHeaderSize; off < len(data); {
		if len(data)-off < recHdrSize {
			break
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		end := off + recHdrSize + n
		if end > len(data) {
			break
		}
		body := data[off+8 : end]
		if crc = crc32.Update(crc, castagnoli, body); crc != binary.LittleEndian.Uint32(data[off+4:]) {
			break
		}
		switch body[0] {
		case recValue:
			if n < valueHdrSize {
				return fmt.Errorf("%w: short value record in pack %d", ErrCorrupt, p.id)
			}
			p.vals = append(p.vals, packVal{
				index: binary.LittleEndian.Uint64(body[1:]),
				off:   binary.LittleEndian.Uint32(body[9:]),
				len:   uint32(n - valueHdrSize),
				at:    uint32(off + recHdrSize + valueHdrSize),
			})
			off = end
			continue
		case recEnd:
			if n == 8 && end == len(data) && binary.LittleEndian.Uint64(body[1:]) == uint64(len(p.vals)) {
				return nil
			}
		}
		break
	}
	return fmt.Errorf("%w: pack %d does not verify", ErrCorrupt, p.id)
}

// closePacks closes the files of the named packs.
func (w *WAL) closePacks() error {
	var err error
	for _, p := range w.packs {
		err = errors.Join(err, p.f.Close())
	}
	w.packs = nil
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

// InstallPacks buffers a root that names the packs with add and without
// drop. Reads find the values in add at once. The root counts once it is
// synced; only then may RemoveCleaned remove the files that add replaces.
func (w *WAL) InstallPacks(add *Pack, drop []uint64) error {
	var err error
	w.packs = slices.DeleteFunc(w.packs, func(p *Pack) bool {
		if slices.Contains(drop, p.id) {
			err = errors.Join(err, p.f.Close())
			return true
		}
		return false
	})
	if add != nil {
		w.packs = append(w.packs, add)
	}
	w.copyState()
	w.rootRecord(w.root)
	return err
}

// FileID names a file of the log: segment N, or pack N.
type FileID struct {
	Pack bool
	N    uint64
}

// Where returns the file that Value reads the n bytes at offset off of the
// data of entry index from, if any holds them.
func (w *WAL) Where(index uint64, off, n uint32) (FileID, bool) {
	if seq, _, ok := w.ValuePlace(index, off, n); ok {
		return FileID{N: seq}, true
	}
	if p, _, ok := w.packPlace(index, off, n); ok {
		return FileID{Pack: true, N: p.id}, true
	}
	return FileID{}, false
}

// CleanFile is a file that cleaning may replace, and its size.
type CleanFile struct {
	FileID
	Size int64
}

// Cleanable returns the files that cleaning may replace while the oldest
// checkpoint whose file this node keeps is at index below: every pack,
// and the oldest segments, in order, whose every entry and value is at or
// below it. Recovery and snapshots need only the values such a checkpoint
// names from them. It never returns the newest two segments with records,
// which hold the newest hard state and root.
func (w *WAL) Cleanable(below uint64) []CleanFile {
	var out []CleanFile
	for i := 0; i < len(w.runs) && w.runs[i].seq+1 < w.runs[len(w.runs)-1].seq; {
		seq := w.runs[i].seq
		ok := true
		for ; i < len(w.runs) && w.runs[i].seq == seq; i++ {
			ok = ok && w.runs[i].top <= below
		}
		size, sealed := w.sizes[seq]
		if !ok || !sealed {
			break
		}
		out = append(out, CleanFile{N: seq, Size: size})
	}
	for _, p := range w.packs {
		out = append(out, CleanFile{Pack: true, N: p.id, Size: p.size})
	}
	return out
}

// RemoveCleaned removes the oldest segments up to segment upto and the
// packs drop, once a synced root names the pack that replaces them, then
// syncs the directory. RemoveBelow may have removed some of the segments
// already.
func (w *WAL) RemoveCleaned(upto uint64, drop []uint64) error {
	for _, p := range w.packs {
		if slices.Contains(drop, p.id) {
			return fmt.Errorf("wal: pack %d is still named", p.id)
		}
	}
	n := 0
	for n < len(w.runs) && w.runs[n].seq <= upto {
		if w.runs[n].seq+1 >= w.runs[len(w.runs)-1].seq {
			return fmt.Errorf("wal: segment %d is not one that cleaning may remove", w.runs[n].seq)
		}
		if n == 0 || w.runs[n-1].seq != w.runs[n].seq {
			if err := w.removeSegment(w.runs[n].seq); err != nil {
				return err
			}
		}
		n++
	}
	w.runs = slices.Delete(w.runs, 0, n)
	for _, id := range drop {
		if err := w.fs.Remove(packName(id)); err != nil {
			return err
		}
	}
	if n == 0 && len(drop) == 0 {
		return nil
	}
	return w.fs.SyncDir()
}

// MapBytes is about what the map of where entries and values are on disk
// takes in memory: per entry its record's offset and crc, the value
// records, and the values of the packs.
func (w *WAL) MapBytes() int64 {
	n := int64(len(w.sizes)) * 32
	for _, s := range w.runs {
		n += 80 + int64(cap(s.offs)+cap(s.seeds))*4
	}
	for _, vs := range w.vals {
		n += 48 + int64(cap(vs))*24
	}
	for _, p := range w.packs {
		n += p.mapBytes()
	}
	return n
}
