package core

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// placer records where the values of the puts of one write start in their
// Raft entry, as the write is decoded.
type placer struct {
	places []place
}

// place is where a value starts in the entry being applied. start is the
// value's first byte, which identifies its slice.
type place struct {
	start *byte
	off   uint32
}

// reset forgets every place. The places point into an entry, and none
// may stay behind, not even past the length, or the core would keep that
// entry alive.
func (pl *placer) reset() {
	clear(pl.places[:cap(pl.places)])
	pl.places = pl.places[:0]
}

// add records that value starts at offset off of the entry. A nil placer
// records nothing.
func (pl *placer) add(value []byte, off int) {
	if pl == nil || len(value) == 0 {
		return
	}
	pl.places = append(pl.places, place{start: &value[0], off: uint32(off)})
}

// values names every value by its Raft position and reads the bytes back
// from the entries in memory. Once its entry left memory, a value comes
// from the log on disk, read by a helper (ValueRead).
type values struct {
	storage *storage
	log     Log
	cache   *cache
	// index is the entry being applied, and entry its data, which a
	// replayed entry has only here; placer knows where the values of its
	// current write start.
	index uint64
	entry []byte
	placer
	// fetched holds the values read from the log for the write or read
	// that runs again now because it needed them at once.
	fetched map[mvcc.Ref][]byte
}

var errNotPlaced = errors.New("core: a value is not where its write was decoded")

// Place returns the position of a value of the write being applied.
func (v *values) Place(value []byte) (mvcc.Ref, error) {
	for _, p := range v.places {
		if p.start == &value[0] {
			off := p.off
			if planted.On("B24") {
				// Planted: the position is one byte off.
				off++
			}
			return mvcc.Ref{Index: v.index, Off: off, Len: uint32(len(value))}, nil
		}
	}
	return mvcc.Ref{}, errNotPlaced
}

// Read returns a copy of a value from its entry in memory, or one from
// the cache or fetched, and false if only the log has it. Responses and
// watch events keep what it returns, maybe for long behind a slow client,
// and must not keep the entry with it. A copy goes to the end of *arena if
// arena is not nil.
func (v *values) Read(r mvcc.Ref, arena *[]byte) ([]byte, bool, error) {
	data, ok := v.entry, v.entry != nil && r.Index == v.index
	if !ok {
		data, ok = v.storage.entryData(r.Index)
	}
	if !ok {
		if b, ok := v.cache.get(r); ok {
			return b, true, nil
		}
		b, ok := v.fetched[r]
		return b, ok, nil
	}
	if uint64(r.Off)+uint64(r.Len) > uint64(len(data)) {
		return nil, false, fmt.Errorf("%w: value at %d+%d is past the end of entry %d", wal.ErrCorrupt, r.Off, r.Len, r.Index)
	}
	b := data[r.Off : r.Off+r.Len]
	if arena == nil {
		return bytes.Clone(b), true, nil
	}
	start := len(*arena)
	*arena = append(*arena, b...)
	return (*arena)[start:len(*arena):len(*arena)], true, nil
}
