package core

import (
	"errors"
	"fmt"
	"math"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
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
// from the entries in memory, or from the log on disk once its entry left
// memory.
type values struct {
	storage *storage
	log     Log
	// index is the entry being applied; placer knows where the values of
	// its current write start.
	index uint64
	placer
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

// Read returns the bytes of a value from its entry. A value read from
// memory is a slice of the entry, which a response holds until it is
// sent; one read from disk is a copy.
func (v *values) Read(r mvcc.Ref) ([]byte, error) {
	last, err := v.storage.LastIndex()
	if err != nil {
		return nil, err
	}
	if r.Index < v.storage.first || r.Index > last {
		return v.log.Value(r.Index, r.Off, r.Len)
	}
	ents, err := v.storage.Entries(r.Index, r.Index+1, math.MaxUint64)
	if err != nil {
		return nil, fmt.Errorf("core: read entry %d: %w", r.Index, err)
	}
	data := ents[0].GetData()
	if uint64(r.Off)+uint64(r.Len) > uint64(len(data)) {
		return nil, fmt.Errorf("core: value at %d+%d is past the end of entry %d", r.Off, r.Len, r.Index)
	}
	return data[r.Off : r.Off+r.Len], nil
}
