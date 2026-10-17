package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"go.etcd.io/raft/v3/raftpb"
)

// Entry format 4 keeps the format-3 batch layout when raw. A zero batch
// count instead means: decoded length (u32), then one zstd frame. Counts
// in a real batch are positive. Positions always refer to the raw entry,
// including its two-byte format header.
//
// Compression runs on the disk writer, before taking the log lock. Raft,
// the peer transport and the in-memory window keep raw entries. Recovery
// and cold readers decode on helpers, never on the committer.
const compressionVersion = 4

func compressed(data []byte) bool {
	return len(data) >= 3 && data[0] == 0 && data[1] == compressionVersion && data[2] == 0
}

// compressible samples at most 1 KiB spread over the entry. Random bytes
// have roughly uniform byte frequencies. Skipping them avoids building
// zstd tables for ciphertext. A false negative only loses a saving.
func compressible(data []byte) bool {
	if len(data) < 256 {
		return false
	}
	var counts [256]int
	n := min(len(data), 1024)
	for i := range n {
		counts[data[i*len(data)/n]]++
	}
	squares := 0
	for _, c := range counts {
		squares += c * c
	}
	return squares > n+n*n/128
}

// CompressEntries returns disk copies of format-4 normal entries. It does
// not mutate entries held by Raft. Older formats and incompressible data
// remain byte-for-byte raw, with no extra tag or allocation per entry.
func (w *WAL) CompressEntries(entries []*raftpb.Entry) ([]*raftpb.Entry, error) {
	out := entries
	copied := false
	for i, e := range entries {
		data := e.GetData()
		if e.GetType() != raftpb.EntryNormal || len(data) < 3 || data[0] != 0 || data[1] != compressionVersion || compressed(data) || !compressible(data[2:]) {
			continue
		}
		if len(data) > MaxEntryBytes {
			return nil, fmt.Errorf("wal: entry exceeds compression limit: %d", len(data))
		}
		if w.encoder == nil {
			var err error
			w.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(1)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
			if err != nil { return nil, err }
		}
		header := binary.LittleEndian.AppendUint32([]byte{0, compressionVersion, 0}, uint32(len(data)))
		packed := w.encoder.EncodeAll(data[2:], header)
		if len(packed) >= len(data) {
			continue
		}
		if !copied {
			out = append([]*raftpb.Entry(nil), entries...)
			copied = true
		}
		out[i] = &raftpb.Entry{Term: e.Term, Index: e.Index, Type: e.Type, Data: packed}
	}
	return out, nil
}

func decodeEntry(dec *zstd.Decoder, data []byte) ([]byte, error) {
	if !compressed(data) {
		return data, nil
	}
	if len(data) < 7 {
		return nil, fmt.Errorf("%w: short compressed entry", ErrCorrupt)
	}
	n := binary.LittleEndian.Uint32(data[3:])
	if n < 3 || n > MaxEntryBytes {
		return nil, fmt.Errorf("%w: decoded entry length %d", ErrCorrupt, n)
	}
	out, err := dec.DecodeAll(data[7:], []byte{0, compressionVersion})
	if err != nil {
		return nil, fmt.Errorf("%w: compressed entry: %w", ErrCorrupt, err)
	}
	if len(out) != int(n) || out[2] == 0 {
		return nil, fmt.Errorf("%w: decoded entry length or batch count", ErrCorrupt)
	}
	return out, nil
}

// compressedReader defers disk I/O and decompression until ReadAt, after
// diskLog.Value releases the append lock. The caller pins the file until
// the read completes, as it does for raw value readers.
type compressedReader struct {
	file       io.ReaderAt
	at         int64
	index, seq uint64
	seed       uint32
	decoder *zstd.Decoder
}

func (r compressedReader) ReadAt(p []byte, off int64) (int, error) {
	e, err := readEntry(r.file, r.at, r.index, r.seq, r.seed, r.decoder)
	if err != nil {
		return 0, err
	}
	if off < 0 || off > int64(len(e.Data)) || int64(len(p)) > int64(len(e.Data))-off {
		return 0, fmt.Errorf("%w: value outside decoded entry %d", ErrCorrupt, r.index)
	}
	return copy(p, e.Data[off:]), nil
}
