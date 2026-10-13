//go:build planted

package wal

import "encoding/binary"

// PayloadEnds returns the offset of the last payload byte of every entry
// record with a payload in the bytes of one segment. The planted bug B04c
// flips a bit there: the record still decodes, and only its checksum
// tells.
func PayloadEnds(data []byte) []int {
	var out []int
	for off := headerSize; off+recHdrSize <= len(data); {
		end := off + recHdrSize + int(binary.LittleEndian.Uint32(data[off:]))
		if end > len(data) {
			break
		}
		if body := data[off+recHdrSize : end]; len(body) > 18 && body[0] == recEntry {
			out = append(out, end-1)
		}
		off = end
	}
	return out
}
