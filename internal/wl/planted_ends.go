//go:build planted

package wl

import "encoding/binary"

// PayloadEnds is for planted bug B04c, whose simulator flips a bit of an
// entry's data at rest: where the last byte of each entry's data lies in
// a log that starts at from, as its segment and the offset in it, up to
// the first record that does not check. seg returns segment k's bytes,
// nil past the last.
func PayloadEnds(seg func(k int64) []byte, from int64) [][2]int64 {
	var b []byte
	for k := from / segSize; ; k++ {
		s := seg(k)
		if s == nil {
			break
		}
		if k == from/segSize {
			if int64(len(s)) < from%segSize {
				return nil
			}
			s = s[from%segSize:]
		}
		b = append(b, s...)
		if int64(len(s)) < segSize {
			break
		}
	}
	var ends [][2]int64
	for off := from; checks(b); {
		n := recHead + int64(binary.LittleEndian.Uint32(b[4:]))
		if b[8] == recEntry && n > walEntHdr {
			at := off + n - 1
			ends = append(ends, [2]int64{at / segSize, at % segSize})
		}
		off, b = off+n, b[n:]
	}
	return ends
}
