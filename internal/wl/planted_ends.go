//go:build planted

package wl

// PayloadEnds is for planted bug B04c, whose simulator flips a bit of an
// entry's data at rest: where the last byte of each entry's data lies in
// a log that starts at from, as its segment and the offset in it, up to
// the first record that does not check. seg returns segment k's bytes,
// nil past the last.
func PayloadEnds(seg func(k int64) []byte, from int64) [][2]int64 {
	var ends [][2]int64
	for _, r := range Records(seg, from) {
		if r.Kind == recEntry && r.Len > walEntHdr {
			at := r.Off + r.Len - 1
			ends = append(ends, [2]int64{at / segSize, at % segSize})
		}
	}
	return ends
}
