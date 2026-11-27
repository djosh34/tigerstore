package check

import "strings"

// witnessText never relocates an accumulated output prefix on append.
// A small explanation stays inline; larger output uses fixed-size linked
// chunks (not a growing slice of data or chunk pointers).
type witnessText struct {
	w           *witness
	first, last *witnessTextChunk
	size        int
	small       [256]byte
}

type witnessTextChunk struct {
	next *witnessTextChunk
	n    int
	data [4096]byte
}

func (b *witnessText) Len() int { return b.size }

func (b *witnessText) append(w *witness, p []byte) {
	b.w = w
	if b.first == nil {
		if len(p) <= len(b.small)-b.size {
			b.size += copy(b.small[b.size:], p)
			return
		}
		b.first = &witnessTextChunk{n: b.size}
		copy(b.first.data[:], b.small[:b.size]) // at most 256 bytes
		b.last = b.first
	}
	for len(p) > 0 {
		if b.last.n == len(b.last.data) {
			b.last.next = &witnessTextChunk{}
			b.last = b.last.next
		}
		n := copy(b.last.data[b.last.n:], p)
		b.last.n += n
		b.size += n
		p = p[n:]
	}
}

// String allocates the final output once, then charges and polls every
// copied chunk. Grow is called while out is empty, so neither it nor any
// later Write relocates an already-populated contiguous prefix.
func (b *witnessText) String() string {
	if b.size == 0 || b.w.stopped {
		return ""
	}
	var out strings.Builder
	out.Grow(b.size)
	copyPart := func(p []byte) bool {
		// A 64-byte memory-copy block is cheap capacity work. Its
		// independent polling frequency is still about 4 MiB per hook.
		if b.w.search.capacityCharge(int64((len(p) + 63) / 64)) {
			return false
		}
		_, _ = out.Write(p)
		return true
	}
	if b.first == nil {
		if !copyPart(b.small[:b.size]) {
			return ""
		}
	} else {
		for c := b.first; c != nil; c = c.next {
			if !copyPart(c.data[:c.n]) {
				return ""
			}
		}
	}
	return out.String()
}
