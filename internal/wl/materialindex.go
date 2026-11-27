package wl

const materialReady = uint64(1) << 63

// These are #897's transport/queue metadata, not main's sort scratch.
// Keep them in shared W rather than padding every P-funded canonical KV.
// The common entry holds a 32-bit byte offset and a 31-bit next ordinal;
// the wide form preserves large-budget consumers without truncation.
func (b *legacyBatch) materialWords() int {
	if b.payload > int64(^uint32(0)) {
		return 2 * b.n
	}
	return b.n
}
func (b *legacyBatch) materialWide() bool { return b.materialWords() != b.n }
func (b *legacyBatch) materialOffset(i int) int64 {
	if i == b.n {
		return b.payload
	}
	if b.materialWide() {
		return int64(b.materialIndex[2*i])
	}
	return int64(uint32(b.materialIndex[i]))
}

func (b *legacyBatch) materialRecord(i int) []byte {
	return b.data[b.materialOffset(i):b.materialOffset(i+1)]
}

func (b *legacyBatch) materialWord(i int) *uint64 {
	if b.materialWide() {
		return &b.materialIndex[2*i+1]
	}
	return &b.materialIndex[i]
}

func (b *legacyBatch) materialNext(i int) uint64 {
	word := *b.materialWord(i)
	next := word &^ materialReady
	if !b.materialWide() {
		next = (word >> 32) & 0x7fffffff
		if next == 0x7fffffff {
			return materialEnd
		}
	} else if next == materialReady-1 {
		return materialEnd
	}
	return next
}

func (b *legacyBatch) setMaterialNext(i int, next uint64) {
	word := b.materialWord(i)
	if b.materialWide() {
		if next == materialEnd {
			next = materialReady - 1
		}
		*word = (*word & materialReady) | next
		return
	}
	if next == materialEnd {
		next = 0x7fffffff
	}
	*word = (*word & (materialReady | uint64(^uint32(0)))) | (next << 32)
}

func (b *legacyBatch) initMaterialSlot(i, at int, ready bool) {
	if b.materialWide() {
		b.materialIndex[2*i] = uint64(at)
	} else {
		b.materialIndex[i] = uint64(at)
	}
	b.setMaterialNext(i, materialEnd)
	if ready {
		*b.materialWord(i) |= materialReady
	}
}

func (b *legacyBatch) materialSlot(at uint64) int {
	lo, hi := 0, b.n
	for lo < hi {
		m := lo + (hi-lo)/2
		if uint64(b.materialOffset(m)) < at {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == b.n || uint64(b.materialOffset(lo)) != at {
		return -1
	}
	return lo
}
