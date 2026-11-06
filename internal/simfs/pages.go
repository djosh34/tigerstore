package simfs

// A file's synced content is a list of pages of PageBytes each, held in
// blocks that every disk and image with those bytes shares: a crash image,
// a copy of the disk and the disk itself hold the same blocks until one of
// them changes a page, which then gets a block of its own (#479).

// block is one page of bytes. It holds no pointers, so the collector
// never scans it.
type block = [PageBytes]byte

// pages is a file's content: size bytes, page i in blocks[i], or zeros
// where the block is nil or past the list. owned says which blocks no
// other pages share, which a change may make in place; any other block is
// never changed again, and a change goes to a new one.
type pages struct {
	blocks []*block
	owned  []bool
	size   int
}

// share returns a copy of p that shares its blocks, which neither then
// changes in place.
func (p *pages) share() pages {
	clear(p.owned)
	return pages{blocks: append([]*block(nil), p.blocks...), size: p.size}
}

// page returns page i for reading, nil for zeros. The caller must not
// change or keep it.
func (p *pages) page(i int) *block {
	if i < len(p.blocks) {
		return p.blocks[i]
	}
	return nil
}

// own returns page i for a change, a new block if it has none or shares
// it, and grows the list to hold it.
func (p *pages) own(i int) *block {
	for len(p.blocks) <= i {
		p.blocks = append(p.blocks, nil)
	}
	for len(p.owned) <= i {
		p.owned = append(p.owned, false)
	}
	if p.owned[i] {
		return p.blocks[i]
	}
	b := new(block)
	if old := p.blocks[i]; old != nil {
		*b = *old
	}
	p.blocks[i], p.owned[i] = b, true
	return b
}

// set makes b page i, as a block p owns: b must be no one else's.
func (p *pages) set(i int, b *block) {
	for len(p.blocks) <= i {
		p.blocks = append(p.blocks, nil)
	}
	for len(p.owned) <= i {
		p.owned = append(p.owned, false)
	}
	p.blocks[i], p.owned[i] = b, true
}

// read fills dst with the bytes at off, zeros past size.
func (p *pages) read(dst []byte, off int) {
	n := max(0, min(len(dst), p.size-off))
	clear(dst[n:])
	for k := 0; k < n; {
		i, at := (off+k)/PageBytes, (off+k)%PageBytes
		m := min(n-k, PageBytes-at)
		if src := p.page(i); src != nil {
			copy(dst[k:k+m], src[at:at+m])
		} else {
			clear(dst[k : k+m])
		}
		k += m
	}
}

// bytes returns a copy of the content.
func (p *pages) bytes() []byte {
	b := make([]byte, p.size)
	p.read(b, 0)
	return b
}

// write writes b at off, growing the content with zeros up to off.
func (p *pages) write(off int, b []byte) {
	for k := 0; k < len(b); {
		i, at := (off+k)/PageBytes, (off+k)%PageBytes
		m := min(len(b)-k, PageBytes-at)
		copy(p.own(i)[at:at+m], b[k:k+m])
		k += m
	}
	p.size = max(p.size, off+len(b))
}

// grow makes the content at least size bytes, the new ones zeros.
func (p *pages) grow(size int) { p.size = max(p.size, size) }

// cut makes the content size bytes if it is longer, so that the bytes past
// size read as zeros if it grows again.
func (p *pages) cut(size int) {
	if size >= p.size {
		return
	}
	if at := size % PageBytes; at != 0 && p.page(size/PageBytes) != nil {
		clear(p.own(size / PageBytes)[at:])
	}
	n := min(len(p.blocks), pageCount(size))
	clear(p.blocks[n:])
	p.blocks = p.blocks[:n]
	p.owned = p.owned[:min(len(p.owned), n)]
	p.size = size
}

// pageCount is how many pages size bytes take.
func pageCount(size int) int { return (size + PageBytes - 1) / PageBytes }
