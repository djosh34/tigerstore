package simfs

// A file's synced content is a list of pages of PageBytes each, held in
// blocks that every disk and image with those bytes shares: a crash image,
// a copy of the disk and the disk itself hold the same blocks until one of
// them changes a page, which then gets a block of its own. A disk with a
// Store moves the synced pages of its large files there, out of memory
// (#479).

// block is one page of bytes. It holds no pointers, so the collector
// never scans it.
type block = [PageBytes]byte

// Store keeps pages of synced bytes outside memory, such as in a file on
// the disk of the machine that runs a simulation. Its slots are not part
// of the simulated disk: what a disk reads back is what it put.
type Store interface {
	// Put writes p to a slot that stays taken while owner is reachable,
	// and returns where.
	Put(owner any, p []byte) (int64, error)
	// Get reads the slot at off into p.
	Get(off int64, p []byte) error
}

// storage is a disk's Store, shared with its copies: the size from which
// a file's synced pages go there, and the first error the Store gave.
type storage struct {
	store Store
	from  int
	err   error
}

// fail keeps the first error of the Store.
func (st *storage) fail(err error) {
	if st.err == nil {
		st.err = err
	}
}

// page is one page of a file's content, in memory or, once b is nil, in
// the Store at at.
type page struct {
	b  *block
	at int64
}

// pages is a file's content: size bytes, page i in blocks[i], or zeros
// where the page is nil or past the list. owned says which pages no other
// pages share and are in memory, which a change may make in place; any
// other page is never changed again, and a change goes to a new one. The
// pages below stored are in the Store, if st has one: a new page below it
// lowers it, so that the next spill stores that page too.
type pages struct {
	blocks []*page
	owned  []bool
	size   int
	st     *storage
	stored int
}

// share returns a copy of p that shares its pages, which neither then
// changes in place.
func (p *pages) share() pages {
	clear(p.owned)
	return pages{blocks: append([]*page(nil), p.blocks...), size: p.size, st: p.st, stored: p.stored}
}

// get returns page i for reading, nil for zeros, read into scratch if it
// is in the Store. The caller must not change or keep it.
func (p *pages) get(i int, scratch *[]byte) []byte {
	if i >= len(p.blocks) || p.blocks[i] == nil {
		return nil
	}
	pg := p.blocks[i]
	if pg.b != nil {
		return pg.b[:]
	}
	if *scratch == nil {
		*scratch = make([]byte, PageBytes)
	}
	if err := p.st.store.Get(pg.at, *scratch); err != nil {
		p.st.fail(err)
		clear(*scratch)
	}
	return *scratch
}

// own returns page i for a change, a new block if it has none, shares it
// or keeps it in the Store, and grows the list to hold it.
func (p *pages) own(i int) *block {
	for len(p.blocks) <= i {
		p.blocks = append(p.blocks, nil)
	}
	for len(p.owned) <= i {
		p.owned = append(p.owned, false)
	}
	if p.owned[i] {
		return p.blocks[i].b
	}
	b := new(block)
	var scratch []byte
	if old := p.get(i, &scratch); old != nil {
		copy(b[:], old)
	}
	p.blocks[i], p.owned[i] = &page{b: b}, true
	p.stored = min(p.stored, i)
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
	p.blocks[i], p.owned[i] = &page{b: b}, true
	p.stored = min(p.stored, i)
}

// read fills dst with the bytes at off, zeros past size.
func (p *pages) read(dst []byte, off int) {
	n := max(0, min(len(dst), p.size-off))
	clear(dst[n:])
	var scratch []byte
	for k := 0; k < n; {
		i, at := (off+k)/PageBytes, (off+k)%PageBytes
		m := min(n-k, PageBytes-at)
		if src := p.get(i, &scratch); src != nil {
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
	if at := size % PageBytes; at != 0 && size/PageBytes < len(p.blocks) && p.blocks[size/PageBytes] != nil {
		clear(p.own(size / PageBytes)[at:])
	}
	n := min(len(p.blocks), pageCount(size))
	clear(p.blocks[n:])
	p.blocks = p.blocks[:n]
	p.owned = p.owned[:min(len(p.owned), n)]
	p.stored = min(p.stored, n)
	p.size = size
}

// spill moves the pages of a file of at least the Store's size there,
// all but the last, which takes the next writes.
func (p *pages) spill() {
	st := p.st
	if st == nil || st.store == nil || st.err != nil || p.size < st.from {
		return
	}
	last := pageCount(p.size) - 1
	for i := p.stored; i < min(last, len(p.blocks)); i++ {
		pg := p.blocks[i]
		if pg == nil || pg.b == nil {
			continue
		}
		at, err := st.store.Put(pg, pg.b[:])
		if err != nil {
			st.fail(err)
			return
		}
		pg.b, pg.at = nil, at
		if i < len(p.owned) {
			p.owned[i] = false
		}
	}
	p.stored = max(p.stored, last)
}

// pageCount is how many pages size bytes take.
func pageCount(size int) int { return (size + PageBytes - 1) / PageBytes }
