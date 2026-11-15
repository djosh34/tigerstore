package wl

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"slices"
	"unsafe"
)

// The tree orders the keys for ranges: the live ones, and the deleted
// ones a read at the compacted revision or later may still see. A directory is a key prefix
// up to and including a '/'; it holds its keys and its subdirectories in
// one sorted array, each named by what follows the directory's path (a
// subdirectory's name ends in '/'), so walking the tree depth first gives
// the keys in byte order. Most names differ in their first 12 bytes,
// which each child keeps inline: a binary search seldom leaves the array.
// Kubernetes' keys are /registry/<resource>/<namespace>/<name>, so a
// directory holds one namespace of one resource, and a create or delete
// touches a small array.

type child struct {
	pfx [12]byte
	id  uint32 // key id, or dir id | dirBit
}

const (
	dirBit = 1 << 31
	noDir  = ^uint32(0)
)

type dirEnt struct {
	poff   uint64
	plen   uint32
	parent uint32
	// The children in order, in chunks of at most chunkMax: an insert or
	// a delete shifts one chunk, however large the directory.
	chunks [][]child
	// The watchers whose range covers every key of the directory, and
	// whether some watcher covers part of it, as of watch generation wgen.
	wgen  uint32
	wfull []int32
	wpart bool
}

type tree struct {
	st    *store
	dirs  []dirEnt
	table []uint64 // dir hash table: tag << 32 | dir id + 1
	mask  uint64
	seed  maphash.Seed
}

func (t *tree) init(st *store) {
	t.st = st
	t.table = make([]uint64, dirStart)
	t.mask = dirStart - 1
	t.seed = maphash.MakeSeed()
	t.dirs = append(t.dirs, dirEnt{parent: noDir})
	t.put(nil, 0)
}

func (t *tree) path(d uint32) []byte {
	e := &t.dirs[d]
	return t.st.karena.get(e.poff, e.plen)
}

func (t *tree) put(path []byte, d uint32) {
	if 2*len(t.dirs) > len(t.table) {
		table := make([]uint64, 2*len(t.table))
		rehash(t.table, table)
		t.table, t.mask = table, uint64(len(table)-1)
	}
	putEntry(t.table, maphash.Bytes(t.seed, path), d)
}

func (t *tree) lookup(path []byte) (uint32, bool) {
	h := maphash.Bytes(t.seed, path)
	for i := h & t.mask; ; i = (i + 1) & t.mask {
		e := t.table[i]
		if e == 0 {
			return 0, false
		}
		if entryHash(e) == uint32(h) {
			d := uint32(e) - 1
			if bytes.Equal(t.path(d), path) {
				return d, true
			}
		}
	}
}

// dirOf returns the directory path of a key.
func dirOf(key []byte) []byte {
	return key[:bytes.LastIndexByte(key, '/')+1]
}

// ensure returns the directory of path, making it and its parents.
func (t *tree) ensure(path []byte) (uint32, error) {
	if d, ok := t.lookup(path); ok {
		return d, nil
	}
	parent, err := t.ensure(dirOf(path[:len(path)-1]))
	if err != nil {
		return 0, err
	}
	poff, err := t.st.karena.add(path)
	if err != nil {
		return 0, err
	}
	d := uint32(len(t.dirs))
	t.dirs = append(t.dirs, dirEnt{poff: poff, plen: uint32(len(path)), parent: parent})
	t.put(path, d)
	pl := t.dirs[parent].plen
	t.insertChild(parent, path[pl:], d|dirBit)
	return d, nil
}

func pfxOf(name []byte) (p [12]byte) {
	copy(p[:], name)
	return p
}

func cmpPfx(a, b *[12]byte) int {
	x, y := binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])
	if x != y {
		if x < y {
			return -1
		}
		return 1
	}
	u, v := binary.BigEndian.Uint32(a[8:]), binary.BigEndian.Uint32(b[8:])
	if u != v {
		if u < v {
			return -1
		}
		return 1
	}
	return 0
}

// name is a child's name in directory d.
func (t *tree) name(d uint32, c child) []byte {
	pl := t.dirs[d].plen
	if c.id&dirBit != 0 {
		return t.path(c.id &^ dirBit)[pl:]
	}
	return t.st.key(c.id)[pl:]
}

// chunkMax is the most children a chunk holds before it splits.
const chunkMax = 2048

// searchIn returns the first child in cs whose name in directory d is not
// before name, and whether it equals it.
func (t *tree) searchIn(d uint32, cs []child, name []byte) (int, bool) {
	p := pfxOf(name)
	lo, hi := 0, len(cs)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		c := cmpPfx(&cs[m].pfx, &p)
		if c == 0 {
			c = bytes.Compare(t.name(d, cs[m]), name)
		}
		if c < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(cs) && bytes.Equal(t.name(d, cs[lo]), name)
}

// search returns the position, chunk and index, of the first child of d
// whose name is not before name, and whether it equals it. The index may
// be its chunk's length: the position after the chunk's last child.
func (t *tree) search(d uint32, name []byte) (int, int, bool) {
	chunks := t.dirs[d].chunks
	if len(chunks) == 0 {
		return 0, 0, false
	}
	// The last chunk whose first child is not after name.
	lo, hi := 1, len(chunks)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(t.name(d, chunks[m][0]), name) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	ci := lo - 1
	i, found := t.searchIn(d, chunks[ci], name)
	return ci, i, found
}

func (t *tree) insertChild(d uint32, name []byte, id uint32) {
	e := &t.dirs[d]
	if len(e.chunks) == 0 {
		e.chunks = [][]child{{{pfx: pfxOf(name), id: id}}}
		return
	}
	ci, i, _ := t.search(d, name)
	c := slices.Insert(e.chunks[ci], i, child{pfx: pfxOf(name), id: id})
	if len(c) <= chunkMax {
		e.chunks[ci] = c
		return
	}
	half := len(c) / 2
	right := append(make([]child, 0, chunkMax), c[half:]...)
	e.chunks[ci] = c[:half:chunkMax]
	e.chunks = slices.Insert(e.chunks, ci+1, right)
}

// insert adds key id, which is not in the tree.
func (t *tree) insert(id uint32) error {
	k := &t.st.keys[id]
	if k.dir == noDir {
		d, err := t.ensure(dirOf(t.st.key(id)))
		if err != nil {
			return err
		}
		k.dir = d
	}
	t.insertChild(k.dir, t.st.key(id)[t.dirs[k.dir].plen:], id)
	return nil
}

func (t *tree) remove(id uint32) {
	k := &t.st.keys[id]
	d := k.dir
	ci, i, ok := t.search(d, t.st.key(id)[t.dirs[d].plen:])
	if !ok {
		return
	}
	e := &t.dirs[d]
	e.chunks[ci] = slices.Delete(e.chunks[ci], i, i+1)
	if len(e.chunks[ci]) == 0 && len(e.chunks) > 1 {
		e.chunks = slices.Delete(e.chunks, ci, ci+1)
	}
}

// bytes estimates the tree's memory: its directories, their table, and a
// child per key.
func (t *tree) bytes() int64 {
	return int64(len(t.dirs))*int64(unsafe.Sizeof(dirEnt{})) + int64(len(t.table))*8 + int64(t.st.nkeys)*int64(unsafe.Sizeof(child{}))
}

// scan calls fn for each live key in [start, end) in order until fn
// returns false; a nil end has no bound.
func (t *tree) scan(start, end []byte, fn func(id uint32) bool) {
	t.visit(0, start, end, fn)
}

func (t *tree) visit(d uint32, start, end []byte, fn func(id uint32) bool) bool {
	e := &t.dirs[d]
	pl := int(e.plen)
	chunks := e.chunks
	ci, i := 0, 0
	seekC, seekI := -1, -1 // the child that holds start
	if start != nil && len(start) > pl && len(chunks) > 0 {
		rem := start[pl:]
		ci, i, _ = t.search(d, rem)
		// The child before the position: a directory there may hold start.
		pc, pi := ci, i-1
		if pi < 0 && pc > 0 {
			pc, pi = pc-1, len(chunks[pc-1])-1
		}
		if pi >= 0 {
			if c := chunks[pc][pi]; c.id&dirBit != 0 && bytes.HasPrefix(rem, t.name(d, c)) {
				ci, i, seekC, seekI = pc, pi, pc, pi
			}
		}
		if seekC < 0 {
			if i == len(chunks[ci]) && ci+1 < len(chunks) {
				ci, i = ci+1, 0
			}
			if i < len(chunks[ci]) {
				if c := chunks[ci][i]; c.id&dirBit != 0 && bytes.HasPrefix(rem, t.name(d, c)) {
					seekC, seekI = ci, i
				}
			}
		}
	}
	for ; ci < len(chunks); ci, i = ci+1, 0 {
		for ; i < len(chunks[ci]); i++ {
			c := chunks[ci][i]
			if c.id&dirBit != 0 {
				sub := c.id &^ dirBit
				if end != nil && bytes.Compare(t.path(sub), end) >= 0 {
					return false
				}
				var s []byte
				if ci == seekC && i == seekI {
					s = start
				}
				if !t.visit(sub, s, end, fn) {
					return false
				}
				continue
			}
			if end != nil && bytes.Compare(t.st.key(c.id), end) >= 0 {
				return false
			}
			// The chunks may change under fn only if fn writes, which no
			// reader does.
			if !fn(c.id) {
				return false
			}
		}
	}
	return true
}
