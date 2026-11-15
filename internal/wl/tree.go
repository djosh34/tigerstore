package wl

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"slices"
)

// The tree orders the live keys for ranges. A directory is a key prefix
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
	poff     uint64
	plen     uint32
	parent   uint32
	children []child
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
	t.table = make([]uint64, 1<<dirSlotLog)
	t.mask = 1<<dirSlotLog - 1
	t.seed = maphash.MakeSeed()
	t.dirs = append(t.dirs, dirEnt{parent: noDir})
	t.put(nil, 0)
}

func (t *tree) path(d uint32) []byte {
	e := &t.dirs[d]
	return t.st.karena.get(e.poff, e.plen)
}

func (t *tree) put(path []byte, d uint32) {
	h := maphash.Bytes(t.seed, path)
	for i := h & t.mask; ; i = (i + 1) & t.mask {
		if t.table[i] == 0 {
			t.table[i] = h>>32<<32 | uint64(d+1)
			return
		}
	}
}

func (t *tree) lookup(path []byte) (uint32, bool) {
	h := maphash.Bytes(t.seed, path)
	tag := h >> 32
	for i := h & t.mask; ; i = (i + 1) & t.mask {
		e := t.table[i]
		if e == 0 {
			return 0, false
		}
		if e>>32 == tag {
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
func (t *tree) ensure(path []byte) uint32 {
	if d, ok := t.lookup(path); ok {
		return d
	}
	parent := t.ensure(dirOf(path[:len(path)-1]))
	d := uint32(len(t.dirs))
	t.dirs = append(t.dirs, dirEnt{poff: t.st.karena.add(path), plen: uint32(len(path)), parent: parent})
	t.put(path, d)
	pl := t.dirs[parent].plen
	t.insertChild(parent, path[pl:], d|dirBit)
	return d
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

// search returns the first child of d whose name is not before name, and
// whether it equals it.
func (t *tree) search(d uint32, name []byte) (int, bool) {
	cs := t.dirs[d].children
	p := pfxOf(name)
	lo, hi := 0, len(cs)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		c := cmpPfx(&cs[m].pfx, &p)
		if c == 0 && (len(name) > 12 || len(t.name(d, cs[m])) > 12) {
			c = bytes.Compare(t.name(d, cs[m]), name)
		} else if c == 0 {
			c = bytes.Compare(t.name(d, cs[m]), name)
		}
		if c < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	found := lo < len(cs) && bytes.Equal(t.name(d, cs[lo]), name)
	return lo, found
}

func (t *tree) insertChild(d uint32, name []byte, id uint32) {
	i, _ := t.search(d, name)
	e := &t.dirs[d]
	e.children = slices.Insert(e.children, i, child{pfx: pfxOf(name), id: id})
}

// insert adds key id, which is not in the tree.
func (t *tree) insert(id uint32) {
	k := &t.st.keys[id]
	if k.dir == noDir {
		k.dir = t.ensure(dirOf(t.st.key(id)))
	}
	t.insertChild(k.dir, t.st.key(id)[t.dirs[k.dir].plen:], id)
}

func (t *tree) remove(id uint32) {
	k := &t.st.keys[id]
	d := k.dir
	i, ok := t.search(d, t.st.key(id)[t.dirs[d].plen:])
	if !ok {
		return
	}
	e := &t.dirs[d]
	e.children = slices.Delete(e.children, i, i+1)
}

// scan calls fn for each live key in [start, end) in order until fn
// returns false; a nil end has no bound.
func (t *tree) scan(start, end []byte, fn func(id uint32) bool) {
	t.visit(0, start, end, fn)
}

func (t *tree) visit(d uint32, start, end []byte, fn func(id uint32) bool) bool {
	e := &t.dirs[d]
	pl := int(e.plen)
	cs := e.children
	i := 0
	seek := -1 // the child that holds start
	if start != nil && len(start) > pl {
		rem := start[pl:]
		i, _ = t.search(d, rem)
		if i > 0 {
			c := cs[i-1]
			if c.id&dirBit != 0 && bytes.HasPrefix(rem, t.name(d, c)) {
				i--
				seek = i
			}
		}
		if seek < 0 && i < len(cs) && cs[i].id&dirBit != 0 && bytes.HasPrefix(rem, t.name(d, cs[i])) {
			seek = i
		}
	}
	for ; i < len(cs); i++ {
		c := cs[i]
		if c.id&dirBit != 0 {
			sub := c.id &^ dirBit
			if end != nil && bytes.Compare(t.path(sub), end) >= 0 {
				return false
			}
			var s []byte
			if i == seek {
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
		if !fn(c.id) {
			return false
		}
		// The array may have changed under fn only if fn writes, which
		// no reader does.
	}
	return true
}
