package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/google/btree"
)

// order keeps the store's keys in byte order, for ranges and walks. A
// directory is a key prefix that ends in '/'. Each directory holds its
// keys and its subdirectories by name: the rest of a key, or of a
// subdirectory's path, after the directory's own path; a subdirectory's
// name ends in '/'. So walking the directories depth first, each in the
// order of its names, gives the keys in byte order.
//
// One B-tree holds every directory's names, ordered by directory number
// and then by name, so each directory's names lie together. A name keeps
// its first namePrefix bytes inline, and most names of one directory
// differ in those: a comparison seldom reads a key's bytes. One B-tree of
// whole keys read two keys from the arena at every comparison, as
// Kubernetes' keys share their first 20 to 60 bytes
// (/registry/<resource>/<namespace>/): creating a key cost about 4 µs at
// 400k keys, 10% of each node's CPU on crd-heavy (#602).
type order struct {
	s *Store
	// dirs are the directories by number; the numbers of removed ones are
	// taken again (free). Directory 0 is the root, of path "".
	dirs   []orderDir
	free   []uint32
	byPath map[string]uint32
	kids   *btree.BTreeG[kid]
	// keys counts the keys, not the subdirectories, and paths the
	// bytes of the directories' paths.
	keys, paths int
	// longest is the longest path a directory had: a walk looks for the
	// directories of its start only in that many bytes of it, so a walk
	// from a key of megabytes does not scan the whole key.
	longest int
	// probe is the name that the kid probeKid stands for while a walk
	// looks for where to start.
	probe []byte
	// last is the directory found last, which the next write's key is
	// often in: its lookup skips the map (find).
	last uint32
}

// orderDir is a directory: its path, the directory that holds it, and
// how many names it holds.
type orderDir struct {
	path   []byte
	parent uint32
	n      uint32
}

// kid is a key or a subdirectory of directory dir: the first bytes of its
// name, the name's length, and the key's slot (Store.rec) or dirKid with
// the subdirectory's number. It holds no pointer.
type kid struct {
	dir uint32
	pfx [namePrefix]byte
	n   uint32
	id  uint32
}

const (
	namePrefix = 12
	dirKid     = 1 << 31
	// probeKid stands for order.probe.
	probeKid = ^uint32(0)
	// kidDegree is the B-tree's degree: a node holds up to 2*kidDegree-1
	// names of 24 bytes.
	kidDegree = 32
)

func newOrder(s *Store) *order {
	o := &order{s: s, byPath: map[string]uint32{}}
	o.kids = btree.NewG(kidDegree, o.less)
	o.newDir(nil, 0)
	return o
}

// Len returns the number of keys.
func (o *order) Len() int { return o.keys }

// bytes estimates what the order takes in memory: per key and directory a
// name in the B-tree, and per directory its path and its entry in dirs and
// byPath.
func (o *order) bytes() int64 {
	dirs := len(o.dirs) - len(o.free)
	return int64(o.keys+dirs)*kidSlot + 2*int64(o.paths) + int64(dirs)*dirSlot
}

// kidSlot estimates a name's room in the B-tree, in nodes about two thirds
// full, and dirSlot a directory's room in dirs and byPath besides its
// path's two copies, its own and byPath's key.
const (
	kidSlot = 36
	dirSlot = 80
)

func (o *order) newDir(path []byte, parent uint32) uint32 {
	var d uint32
	if n := len(o.free); n > 0 {
		d, o.free = o.free[n-1], o.free[:n-1]
	} else {
		d = uint32(len(o.dirs))
		o.dirs = append(o.dirs, orderDir{})
	}
	o.dirs[d] = orderDir{path: path, parent: parent}
	o.byPath[string(path)] = d
	o.paths += len(path)
	o.longest = max(o.longest, len(path))
	return d
}

// dirOf returns the directory part of a key or path: up to and including
// its last '/', "" if it has none.
func dirOf(key []byte) []byte { return key[:bytes.LastIndexByte(key, '/')+1] }

// name returns the name of k.
func (o *order) name(k kid) []byte {
	skip := len(o.dirs[k.dir].path)
	switch {
	case k.id == probeKid:
		return o.probe
	case k.id&dirKid != 0:
		return o.dirs[k.id&^dirKid].path[skip:]
	default:
		return o.s.key(o.s.rec(k.id))[skip:]
	}
}

func (o *order) less(a, b kid) bool {
	if a.dir != b.dir {
		return a.dir < b.dir
	}
	x, y := binary.BigEndian.Uint64(a.pfx[:]), binary.BigEndian.Uint64(b.pfx[:])
	if x == y {
		x, y = uint64(binary.BigEndian.Uint32(a.pfx[8:])), uint64(binary.BigEndian.Uint32(b.pfx[8:]))
	}
	if x != y {
		return x < y
	}
	if a.n <= namePrefix || b.n <= namePrefix {
		// One name ends within the prefix, padded with zeros: the
		// shorter one comes first.
		return a.n < b.n
	}
	return bytes.Compare(o.name(a), o.name(b)) < 0
}

func kidOf(dir uint32, name []byte, id uint32) kid {
	k := kid{dir: dir, n: uint32(len(name)), id: id}
	copy(k.pfx[:], name)
	return k
}

// find returns the number of the directory of path, if there is one.
func (o *order) find(path []byte) (uint32, bool) {
	if d := o.last; d < uint32(len(o.dirs)) && bytes.Equal(o.dirs[d].path, path) && (d == 0 || o.dirs[d].path != nil) {
		return d, true
	}
	d, ok := o.byPath[string(path)]
	if ok {
		o.last = d
	}
	return d, ok
}

// dir returns the number of the directory of path, making it and the
// directories above it if they are not there.
func (o *order) dir(path []byte) uint32 {
	if d, ok := o.find(path); ok {
		return d
	}
	parent := o.dir(dirOf(path[:len(path)-1]))
	d := o.newDir(bytes.Clone(path), parent)
	o.add(kidOf(parent, path[len(o.dirs[parent].path):], d|dirKid))
	return d
}

func (o *order) add(k kid) {
	o.kids.ReplaceOrInsert(k)
	o.dirs[k.dir].n++
}

// Insert adds the key of slot id, which the order does not hold.
func (o *order) Insert(id uint32) {
	key := o.s.key(o.s.rec(id))
	path := dirOf(key)
	d := o.dir(path)
	o.add(kidOf(d, key[len(path):], id))
	o.keys++
}

// Delete removes the key of slot id, whose bytes are still in the
// arena, and the directories it leaves empty.
func (o *order) Delete(id uint32) {
	key := o.s.key(o.s.rec(id))
	path := dirOf(key)
	d, ok := o.find(path)
	if !ok {
		return
	}
	if _, ok := o.kids.Delete(kidOf(d, key[len(path):], id)); !ok {
		return
	}
	o.keys--
	o.dirs[d].n--
	for d != 0 && o.dirs[d].n == 0 {
		dir := o.dirs[d]
		o.kids.Delete(kidOf(dir.parent, dir.path[len(o.dirs[dir.parent].path):], d|dirKid))
		o.dirs[dir.parent].n--
		delete(o.byPath, string(dir.path))
		o.paths -= len(dir.path)
		o.dirs[d] = orderDir{}
		o.free = append(o.free, d)
		d = dir.parent
	}
}

// walk calls fn for the slot of every key from start on, and before end
// if end is not nil, in order, until fn returns false. fn may walk too,
// but must not change the order.
func (o *order) walk(start, end []byte, fn func(id uint32) bool) {
	// The deepest directory whose path is a prefix of start: no
	// subdirectory of it is a prefix of start, so each of its kids holds
	// either only keys before start or only keys from it on. Every
	// directory's parents are there, so it is found one '/' at a time.
	d := uint32(0)
	in := start[:min(len(start), o.longest)]
	for {
		at := len(o.dirs[d].path)
		i := bytes.IndexByte(in[at:], '/')
		if i < 0 {
			break
		}
		sub, ok := o.byPath[string(in[:at+i+1])]
		if !ok {
			break
		}
		d = sub
	}
	old := o.probe
	o.probe = start[len(o.dirs[d].path):]
	more := o.ascend(kidOf(d, o.probe, probeKid), false, end, fn)
	o.probe = old
	// Then what follows each directory in the ones above it.
	for more && d != 0 {
		dir := o.dirs[d]
		more = o.ascend(kidOf(dir.parent, dir.path[len(o.dirs[dir.parent].path):], d|dirKid), true, end, fn)
		d = dir.parent
	}
}

// ascend calls fn for the keys of from's directory and its
// subdirectories, from kid from on, or after it if after is set, in
// order, while they are before end; it reports whether fn wants more.
func (o *order) ascend(from kid, after bool, end []byte, fn func(id uint32) bool) bool {
	more := true
	o.kids.AscendGreaterOrEqual(from, func(k kid) bool {
		if k.dir != from.dir {
			return false
		}
		if after && k.id == from.id {
			return true
		}
		more = o.visit(k, end, fn)
		return more
	})
	return more
}

// visit calls fn for key k, or for every key of subdirectory k, while
// they are before end; it reports whether fn wants more.
func (o *order) visit(k kid, end []byte, fn func(id uint32) bool) bool {
	if k.id&dirKid != 0 {
		return o.ascend(kid{dir: k.id &^ dirKid}, false, end, fn)
	}
	if end != nil && bytes.Compare(o.s.key(o.s.rec(k.id)), end) >= 0 {
		return false
	}
	return fn(k.id)
}
