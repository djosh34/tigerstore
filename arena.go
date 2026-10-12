package main

// Direct comparisons let the compiler borrow byte slices as strings.
func compareBytesString(a []byte, b string) int {
	if string(a) == b {
		return 0
	}
	if string(a) < b {
		return -1
	}
	return 1
}

const (
	leafSlots  = 128
	leafData   = 2944
	fanout     = 64
	innerBit   = uint32(1 << 31)
	slabLeaves = 256
	maxKey     = 96
)

type slot struct {
	head   uint32
	off, n uint16
}
type leaf struct {
	n, plen, used, pad uint16
	next               uint32
	prefix             [maxKey]byte
	slots              [leafSlots]slot
	data               [leafData]byte
}
type edge struct {
	count uint64
	child uint32
	n     uint16
	key   [maxKey]byte
}
type inner struct {
	n uint16
	e [fanout]edge
}
type packedKey struct {
	key  [maxKey]byte
	n    uint16
	head uint32
}
type arena struct {
	slabs  [][]leaf
	leaves uint32
	nodes  []inner
	root   uint32
}

func newArena() *arena { a := &arena{}; a.root = a.newLeaf(); return a }
func (a *arena) newLeaf() uint32 {
	if a.leaves%slabLeaves == 0 {
		a.slabs = append(a.slabs, make([]leaf, slabLeaves))
	}
	a.leaves++
	return a.leaves
}
func (a *arena) lp(id uint32) *leaf  { v := id - 1; return &a.slabs[v/slabLeaves][v%slabLeaves] }
func (a *arena) ip(id uint32) *inner { return &a.nodes[(id&^innerBit)-1] }
func (a *arena) newInner() uint32 {
	a.nodes = append(a.nodes, inner{})
	return uint32(len(a.nodes)) | innerBit
}
func (l *leaf) search(k string) int {
	p := int(l.plen)
	m := min(p, len(k))
	c := compareBytesString(l.prefix[:m], k[:m])
	if c > 0 || c == 0 && len(k) < p {
		return 0
	}
	if c < 0 {
		return int(l.n)
	}
	suffix := k[p:]
	lo, hi := 0, int(l.n)
	for lo < hi {
		mid := (lo + hi) / 2
		s := l.slots[mid]
		if string(l.data[s.off:s.off+s.n]) < suffix {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
func (l *leaf) equal(i int, k string) bool {
	if i >= int(l.n) {
		return false
	}
	s := l.slots[i]
	return len(k) == int(l.plen+s.n) && string(l.prefix[:l.plen]) == k[:l.plen] && string(l.data[s.off:s.off+s.n]) == k[l.plen:]
}
func (l *leaf) less(i int, k string) bool {
	p := int(l.plen)
	m := min(p, len(k))
	c := compareBytesString(l.prefix[:m], k[:m])
	if c != 0 {
		return c < 0
	}
	if len(k) < p {
		return false
	}
	s := l.slots[i]
	return string(l.data[s.off:s.off+s.n]) < k[p:]
}
func (l *leaf) unpack(i int) packedKey {
	var v packedKey
	s := l.slots[i]
	v.n = l.plen + s.n
	v.head = s.head
	copy(v.key[:], l.prefix[:l.plen])
	copy(v.key[l.plen:], l.data[s.off:s.off+s.n])
	return v
}
func (l *leaf) count() (n uint64) {
	for i := 0; i < int(l.n); i++ {
		n += alive(l.slots[i].head)
	}
	return
}
func nodeSearch(n *inner, k string) int {
	lo, hi := 0, int(n.n)
	for lo < hi {
		m := (lo + hi) / 2
		if string(n.e[m].key[:n.e[m].n]) <= k {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return max(0, lo-1)
}
func (a *arena) find(k string) (uint32, int) {
	id := a.root
	for id&innerBit != 0 {
		n := a.ip(id)
		id = n.e[nodeSearch(n, k)].child
	}
	return id, a.lp(id).search(k)
}
func (a *arena) Get(k string) uint32 {
	id, i := a.find(k)
	l := a.lp(id)
	if !l.equal(i, k) {
		return 0
	}
	return l.slots[i].head
}
func common(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
func (l *leaf) pack(v []packedKey) {
	l.n = uint16(len(v))
	l.used = 0
	if len(v) == 0 {
		l.plen = 0
		return
	}
	p := common(v[0].key[:v[0].n], v[len(v)-1].key[:v[len(v)-1].n])
	l.plen = uint16(p)
	copy(l.prefix[:], v[0].key[:p])
	for i := range v {
		suffix := v[i].key[p:v[i].n]
		if int(l.used)+len(suffix) > leafData {
			panic("leaf pack overflow")
		}
		l.slots[i] = slot{v[i].head, l.used, uint16(len(suffix))}
		copy(l.data[l.used:], suffix)
		l.used += uint16(len(suffix))
	}
}
func fits(v []packedKey) bool {
	if len(v) > leafSlots {
		return false
	}
	if len(v) == 0 {
		return true
	}
	p := common(v[0].key[:v[0].n], v[len(v)-1].key[:v[len(v)-1].n])
	n := 0
	for i := range v {
		n += int(v[i].n) - p
	}
	return n <= leafData
}
func (a *arena) edge(id uint32) edge {
	var e edge
	e.child = id
	if id&innerBit == 0 {
		l := a.lp(id)
		v := l.unpack(0)
		e.key = v.key
		e.n = v.n
		e.count = l.count()
	} else {
		n := a.ip(id)
		e.key = n.e[0].key
		e.n = n.e[0].n
		for i := 0; i < int(n.n); i++ {
			e.count += n.e[i].count
		}
	}
	return e
}
func (a *arena) Put(k string, h uint32) {
	if len(k) > maxKey {
		panic("key too long for experiment")
	}
	right, _ := a.put(a.root, k, h)
	if right != 0 {
		old := a.root
		r := a.newInner()
		n := a.ip(r)
		n.n = 2
		n.e[0] = a.edge(old)
		n.e[1] = a.edge(right)
		a.root = r
	}
}
func (a *arena) put(id uint32, k string, h uint32) (uint32, int64) {
	if id&innerBit == 0 {
		l := a.lp(id)
		p := l.search(k)
		if l.equal(p, k) {
			old := alive(l.slots[p].head)
			l.slots[p].head = h
			return 0, int64(alive(h)) - int64(old)
		}
		// Preserve the existing common prefix and append one suffix where possible.
		plen := int(l.plen)
		if l.n > 0 && int(l.n) < leafSlots && len(k) >= plen && k[:plen] == string(l.prefix[:plen]) && int(l.used)+len(k)-plen <= leafData {
			copy(l.slots[p+1:], l.slots[p:l.n])
			l.slots[p] = slot{h, l.used, uint16(len(k) - plen)}
			copy(l.data[l.used:], k[plen:])
			l.used += uint16(len(k) - plen)
			l.n++
			return 0, int64(alive(h))
		}
		var v [leafSlots + 1]packedKey
		for i := 0; i < p; i++ {
			v[i] = l.unpack(i)
		}
		v[p].n = uint16(len(k))
		v[p].head = h
		copy(v[p].key[:], k)
		for i := p; i < int(l.n); i++ {
			v[i+1] = l.unpack(i)
		}
		n := int(l.n) + 1
		if fits(v[:n]) {
			l.pack(v[:n])
			return 0, int64(alive(h))
		}
		right := a.newLeaf()
		r := a.lp(right)
		r.next = l.next
		l.next = right
		m := n / 2
		if !fits(v[:m]) || !fits(v[m:n]) {
			for candidate := 1; candidate < n; candidate++ {
				if fits(v[:candidate]) && fits(v[candidate:n]) {
					m = candidate
					break
				}
			}
		}
		l.pack(v[:m])
		r.pack(v[m:n])
		return right, int64(alive(h))
	}
	n := a.ip(id)
	p := nodeSearch(n, k)
	child := n.e[p].child
	right, delta := a.put(child, k, h)
	n = a.ip(id) // recursive append may move the inner arena
	if right == 0 {
		n.e[p].count = uint64(int64(n.e[p].count) + delta)
		if k < string(n.e[p].key[:n.e[p].n]) {
			n.e[p].n = uint16(len(k))
			copy(n.e[p].key[:], k)
		}
		return 0, delta
	}
	n.e[p] = a.edge(child)
	e := a.edge(right)
	if int(n.n) < fanout {
		copy(n.e[p+2:], n.e[p+1:n.n])
		n.e[p+1] = e
		n.n++
		return 0, delta
	}
	var es [fanout + 1]edge
	copy(es[:], n.e[:p+1])
	es[p+1] = e
	copy(es[p+2:], n.e[p+1:])
	rID := a.newInner()
	n = a.ip(id)
	r := a.ip(rID)
	m := (fanout + 1) / 2
	n.n = uint16(m)
	copy(n.e[:], es[:m])
	r.n = uint16(fanout + 1 - m)
	copy(r.e[:], es[m:])
	return rID, delta
}
func (a *arena) rank(k string) (v uint64) {
	id := a.root
	for id&innerBit != 0 {
		n := a.ip(id)
		p := nodeSearch(n, k)
		for i := 0; i < p; i++ {
			v += n.e[i].count
		}
		id = n.e[p].child
	}
	l := a.lp(id)
	p := l.search(k)
	for i := 0; i < p; i++ {
		v += alive(l.slots[i].head)
	}
	return
}
func (a *arena) Count(start, end string) uint64 { return a.rank(end) - a.rank(start) }
func (a *arena) Walk(start, end string, f func(uint32) bool) {
	id, p := a.find(start)
	for id != 0 {
		l := a.lp(id)
		for ; p < int(l.n); p++ {
			if !l.less(p, end) || !f(l.slots[p].head) {
				return
			}
		}
		id = l.next
		p = 0
	}
}

func (a *arena) Page(start, end string, out []result) (n int) {
	id, p := a.find(start)
	for id != 0 {
		l := a.lp(id)
		for ; p < int(l.n); p++ {
			if !l.less(p, end) {
				return
			}
			s := l.slots[p]
			if alive(s.head) != 0 {
				copy(out[n].key[:], l.prefix[:l.plen])
				copy(out[n].key[l.plen:], l.data[s.off:s.off+s.n])
				out[n].n = l.plen + s.n
				out[n].head = s.head
				n++
				if n == len(out) {
					return
				}
			}
		}
		id = l.next
		p = 0
	}
	return
}

// Bulk load to 75% slot occupancy. Real inserts, prefix changes and splits use Put.
func (a *arena) bulk(n int, key func(int) string, head func(int) uint32) {
	a.slabs = nil
	a.leaves = 0
	a.nodes = nil
	ids := make([]uint32, 0, (n+95)/96)
	var v [96]packedKey
	for i := 0; i < n; {
		end := min(i+96, n)
		for j := i; j < end; j++ {
			k := key(j)
			v[j-i] = packedKey{n: uint16(len(k)), head: head(j)}
			copy(v[j-i].key[:], k)
		}
		for !fits(v[:end-i]) {
			end--
		}
		id := a.newLeaf()
		a.lp(id).pack(v[:end-i])
		if len(ids) > 0 {
			a.lp(ids[len(ids)-1]).next = id
		}
		ids = append(ids, id)
		i = end
	}
	for len(ids) > 1 {
		next := make([]uint32, 0, (len(ids)+47)/48)
		for i := 0; i < len(ids); i += 48 {
			id := a.newInner()
			n := a.ip(id)
			end := min(i+48, len(ids))
			n.n = uint16(end - i)
			for j := i; j < end; j++ {
				n.e[j-i] = a.edge(ids[j])
			}
			next = append(next, id)
		}
		ids = next
	}
	a.root = ids[0]
}
