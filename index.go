// Question: at 10M Kubernetes-shaped keys, is a prefix-compressed arena B+tree
// worth its complexity versus google/btree and a sorted slice with a write buffer?
// Throwaway experiment, not production storage.
package main

import (
	"github.com/google/btree"
	"sort"
	"strings"
)

const dead uint32 = 1 << 31

func alive(h uint32) uint64 {
	if h != 0 && h&dead == 0 {
		return 1
	}
	return 0
}

type item struct {
	key  string
	head uint32
}
type index interface {
	Get(string) uint32
	Put(string, uint32)
	Walk(string, string, func(uint32) bool)
	Count(string, string) uint64
}

func lower(a []item, k string) int {
	return sort.Search(len(a), func(i int) bool { return a[i].key >= k })
}

type stock struct{ t *btree.BTreeG[item] }

func newStock() *stock {
	return &stock{btree.NewG[item](32, func(a, b item) bool { return a.key < b.key })}
}
func (s *stock) Get(k string) uint32 {
	v, ok := s.t.Get(item{key: k})
	if !ok {
		return 0
	}
	return v.head
}
func (s *stock) Put(k string, h uint32) { s.t.ReplaceOrInsert(item{k, h}) }
func (s *stock) Walk(a, b string, f func(uint32) bool) {
	s.t.AscendRange(item{key: a}, item{key: b}, func(v item) bool { return f(v.head) })
}
func (s *stock) Count(a, b string) (n uint64) {
	s.Walk(a, b, func(h uint32) bool { n += alive(h); return true })
	return
}

// The slice buffer is a map. Range materializes a sorted buffer on demand.
// Flush sorts and merges the complete base. No flush work is deferred past Put.
// A prefix-live-count array supports current counts even with tombstones.
const bufferSize = 65536

type buffered struct {
	base       []item
	prefix     []uint32
	buf        map[string]uint32
	ordered    []item
	dirty      bool
	flushes    int
	maxFlushNS int64
}

func newBuffered(a []item) *buffered {
	s := &buffered{base: a, buf: make(map[string]uint32)}
	s.recount()
	return s
}
func (s *buffered) recount() {
	s.prefix = make([]uint32, len(s.base)+1)
	for i, v := range s.base {
		s.prefix[i+1] = s.prefix[i] + uint32(alive(v.head))
	}
}
func (s *buffered) baseGet(k string) uint32 {
	p := lower(s.base, k)
	if p < len(s.base) && s.base[p].key == k {
		return s.base[p].head
	}
	return 0
}
func (s *buffered) Get(k string) uint32 {
	if h, ok := s.buf[k]; ok {
		return h
	}
	return s.baseGet(k)
}
func (s *buffered) Put(k string, h uint32) {
	s.buf[k] = h
	s.dirty = true
	if len(s.buf) >= bufferSize {
		s.flush()
	}
}
func (s *buffered) sorted() {
	if !s.dirty {
		return
	}
	s.ordered = s.ordered[:0]
	for k, h := range s.buf {
		s.ordered = append(s.ordered, item{k, h})
	}
	sort.Slice(s.ordered, func(i, j int) bool { return s.ordered[i].key < s.ordered[j].key })
	s.dirty = false
}
func (s *buffered) merge(f func(item) bool, a, b string) {
	s.sorted()
	i, j := lower(s.base, a), lower(s.ordered, a)
	for i < len(s.base) || j < len(s.ordered) {
		var v item
		if j == len(s.ordered) || (i < len(s.base) && s.base[i].key < s.ordered[j].key) {
			v = s.base[i]
			i++
		} else {
			v = s.ordered[j]
			j++
			if i < len(s.base) && s.base[i].key == v.key {
				i++
			}
		}
		if v.key >= b || !f(v) {
			return
		}
	}
}
func (s *buffered) Walk(a, b string, f func(uint32) bool) {
	s.merge(func(v item) bool { return f(v.head) }, a, b)
}
func (s *buffered) Count(a, b string) uint64 {
	s.sorted()
	n := int64(s.prefix[lower(s.base, b)] - s.prefix[lower(s.base, a)])
	for i := lower(s.ordered, a); i < len(s.ordered) && s.ordered[i].key < b; i++ {
		v := s.ordered[i]
		n += int64(alive(v.head)) - int64(alive(s.baseGet(v.key)))
	}
	return uint64(n)
}
func (s *buffered) doFlush() {
	if len(s.buf) == 0 {
		return
	}
	s.sorted()
	a := make([]item, 0, len(s.base)+len(s.buf))
	s.merge(func(v item) bool { a = append(a, v); return true }, "", strings.Repeat("\xff", 100))
	s.base = a
	clear(s.buf)
	s.ordered = s.ordered[:0]
	s.dirty = false
	s.recount()
	s.flushes++
}
