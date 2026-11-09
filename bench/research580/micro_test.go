// Package research580 measures, on the bench runner, the primitives of a
// pointer-free, preallocated, wire-form store (research #580): keys and
// already-encoded KeyValue records in one arena, a hash index for point
// reads, a sorted index for ranges, replies gathered straight into TLS
// record buffers and sealed in place. Nothing here is production code.
package research580

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"math/rand"
	rv2 "math/rand/v2"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/k8ssize"
	"github.com/google/btree"
)

const (
	record   = 16 << 10 // TLS record plaintext
	groupLen = 300      // keys per LIST prefix, about the bench's mean page
)

type store struct {
	arena   []byte   // all keys and wire-form records, no pointers inside
	keyOff  []uint64 // key i at arena[keyOff[i]:keyOff[i]+keyLen[i]]
	keyLen  []uint16
	recOff  []uint64 // wire-form "field 2, len, KeyValue" of key i
	recLen  []uint32
	modRev  []int64
	slots   []uint64 // open addressing: 16-bit tag << 32 | (i+1)
	mask    uint64
	sorted  []uint32 // key order
	groups  [][2]int // [start, end) in sorted per LIST prefix
	seed    maphash.Seed
	tail    uint64 // arena append point for writes
	meanVal float64
	contig  bool
}

var st *store

// resourceCount is the object count per resource of the k8ssize dump
// (research #577's run 42694258036 printed it), so resources are drawn as
// the bench draws them.
var resourceCount = map[string]int{"apiservices": 22, "certificatesigningrequests": 1, "clusterrolebindings": 64, "clusterroles": 77, "configmaps": 385, "controllerrevisions": 2, "csinodes": 1, "daemonsets": 2, "deployments": 122, "endpoints": 122, "endpointslices": 362, "events": 3895, "flowschemas": 11, "leases": 244, "minions": 1, "namespaces": 17, "other": 127, "pods": 129, "priorityclasses": 2, "prioritylevelconfigurations": 8, "replicasets": 422, "rolebindings": 12, "roles": 12, "secrets": 121, "serviceaccounts": 61, "services": 122, "storageclasses": 1}

func n() int {
	if s := os.Getenv("R580_KEYS"); s != "" {
		v, _ := strconv.Atoi(s)
		return v
	}
	return 1_000_000
}

func appendKV(b, key, val []byte, create, mod, ver int64) []byte {
	kvLen := 1 + uvlen(len(key)) + len(key) + 1 + uvlen(int(create)) + 1 + uvlen(int(mod)) + 1 + uvlen(int(ver)) + 1 + uvlen(len(val)) + len(val)
	b = append(b, 0x12)
	b = binary.AppendUvarint(b, uint64(kvLen))
	b = append(b, 0x0a)
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = append(b, key...)
	b = append(b, 0x10)
	b = binary.AppendUvarint(b, uint64(create))
	b = append(b, 0x18)
	b = binary.AppendUvarint(b, uint64(mod))
	b = append(b, 0x20)
	b = binary.AppendUvarint(b, uint64(ver))
	b = append(b, 0x2a)
	b = binary.AppendUvarint(b, uint64(len(val)))
	return append(b, val...)
}

func uvlen(v int) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func (s *store) key(i uint32) []byte {
	return s.arena[s.keyOff[i] : s.keyOff[i]+uint64(s.keyLen[i])]
}

func (s *store) find(k []byte) (uint32, bool) {
	h := maphash.Bytes(s.seed, k)
	tag := h >> 48
	for p := h & s.mask; ; p = (p + 1) & s.mask {
		e := s.slots[p]
		if e == 0 {
			return 0, false
		}
		if e>>32 == tag {
			i := uint32(e) - 1
			if bytes.Equal(s.key(i), k) {
				return i, true
			}
		}
	}
}

// build lays the records out in arrival order (shuffled against key
// order), as a store that appends writes does, unless contiguous.
func build(keys int, contiguous bool) *store {
	r := rand.New(rand.NewSource(1))
	r2 := rv2.New(rv2.NewPCG(1, 2))
	res := k8ssize.Resources()
	s := &store{seed: maphash.MakeSeed()}
	names := make([][]byte, keys)
	vals := make([]int, keys)
	var total, vtotal int
	var weighted []string
	for _, rs := range res {
		for range resourceCount[rs] {
			weighted = append(weighted, rs)
		}
	}
	for g := 0; g*groupLen < keys; g++ {
		rs := weighted[r.Intn(len(weighted))]
		for j := 0; j < groupLen && g*groupLen+j < keys; j++ {
			i := g*groupLen + j
			names[i] = fmt.Appendf(nil, "/registry/%s/bench-%d-ns%04d/object-%07d", rs, g%6, g, i)
			vals[i] = k8ssize.Buckets[k8ssize.Draw(r2, rs)].Size
			total += 2*len(names[i]) + vals[i] + 48
			vtotal += vals[i]
		}
	}
	s.meanVal = float64(vtotal) / float64(keys)
	s.arena = make([]byte, 0, total+total/2+(64<<20))
	s.keyOff = make([]uint64, keys)
	s.keyLen = make([]uint16, keys)
	s.recOff = make([]uint64, keys)
	s.recLen = make([]uint32, keys)
	s.modRev = make([]int64, keys)
	order := r.Perm(keys)
	if contiguous {
		for i := range order {
			order[i] = i
		}
	}
	val := make([]byte, 64<<10)
	r.Read(val)
	for _, i := range order {
		s.keyOff[i] = uint64(len(s.arena))
		s.keyLen[i] = uint16(len(names[i]))
		s.arena = append(s.arena, names[i]...)
		s.modRev[i] = int64(i + 1)
		s.recOff[i] = uint64(len(s.arena))
		s.arena = appendKV(s.arena, names[i], val[:vals[i]], int64(i+1), int64(i+1), 1)
		s.recLen[i] = uint32(uint64(len(s.arena)) - s.recOff[i])
	}
	s.tail = uint64(len(s.arena))
	size := 1
	for size < 2*keys {
		size <<= 1
	}
	s.slots = make([]uint64, size)
	s.mask = uint64(size - 1)
	for i := 0; i < keys; i++ {
		h := maphash.Bytes(s.seed, names[i])
		p := h & s.mask
		for s.slots[p] != 0 {
			p = (p + 1) & s.mask
		}
		s.slots[p] = h>>48<<32 | uint64(i+1)
	}
	s.sorted = make([]uint32, keys)
	for i := range s.sorted {
		s.sorted[i] = uint32(i)
	}
	sort.Slice(s.sorted, func(a, b int) bool { return bytes.Compare(names[s.sorted[a]], names[s.sorted[b]]) < 0 })
	for g := 0; g*groupLen < keys; g++ {
		pre := names[g*groupLen][:bytes.LastIndexByte(names[g*groupLen], '/')+1]
		lo := sort.Search(keys, func(x int) bool { return bytes.Compare(names[s.sorted[x]], pre) >= 0 })
		hi := lo
		for hi < keys && bytes.HasPrefix(names[s.sorted[hi]], pre) {
			hi++
		}
		s.groups = append(s.groups, [2]int{lo, hi})
	}
	return s
}

func get(b *testing.B, contiguous bool) *store {
	if st == nil || st.contig != contiguous {
		st = nil
		st = build(n(), contiguous)
		st.contig = contiguous
		b.Logf("keys=%d meanValue=%.0f arena=%d MiB", n(), st.meanVal, len(st.arena)>>20)
	}
	return st
}

func newSealer() (cipher.AEAD, []byte) {
	blk, _ := aes.NewCipher(make([]byte, 16))
	g, _ := cipher.NewGCM(blk)
	return g, make([]byte, 12)
}

// BenchmarkIndexHashGet is a point lookup in the pointer-free hash index.
func BenchmarkIndexHashGet(b *testing.B) {
	s := get(b, false)
	r := rand.New(rand.NewSource(2))
	keys := len(s.keyOff)
	probe := make([][]byte, 1<<16)
	for i := range probe {
		probe[i] = bytes.Clone(s.key(uint32(r.Intn(keys))))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.find(probe[i&(len(probe)-1)]); !ok {
			b.Fatal("missing")
		}
	}
}

// BenchmarkIndexBTreeGet is a point lookup in a google/btree of uint32
// handles compared through the arena, the shape of today's key index.
func BenchmarkIndexBTreeGet(b *testing.B) {
	s := get(b, false)
	t := btree.NewG[uint32](32, func(x, y uint32) bool { return bytes.Compare(s.key(x), s.key(y)) < 0 })
	for i := range s.keyOff {
		t.ReplaceOrInsert(uint32(i))
	}
	r := rand.New(rand.NewSource(2))
	probe := make([]uint32, 1<<16)
	for i := range probe {
		probe[i] = uint32(r.Intn(len(s.keyOff)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := t.Get(probe[i&(len(probe)-1)]); !ok {
			b.Fatal("missing")
		}
	}
}

// BenchmarkGetReply is a whole GET reply: hash lookup, the wire-form
// record copied into a TLS record buffer after a 64-byte header, sealed.
func BenchmarkGetReply(b *testing.B) {
	s := get(b, false)
	g, nonce := newSealer()
	buf := make([]byte, 0, 64<<10)
	r := rand.New(rand.NewSource(3))
	probe := make([][]byte, 1<<16)
	for i := range probe {
		probe[i] = bytes.Clone(s.key(uint32(r.Intn(len(s.keyOff)))))
	}
	var bytesOut int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k, _ := s.find(probe[i&(len(probe)-1)])
		buf = buf[:64]
		buf = append(buf, s.arena[s.recOff[k]:s.recOff[k]+uint64(s.recLen[k])]...)
		bytesOut += len(buf)
		g.Seal(buf[:0], nonce, buf, nil)
	}
	b.ReportMetric(float64(bytesOut)/float64(b.N), "B/reply")
}

func benchList(b *testing.B, contiguous bool) {
	s := get(b, contiguous)
	g, nonce := newSealer()
	buf := make([]byte, 0, record+g.Overhead()+(64<<10))
	r := rand.New(rand.NewSource(4))
	var kvs, out int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gr := s.groups[r.Intn(len(s.groups))]
		buf = buf[:64]
		for _, k := range s.sorted[gr[0]:gr[1]] {
			buf = append(buf, s.arena[s.recOff[k]:s.recOff[k]+uint64(s.recLen[k])]...)
			kvs++
			for len(buf) >= record {
				g.Seal(buf[:0], nonce, buf[:record], nil)
				out += record
				n := copy(buf, buf[record:])
				buf = buf[:n]
			}
		}
		g.Seal(buf[:0], nonce, buf, nil)
		out += len(buf)
	}
	b.ReportMetric(float64(kvs)/float64(b.N), "KVs/list")
	b.ReportMetric(float64(out)/float64(b.N), "B/list")
}

// BenchmarkListGatherScattered is a LIST page from the sorted index, its
// records scattered over the arena, gathered and sealed in 16 KiB records.
func BenchmarkListGatherScattered(b *testing.B) { benchList(b, false) }

// BenchmarkListGatherContiguous is the same with records in key order.
func BenchmarkListGatherContiguous(b *testing.B) { benchList(b, true) }

// BenchmarkUpdateCAS is a mixed-k8s update applied in a wire-form store:
// find, compare the mod revision, append the new record to the arena
// log, repoint the slot, build the watch event (new and previous record).
func BenchmarkUpdateCAS(b *testing.B) {
	s := get(b, false)
	r := rand.New(rand.NewSource(5))
	keys := len(s.keyOff)
	probe := make([][]byte, 1<<16)
	for i := range probe {
		probe[i] = bytes.Clone(s.key(uint32(r.Intn(keys))))
	}
	val := make([]byte, 64<<10)
	r.Read(val)
	event := make([]byte, 0, 256<<10)
	start := s.tail
	rev := int64(keys + 1)
	var vb int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k, _ := s.find(probe[i&(len(probe)-1)])
		if s.modRev[k] < 0 {
			b.Fatal("cas")
		}
		if uint64(cap(s.arena))-s.tail < 128<<10 {
			s.tail = start // a ring in the real thing
		}
		vl := int(s.recLen[k]) - 64 // about the old value's size
		if vl < 64 {
			vl = 64
		}
		vb += vl
		key := s.key(k)
		rec := appendKV(s.arena[s.tail:s.tail], key, val[:vl], 1, rev, 2)
		event = append(event[:0], rec...)
		event = append(event, s.arena[s.recOff[k]:s.recOff[k]+uint64(s.recLen[k])]...)
		s.recOff[k] = s.tail
		s.recLen[k] = uint32(len(rec))
		s.modRev[k] = rev
		s.tail += uint64(len(rec))
		rev++
	}
	b.ReportMetric(float64(vb)/float64(b.N), "valueB/op")
}

// BenchmarkCRC32CValue is the checksum today's reads pay per value.
func BenchmarkCRC32CValue(b *testing.B) {
	t := crc32.MakeTable(crc32.Castagnoli)
	v := make([]byte, 1458)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		crc32.Update(0, t, v)
	}
}
