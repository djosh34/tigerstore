// Standalone primitive microbenchmarks for #577; not a tigerstore handler.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/djosh34/tigerstore/internal/k8ssize"
	"github.com/google/btree"
	"golang.org/x/sys/unix"
)

var sink uint64
var duration time.Duration
var repetitions int
var clockCost int64
var enc = json.NewEncoder(os.Stdout)

func cpu() int64 {
	var t unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &t); err != nil {
		panic(err)
	}
	return t.Nano()
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}

type result struct {
	Group     string  `json:"group"`
	Primitive string  `json:"primitive"`
	Size      int     `json:"size"`
	Entries   int     `json:"entries,omitempty"`
	Rep       int     `json:"rep"`
	Ops       uint64  `json:"ops"`
	CPU       int64   `json:"cpu_ns"`
	NS        float64 `json:"ns_op"`
	Allocs    float64 `json:"allocs_op"`
	Bytes     float64 `json:"bytes_op"`
}

func emit(group, name string, size, entries, rep int, ops uint64, ns int64, alloc, bytes uint64) {
	check(enc.Encode(result{group, name, size, entries, rep, ops, ns, float64(ns) / float64(ops), float64(alloc) / float64(ops), float64(bytes) / float64(ops)}))
}

// setup/undo are deliberately outside the measured interval. A chunk is fully
// restored before the next chunk, keeping cardinality and load factor constant.
func measure(name string, size, chunk int, setup, run, undo func(int, int)) {
	for rep := 0; rep < repetitions; rep++ {
		runtime.GC()
		var ns int64
		var ops, alloc, bytes uint64
		at := 0
		for ns < int64(duration) {
			if setup != nil {
				setup(at, chunk)
			}
			var a, b runtime.MemStats
			runtime.ReadMemStats(&a)
			t := cpu()
			run(at, chunk)
			ns += max(1, cpu()-t-clockCost)
			runtime.ReadMemStats(&b)
			alloc += b.Mallocs - a.Mallocs
			bytes += b.TotalAlloc - a.TotalAlloc
			if undo != nil {
				undo(at, chunk)
			}
			at += chunk
			ops += uint64(chunk)
		}
		emit("A", name, size, 0, rep, ops, ns, alloc, bytes)
	}
}

// A record and hash entry are respectively 8 and 16 bytes, with no Go pointers.
type record struct{ off, length uint32 }
type entry struct {
	hash  uint64
	slot  uint32
	state uint32
}
type arena struct {
	data    []byte
	records []record
}

func (a *arena) key(i uint32) string {
	r := a.records[i]
	return unsafe.String(&a.data[r.off], int(r.length))
}
func (a *arena) add(s string) uint32 {
	i := uint32(len(a.records))
	a.records = append(a.records, record{uint32(len(a.data)), uint32(len(s))})
	a.data = append(a.data, s...)
	return i
}

type flat struct {
	a     *arena
	table []entry
	mask  uint64
}

func newFlat(a *arena, n int) *flat {
	cap := 1
	for float64(cap)*0.65 < float64(n) {
		cap *= 2
	}
	return &flat{a, make([]entry, cap), uint64(cap - 1)}
}
func (h *flat) lookup(s string) (uint32, bool) {
	v := xxhash.Sum64String(s)
	for p := v & h.mask; ; p = (p + 1) & h.mask {
		e := h.table[p]
		if e.state == 0 {
			return 0, false
		}
		if e.state == 1 && e.hash == v && h.a.key(e.slot) == s {
			return e.slot, true
		}
	}
}
func (h *flat) insert(id uint32) {
	s := h.a.key(id)
	v := xxhash.Sum64String(s)
	tomb := -1
	for p := v & h.mask; ; p = (p + 1) & h.mask {
		e := h.table[p]
		if e.state == 2 && tomb < 0 {
			tomb = int(p)
		}
		if e.state == 1 && e.hash == v && h.a.key(e.slot) == s {
			h.table[p].slot = id
			return
		}
		if e.state == 0 {
			if tomb >= 0 {
				p = uint64(tomb)
			}
			h.table[p] = entry{v, id, 1}
			return
		}
	}
}
func (h *flat) remove(s string) bool {
	v := xxhash.Sum64String(s)
	for p := v & h.mask; ; p = (p + 1) & h.mask {
		e := &h.table[p]
		if e.state == 0 {
			return false
		}
		if e.state == 1 && e.hash == v && h.a.key(e.slot) == s {
			e.state = 2
			return true
		}
	}
}

// Stage 1 hashes all keys, stage 2 reads candidate entries, stage 3 compares
// arena keys. Independent loads can overlap without architecture-specific asm.
func (h *flat) batch(keys []string) uint64 {
	var hashes, positions [16]uint64
	var es [16]entry
	var sum uint64
	for i, s := range keys {
		hashes[i] = xxhash.Sum64String(s)
		positions[i] = hashes[i] & h.mask
	}
	for i := range keys {
		es[i] = h.table[positions[i]]
	}
	for i, s := range keys {
		v, p, e := hashes[i], positions[i], es[i]
		for e.state != 0 {
			if e.state == 1 && e.hash == v && h.a.key(e.slot) == s {
				sum += uint64(e.slot)
				break
			}
			p = (p + 1) & h.mask
			e = h.table[p]
		}
	}
	return sum
}

// Simple cache-friendlier ordered reference: contiguous 16-byte records (8-byte
// lexical prefix, 4-byte arena offset, 4-byte length). O(log n) seek, O(n) writes.
// Common /registry/ is omitted from prefixes only; full comparisons are retained.
type orderedRecord struct {
	prefix uint64
	rec    record
}
type ordered struct {
	a    *arena
	rows []orderedRecord
}

func prefix(s string) uint64 {
	s = strings.TrimPrefix(s, "/registry/")
	var b [8]byte
	copy(b[:], s)
	return binary.BigEndian.Uint64(b[:])
}
func (o *ordered) key(r orderedRecord) string {
	return unsafe.String(&o.a.data[r.rec.off], int(r.rec.length))
}
func (o *ordered) seek(s string) int {
	p := prefix(s)
	return sort.Search(len(o.rows), func(i int) bool { r := o.rows[i]; return r.prefix > p || (r.prefix == p && o.key(r) >= s) })
}
func (o *ordered) insert(id uint32) {
	s := o.a.key(id)
	i := o.seek(s)
	if i < len(o.rows) && o.key(o.rows[i]) == s {
		return
	}
	o.rows = append(o.rows, orderedRecord{})
	copy(o.rows[i+1:], o.rows[i:])
	o.rows[i] = orderedRecord{prefix(s), o.a.records[id]}
}
func (o *ordered) remove(s string) bool {
	i := o.seek(s)
	if i == len(o.rows) || o.key(o.rows[i]) != s {
		return false
	}
	copy(o.rows[i:], o.rows[i+1:])
	o.rows = o.rows[:len(o.rows)-1]
	return true
}
func data(n int) (*arena, []string, []int) {
	a := &arena{data: make([]byte, 0, n*64), records: make([]record, 0, n)}
	r := rand.New(rand.NewPCG(577, 1))
	for i := 0; i < n; i++ {
		resource := k8ssize.Buckets[k8ssize.Draw(r, "")].Resource
		sep := "/"
		switch resource {
		case "apiservices", "certificatesigningrequests", "clusterrolebindings", "clusterroles", "csinodes", "flowschemas", "minions", "namespaces", "priorityclasses", "prioritylevelconfigurations", "storageclasses":
			sep = "-"
		}
		a.add(fmt.Sprintf("/registry/%s/bench-%03d-%04d%sobject-%012d", resource, i%16, r.IntN(64), sep, i))
	}
	// Query keys have a separate backing arena (no string-pointer equality shortcut).
	qbytes := slices.Clone(a.data)
	q := make([]string, n)
	for i, rec := range a.records {
		q[i] = unsafe.String(&qbytes[rec.off], int(rec.length))
	}
	return a, q, r.Perm(n)
}
func indexes(n int, hashOnly bool) {
	a, q, order := data(n)
	id := func(i int) int { return order[i%n] }
	key := func(i int) string { return q[id(i)] }
	h := newFlat(a, n)
	minLen, maxLen := 1000, 0
	groups := map[string]int{}
	for _, s := range q {
		minLen = min(minLen, len(s))
		maxLen = max(maxLen, len(s))
		groups[s[:len(s)-19]]++
	}
	var listKeys uint64
	for _, count := range groups {
		listKeys += uint64(count * min(count, 500))
	}
	check(enc.Encode(map[string]any{"dataset": n, "arena_bytes": len(a.data), "key_length_min": minLen, "key_length_max": maxLen, "key_length_mean": float64(len(a.data)) / float64(n), "hash_capacity": len(h.table), "hash_load": float64(n) / float64(len(h.table)), "namespace_list_mean_keys": float64(listKeys) / float64(n)}))
	groups = nil
	for i := 0; i < n; i++ {
		h.insert(uint32(i))
	}
	measure("flat.lookup", n, 2048, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			v, ok := h.lookup(key(i))
			if !ok {
				panic("flat miss")
			}
			s += uint64(v)
		}
		sink = s
	}, nil)
	for _, b := range []int{8, 16} {
		measure(fmt.Sprintf("flat.batch%d", b), n, 2048, nil, func(at, k int) {
			var keys [16]string
			var s uint64
			for i := at; i < at+k; i += b {
				for j := 0; j < b; j++ {
					keys[j] = key(i + j)
				}
				s += h.batch(keys[:b])
			}
			sink = s
		}, nil)
	}
	remove := func(at, k int) {
		for i := at; i < at+k; i++ {
			if !h.remove(key(i)) {
				panic("remove miss")
			}
		}
	}
	insert := func(at, k int) {
		for i := at; i < at+k; i++ {
			h.insert(uint32(id(i)))
		}
	}
	measure("flat.insert", n, 2048, remove, insert, nil)
	measure("flat.delete", n, 2048, nil, remove, insert)
	h = nil
	runtime.GC()
	if hashOnly {
		return
	}
	m := make(map[string]uint32, n)
	for i := 0; i < n; i++ {
		m[a.key(uint32(i))] = uint32(i)
	}
	measure("go-map.lookup", n, 2048, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			s += uint64(m[key(i)])
		}
		sink = s
	}, nil)
	m = nil
	runtime.GC()
	t := btree.NewG[string](32, func(a, b string) bool { return a < b })
	for i := 0; i < n; i++ {
		t.ReplaceOrInsert(a.key(uint32(i)))
	}
	measure("btree.seek", n, 1024, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			t.AscendGreaterOrEqual(key(i), func(v string) bool { s += uint64(len(v)); return false })
		}
		sink = s
	}, nil)
	// Namespace-prefix lower bounds, with a namespace boundary check, up to 500.
	listPrefix := func(i int) string { s := key(i); return s[:len(s)-19] }
	measure("btree.namespace-list500", n, 64, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			p := listPrefix(i)
			c := 0
			t.AscendGreaterOrEqual(p, func(v string) bool {
				if c == 500 || !strings.HasPrefix(v, p) {
					return false
				}
				s += uint64(len(v))
				c++
				return true
			})
		}
		sink = s
	}, nil)
	measure("btree.seek-iterate500", n, 64, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			c := 0
			t.AscendGreaterOrEqual(key(i), func(v string) bool {
				if c == 500 {
					return false
				}
				s += uint64(len(v))
				c++
				return true
			})
		}
		sink = s
	}, nil)
	tr := func(at, k int) {
		for i := at; i < at+k; i++ {
			if _, ok := t.Delete(key(i)); !ok {
				panic("btree delete miss")
			}
		}
	}
	ti := func(at, k int) {
		for i := at; i < at+k; i++ {
			t.ReplaceOrInsert(a.key(uint32(id(i))))
		}
	}
	measure("btree.insert", n, 256, tr, ti, nil)
	measure("btree.delete", n, 256, nil, tr, ti)
	t = nil
	runtime.GC()
	o := &ordered{a: a, rows: make([]orderedRecord, n, n+2048)}
	for i := 0; i < n; i++ {
		s := a.key(uint32(i))
		o.rows[i] = orderedRecord{prefix(s), a.records[i]}
	}
	slices.SortFunc(o.rows, func(x, y orderedRecord) int { return strings.Compare(o.key(x), o.key(y)) })
	measure("array.seek", n, 1024, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			s += uint64(o.seek(key(i)))
		}
		sink = s
	}, nil)
	measure("array.namespace-list500", n, 64, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			p := listPrefix(i)
			j := o.seek(p)
			for c := 0; c < 500 && j < len(o.rows); c++ {
				v := o.key(o.rows[j])
				if !strings.HasPrefix(v, p) {
					break
				}
				s += uint64(len(v))
				j++
			}
		}
		sink = s
	}, nil)
	measure("array.seek-iterate500", n, 64, nil, func(at, k int) {
		var s uint64
		for i := at; i < at+k; i++ {
			j := o.seek(key(i))
			for c := 0; c < 500 && j < len(o.rows); c++ {
				s += uint64(o.rows[j].rec.length)
				j++
			}
		}
		sink = s
	}, nil)
	or := func(at, k int) {
		for i := at; i < at+k; i++ {
			if !o.remove(key(i)) {
				panic("array delete miss")
			}
		}
	}
	oi := func(at, k int) {
		for i := at; i < at+k; i++ {
			o.insert(uint32(id(i)))
		}
	}
	measure("array.insert", n, 8, or, oi, nil)
	measure("array.delete", n, 8, nil, or, oi)
	runtime.KeepAlive(a)
	runtime.KeepAlive(q)
}
func chase(bytes int) {
	lines := bytes / 64
	buf := make([]uint32, bytes/4)
	r := rand.New(rand.NewPCG(577, 2))
	p := make([]uint32, lines)
	for i := range p {
		p[i] = uint32(i)
	}
	r.Shuffle(lines, func(i, j int) { p[i], p[j] = p[j], p[i] })
	for i, x := range p {
		buf[int(x)*16] = p[(i+1)%lines]
	}
	var starts [8]uint32
	for i := range starts {
		starts[i] = p[i*lines/8]
	}
	p = nil
	runtime.GC()
	x := starts[0]
	measure("chase.serial", bytes, 65536, nil, func(_, k int) {
		for i := 0; i < k; i++ {
			x = buf[int(x)*16]
		}
		sink = uint64(x)
	}, nil)
	a, b, c, d, e, f, g, h := starts[0], starts[1], starts[2], starts[3], starts[4], starts[5], starts[6], starts[7]
	measure("chase.mlp8", bytes, 65536, nil, func(_, k int) {
		for i := 0; i < k; i += 8 {
			a = buf[int(a)*16]
			b = buf[int(b)*16]
			c = buf[int(c)*16]
			d = buf[int(d)*16]
			e = buf[int(e)*16]
			f = buf[int(f)*16]
			g = buf[int(g)*16]
			h = buf[int(h)*16]
		}
		sink = uint64(a + b + c + d + e + f + g + h)
	}, nil)
	runtime.KeepAlive(buf)
}
func main() {
	smoke := flag.Bool("smoke", false, "small correctness/integration smoke, not a measurement")
	flag.DurationVar(&duration, "duration", 300*time.Millisecond, "minimum measured CPU per repetition")
	flag.IntVar(&repetitions, "reps", 3, "repetitions")
	group := flag.String("group", "all", "A, B, all")
	flag.Parse()
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	debug.SetGCPercent(-1)
	samples := make([]int64, 1001)
	for i := range samples {
		t := cpu()
		samples[i] = cpu() - t
	}
	slices.Sort(samples)
	clockCost = samples[len(samples)/2]
	check(enc.Encode(map[string]any{"metadata": true, "clock_pair_ns": clockCost, "duration_ns": int64(duration), "go": runtime.Version(), "smoke": *smoke, "key_distribution": "k8ssize.Draw weighted resources,16 owners,64 namespaces; keys one random permutation per pass; query bytes independent"}))
	if *group == "all" || *group == "A" {
		if *smoke {
			chase(4 << 20)
			indexes(4096, false)
		} else {
			for _, s := range []int{4 << 20, 32 << 20, 128 << 20, 512 << 20, 1 << 30} {
				chase(s)
				runtime.GC()
			}
			indexes(100000, false)
			runtime.GC()
			indexes(1000000, false)
			runtime.GC()
			indexes(10000000, true)
			runtime.GC()
		}
	}
	if *group == "all" || *group == "B" {
		raftBench(*smoke)
	}
	runtime.KeepAlive(sink)
}
