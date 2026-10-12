package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"strings"
	"time"
	"unsafe"
)

const versionSlab = 65536

type version struct {
	rev, create, number, lease, location uint64
	prev, length, flags                  uint32
	pad                                  [3]uint32
}
type versions struct {
	slabs [][]version
	n     uint32
}

func (v *versions) add(x version) uint32 {
	if v.n%versionSlab == 0 {
		v.slabs = append(v.slabs, make([]version, versionSlab))
	}
	v.n++
	*v.at(v.n) = x
	return v.n
}
func (v *versions) at(h uint32) *version {
	h = (h &^ dead) - 1
	return &v.slabs[h/versionSlab][h%versionSlab]
}
func (v *versions) lookup(h uint32, r uint64) uint32 {
	h &^= dead
	for h != 0 {
		x := v.at(h)
		if x.rev <= r {
			if x.flags != 0 {
				return 0
			}
			return h
		}
		h = x.prev
	}
	return 0
}
func (v *versions) build(n int) {
	m := (n + 9) / 10
	for i := 0; i < n; i++ {
		v.add(version{rev: uint64(i + 1), create: uint64(i + 1), number: 1, location: uint64(i) * 1024, length: 1024})
	}
	for wave := 0; wave < 3; wave++ {
		for rank, i := 0, 0; i < n; rank, i = rank+1, i+10 {
			prev := uint32(i + 1)
			if wave > 0 {
				prev = uint32(n + (wave-1)*m + rank + 1)
			}
			v.add(version{rev: uint64(n + wave*m + rank + 1), create: uint64(i + 1), number: uint64(wave + 2), location: uint64(i) * 1024, prev: prev, length: 1024})
		}
	}
}
func initialHead(i, n int) uint32 {
	if i%10 != 0 {
		return uint32(i + 1)
	}
	return uint32(n + 2*((n+9)/10) + i/10 + 1)
}

var resources = []string{"configmaps", "deployments", "endpoints", "events", "jobs", "pods", "replicasets", "secrets", "services", "statefulsets"}
var names [2000]uint64

func init() {
	r := rand.New(rand.NewPCG(41, 93))
	for i := range names {
		names[i] = r.Uint64()
	}
	sort.Slice(names[:], func(i, j int) bool { return names[i] < names[j] })
}
func key(i int) string {
	group := i / 1000
	return fmt.Sprintf("/registry/%s/ns-%05d/app-%016x", resources[(group/1000)%10], group%1000, names[2*(i%1000)])
}
func insertKey(i, n int) string {
	group := (i * 7919) % (max(1, n/1000))
	rank := (i / max(1, n/1000)) % 1000
	return fmt.Sprintf("/registry/%s/ns-%05d/app-%016x", resources[(group/1000)%10], group%1000, names[2*rank+1])
}
func prefix(i int, wide bool) string {
	group := i / 1000
	r := resources[(group/1000)%10]
	if wide {
		return "/registry/" + r + "/"
	}
	return fmt.Sprintf("/registry/%s/ns-%05d/", r, group%1000)
}
func prefixEnd(p string) string { b := []byte(p); b[len(b)-1]++; return string(b) }
func (s *buffered) flush() {
	t := time.Now()
	s.doFlush()
	s.maxFlushNS = max(s.maxFlushNS, time.Since(t).Nanoseconds())
}
func deadline() {
	if !time.Now().Before(time.Date(2026, 10, 13, 22, 0, 0, 0, time.UTC)) {
		fmt.Fprintln(os.Stderr, "deadline reached")
		os.Exit(2)
	}
}
func metric(name string) uint64 {
	s := []metrics.Sample{{Name: name}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
func cpuGC() float64 {
	s := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(s)
	return s[0].Value.Float64()
}
func emit(x any) {
	if err := json.NewEncoder(os.Stdout).Encode(x); err != nil {
		panic(err)
	}
}

var sink uint64

func measure(name string, reps, ops int, f func(int) uint64) {
	for rep := 0; rep < reps; rep++ {
		deadline()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		c := cpuGC()
		t := time.Now()
		var sum uint64
		for i := 0; i < ops; i++ {
			sum += f(i + rep*ops)
		}
		ns := time.Since(t).Nanoseconds()
		runtime.ReadMemStats(&b)
		sink ^= sum
		emit(map[string]any{"op": name, "rep": rep, "ops": ops, "ns_op": float64(ns) / float64(ops), "alloc_B_op": float64(b.TotalAlloc-a.TotalAlloc) / float64(ops), "gc_cycles": b.NumGC - a.NumGC, "gc_cpu_ms": (cpuGC() - c) * 1000, "gc_pause_ms": float64(b.PauseTotalNs-a.PauseTotalNs) / 1e6, "checksum": sum})
	}
}
func main() {
	kind := flag.String("index", "arena", "arena,btree,slice")
	n := flag.Int("n", 10000000, "initial keys")
	reps := flag.Int("reps", 5, "repetitions")
	ops := flag.Int("ops", 200000, "point operations per repetition")
	ins := flag.Int("inserts", 20000, "new keys per repetition")
	mode := flag.String("load", "bulk", "bulk or random tree load")
	flag.Parse()
	deadline()
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(100)
	var baseline runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&baseline)
	start := time.Now()
	var v versions
	v.build(*n)
	runtime.GC()
	var vm runtime.MemStats
	runtime.ReadMemStats(&vm)
	var idx index
	switch *kind {
	case "arena":
		a := newArena()
		if *mode == "random" {
			perm := rand.New(rand.NewPCG(101, 102)).Perm(*n)
			for _, i := range perm {
				a.Put(key(i), initialHead(i, *n))
			}
		} else {
			a.bulk(*n, key, func(i int) uint32 { return initialHead(i, *n) })
		}
		idx = a
	case "btree":
		a := newStock()
		if *mode == "random" {
			perm := rand.New(rand.NewPCG(101, 102)).Perm(*n)
			for _, i := range perm {
				a.Put(key(i), initialHead(i, *n))
			}
		} else {
			for i := 0; i < *n; i++ {
				a.Put(key(i), initialHead(i, *n))
			}
		}
		idx = a
	case "slice":
		a := make([]item, *n)
		for i := range a {
			a[i] = item{key(i), initialHead(i, *n)}
		}
		idx = newBuffered(a)
	default:
		panic("unknown index")
	}
	runtime.GC()
	var loaded runtime.MemStats
	runtime.ReadMemStats(&loaded)
	emit(map[string]any{"stage": "loaded", "index": *kind, "load": *mode, "keys": *n, "seconds": time.Since(start).Seconds(), "heap_B_key": float64(loaded.HeapAlloc-baseline.HeapAlloc) / float64(*n), "index_B_key": float64(loaded.HeapAlloc-vm.HeapAlloc) / float64(*n), "versions_B_key": float64(vm.HeapAlloc-baseline.HeapAlloc) / float64(*n), "scan_heap_MB": float64(metric("/gc/scan/heap:bytes")) / 1e6, "heap_objects": loaded.HeapObjects - baseline.HeapObjects, "version_size": unsafe.Sizeof(version{}), "leaf_size": unsafe.Sizeof(leaf{}), "head": idx.Get(key(*n / 2))})
	// A whole forced collection is wall time and GC CPU, not an STW pause.
	for r := 0; r < 5; r++ {
		deadline()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		c := cpuGC()
		t := time.Now()
		runtime.GC()
		wall := time.Since(t)
		runtime.ReadMemStats(&after)
		emit(map[string]any{"stage": "forced_gc", "rep": r, "wall_ms": float64(wall.Nanoseconds()) / 1e6, "cpu_ms": (cpuGC() - c) * 1000, "stw_ms": float64(after.PauseTotalNs-before.PauseTotalNs) / 1e6})
	}
	// Prebuilt request strings are outside timed loops and memory-per-key figures.
	const qn = 262144
	q := make([]string, qn)
	hist := make([]string, qn)
	miss := make([]string, qn)
	ids := make([]int, qn)
	lo := make([]string, 4096)
	hi := make([]string, 4096)
	wide := make([]string, 10)
	wideEnd := make([]string, 10)
	rng := rand.New(rand.NewPCG(123, 456))
	for i := range q {
		ids[i] = rng.IntN(*n)
		q[i] = key(ids[i])
		hist[i] = key(rng.IntN((*n+9)/10) * 10)
		miss[i] = q[i] + "!"
	}
	for i := range lo {
		lo[i] = prefix(rng.IntN(*n), false)
		hi[i] = prefixEnd(lo[i])
	}
	for i := range wide {
		wide[i] = "/registry/" + resources[i] + "/"
		wideEnd[i] = prefixEnd(wide[i])
	}
	// Read correctness includes gets, counts, ordered pages and retained revision.
	for i := 0; i < 10000; i++ {
		if got := idx.Get(q[i]); got != initialHead(ids[i], *n) {
			panic(fmt.Sprintf("get mismatch %s: %d", q[i], got))
		}
		if idx.Get(miss[i]) != 0 {
			panic("miss found")
		}
	}
	expected := uint64(min(1000, *n))
	for i := 0; i < len(lo); i++ {
		if got := idx.Count(lo[i], hi[i]); got != expected {
			panic(fmt.Sprintf("count %s = %d want %d", lo[i], got, expected))
		}
	}
	emit(map[string]any{"stage": "validated", "get_samples": 10000, "namespace_counts": 4096})
	runtime.GC()
	out := make([]result, 100)
	benchReads := func(suffix string) {
		measure("prefix_keys100"+suffix, *reps, 20000, func(i int) uint64 {
			n := idx.Page(lo[i%len(lo)], hi[i%len(lo)], out)
			var sum uint64
			for j := 0; j < n; j++ {
				sum += uint64(out[j].head) + uint64(out[j].key[out[j].n-1])
			}
			return sum
		})
		measure("get"+suffix, *reps, *ops, func(i int) uint64 { return uint64(idx.Get(q[i&(qn-1)])) })
		measure("miss"+suffix, *reps, *ops, func(i int) uint64 { return uint64(idx.Get(miss[i&(qn-1)])) })
		revision := uint64(*n + (*n+9)/10)
		measure("mvcc_hot4"+suffix, *reps, *ops, func(i int) uint64 { return uint64(v.lookup(idx.Get(hist[i&(qn-1)]), revision)) })
		measure("prefix_limit100"+suffix, *reps, 20000, func(i int) uint64 {
			n := 0
			var sum uint64
			idx.Walk(lo[i%len(lo)], hi[i%len(lo)], func(h uint32) bool {
				if alive(h) != 0 {
					sum += uint64(h)
					n++
				}
				return n < 100
			})
			return sum
		})
		measure("count_ns1000"+suffix, *reps, 20000, func(i int) uint64 { return idx.Count(lo[i%len(lo)], hi[i%len(lo)]) })
		measure("count_resource1m"+suffix, *reps, 100, func(i int) uint64 { return idx.Count(wide[i%10], wideEnd[i%10]) })
		measure("historical_count_ns1000"+suffix, *reps, 2000, func(i int) uint64 {
			var count uint64
			idx.Walk(lo[i%len(lo)], hi[i%len(lo)], func(h uint32) bool {
				if v.lookup(h, revision) != 0 {
					count++
				}
				return true
			})
			return count
		})
	}
	benchReads("")
	// Retained-history overwrites include version append and index update, as creates do.
	nextRev := uint64(*n + 3*((*n+9)/10))
	measure("put_overwrite", *reps, *ops, func(i int) uint64 {
		k := q[i&(qn-1)]
		old := idx.Get(k)
		x := *v.at(old)
		nextRev++
		x.rev = nextRev
		x.number++
		x.prev = old
		h := v.add(x)
		idx.Put(k, h)
		return uint64(h)
	})
	measure("put_overwrite_drain", 1, 1, func(int) uint64 {
		if s, ok := idx.(*buffered); ok {
			s.flush()
		}
		return 0
	})
	// Compare reads with the same 32K recent updates, including an unflushed buffer.
	for i := 0; i < 32768; i++ {
		k := q[i]
		old := idx.Get(k)
		x := *v.at(old)
		nextRev++
		x.rev = nextRev
		x.number++
		x.prev = old
		h := v.add(x)
		idx.Put(k, h)
	}
	measure("first_range_after_writes", 1, 1, func(int) uint64 {
		var sum uint64
		n := 0
		idx.Walk(lo[0], hi[0], func(h uint32) bool { sum += uint64(h); n++; return n < 100 })
		return sum
	})
	benchReads("_after_overwrites")
	// Drain the read experiment's pending buffer outside create timing.
	if s, ok := idx.(*buffered); ok {
		s.flush()
	}
	newKeys := make([]string, *reps**ins)
	for i := range newKeys {
		newKeys[i] = insertKey(i, *n)
	}
	measure("put_create", *reps, *ins, func(i int) uint64 {
		nextRev++
		h := v.add(version{rev: nextRev, create: nextRev, number: 1, length: 1024})
		idx.Put(newKeys[i], h)
		return uint64(h)
	})
	measure("put_create_drain", 1, 1, func(int) uint64 {
		if s, ok := idx.(*buffered); ok {
			s.flush()
		}
		return 0
	})
	for i, k := range newKeys {
		if idx.Get(k) == 0 {
			panic(fmt.Sprintf("missing insert %d", i))
		}
	}
	emit(map[string]any{"stage": "insert_validation", "new_keys": len(newKeys), "all_found": true})
	if s, ok := idx.(*buffered); ok {
		emit(map[string]any{"stage": "buffer", "flushes": s.flushes, "pending": len(s.buf), "max_flush_ms": float64(s.maxFlushNS) / 1e6})
	}
	// Keep everything being measured alive, including index and history.
	runtime.GC()
	runtime.KeepAlive(idx)
	runtime.KeepAlive(v)
	runtime.KeepAlive(q)
	runtime.KeepAlive(hist)
	runtime.KeepAlive(miss)
	emit(map[string]any{"stage": "done", "sink": sink, "timestamp": time.Now().UTC().Format(time.RFC3339), "key_length": len(key(0)), "notes": strings.Join([]string{"prefix_keys100 copies keys and heads; prefix_limit100 returns heads only", "no WAL, values, TLS, RPC or etcd baseline"}, "; ")})
}
