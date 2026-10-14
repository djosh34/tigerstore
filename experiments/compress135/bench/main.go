// Question (#135): does compressing Kubernetes values pay off for tigerstore,
// and how: per value, per value with a zstd dictionary per resource type,
// or per Raft entry?
//
// Throwaway. Reads an etcdctl JSON dump of the live keys (-snap) and an
// etcdctl watch JSON stream of every write during the workload (-trace),
// both private to the runner. Prints only aggregates.
package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"google.golang.org/protobuf/encoding/protowire"
)

type rec struct {
	key, val []byte
	res, fmt string
}

var out io.Writer = os.Stdout

func p(f string, a ...any) { fmt.Fprintf(out, f, a...) }

func main() {
	snapPath := flag.String("snap", "", "etcdctl get -w json dump")
	tracePath := flag.String("trace", "", "etcdctl watch -w json stream")
	scratch := flag.String("scratch", "", "private dir for the cold-read files")
	reps := flag.Int("reps", 5, "repetitions per timing")
	quick := flag.Bool("quick", false, "short timings for a local smoke run")
	flag.Parse()
	REPS = *reps
	if *quick {
		minPass = 5 * time.Millisecond
	}
	snap := loadSnap(*snapPath)
	trace := loadTrace(*tracePath)
	p("libzstd %s, Go %s, GOMAXPROCS %d, reps %d\n", zversion(), runtime.Version(), runtime.GOMAXPROCS(0), REPS)

	var snapTrain, snapTest []rec
	for _, r := range snap {
		h := sha256.Sum256(r.key)
		if binary.BigEndian.Uint64(h[:8])%5 == 0 {
			snapTrain = append(snapTrain, r)
		} else {
			snapTest = append(snapTest, r)
		}
	}
	cut := len(trace) * 3 / 10
	traceEarly, traceLate := trace[:cut], trace[cut:]

	section("Corpus")
	describe("live snapshot", snap)
	describe("write trace (every put in revision order)", trace)
	p("snapshot split by key hash: %d train / %d test values. trace split by order: first %d puts train, last %d evaluate.\n",
		len(snapTrain), len(snapTest), len(traceEarly), len(traceLate))
	shares("live snapshot", snap)
	shares("write trace", trace)

	section("Dictionaries")
	dSnap := buildDicts("snapshot-trained", snapTrain)
	dEarly := buildDicts("trained on first 30% of writes", traceEarly)

	codecs := makeCodecs(dSnap)
	section("Per-value ratio, live snapshot test split (stored bytes incl. 1-byte tag and raw fallback / raw bytes)")
	pvTable(codecs, snapTest)
	section("Per-value by resource, zstd 1 with and without per-resource dictionary (snapshot test split)")
	byGroup(codecs, snapTest, func(r rec) string { return r.res }, "z1", "z1+dict", "lz4", "s2")
	section("Per-value by format, snapshot test split (built-in protobuf with the k8s\\x00 prefix against CRD and aggregator JSON)")
	byGroup(codecs, snapTest, func(r rec) string { return r.fmt }, "z1", "z3", "z1+dict", "k1", "k1+dict", "lz4", "s2")
	byGroup(codecs, snapTest, func(r rec) string { return r.fmt + " " + sizeBin(len(r.val))[2:] }, "z1", "z1+dict", "lz4")
	section("Per-value by size, snapshot test split")
	byGroup(codecs, snapTest, func(r rec) string { return sizeBin(len(r.val)) }, "z1", "z1+dict", "lz4", "s2")

	section("Per-value CPU on one core, snapshot test split (median of reps; ns per KiB of raw value)")
	cpuTable(codecs, snapTest)

	codecsLate := makeCodecs(dEarly)
	section("Write trace, last 70% of puts, dictionaries trained on the first 30% (aging check)")
	pvTable(pick(codecsLate, "none", "z1", "z1+dict", "z1+global", "z3+dict", "k1+dict", "lz4", "s2"), traceLate)
	byGroup(codecsLate, traceLate, func(r rec) string { return r.fmt }, "z1", "z1+dict", "lz4")

	section("Bytes per Put on disk, write trace last 70%, tigerstore framing, entries of N puts in arrival order")
	perEntry(codecsLate, traceLate)

	section("managedFields")
	managed(codecs, snap, snapTest, trace)

	section("Encrypted at rest (k8s:enc:aesgcm:v1, AES-256-GCM over the real snapshot test values) and random controls")
	controls(codecs, snapTest, trace)

	section("Point read cost")
	pointRead(codecsLate, traceLate, dEarly, *scratch)
	p("\ndone\n")
}

func section(s string) { p("\n## %s\n\n", s) }

// ---------- loading ----------

type kv struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func mk(k, v []byte) rec { return rec{k, v, resource(k), format(v)} }

func loadSnap(path string) []rec {
	f, err := os.Open(path)
	must(err)
	defer f.Close()
	var d struct{ Kvs []kv }
	must(json.NewDecoder(f).Decode(&d))
	var rs []rec
	for _, x := range d.Kvs {
		rs = append(rs, mk(x.Key, x.Value))
	}
	sort.Slice(rs, func(i, j int) bool { return bytes.Compare(rs[i].key, rs[j].key) < 0 })
	return rs
}

func loadTrace(path string) []rec {
	f, err := os.Open(path)
	must(err)
	defer f.Close()
	dec := json.NewDecoder(f)
	var rs []rec
	for {
		var w struct {
			Events []struct {
				Type int `json:"type"`
				Kv   kv  `json:"kv"`
			}
		}
		if err := dec.Decode(&w); err == io.EOF {
			break
		} else if err != nil {
			p("trace: stopped at decode error after %d puts: %v\n", len(rs), err)
			break
		}
		for _, e := range w.Events {
			if e.Type == 0 {
				rs = append(rs, mk(e.Kv.Key, e.Kv.Value))
			}
		}
	}
	return rs
}

var knownGroups = map[string]bool{"apps": true, "batch": true, "coordination.k8s.io": true, "discovery.k8s.io": true, "networking.k8s.io": true, "storage.k8s.io": true, "rbac.authorization.k8s.io": true, "apiextensions.k8s.io": true, "apiregistration.k8s.io": true, "scheduling.k8s.io": true, "node.k8s.io": true, "admissionregistration.k8s.io": true, "flowcontrol.apiserver.k8s.io": true, "certificates.k8s.io": true, "resource.k8s.io": true, "autoscaling": true, "policy": true}

// resource names the resource type from the key; never an object name.
func resource(k []byte) string {
	ps := strings.Split(string(k), "/")
	if len(ps) < 4 || ps[1] != "registry" {
		return "other"
	}
	if ps[2] == "services" {
		return "services/" + ps[3]
	}
	if knownGroups[ps[2]] {
		return ps[3]
	}
	if ps[2] == "shop.example.com" {
		return ps[3] + "(crd)"
	}
	if strings.Contains(ps[2], ".") {
		return "other-crd"
	}
	return ps[2]
}

func format(v []byte) string {
	switch {
	case bytes.HasPrefix(v, []byte("k8s\x00")):
		return "protobuf"
	case bytes.HasPrefix(v, []byte("k8s:enc:")):
		return "encrypted"
	case json.Valid(v):
		return "json"
	}
	return "other"
}

func sizeBin(n int) string {
	switch {
	case n < 256:
		return "a <256 B"
	case n < 1024:
		return "b 256 B-1 KiB"
	case n < 4096:
		return "c 1-4 KiB"
	case n < 16384:
		return "d 4-16 KiB"
	}
	return "e >=16 KiB"
}

func describe(name string, rs []rec) {
	var vb, kb int
	for _, r := range rs {
		vb += len(r.val)
		kb += len(r.key)
	}
	sz := make([]int, len(rs))
	for i, r := range rs {
		sz[i] = len(r.val)
	}
	sort.Ints(sz)
	p("%s: %d values, %d value bytes (mean %.0f, median %d, p90 %d, max %d), %d key bytes\n",
		name, len(rs), vb, float64(vb)/float64(len(rs)), sz[len(sz)/2], sz[len(sz)*9/10], sz[len(sz)-1], kb)
}

func shares(name string, rs []rec) {
	type agg struct{ n, b int }
	byRes, byFmt := map[string]*agg{}, map[string]*agg{}
	total := 0
	for _, r := range rs {
		for _, m := range []struct {
			m map[string]*agg
			k string
		}{{byRes, r.res}, {byFmt, r.fmt}} {
			a := m.m[m.k]
			if a == nil {
				a = &agg{}
				m.m[m.k] = a
			}
			a.n++
			a.b += len(r.val)
		}
		total += len(r.val)
	}
	p("\n%s by format:\n\n| Format | Values | Value bytes | Share of bytes |\n| --- | ---: | ---: | ---: |\n", name)
	for _, k := range sortedKeys(byFmt, func(a *agg) int { return a.b }) {
		p("| %s | %d | %d | %.2f%% |\n", k, byFmt[k].n, byFmt[k].b, 100*float64(byFmt[k].b)/float64(total))
	}
	p("\n%s by resource:\n\n| Resource | Values | Value bytes | Share of bytes |\n| --- | ---: | ---: | ---: |\n", name)
	for _, k := range sortedKeys(byRes, func(a *agg) int { return a.b }) {
		p("| %s | %d | %d | %.2f%% |\n", k, byRes[k].n, byRes[k].b, 100*float64(byRes[k].b)/float64(total))
	}
}

func sortedKeys[T any](m map[string]T, w func(T) int) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return w(m[ks[i]]) > w(m[ks[j]]) })
	return ks
}

// ---------- dictionaries ----------

type dictEnt struct {
	raw   []byte
	cd    map[int]*cdict // per level
	s2    *s2.Dict
	kenc  map[int]*zstd.Encoder
	owner string
}

type dicts struct {
	name   string
	byRes  map[string]*dictEnt
	global *dictEnt
	kdec   *zstd.Decoder
}

func (d *dicts) get(res string) *dictEnt {
	if e := d.byRes[res]; e != nil {
		return e
	}
	return d.global
}

func newDictEnt(raw []byte, owner string) *dictEnt {
	e := &dictEnt{raw: raw, cd: map[int]*cdict{}, kenc: map[int]*zstd.Encoder{}, owner: owner}
	for _, l := range []int{1, 2, 3} {
		e.cd[l] = newCDict(raw, l)
	}
	for _, l := range []int{1, 3} {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(raw), zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(l)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
		must(err)
		e.kenc[l] = enc
	}
	e.s2 = s2.MakeDict(raw, []byte("k8s\x00\n"))
	return e
}

const dictCap = 32 << 10

func buildDicts(name string, train []rec) *dicts {
	d := &dicts{name: name, byRes: map[string]*dictEnt{}}
	group := map[string][][]byte{}
	var all [][]byte
	for _, r := range train {
		group[r.res] = append(group[r.res], r.val)
		all = append(all, r.val)
	}
	t0 := cpuNow()
	g, err := trainDict(all, dictCap)
	must(err)
	tg := cpuNow() - t0
	d.global = newDictEnt(g, "global")
	var total, trained, fellBack int
	var tcpu time.Duration
	for _, res := range sortedKeys(group, func(v [][]byte) int { return len(v) }) {
		ss := group[res]
		n := 0
		for _, s := range ss {
			n += len(s)
		}
		// Small groups share the global dictionary.
		if len(ss) < 20 || n < 16<<10 {
			fellBack++
			continue
		}
		c := min(dictCap, max(4<<10, n/4))
		t0 := cpuNow()
		raw, err := trainDict(ss, c)
		tcpu += cpuNow() - t0
		if err != nil {
			fellBack++
			continue
		}
		d.byRes[res] = newDictEnt(raw, res)
		total += len(raw)
		trained++
	}
	// Build the restart cost: decoder tables for every dictionary, as a
	// node would on start, and one zstd 1 encoder table.
	t0 = cpuNow()
	w0 := time.Now()
	for range 10 {
		for _, e := range d.byRes {
			newCDict(e.raw, 1).free()
		}
	}
	tb := (cpuNow() - t0) / 10
	wb := time.Since(w0) / 10
	var raws [][]byte
	for _, e := range d.byRes {
		raws = append(raws, e.raw)
	}
	raws = append(raws, g)
	d.kdec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(raws...), zstd.WithDecoderConcurrency(1))
	must(err)
	p("%s: %d per-resource dictionaries, %d bytes in total, plus a %d-byte global one for %d small resource groups. "+
		"Training CPU %.0f ms per-resource total, %.0f ms global (libzstd fastCover defaults). Building encoder+decoder tables for all per-resource dictionaries once: %.2f ms CPU (%.2f ms wall).\n",
		name, trained, total, len(g), fellBack, ms(tcpu), ms(tg), ms(tb), ms(wb))
	return d
}

func ms(d time.Duration) float64 { return float64(d) / 1e6 }

// ---------- codecs ----------

type codec struct {
	name    string
	needLen bool
	comp    func(dst, src []byte, res string) []byte
	decomp  func(dst, src []byte, n int, res string) []byte
}

// store appends the stored form: tag 0 + raw, or tag 1 [+ uvarint length] + compressed.
func (c *codec) store(dst, v []byte, res string) []byte {
	start := len(dst)
	if c.comp == nil {
		return append(append(dst, 0), v...)
	}
	dst = append(dst, 1)
	if c.needLen {
		dst = binary.AppendUvarint(dst, uint64(len(v)))
	}
	dst = c.comp(dst, v, res)
	if len(dst)-start >= 1+len(v) {
		dst = append(append(dst[:start], 0), v...)
	}
	return dst
}

func (c *codec) load(dst, s []byte, res string) []byte {
	if s[0] == 0 {
		return append(dst[:0], s[1:]...)
	}
	s = s[1:]
	n := -1
	if c.needLen {
		u, k := binary.Uvarint(s)
		n, s = int(u), s[k:]
	}
	return c.decomp(dst, s, n, res)
}

func appendInto(dst []byte, bound int, f func(b []byte) []byte) []byte {
	dst = grow(dst, bound)
	o := f(dst[len(dst) : len(dst)+bound])
	if len(o) > 0 && unsafe.SliceData(o) != unsafe.SliceData(dst[len(dst):len(dst)+1]) {
		panic("encoder reallocated")
	}
	return dst[:len(dst)+len(o)]
}

func makeCodecs(d *dicts) []*codec {
	var cs []*codec
	cs = append(cs, &codec{name: "none"})
	for _, l := range []int{1, 2, 3} {
		z := newCZ(l)
		cs = append(cs, &codec{name: fmt.Sprintf("z%d", l),
			comp: func(dst, src []byte, _ string) []byte { return z.compress(dst, src, nil) },
			decomp: func(dst, src []byte, _ int, _ string) []byte {
				o, err := z.decompress(dst, src, nil)
				must(err)
				return o
			}})
	}
	for _, l := range []int{1, 2, 3} {
		z := newCZ(l)
		cs = append(cs, &codec{name: fmt.Sprintf("z%d+dict", l),
			comp: func(dst, src []byte, res string) []byte { return z.compress(dst, src, d.get(res).cd[l]) },
			decomp: func(dst, src []byte, _ int, res string) []byte {
				o, err := z.decompress(dst, src, d.get(res).cd[l])
				must(err)
				return o
			}})
	}
	zg := newCZ(1)
	cs = append(cs, &codec{name: "z1+global",
		comp: func(dst, src []byte, _ string) []byte { return zg.compress(dst, src, d.global.cd[1]) },
		decomp: func(dst, src []byte, _ int, _ string) []byte {
			o, err := zg.decompress(dst, src, d.global.cd[1])
			must(err)
			return o
		}})
	kdec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	must(err)
	for _, l := range []int{1, 3} {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(l)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
		must(err)
		cs = append(cs, &codec{name: fmt.Sprintf("k%d", l),
			comp: func(dst, src []byte, _ string) []byte { return enc.EncodeAll(src, dst) },
			decomp: func(dst, src []byte, _ int, _ string) []byte {
				o, err := kdec.DecodeAll(src, dst[:0])
				must(err)
				return o
			}})
		cs = append(cs, &codec{name: fmt.Sprintf("k%d+dict", l),
			comp: func(dst, src []byte, res string) []byte { return d.get(res).kenc[l].EncodeAll(src, dst) },
			decomp: func(dst, src []byte, _ int, _ string) []byte {
				o, err := d.kdec.DecodeAll(src, dst[:0])
				must(err)
				return o
			}})
	}
	var lc lz4.Compressor
	cs = append(cs, &codec{name: "lz4", needLen: true,
		comp: func(dst, src []byte, _ string) []byte {
			return appendInto(dst, lz4.CompressBlockBound(len(src)), func(b []byte) []byte {
				n, err := lc.CompressBlock(src, b)
				must(err)
				if n == 0 { // incompressible: make the caller fall back to raw
					n = len(b)
				}
				return b[:n]
			})
		},
		decomp: func(dst, src []byte, n int, _ string) []byte {
			dst = grow(dst[:0], n)[:n]
			got, err := lz4.UncompressBlock(src, dst)
			must(err)
			return dst[:got]
		}})
	cs = append(cs, &codec{name: "s2",
		comp: func(dst, src []byte, _ string) []byte {
			return appendInto(dst, s2.MaxEncodedLen(len(src)), func(b []byte) []byte { return s2.Encode(b, src) })
		},
		decomp: func(dst, src []byte, _ int, _ string) []byte {
			o, err := s2.Decode(dst[:cap(dst)], src)
			must(err)
			return o
		}})
	cs = append(cs, &codec{name: "s2better",
		comp: func(dst, src []byte, _ string) []byte {
			return appendInto(dst, s2.MaxEncodedLen(len(src)), func(b []byte) []byte { return s2.EncodeBetter(b, src) })
		},
		decomp: func(dst, src []byte, _ int, _ string) []byte {
			o, err := s2.Decode(dst[:cap(dst)], src)
			must(err)
			return o
		}})
	cs = append(cs, &codec{name: "s2+dict",
		comp: func(dst, src []byte, res string) []byte {
			return appendInto(dst, s2.MaxEncodedLen(len(src)), func(b []byte) []byte { return d.get(res).s2.Encode(b, src) })
		},
		decomp: func(dst, src []byte, _ int, res string) []byte {
			o, err := d.get(res).s2.Decode(dst[:cap(dst)], src)
			must(err)
			return o
		}})
	return cs
}

func pick(cs []*codec, names ...string) []*codec {
	var o []*codec
	for _, n := range names {
		for _, c := range cs {
			if c.name == n {
				o = append(o, c)
			}
		}
	}
	return o
}

// ---------- per value ----------

type stored struct {
	buf  []byte
	offs []int
}

func (s *stored) get(i int) []byte { return s.buf[s.offs[i]:s.offs[i+1]] }

func storeAll(c *codec, rs []rec) *stored {
	s := &stored{offs: []int{0}}
	for _, r := range rs {
		s.buf = c.store(s.buf, r.val, r.res)
		s.offs = append(s.offs, len(s.buf))
	}
	var dst []byte
	for i, r := range rs {
		dst = c.load(dst, s.get(i), r.res)
		if !bytes.Equal(dst, r.val) {
			panic("roundtrip " + c.name)
		}
	}
	return s
}

func pvTable(cs []*codec, rs []rec) {
	raw := 0
	for _, r := range rs {
		raw += len(r.val)
	}
	p("| Codec | Stored / raw | Median per-value ratio | Values left raw | Bytes saved per value |\n| --- | ---: | ---: | ---: | ---: |\n")
	for _, c := range cs {
		s := storeAll(c, rs)
		var ratios []float64
		fb := 0
		for i, r := range rs {
			v := s.get(i)
			if v[0] == 0 {
				fb++
			}
			if len(r.val) > 0 {
				ratios = append(ratios, float64(len(v))/float64(len(r.val)))
			}
		}
		sort.Float64s(ratios)
		p("| %s | %.2f%% | %.2f%% | %d | %.0f |\n", c.name, 100*float64(len(s.buf))/float64(raw), 100*ratios[len(ratios)/2], fb, float64(raw-len(s.buf))/float64(len(rs)))
	}
}

func byGroup(cs []*codec, rs []rec, key func(rec) string, names ...string) {
	sel := pick(cs, names...)
	type agg struct {
		n, raw int
		st     []int
	}
	m := map[string]*agg{}
	sts := make([]*stored, len(sel))
	for i, c := range sel {
		sts[i] = storeAll(c, rs)
	}
	for j, r := range rs {
		a := m[key(r)]
		if a == nil {
			a = &agg{st: make([]int, len(sel))}
			m[key(r)] = a
		}
		a.n++
		a.raw += len(r.val)
		for i := range sel {
			a.st[i] += len(sts[i].get(j))
		}
	}
	p("\n| Group | Values | Raw bytes |")
	for _, c := range sel {
		p(" %s |", c.name)
	}
	p("\n| --- | ---: | ---: |%s\n", strings.Repeat(" ---: |", len(sel)))
	ks := sortedKeys(m, func(a *agg) int { return a.raw })
	if len(ks[0]) > 2 && ks[0][1] == ' ' {
		sort.Strings(ks) // size bins
	}
	for _, k := range ks {
		a := m[k]
		p("| %s | %d | %d |", k, a.n, a.raw)
		for i := range sel {
			p(" %.2f%% |", 100*float64(a.st[i])/float64(a.raw))
		}
		p("\n")
	}
}

// ---------- timing ----------

var REPS = 5
var minPass = 150 * time.Millisecond

func cpuNow() time.Duration {
	var ru syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// timeit returns the median CPU and wall ns of one call of f over REPS reps,
// each rep repeating f for at least minPass.
func timeit(f func()) (cpu, wall float64) {
	f()
	var cs, ws []float64
	for range REPS {
		runtime.GC()
		c0, w0 := cpuNow(), time.Now()
		n := 0
		for time.Since(w0) < minPass {
			f()
			n++
		}
		cs = append(cs, float64(cpuNow()-c0)/float64(n))
		ws = append(ws, float64(time.Since(w0))/float64(n))
	}
	sort.Float64s(cs)
	sort.Float64s(ws)
	return cs[len(cs)/2], ws[len(ws)/2]
}

func cpuTable(cs []*codec, rs []rec) {
	raw := 0
	for _, r := range rs {
		raw += len(r.val)
	}
	kib := float64(raw) / 1024
	p("| Codec | Compress CPU ns/KiB | Decompress CPU ns/KiB | Compress ns/value | Decompress ns/value | Compress wall ns/KiB | Decompress wall ns/KiB |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, c := range cs {
		if c.comp == nil {
			continue
		}
		s := storeAll(c, rs)
		var buf, dst []byte
		cc, cw := timeit(func() {
			for _, r := range rs {
				buf = c.store(buf[:0], r.val, r.res)
			}
		})
		dc, dw := timeit(func() {
			for i, r := range rs {
				dst = c.load(dst, s.get(i), r.res)
			}
		})
		n := float64(len(rs))
		p("| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f |\n", c.name, cc/kib, dc/kib, cc/n, dc/n, cw/kib, dw/kib)
	}
}

// ---------- entries ----------

const walRecord = 17 // u32 length, u32 crc, u8 type, u64 term, ... as #109 counts it

// putWrite appends one write as core/codec.go frames it, with a Put body.
func putWrite(dst []byte, id uint64, key, val []byte) []byte {
	var put []byte
	put = protowire.AppendTag(put, 1, protowire.BytesType)
	put = protowire.AppendBytes(put, key)
	put = protowire.AppendTag(put, 2, protowire.BytesType)
	put = protowire.AppendBytes(put, val)
	dst = binary.AppendUvarint(dst, 2)                  // origin node
	dst = binary.AppendUvarint(dst, 0x9e3779b97f4a7c15) // random boot ID
	dst = binary.AppendUvarint(dst, id)
	dst = append(dst, 1)
	dst = binary.AppendUvarint(dst, uint64(len(put)))
	return append(dst, put...)
}

type entry struct {
	raw   []byte
	first int // index of the first put
	offs  []int
}

func entries(rs []rec, n int, val func(i int) []byte) []entry {
	var es []entry
	for s := 0; s < len(rs); s += n {
		e := min(s+n, len(rs))
		b := binary.AppendUvarint(nil, uint64(e-s))
		var offs []int
		for i := s; i < e; i++ {
			offs = append(offs, len(b))
			b = putWrite(b, uint64(1<<20+i), rs[i].key, val(i))
		}
		es = append(es, entry{b, s, offs})
	}
	return es
}

func perEntry(cs []*codec, rs []rec) {
	rawVal := func(i int) []byte { return rs[i].val }
	pvNames := []string{"z1", "z1+dict", "k1+dict", "lz4", "s2"}
	peNames := []string{"z1", "z3", "z1+global", "k1", "lz4", "s2"}
	vbytes := 0
	for _, r := range rs {
		vbytes += len(r.val)
	}
	p("Mean value %.0f bytes, mean key %.0f bytes. Per-entry rows compress the whole entry (keys and framing too) as one block with a 1-byte tag. Model cap = 336 MiB/s (#28's disk, as #109 uses it) / (3 replicas x bytes per Put); a model, not a measurement.\n\n",
		float64(vbytes)/float64(len(rs)), func() float64 {
			k := 0
			for _, r := range rs {
				k += len(r.key)
			}
			return float64(k) / float64(len(rs))
		}())
	p("| Scheme | N=1 B/Put | N=16 B/Put | N=64 B/Put | N=256 B/Put | Saved at N=64 | Model cap at N=64, Put/s |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	row := func(name string, f func(n int) float64) {
		var v []float64
		for _, n := range []int{1, 16, 64, 256} {
			v = append(v, f(n))
		}
		base := bpp(entries(rs, 64, rawVal), len(rs), nil)
		p("| %s | %.0f | %.0f | %.0f | %.0f | %.1f%% | %.0f |\n", name, v[0], v[1], v[2], v[3], 100*(1-v[2]/base), 336*1024*1024/(3*v[2]))
	}
	row("raw", func(n int) float64 { return bpp(entries(rs, n, rawVal), len(rs), nil) })
	for _, c := range pick(cs, pvNames...) {
		s := storeAll(c, rs)
		row("per value "+c.name, func(n int) float64 { return bpp(entries(rs, n, s.get), len(rs), nil) })
	}
	for _, c := range pick(cs, peNames...) {
		row("per entry "+c.name, func(n int) float64 { return bpp(entries(rs, n, rawVal), len(rs), c) })
	}
	p("\nPer-entry CPU on one core, N=64 (ns per KiB of raw entry bytes, median of reps) and the cost to read one value back:\n\n")
	p("| Codec | Compress ns/KiB | Decompress ns/KiB | Decode one N=64 entry, ns | Decode one N=256 entry, ns |\n| --- | ---: | ---: | ---: | ---: |\n")
	for _, c := range pick(cs, peNames...) {
		es := entries(rs, 64, rawVal)
		raw := 0
		var st [][]byte
		for _, e := range es {
			raw += len(e.raw)
			st = append(st, c.store(nil, e.raw, "entry"))
		}
		kib := float64(raw) / 1024
		var buf, dst []byte
		cc, _ := timeit(func() {
			for _, e := range es {
				buf = c.store(buf[:0], e.raw, "entry")
			}
		})
		dc, _ := timeit(func() {
			for _, s := range st {
				dst = c.load(dst, s, "entry")
			}
		})
		es256 := entries(rs, 256, rawVal)
		var st256 [][]byte
		for _, e := range es256 {
			st256 = append(st256, c.store(nil, e.raw, "entry"))
		}
		dc256, _ := timeit(func() {
			for _, s := range st256 {
				dst = c.load(dst, s, "entry")
			}
		})
		p("| %s | %.0f | %.0f | %.0f | %.0f |\n", c.name, cc/kib, dc/kib, dc/float64(len(st)), dc256/float64(len(st256)))
	}
}

func bpp(es []entry, puts int, c *codec) float64 {
	total := 0
	for _, e := range es {
		if c == nil {
			total += walRecord + len(e.raw)
		} else {
			total += walRecord + len(c.store(nil, e.raw, "entry"))
		}
	}
	return float64(total) / float64(puts)
}

// ---------- managedFields ----------

// rewrite returns msg with every field num passed through f; f returning nil drops it.
func rewrite(msg []byte, num protowire.Number, f func([]byte) []byte) ([]byte, bool) {
	var o []byte
	for len(msg) > 0 {
		n, t, k := protowire.ConsumeTag(msg)
		if k < 0 {
			return nil, false
		}
		m := protowire.ConsumeFieldValue(n, t, msg[k:])
		if m < 0 {
			return nil, false
		}
		if n == num && t == protowire.BytesType {
			v, _ := protowire.ConsumeBytes(msg[k:])
			if nv := f(v); nv != nil {
				o = protowire.AppendTag(o, n, t)
				o = protowire.AppendBytes(o, nv)
			}
		} else {
			o = append(o, msg[:k+m]...)
		}
		msg = msg[k+m:]
	}
	return o, true
}

// stripMF returns the value without metadata.managedFields, and whether it parsed.
func stripMF(r rec) (with, without []byte, ok bool) {
	switch r.fmt {
	case "protobuf":
		ok := true
		body, good := rewrite(r.val[4:], 2, func(raw []byte) []byte { // runtime.Unknown.raw
			o, g := rewrite(raw, 1, func(meta []byte) []byte { // object.metadata
				m, g := rewrite(meta, 17, func([]byte) []byte { return nil }) // managedFields
				ok = ok && g
				if m == nil {
					m = []byte{}
				}
				return m
			})
			ok = ok && g
			return o
		})
		if !good || !ok {
			return nil, nil, false
		}
		return r.val, append([]byte("k8s\x00"), body...), true
	case "json":
		var m map[string]any
		d := json.NewDecoder(bytes.NewReader(r.val))
		d.UseNumber()
		if d.Decode(&m) != nil {
			return nil, nil, false
		}
		w, _ := json.Marshal(m)
		if md, ok := m["metadata"].(map[string]any); ok {
			delete(md, "managedFields")
		}
		wo, _ := json.Marshal(m)
		return w, wo, true
	}
	return nil, nil, false
}

func managed(cs []*codec, snap, test, trace []rec) {
	for _, set := range []struct {
		name string
		rs   []rec
	}{{"live snapshot", snap}, {"write trace", trace}} {
		type agg struct{ raw, mf, n, bad int }
		m := map[string]*agg{}
		for _, r := range set.rs {
			a := m[r.fmt]
			if a == nil {
				a = &agg{}
				m[r.fmt] = a
			}
			w, wo, ok := stripMF(r)
			if !ok {
				a.bad++
				continue
			}
			a.n++
			a.raw += len(w)
			a.mf += len(w) - len(wo)
		}
		p("%s, raw bytes:\n\n| Format | Values parsed | Not parsed | Bytes | managedFields bytes | Share |\n| --- | ---: | ---: | ---: | ---: | ---: |\n", set.name)
		for _, k := range sortedKeys(m, func(a *agg) int { return a.raw }) {
			a := m[k]
			if a.raw == 0 {
				continue
			}
			p("| %s | %d | %d | %d | %d | %.2f%% |\n", k, a.n, a.bad, a.raw, a.mf, 100*float64(a.mf)/float64(a.raw))
		}
		p("\n")
	}
	p("Compressed, snapshot test split, per value (JSON values re-marshaled both ways so only managedFields differs):\n\n| Format | Codec | Stored with | Stored without | managedFields share of stored bytes |\n| --- | --- | ---: | ---: | ---: |\n")
	for _, f := range []string{"protobuf", "json"} {
		for _, c := range pick(cs, "z1", "z1+dict", "lz4") {
			var a, b int
			for _, r := range test {
				if r.fmt != f {
					continue
				}
				w, wo, ok := stripMF(r)
				if !ok {
					continue
				}
				a += len(c.store(nil, w, r.res))
				b += len(c.store(nil, wo, r.res))
			}
			if a > 0 {
				p("| %s | %s | %d | %d | %.2f%% |\n", f, c.name, a, b, 100*float64(a-b)/float64(a))
			}
		}
	}
}

// ---------- encryption and random controls ----------

func controls(cs []*codec, test, trace []rec) {
	key := make([]byte, 32)
	rand.Read(key)
	blk, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(blk)
	enc := func(rs []rec) []rec {
		var o []rec
		for _, r := range rs {
			nonce := make([]byte, 12)
			rand.Read(nonce)
			v := append([]byte("k8s:enc:aesgcm:v1:key1:"), nonce...)
			v = gcm.Seal(v, nonce, r.val, r.key)
			o = append(o, rec{r.key, v, r.res, "encrypted"})
		}
		return o
	}
	rnd := func(sizes []int, res string) []rec {
		var o []rec
		for i, n := range sizes {
			v := make([]byte, n)
			rand.Read(v)
			o = append(o, rec{[]byte(fmt.Sprintf("/registry/random/%d", i)), v, res, "other"})
		}
		return o
	}
	var secrets []rec
	for _, r := range test {
		if r.res == "secrets" {
			secrets = append(secrets, r)
		}
	}
	rep := func(n, sz int) []int {
		s := make([]int, n)
		for i := range s {
			s[i] = sz
		}
		return s
	}
	var traceSizes []int
	for _, r := range trace {
		traceSizes = append(traceSizes, len(r.val))
	}
	if len(traceSizes) > 4000 {
		traceSizes = traceSizes[:4000]
	}
	sets := []struct {
		name string
		rs   []rec
	}{
		{"all test values encrypted", enc(test)},
		{"test Secrets in plaintext", secrets},
		{"test Secrets encrypted", enc(secrets)},
		{"random 1 KiB x2000", rnd(rep(2000, 1024), "random")},
		{"random 10 KiB x300", rnd(rep(300, 10240), "random")},
		{"random bytes at the trace's first 4000 sizes (mixed-k8s bench style)", rnd(traceSizes, "random")},
	}
	sel := pick(cs, "z1", "z1+dict", "z3", "k1", "lz4", "s2")
	p("| Set | Codec | Stored / raw | Values left raw | Compress CPU ns/KiB | Decompress CPU ns/KiB |\n| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, s := range sets {
		raw := 0
		for _, r := range s.rs {
			raw += len(r.val)
		}
		kib := float64(raw) / 1024
		for _, c := range sel {
			st := storeAll(c, s.rs)
			fb := 0
			for i := range s.rs {
				if st.get(i)[0] == 0 {
					fb++
				}
			}
			var buf, dst []byte
			cc, _ := timeit(func() {
				for _, r := range s.rs {
					buf = c.store(buf[:0], r.val, r.res)
				}
			})
			dc, _ := timeit(func() {
				for i, r := range s.rs {
					dst = c.load(dst, st.get(i), r.res)
				}
			})
			p("| %s | %s | %.2f%% | %d of %d | %.0f | %.0f |\n", s.name, c.name, 100*float64(len(st.buf))/float64(raw), fb, len(s.rs), cc/kib, dc/kib)
		}
	}
}

// ---------- point reads ----------

func pct(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return -1
	}
	sort.Float64s(xs)
	return xs[min(len(xs)-1, int(q*float64(len(xs))))]
}

func pointRead(cs []*codec, rs []rec, d *dicts, scratch string) {
	p("In memory, one value at a time, warm decoder (time.Now around each call, so about 30-50 ns of the result is the clock):\n\n")
	p("| Codec | p50 ns | p99 ns | p50 ns for values over 4 KiB |\n| --- | ---: | ---: | ---: |\n")
	for _, c := range pick(cs, "none", "z1", "z1+dict", "k1", "k1+dict", "lz4", "s2") {
		s := storeAll(c, rs)
		var dst []byte
		var all, big []float64
		for rep := range 3 {
			for i, r := range rs {
				t := time.Now()
				dst = c.load(dst, s.get(i), r.res)
				ns := float64(time.Since(t))
				if rep == 2 {
					all = append(all, ns)
					if len(r.val) > 4096 {
						big = append(big, ns)
					}
				}
			}
		}
		p("| %s | %.0f | %.0f | %.0f |\n", c.name, pct(all, .5), pct(all, .99), pct(big, .5))
	}
	// A decoder built for the read: the worst case if decoders were not kept.
	z1 := pick(cs, "z1")[0]
	z1d := pick(cs, "z1+dict")[0]
	s1, sd := storeAll(z1, rs), storeAll(z1d, rs)
	var plain, dict []float64
	var dst []byte
	for i, r := range rs {
		if i%5 != 0 {
			continue
		}
		var err error
		t := time.Now()
		if v := s1.get(i); v[0] == 1 {
			dst, err = coldDecompress(dst, v[1:], nil)
			must(err)
		}
		plain = append(plain, float64(time.Since(t)))
		t = time.Now()
		if v := sd.get(i); v[0] == 1 {
			dst, err = coldDecompress(dst, v[1:], d.get(r.res).raw)
			must(err)
		}
		dict = append(dict, float64(time.Since(t)))
	}
	p("\nFresh libzstd decoder per read: z1 p50 %.0f ns, p99 %.0f ns; z1+dict (decoder plus dictionary tables built per read) p50 %.0f ns, p99 %.0f ns.\n",
		pct(plain, .5), pct(plain, .99), pct(dict, .5), pct(dict, .99))

	if scratch == "" {
		return
	}
	// Cold disk: O_DIRECT reads of the 4 KiB pages that hold the value.
	type layout struct {
		name   string
		file   []byte
		ext    func(i int) (off, n int)
		decode func(i int, b []byte) []byte
	}
	var ls []layout
	rawS := storeAll(pick(cs, "none")[0], rs)
	for _, c := range []*codec{pick(cs, "none")[0], z1d, pick(cs, "lz4")[0]} {
		s := rawS
		if c.name != "none" {
			s = storeAll(c, rs)
		}
		ls = append(ls, layout{"per value " + c.name, s.buf,
			func(i int) (int, int) { return s.offs[i], s.offs[i+1] - s.offs[i] },
			func(i int, b []byte) []byte { dst = c.load(dst, b, rs[i].res); return dst }})
	}
	for _, n := range []int{64, 256} {
		es := entries(rs, n, func(i int) []byte { return rs[i].val })
		var file []byte
		eoff := []int{0}
		for _, e := range es {
			file = z1.store(file, e.raw, "entry")
			eoff = append(eoff, len(file))
		}
		ls = append(ls, layout{fmt.Sprintf("per entry z1, N=%d", n), file,
			func(i int) (int, int) { e := i / n; return eoff[e], eoff[e+1] - eoff[e] },
			func(i int, b []byte) []byte { dst = z1.load(dst, b, "entry"); return dst }})
	}
	idx := mrand.New(mrand.NewSource(7)).Perm(len(rs))[:min(1000, len(rs))]
	buf := alignedBuf(8 << 20)
	type res struct{ io, dec, pages []float64 }
	results := make([]res, len(ls))
	for li, l := range ls {
		path := filepath.Join(scratch, fmt.Sprintf("layout%d", li))
		must(os.WriteFile(path, l.file, 0o600))
		f, _ := os.OpenFile(path, os.O_RDWR, 0)
		must(f.Sync())
		f.Close()
	}
	for rep := range REPS {
		for li, l := range ls {
			path := filepath.Join(scratch, fmt.Sprintf("layout%d", li))
			fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECT, 0)
			must(err)
			for _, i := range idx {
				off, n := l.ext(i)
				a := off &^ 4095
				e := (off + n + 4095) &^ 4095
				t := time.Now()
				got, err := syscall.Pread(fd, buf[:e-a], int64(a))
				must(err)
				_ = got
				t1 := time.Now()
				v := l.decode(i, buf[off-a:off-a+n])
				t2 := time.Now()
				if len(v) == 0 && len(rs[i].val) > 0 {
					panic("empty")
				}
				if rep > 0 {
					results[li].io = append(results[li].io, float64(t1.Sub(t)))
					results[li].dec = append(results[li].dec, float64(t2.Sub(t1)))
					results[li].pages = append(results[li].pages, float64((e-a)/4096))
				}
			}
			syscall.Close(fd)
		}
	}
	p("\nCold disk point read: O_DIRECT pread of the 4 KiB pages holding one value, then decode. %d random values of the trace's last 70%%, layouts interleaved, %d timed reps after one warmup rep:\n\n", len(idx), REPS-1)
	p("| Layout | File bytes | Mean pages | Read p50 us | Read p99 us | Decode p50 us | Decode p99 us |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for li, l := range ls {
		r := results[li]
		mean := 0.0
		for _, x := range r.pages {
			mean += x
		}
		p("| %s | %d | %.2f | %.0f | %.0f | %.1f | %.1f |\n", l.name, len(l.file), mean/float64(len(r.pages)),
			pct(r.io, .5)/1e3, pct(r.io, .99)/1e3, pct(r.dec, .5)/1e3, pct(r.dec, .99)/1e3)
	}
}

func alignedBuf(n int) []byte {
	b := make([]byte, n+4096)
	o := int(uintptr(unsafe.Pointer(&b[0])) & 4095)
	if o != 0 {
		o = 4096 - o
	}
	return b[o : o+n]
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
