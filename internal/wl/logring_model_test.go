package wl

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestTheLogRingModel checks the log ring's rules (wal.go's head,
// logring.go) against a randomized model: a ring of a few small segments,
// which the log wraps many times; appends of entries of every size, each
// batch synced; cuts at durable records, as a checkpoint that has taken in
// what the store names before them makes; and crashes, each of which
// tears the last sync, every sector of it kept or lost on its own, a lost
// one holding what the space held before: zeros of the format, or a lap
// before's bytes. After each crash the log opens again, and:
//
//   - every record it reads is the one written at that offset since the
//     cut: no stale record reads as current (rule 1);
//   - every entry synced before the torn sync is there (rule 2);
//   - no crash reads as damage (ErrCorruptLog);
//   - the ring's files, segments and spares, stay as many as it was
//     formatted with (rule 3).
//
// TIGERSTORE_RING_CASES sets how many seeds it runs; a failing seed is
// rerun with TIGERSTORE_RING_SEED.
func TestTheLogRingModel(t *testing.T) {
	cases := 16
	if v, err := strconv.Atoi(os.Getenv("TIGERSTORE_RING_CASES")); err == nil {
		cases = v
	}
	first := uint64(1)
	if v, err := strconv.ParseUint(os.Getenv("TIGERSTORE_RING_SEED"), 10, 64); err == nil {
		first, cases = v, 1
	}
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	for seed := first; seed < first+uint64(cases); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) { runLogRingModel(t, seed) })
	}
}

// ringEnt is the model's entry: where its data lies, and the data.
type ringEnt struct {
	off  int64
	data string
}

func runLogRingModel(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed^0x5bd1e995))
	dir := t.TempDir()
	const stage = 64 << 10
	segs := 4 + rng.Int64N(5)
	ring := segs * segSize
	hist := map[uint64]ringEnt{} // the log's entries since its cut, by index
	next := uint64(1)
	cut := int64(0) // where the newest "checkpoint" left the log
	laps, crashes := int64(0), 0
	// lost are the lengths of the entries the last crash took, by index:
	// Raft's leader sends entries of those indexes again, conflicting
	// ones of the same size among them, so that records start where a
	// crash left records it lost.
	lost := map[uint64]int{}

	var w *wal
	open := func() {
		t.Helper()
		var got []uint64
		var err error
		w, err = openWALRing(sysEnv{}, dir, stage, ring, cut, cut, func(r *walRecord) error {
			if r.kind != recEntry {
				return nil
			}
			e, ok := hist[r.index]
			if !ok || e.off != r.off || e.data != string(r.data) {
				return fmt.Errorf("entry %d read at %d as %q: the log wrote %+v there (ok %v)", r.index, r.off, abbrev(string(r.data)), abbrev(e.data), ok)
			}
			got = append(got, r.index)
			return nil
		})
		if err != nil {
			t.Fatalf("seed %d, after %d crashes: open: %v", seed, crashes, err)
		}
		// What the log holds since its cut is a run of entries, and what
		// it lost past them goes from the model.
		for i, idx := range got {
			if i > 0 && idx != got[i-1]+1 {
				t.Fatalf("seed %d: entries read out of order: %v", seed, got)
			}
		}
		last := uint64(0)
		if len(got) > 0 {
			last = got[len(got)-1]
		} else {
			for idx, e := range hist {
				if e.off < cut && idx > last {
					last = idx
				}
			}
		}
		clear(lost)
		for idx, e := range hist {
			if idx > last {
				lost[idx] = len(e.data)
				delete(hist, idx)
			}
		}
		next = last + 1
		if n := countRingFiles(t, dir); n != segs {
			t.Fatalf("seed %d: the ring holds %d files, formatted with %d", seed, n, segs)
		}
	}
	open()
	steps := 300 + rng.IntN(300)
	for step := range steps {
		// Cut before the log since the cut would fill the ring: at the
		// newest durable entry's record, all before it taken in.
		if w.off-cut > (segs-3)*segSize {
			best := int64(-1)
			for _, e := range hist {
				if r := e.off - walEntHdr; r <= w.stable && r > best {
					best = r
				}
			}
			if best > cut {
				w.cut(best)
				cut = best
				for idx, e := range hist {
					if e.off < cut {
						delete(hist, idx)
					}
				}
			}
		}
		var ents []*pb.Entry
		for range 1 + rng.IntN(8) {
			n := rng.IntN(200)
			if rng.IntN(8) == 0 {
				n = rng.IntN(6 << 10)
			}
			data := fmt.Sprintf("%d:%d:", seed, next) + strings.Repeat(string(rune('a'+next%26)), n)
			if l, ok := lost[next]; ok && rng.IntN(2) == 0 {
				// The same size as the lost one, other bytes.
				data = strings.Repeat(string(rune('A'+(int(next)+crashes)%26)), l)
			}
			ents = append(ents, testEntry(next, 1, data))
			delete(lost, next)
			next++
		}
		if rng.IntN(4) == 0 {
			// The batch ends the log at a block's end: a record a crash
			// left past it starts where the next one would, and only the
			// zeros a start writes keep it from reading as current.
			last := ents[len(ents)-1]
			end := w.off + walRecSize(ents)
			pad := (block - end%block) % block
			last.Data = append(last.Data, strings.Repeat("z", int(pad))...)
		}
		if !w.fits(ents) {
			t.Fatalf("seed %d, step %d: the stage holds no more", seed, step)
		}
		// What the sync's blocks held before it, for a crash to tear.
		from := w.done.Load() &^ (block - 1)
		before := readRingRange(dir, from, w.off+walRecSize(ents))
		if err := w.write(ents, nil); err != nil {
			t.Fatal(err)
		}
		for i, e := range ents {
			hist[e.GetIndex()] = ringEnt{off: w.offs[i], data: string(e.GetData())}
		}
		to := (w.off + block - 1) &^ (block - 1)
		syncWAL(t, w)
		laps = max(laps, w.off/ring)
		if rng.IntN(25) != 0 {
			continue
		}
		// A crash, which tears this last sync: each sector its own.
		crashes++
		if os.Getenv("RINGDBG") != "" {
			names, _ := os.ReadDir(dir)
			var ns []string
			for _, e := range names {
				ns = append(ns, e.Name())
			}
			t.Logf("crash %d at step %d: segs %d, cut %d, the torn sync %d..%d, the log's end %d, stable %d; files %v", crashes, step, segs, cut, from, to, w.off, w.stable, ns)
		}
		w.close()
		tearRingRange(t, rng, dir, before)
		open()
	}
	w.close()
	t.Logf("seed %d: a ring of %d segments, %d steps, %d crashes, %d laps", seed, segs, steps, crashes, laps)
	if laps < 2 {
		t.Fatalf("seed %d: the log wrapped the ring %d times; the model needs it to wrap", seed, laps)
	}
}

// walRecSize is what ents add to the log.
func walRecSize(ents []*pb.Entry) int64 {
	var n int64
	for _, e := range ents {
		n += walEntHdr + int64(len(e.GetData()))
	}
	return n
}

// readRingRange returns what the segment files hold from log offset from
// to to, by offset: what a file not made yet holds is unknown, and left
// out.
func readRingRange(dir string, from, to int64) map[int64][]byte {
	out := map[int64][]byte{}
	for k := from / segSize; k*segSize < to; k++ {
		b, err := os.ReadFile(segPath(dir, k))
		if err != nil {
			continue
		}
		lo, hi := max(from, k*segSize)-k*segSize, min(to, (k+1)*segSize)-k*segSize
		if hi > int64(len(b)) {
			continue
		}
		out[k*segSize+lo] = append([]byte(nil), b[lo:hi]...)
	}
	return out
}

// tearRingRange puts back, in each sector the last sync wrote, what the
// space held before it (before), at random: a write torn at sectors.
func tearRingRange(t *testing.T, rng *rand.Rand, dir string, before map[int64][]byte) {
	t.Helper()
	all := rng.IntN(4) == 0 // the sync reached the disk whole
	for at, old := range before {
		k := at / segSize
		path := segPath(dir, k)
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for s := int64(0); s < int64(len(old)); s += sector {
			if all || rng.IntN(2) == 0 {
				continue
			}
			pos := at - k*segSize + s
			copy(b[pos:pos+min(sector, int64(len(old))-s)], old[s:])
		}
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// countRingFiles counts the ring's files in dir: segments and spares.
func countRingFiles(t *testing.T, dir string) int64 {
	t.Helper()
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, e := range names {
		if ext := filepath.Ext(e.Name()); ext == ".seg" || ext == spareExt {
			n++
		}
	}
	return n
}

func abbrev(s string) string {
	if len(s) > 24 {
		return s[:24] + "…"
	}
	return s
}

// A start writes zeros over its last segment past the log's end: a
// record a crash left there, past the one it tore, lies at its own
// offset, and would read as current once the log's next records end
// where it starts (logring.go). Here entry 3 is torn and entry 4, a block
// on, kept; after the start, entry 3 is written again, a block long, so
// that the log ends where entry 4 lay, and after a clean stop entry 4
// must not come back.
func TestARingStartLeavesNoRecordPastItsEnd(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	dir := t.TempDir()
	const stage, ring = 64 << 10, 8 * (16 << 10)
	open := func() (*wal, []uint64) {
		t.Helper()
		var got []uint64
		w, err := openWALRing(sysEnv{}, dir, stage, ring, 0, 0, func(r *walRecord) error {
			if r.kind == recEntry {
				got = append(got, r.index)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return w, got
	}
	blockLong := func(i uint64, c string) *pb.Entry {
		return testEntry(i, 1, strings.Repeat(c, block-walEntHdr))
	}
	w, _ := open()
	if err := w.write([]*pb.Entry{blockLong(1, "a"), blockLong(2, "b"), blockLong(3, "c"), blockLong(4, "d")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	w.close()
	// The crash tore entry 3's first sector, and kept entry 4.
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(b[2*block:2*block+sector], strings.Repeat("x", sector))
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	w, got := open()
	if fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("after the crash the log holds %v, want [1 2]", got)
	}
	if err := w.write([]*pb.Entry{blockLong(3, "e")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	w.close()
	w, got = open()
	w.close()
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("after a clean stop the log holds %v, want [1 2 3]: entry 4, which the crash left past the end, read as current", got)
	}
}
