package wl

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// scanKeys returns the live keys in [start, end) as the tree orders them.
func scanKeys(st *store, start, end string) []string {
	var out []string
	for _, id := range scanIDs(st, start, end) {
		if st.exists(id) {
			out = append(out, string(st.key(id)))
		}
	}
	return out
}

// scanIDs returns every key the tree holds in [start, end), deleted ones
// too.
func scanIDs(st *store, start, end string) []uint32 {
	var out []uint32
	var e []byte
	if end != "" {
		e = []byte(end)
	}
	st.tree.scan([]byte(start), e, func(id uint32) bool {
		out = append(out, id)
		return true
	})
	return out
}

func TestTreeKeepsOrderThroughRemovesAndReturns(t *testing.T) {
	st, l := newTestStore(t)
	for i := range 100 {
		putValue(t, st, l, fmt.Sprintf("/d/%03d", i), "v")
	}
	del := func(k string) {
		id, _ := st.find([]byte(k))
		st.rev++
		if _, _, err := st.del(id, st.rev, true); err != nil {
			t.Fatal(err)
		}
		st.endApply()
	}
	for i := 0; i < 100; i += 2 {
		del(fmt.Sprintf("/d/%03d", i))
	}
	putValue(t, st, l, "/d/010", "back")
	var want []string
	for i := 1; i < 100; i += 2 {
		want = append(want, fmt.Sprintf("/d/%03d", i))
	}
	want = append(want, "/d/010")
	slices.Sort(want)
	if got := scanKeys(st, "/d/", "/d0"); !slices.Equal(got, want) {
		t.Fatalf("scan after removes:\n got %q\nwant %q", got, want)
	}
	if got := scanKeys(st, "/d/011", "/d/015"); !slices.Equal(got, []string{"/d/011", "/d/013"}) {
		t.Fatalf("a range from a key: %q", got)
	}
	// The deleted keys stay for reads at past revisions until compaction
	// trims them.
	if k := len(scanIDs(st, "/d/", "/d0")); k != 100 {
		t.Fatalf("the tree holds %d keys before compaction, want 100", k)
	}
	st.compact(st.rev)
	st.trimSteps(1 << 20)
	if k := len(scanIDs(st, "/d/", "/d0")); k != len(want) {
		t.Fatalf("the tree holds %d keys after compaction, want the %d live", k, len(want))
	}
}

func TestTreeKeepsOrderAcrossChunks(t *testing.T) {
	st, l := newTestStore(t)
	const n = 5 * chunkMax
	// Inserted out of order, so that chunks split in the middle.
	for i := range n {
		putValue(t, st, l, fmt.Sprintf("/c/%06d", (i*7919)%n), "")
	}
	putValue(t, st, l, "/c/sub/x", "") // a subdirectory among the keys
	got := scanKeys(st, "/c/", "/c0")
	if len(got) != n+1 || !slices.IsSorted(got) {
		t.Fatalf("%d keys, sorted %v", len(got), slices.IsSorted(got))
	}
	d, _ := st.tree.lookup([]byte("/c/"))
	for _, c := range st.tree.dirs[d].chunks {
		if len(c) > chunkMax {
			t.Fatalf("a chunk of %d children", len(c))
		}
	}
	// A range that starts in the middle of a chunk, and one at a chunk's
	// first key.
	from := scanKeys(st, "/c/004096", "/c/004100")
	if !slices.Equal(from, []string{"/c/004096", "/c/004097", "/c/004098", "/c/004099"}) {
		t.Fatalf("a range in the middle: %q", from)
	}
	first := string(st.key(st.tree.dirs[d].chunks[1][0].id))
	if got := scanKeys(st, first, ""); got[0] != first {
		t.Fatalf("a range from a chunk's first key starts at %q", got[0])
	}
	if got := scanKeys(st, "/c/s", "/c/t"); !slices.Equal(got, []string{"/c/sub/x"}) {
		t.Fatalf("a range into the subdirectory: %q", got)
	}
}

// boundTreeWork checks each mutation, not elapsed time: a slow runner or
// -race must not change the result. At 200,000 keys, a split may copy and
// shift a few chunks' worth of entries, move a few hundred chunk headers,
// and binary-search the index. Four chunks leave room for that, but not
// for shifting a directory-sized array on every scattered insert/delete.
func boundTreeWork(t *testing.T, tr *tree) func() {
	t.Helper()
	work, total := 0, 0
	tr.testWork = func(n int) {
		work += n
		total += n
	}
	t.Cleanup(func() {
		tr.testWork = nil
		if total == 0 {
			t.Error("no tree work measured")
		}
	})
	return func() {
		t.Helper()
		if work > 4*chunkMax {
			t.Fatalf("one tree mutation did %d units of work, want at most %d", work, 4*chunkMax)
		}
		work = 0
	}
}

func TestInsertsIntoALargeDirectoryStayCheap(t *testing.T) {
	st, l := newTestStore(t)
	const n = 200000
	checkWork := boundTreeWork(t, &st.tree)
	for i := range n {
		putValue(t, st, l, fmt.Sprintf("/big/%07d", (i*104729)%n), "")
		checkWork()
	}
	got := scanKeys(st, "/big/", "/big0")
	if len(got) != n {
		t.Fatalf("%d keys after inserts, want %d", len(got), n)
	}
	for i, key := range got {
		if want := fmt.Sprintf("/big/%07d", i); key != want {
			t.Fatalf("key %d is %q, want %q", i, key, want)
		}
	}
}

func TestDeletesFromALargeDirectoryStayCheap(t *testing.T) {
	st, l := newTestStore(t)
	const n = 200000
	for i := range n {
		putValue(t, st, l, fmt.Sprintf("/big/%07d", i), "")
	}
	for i := range n / 4 {
		id, _ := st.find(fmt.Appendf(nil, "/big/%07d", 4*i))
		st.rev++
		if _, _, err := st.del(id, st.rev, true); err != nil {
			t.Fatal(err)
		}
	}
	st.endApply()
	if got := len(scanKeys(st, "/big/", "/big0")); got != n-n/4 {
		t.Fatalf("%d keys left, want %d", got, n-n/4)
	}
	// del leaves tombstones in the tree for historical reads. Measure
	// the actual removals during compaction, one key's trim at a time.
	checkWork := boundTreeWork(t, &st.tree)
	st.compact(st.rev)
	for st.trimming {
		st.trimSteps(1)
		checkWork()
	}
	if got := len(scanIDs(st, "/big/", "/big0")); got != n-n/4 {
		t.Fatalf("%d tree keys after compaction, want %d", got, n-n/4)
	}
}

// The directory index doubles as directories come: at its old fixed size
// of 1,048,576 slots, the next directory's insert looked for a free slot
// forever.
func TestTheTreeTakesDirectoriesPastItsFirstIndex(t *testing.T) {
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	const dirs = 1<<20 + 1000
	path := make([]byte, 0, 16)
	for i := range dirs {
		path = fmt.Appendf(path[:0], "/d%07d/", i)
		if _, err := st.tree.ensure(path); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < dirs; i += 997 {
		path = fmt.Appendf(path[:0], "/d%07d/", i)
		if _, ok := st.tree.lookup(path); !ok {
			t.Fatalf("directory %d is not found", i)
		}
	}
}

// A key under tens of thousands of slashes takes a few directories of
// bounded paths, not one per slash: each directory keeps its whole path,
// and 65,536 of them filled the directory arena in the apply. Keys past
// the bounds, in depth or in length, still range in byte order beside
// shallow ones, whatever the range's ends.
func TestDeepKeysTakeBoundedDirectoriesAndKeepTheirOrder(t *testing.T) {
	st, l := newTestStore(t)
	deep := strings.Repeat("/", 65536) + "k"
	dirBytes := func() int {
		n := 0
		for _, e := range st.tree.dirs {
			n += int(e.plen)
		}
		return n
	}
	before := dirBytes()
	putValue(t, st, l, deep, "v")
	if used := dirBytes() - before; used > maxDirDepth*maxDirPath {
		t.Fatalf("one key took %d bytes of directory paths, past %d", used, maxDirDepth*maxDirPath)
	}
	long := strings.Repeat("x", 600)
	levels := strings.Repeat("d/", 40)
	keys := []string{
		deep,
		"/a/" + levels + "k1", "/a/" + levels + "k2", "/a/" + levels + "sub/k3",
		"/a/" + strings.Repeat("d/", 31) + "k4", "/a/" + strings.Repeat("d/", 30) + "k5",
		"/a/b", "/a/c/d", "/a/d", "/a/da", "/a/d/z",
		"/b/" + long + "/k", "/b/" + long + "/j/k", "/b/" + long + "z", "/b/x/" + long, "/b/x/y",
		"/b/" + long[:500] + "/k", "/b/" + long[:500] + "/m/k",
	}
	for _, k := range keys {
		putValue(t, st, l, k, "v")
	}
	sorted := slices.Sorted(slices.Values(keys))
	if got := scanKeys(st, "", ""); !slices.Equal(got, sorted) {
		t.Fatalf("the tree's order:\n%q\nwant\n%q", got, sorted)
	}
	// Every range between two of the keys, and from just past one.
	for i := range sorted {
		for j := i; j < len(sorted); j++ {
			if got := scanKeys(st, sorted[i], sorted[j]); !slices.Equal(got, sorted[i:j]) {
				t.Fatalf("[%q, %q): %q, want %q", sorted[i], sorted[j], got, sorted[i:j])
			}
			if got := scanKeys(st, sorted[i]+"\x00", sorted[j]); j > i && !slices.Equal(got, sorted[i+1:j]) {
				t.Fatalf("(%q, %q): %q, want %q", sorted[i], sorted[j], got, sorted[i+1:j])
			}
		}
	}
}

// Namespace churn, Kubernetes' normal life, leaves no directory behind:
// a directory whose last key compaction removed goes, and its path's
// bytes and its id are taken again (#722). On a budget of 16 MiB, 200
// rounds each create 10
// namespaces of 5 keys, under two resources, then delete and compact
// them; the directories, their index and the key slab stay at what one
// round needs, and every key kept, in directories of its own and in one
// the churn shared, is still found and ranged.
func TestChurnLeavesNoDirectoryBehind(t *testing.T) {
	st, l := newTestStore(t)
	st.setBudget(16<<20, 0)
	for i := range 20 {
		putValue(t, st, l, fmt.Sprintf("/registry/keep/k%02d", i), "kept")
		putValue(t, st, l, fmt.Sprintf("/registry/pods/default/p%02d", i), "kept")
	}
	round := func(r int) {
		var ids []uint32
		for ns := range 10 {
			for _, res := range []string{"pods", "secrets"} {
				for k := range 5 {
					key := fmt.Sprintf("/registry/%s/ns-%d-%d/k%d", res, r, ns, k)
					putValue(t, st, l, key, "churn")
					id, _ := st.find([]byte(key))
					ids = append(ids, id)
				}
			}
		}
		for _, id := range ids {
			st.rev++
			if _, _, err := st.del(id, st.rev, false); err != nil {
				t.Fatal(err)
			}
			st.endApply()
		}
		putValue(t, st, l, "/registry/keep/last", "kept") // past the deletes
		st.compact(st.rev)
		st.trimSteps(1 << 20)
	}
	round(0)
	dirs, table, used := len(st.tree.dirs)-len(st.tree.free), len(st.tree.table), st.kmem.used
	for r := 1; r < 200; r++ {
		round(r)
	}
	if got := len(st.tree.dirs) - len(st.tree.free); got != dirs {
		t.Fatalf("%d directories after 200 rounds of churn, %d after one", got, dirs)
	}
	if len(st.tree.dirs) > dirs+25 || len(st.tree.table) != table {
		t.Fatalf("the directory table holds %d slots and the index %d after 200 rounds, %d and %d after one",
			len(st.tree.dirs), len(st.tree.table), dirs, table)
	}
	if st.kmem.used > used {
		t.Fatalf("the key slab holds %d bytes after 200 rounds, %d after one", st.kmem.used, used)
	}
	for _, p := range []string{"/registry/keep/", "/registry/pods/default/"} {
		n := 0
		st.tree.scan([]byte(p), prefixEnd([]byte(p)), func(id uint32) bool {
			if st.exists(id) {
				n++
			}
			return true
		})
		if want := 20 + b2i(p == "/registry/keep/"); int64(n) != want {
			t.Fatalf("%s ranges over %d keys, want %d", p, n, want)
		}
	}
	for i := range 20 {
		for _, k := range []string{fmt.Sprintf("/registry/keep/k%02d", i), fmt.Sprintf("/registry/pods/default/p%02d", i)} {
			if got, ok := valueAt(t, st, k, st.rev); !ok || got != "kept" {
				t.Fatalf("%s reads %q, %v after the churn", k, got, ok)
			}
		}
	}
	// A namespace used again after its directory went is made again.
	putValue(t, st, l, "/registry/pods/ns-0-0/again", "v")
	if got, ok := valueAt(t, st, "/registry/pods/ns-0-0/again", st.rev); !ok || got != "v" {
		t.Fatalf("a key in a directory made again reads %q, %v", got, ok)
	}
}
