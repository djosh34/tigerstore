package wl

import (
	"fmt"
	"slices"
	"testing"
	"time"
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
		if _, err := st.del(id, st.rev); err != nil {
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

func TestInsertsIntoALargeDirectoryStayCheap(t *testing.T) {
	st, l := newTestStore(t)
	const n = 200000
	start := time.Now()
	for i := range n {
		putValue(t, st, l, fmt.Sprintf("/big/%07d", (i*104729)%n), "")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("%d inserts into one directory took %v", n, d)
	}
}

func TestDeletesFromALargeDirectoryStayCheap(t *testing.T) {
	st, l := newTestStore(t)
	const n = 200000
	for i := range n {
		putValue(t, st, l, fmt.Sprintf("/big/%07d", i), "")
	}
	start := time.Now()
	for i := range n / 4 {
		id, _ := st.find(fmt.Appendf(nil, "/big/%07d", 4*i))
		st.rev++
		if _, err := st.del(id, st.rev); err != nil {
			t.Fatal(err)
		}
	}
	st.endApply()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("%d deletes from a directory of %d took %v", n/4, n, d)
	}
	if got := len(scanKeys(st, "/big/", "/big0")); got != n-n/4 {
		t.Fatalf("%d keys left, want %d", got, n-n/4)
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
