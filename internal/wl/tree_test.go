package wl

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func scanKeys(st *store, start, end string) []string {
	var out []string
	var e []byte
	if end != "" {
		e = []byte(end)
	}
	st.tree.scan([]byte(start), e, func(id uint32) bool {
		out = append(out, string(st.key(id)))
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
	d, _ := st.tree.lookup([]byte("/d/"))
	if e := &st.tree.dirs[d]; e.tombs > minCompact && 2*e.tombs > len(e.children) {
		t.Fatalf("a directory holds %d removed of %d children uncompacted", e.tombs, len(e.children))
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
