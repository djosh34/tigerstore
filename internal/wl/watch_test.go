package wl

import (
	"fmt"
	"slices"
	"testing"
)

type seen struct {
	key   string
	rev   int64
	del   bool
	value string
	prev  string
}

func historyOf(t *testing.T, n *node, x watcher, from int64) []seen {
	t.Helper()
	x.prefix = len(x.end) > 0 && slices.Equal(x.end, prefixEnd(x.key))
	var out []seen
	err := n.history(&x, from, func(evs []event) {
		for _, ev := range evs {
			s := seen{key: string(n.st.key(ev.key)), rev: ev.rev, del: ev.del}
			if ev.kv != nil {
				s.value = string(recValue(ev.kv))
			}
			if ev.prev != nil {
				s.prev = string(recValue(ev.prev))
			}
			out = append(out, s)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWatchHistoryReplaysChangesInRevisionOrder(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	putValue(t, n.st, l, "/a/1", "one")   // 2
	putValue(t, n.st, l, "/b/1", "other") // 3
	putValue(t, n.st, l, "/a/2", "two")   // 4
	putValue(t, n.st, l, "/a/1", "uno")   // 5
	id, _ := n.st.find([]byte("/a/2"))
	n.st.rev++
	if _, err := n.st.del(id, n.st.rev); err != nil { // 6
		t.Fatal(err)
	}
	n.st.endApply()

	got := historyOf(t, n, watcher{key: []byte("/a/"), end: prefixEnd([]byte("/a/")), prevKV: true}, 3)
	want := []seen{
		{key: "/a/2", rev: 4, value: "two"},
		{key: "/a/1", rev: 5, value: "uno", prev: "one"},
		{key: "/a/2", rev: 6, del: true, prev: "two"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("history from 3:\n got %+v\nwant %+v", got, want)
	}
	got = historyOf(t, n, watcher{key: []byte("/a/1"), noPut: true}, 1)
	if len(got) != 0 {
		t.Fatalf("a watch of deletes only of /a/1 got %+v", got)
	}
}

func TestWatchHistoryComesInChunks(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	for i := range historyChunk + 1 {
		putValue(t, n.st, l, fmt.Sprintf("/k/%05d", i), "v")
	}
	var sizes []int
	x := watcher{key: []byte("/k/"), end: prefixEnd([]byte("/k/"))}
	if err := n.history(&x, 1, func(evs []event) { sizes = append(sizes, len(evs)) }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sizes, []int{historyChunk, 1}) {
		t.Fatalf("chunks of %v", sizes)
	}
}
