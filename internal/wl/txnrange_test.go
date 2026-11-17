package wl

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// A Txn's range of keys reads the values of the records it returns
// alone (#731's ruling, B5), and one sorted by value is sized by them
// (B8). Each on the store alone: no node runs.

// vLog is a log whose every byte is 'v': values of hundreds of MiB lie
// in it while the test holds none of them.
type vLog struct{}

func (vLog) read(p []byte, _ int64) error {
	for i := range p {
		p[i] = 'v'
	}
	return nil
}

// txnRangeStore is a store of keys /a/000 on, each put at its own
// revision with a value of size bytes of 'v' that lies in the log alone.
func txnRangeStore(t *testing.T, budget int64, keys, size int) *node {
	t.Helper()
	st, _ := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	st.log = vLog{}
	n := &node{st: st, sizes: sizeFor(budget)}
	n.connOut = n.sizes.connOut(1)
	value := bytes.Repeat([]byte("v"), size)
	for i := range keys {
		id, err := st.intern(fmt.Appendf(nil, "/a/%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		st.rev++
		if _, err := st.put(id, value, 0, st.rev, int64(i*size), false); err != nil {
			t.Fatal(err)
		}
		st.endApply()
		k := &st.keys[id]
		st.slab.free(k.rec, int(k.rlen))
		k.rec, k.rlen = 0, 0
	}
	return n
}

// On a 96 MiB node with 384 values of 1 MiB out of memory, a Txn's range
// sorted by mod revision with a limit of one loads the one value it
// returns; before, it loaded all 384 MiB on the loop. Filtered by
// revision with nothing passing, it loads none.
func TestATxnsRangeReadsOnlyTheValuesItReturns(t *testing.T) {
	const keys = 384
	n := txnRangeStore(t, 96<<20, keys, 1<<20)
	st := n.st
	cases := []struct {
		name  string
		r     rangeReq
		want  []string
		more  bool
		loads uint64
	}{
		{"sorted by mod revision, descending, limit 1", rangeReq{limit: 1, sortTarget: sortByMod, sortOrder: sortDescend}, []string{"/a/383"}, true, 1},
		{"sorted by create revision, limit 1, filtered", rangeReq{limit: 1, sortTarget: sortByCreate, sortOrder: sortAscend, minMod: 101}, []string{"/a/099"}, true, 1},
		{"filtered by mod revision, none passing", rangeReq{minMod: st.rev + 1}, nil, false, 0},
		{"limit 1, keys only", rangeReq{limit: 1, keysOnly: true}, []string{"/a/000"}, true, 0},
	}
	for _, c := range cases {
		c.r.key, c.r.end = []byte("/a/"), []byte("/b")
		before := st.loads
		tr := txnRun{reply: true}
		count, more, err := n.txnRange(&tr, &c.r)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var got []string
		for _, rec := range tr.kvs {
			got = append(got, string(parseKV(rec).key))
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) || count != keys || more != c.more || tr.big {
			t.Fatalf("%s: %v, count %d, more %v, refused %v; want %v, count %d, more %v", c.name, got, count, more, tr.big, c.want, keys, c.more)
		}
		if loads := st.loads - before; loads != c.loads {
			t.Fatalf("%s: loaded %d values, want %d", c.name, loads, c.loads)
		}
		dropRecords(st)
	}
}

// A Txn's range sorted by value is sized by the records it returns: of
// four keys, two of 1,200 KiB, the two tiny ones sorted first under a
// limit of 2 are answered, as etcd answers them; before, the two largest
// sized it, and it was refused (B8).
func TestATxnsRangeSortedByValueIsSizedByWhatItReturns(t *testing.T) {
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n := &node{st: st, sizes: sizeFor(4 << 30)}
	n.connOut = n.sizes.connOut(1)
	for i, v := range []string{"a", "b", strings.Repeat("z", 1200<<10), strings.Repeat("z", 1200<<10)} {
		putValue(t, st, l, fmt.Sprintf("/a/%d", i), v)
	}
	if n.oneShot() < 2*1200<<10+1024 {
		t.Fatalf("one piece is %d KiB: the candidates do not fit it, and the test shows nothing", n.oneShot()>>10)
	}
	r := rangeReq{key: []byte("/a/"), end: []byte("/b"), limit: 2, sortTarget: sortByValue, sortOrder: sortAscend}
	tr := txnRun{reply: true}
	count, more, err := n.txnRange(&tr, &r)
	if err != nil {
		t.Fatal(err)
	}
	if tr.big || len(tr.kvs) != 2 || string(parseKV(tr.kvs[0]).value) != "a" || string(parseKV(tr.kvs[1]).value) != "b" || count != 4 || !more {
		t.Fatalf("refused %v, %d records, count %d, more %v; want a and b, count 4, more", tr.big, len(tr.kvs), count, more)
	}
}

// A Txn's range sorted by value whose candidates pass one piece is
// refused as a Range is, with no value read (#731's ruling); its writes
// apply (TestTheRepliesBuiltAtOnceAreBounded).
func TestATxnsRangeSortedByValuePastOnePieceIsRefused(t *testing.T) {
	n := txnRangeStore(t, 96<<20, 4, 1<<20)
	if n.oneShot() >= 4<<20 {
		t.Fatalf("one piece is %d KiB: the candidates fit it, and the test shows nothing", n.oneShot()>>10)
	}
	before := n.st.loads
	r := rangeReq{key: []byte("/a/"), end: []byte("/b"), limit: 1, sortTarget: sortByValue, sortOrder: sortAscend}
	tr := txnRun{reply: true}
	if _, _, err := n.txnRange(&tr, &r); err != nil {
		t.Fatal(err)
	}
	if !tr.big || !tr.byValue || len(tr.kvs) != 0 || n.st.loads != before {
		t.Fatalf("refused %v by value %v, %d records, %d loads; want it refused by value, nothing read", tr.big, tr.byValue, len(tr.kvs), n.st.loads-before)
	}
}
