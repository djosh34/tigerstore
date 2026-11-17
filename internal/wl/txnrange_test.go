package wl

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
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

// A Txn's range answers as a Range does (rangeKVs, etcd's executeRange):
// the same records in the same order, ties too, count and more, for
// random stores and random ranges with every sort, filter, limit and
// revision. A sort with order none reads only the first limit+1 keys in
// key order, as etcd's rangeLimit does, and sorts those (d-B Opus's
// probe: sorting them all, a Txn answered /a/2 where etcd answers /a/1).
func TestATxnsRangeAnswersAsARangeDoes(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		st, l := newTestStore(t)
		n := &node{st: st, sizes: sizeFor(4 << 30)}
		n.connOut = n.sizes.connOut(1)
		const keys = 10
		ids := make([]uint32, keys)
		for i := range ids {
			id, err := st.intern(fmt.Appendf(nil, "/a/%02d", i))
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = id
		}
		for range 30 {
			st.rev++
			used := map[int]bool{}
			for range 1 + rng.Intn(3) {
				i := rng.Intn(keys)
				if used[i] {
					continue
				}
				used[i] = true
				if rng.Intn(4) == 0 {
					if _, _, err := st.del(ids[i], st.rev, false); err != nil {
						t.Fatal(err)
					}
					continue
				}
				v := string([]byte{byte('a' + rng.Intn(3))}) // values tie
				if _, err := st.put(ids[i], []byte(v), int64(rng.Intn(2)), st.rev, l.add(v), false); err != nil {
					t.Fatal(err)
				}
			}
			st.endApply()
		}
		if rng.Intn(2) == 0 {
			dropRecords(st)
		}
		for q := range 200 {
			a, b := rng.Intn(keys), rng.Intn(keys+1)
			r := rangeReq{key: fmt.Appendf(nil, "/a/%02d", a)}
			switch rng.Intn(4) {
			case 0: // one key
			case 1:
				r.key, r.end = []byte{0}, []byte{0}
			default:
				r.end = fmt.Appendf(nil, "/a/%02d", b)
			}
			if rng.Intn(2) == 0 {
				r.rev = int64(1 + rng.Intn(int(st.rev)))
			}
			r.limit = int64(rng.Intn(5))
			r.sortTarget, r.sortOrder = uint64(rng.Intn(5)), uint64(rng.Intn(3))
			r.keysOnly, r.countOnly = rng.Intn(4) == 0, rng.Intn(6) == 0
			for _, f := range []*int64{&r.minMod, &r.maxMod, &r.minCreate, &r.maxCreate} {
				if rng.Intn(3) == 0 {
					*f = int64(rng.Intn(int(st.rev)))
				}
			}
			if len(r.end) > 0 && (len(r.end) != 1 || r.end[0] != 0) && bytes.Compare(r.key, r.end) >= 0 {
				r.end = nil
			}
			want, wantCount, wantMore, err := n.rangeKVs(&r, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			tr := txnRun{reply: true}
			count, more, err := n.txnRange(&tr, &r)
			if err != nil {
				t.Fatal(err)
			}
			shaped := func(kvs [][]byte) string {
				var out [][]byte
				for _, kv := range kvs {
					if r.keysOnly {
						kv = keysOnly(kv, &r) // as the Txn's caller does
					}
					out = append(out, kv)
				}
				return fmt.Sprintf("%x", out)
			}
			if shaped(want) != shaped(tr.kvs) || count != wantCount || more != wantMore {
				t.Fatalf("seed %d, range %d %+v: %d records, count %d, more %v; a Range gives %d, %d, %v", seed, q, r, len(tr.kvs), count, more, len(want), wantCount, wantMore)
			}
		}
		_ = st.close()
	}
}

// A Txn's range sorted by value is refused past one piece where a Range
// is (serveRange: past it before the next key), no sooner: two values of
// 1.5 MiB over a piece of 2 MiB are read, sorted and served under a limit
// of one by both (d-B Opus's probe: the Txn summed every candidate and
// refused it).
func TestATxnsRangeSortedByValueIsRefusedWhereARangeIs(t *testing.T) {
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n := &node{st: st, sizes: sizeFor(4 << 30)}
	n.connOut = 64 << 10
	v := strings.Repeat("v", 1536<<10)
	r := rangeReq{key: []byte("/a/"), end: []byte("/b"), limit: 1, sortTarget: sortByValue, sortOrder: sortAscend}
	for i := range 3 {
		putValue(t, st, l, fmt.Sprintf("/a/%d", i), v)
		st.noWait = true
		_, _, _, err := n.rangeKVs(&r, nil, n.oneShot())
		st.noWait = false
		tr := txnRun{reply: true}
		if _, _, err := n.txnRange(&tr, &r); err != nil {
			t.Fatal(err)
		}
		if refused := errors.Is(err, errBig); refused != tr.byValue || refused != (i == 2) {
			t.Fatalf("of %d values of 1.5 MiB, a Range sorted by value is refused %v, the same range in a Txn %v; want both %v", i+1, refused, tr.byValue, i == 2)
		}
	}
}

// A Txn's range of many keys keeps no more scratch past it than selKeep
// keys (d-B Opus's probe: 200,000 keys left 3 MiB held for good).
func TestATxnsRangeOfManyKeysKeepsNoScratch(t *testing.T) {
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n := &node{st: st, sizes: sizeFor(96 << 20)}
	n.connOut = n.sizes.connOut(1)
	const keys = 4 * selKeep
	for i := range keys {
		id, err := st.intern(fmt.Appendf(nil, "/k/%07d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.put(id, []byte("v"), 0, 1, l.add("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	st.rev = 1
	st.endApply()
	r := rangeReq{key: []byte("/k/"), end: []byte("/l")}
	tr := txnRun{reply: true}
	count, _, err := n.txnRange(&tr, &r)
	if err != nil || count != keys || len(tr.kvs) != keys {
		t.Fatalf("%d records, count %d, %v; want %d", len(tr.kvs), count, err, keys)
	}
	if cap(n.selKeys) > selKeep {
		t.Fatalf("the scratch keeps %d keys past the Txn", cap(n.selKeys))
	}
}

// A Txn of many ranges sorted by value, each within one piece, holds one
// piece of candidates at a time and its reply's records: on a 96 MiB
// node, 128 ranges each over one value of 1 MiB out of memory, keys only,
// loaded all 128 MiB into the store before (the re-check of 7ab6166c,
// area B, Astra's probe).
func TestATxnOfManyRangesSortedByValueStaysWithinTheBudget(t *testing.T) {
	const budget = 96 << 20
	n := txnRangeStore(t, budget, 128, 1<<20)
	n.st.setBudget(budget, 0)
	req := &pb.TxnRequest{}
	for i := range 128 {
		key := fmt.Sprintf("/a/%03d", i)
		req.Success = append(req.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{
			Key: []byte(key), RangeEnd: []byte(key + "\x00"), SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND, KeysOnly: true,
		}}})
	}
	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	tr := txnRun{reply: true, req: wire, rev: n.st.rev + 1}
	if _, _, err = n.runBranch(&tr, wire, true, 0); err != nil {
		t.Fatal(err)
	}
	if tr.big || len(tr.kvs) != 128 {
		t.Fatalf("refused %v, %d records; want all 128 keys", tr.big, len(tr.kvs))
	}
	if used := n.st.slab.used; used > 4<<20 {
		t.Fatalf("the store holds %d MiB of records after the Txn's ranges", used>>20)
	}
}
