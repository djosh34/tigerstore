package wl

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// Overlapping historical ranges must consume a complete batch before
// trimming it. Waiting for the last of several shared offset groups
// repeated the same two ranges forever on 5421104d (#887's review).
// This is Astra's five-range reproduction, with a page cache that holds
// nothing and keys-only replies, so reply-room waits cannot hide it.
func TestOverlappingColdRangesComplete(t *testing.T) {
	c, s0 := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(96<<20, 0)
	st.log = &coldFetchLog{memLog: log}
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	n.fetching = map[int64]bool{}
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	c.flow.conn = 0
	sh := &heldFetchShell{}
	n.shell = sh
	key := func(i int) string { return fmt.Sprintf("/f/%02d", i) }
	for i := range 11 {
		putValue(t, st, log, key(i), strings.Repeat("v", 900<<10))
	}
	rev := st.rev
	for i := range 11 {
		putValue(t, st, log, key(i), "updated")
	}
	f, err := os.CreateTemp(t.TempDir(), "log")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err = f.Write(log.b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	ranges := [][2]int{{9, 10}, {0, 2}, {6, 7}, {1, 3}, {7, 9}}
	for i, r := range ranges {
		s := s0
		if i > 0 {
			s = c.newStream(uint32(i*2+1), kRange)
		}
		msg := appendBytesField(nil, 1, []byte(key(r[0])))
		msg = appendBytesField(msg, 2, []byte(key(r[1]+1)))
		msg = appendVarintField(msg, 4, uint64(rev))
		msg = appendVarintField(msg, 5, sortAscend)
		msg = appendVarintField(msg, 6, sortByValue)
		msg = appendVarintField(msg, 8, 1)
		var req rangeReq
		if err := parseRange(msg, &req); err != nil {
			t.Fatal(err)
		}
		n.serveRange(c, s, &req, msg)
	}
	handoffs := 0
	for range 80 {
		if len(sh.work) == 0 {
			break
		}
		work := sh.work[0]
		sh.work = sh.work[1:]
		if err := work()(); err != nil {
			t.Fatal(err)
		}
		handoffs++
	}
	if len(n.awayReads) > 0 || len(n.roomWait) > 0 {
		t.Fatalf("after %d handoffs, %d ranges wait for values and %d for room", handoffs, len(n.awayReads), len(n.roomWait))
	}
	if handoffs > 10 {
		t.Fatalf("five cold ranges took %d handoffs, want at most 10", handoffs)
	}
	replies := 0
	for _, h := range c.flow.held {
		if len(h.data) == 0 {
			continue // trailers
		}
		var reply pb.RangeResponse
		if err := proto.Unmarshal(h.data[5:], &reply); err != nil {
			t.Fatal(err)
		}
		r := ranges[h.sid/2]
		if len(reply.Kvs) != r[1]-r[0]+1 {
			t.Fatalf("range %v returned %d keys", r, len(reply.Kvs))
		}
		for j, kv := range reply.Kvs {
			if string(kv.Key) != key(r[0]+j) || len(kv.Value) != 0 {
				t.Fatalf("range %v did not return its keys only, sorted by value", r)
			}
		}
		replies++
	}
	if replies != len(ranges) {
		t.Fatalf("%d replies, want %d", replies, len(ranges))
	}
	t.Logf("five overlapping cold ranges completed in %d handoffs", handoffs)
}

// Request waiters must not grow an offset list per value per range.
// Eight waiters of 65,536 one-byte values held 4 MiB of such lists,
// uncounted by the 64 KiB fetch, on 5421104d.
func TestColdRangeWaitersHoldNoPerValueLists(t *testing.T) {
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	st.setBudget(96<<20, 0)
	sh := &heldFetchShell{}
	n := &node{st: st, shell: sh, fetching: map[int64]bool{}}
	vs := make([]logValue, 65536)
	for i := range vs {
		vs[i] = logValue{off: int64(i), len: 1}
	}
	for i := range 8 {
		st.away = vs
		n.awaitValues(handle(i), nil)
	}
	for _, r := range n.awayReads {
		v := reflect.ValueOf(r)
		for i := range v.NumField() {
			f := v.Field(i)
			if f.Kind() == reflect.Slice && f.Type() != reflect.TypeFor[[]byte]() && f.Len() > 0 {
				t.Fatalf("a waiting request holds %d per-value entries in %s", f.Len(), v.Type().Field(i).Name)
			}
		}
	}
}

// Identity checks must not keep the old store and its Go tables alive
// after an install. A token is enough, for both kinds of queued work.
func TestFetchWaitersDoNotKeepAStorePointer(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[roomRead](), reflect.TypeFor[valueFetch]()} {
		for field := range typ.Fields() {
			if field.Type == reflect.TypeFor[*store]() {
				t.Errorf("%s keeps the replaced store in %s", typ.Name(), field.Name)
			}
		}
	}
}

// Resetting the active head must release its request without letting its
// late result consume the next head. Closing the client releases that
// next request too, while neither canceled call writes a reply.
func TestColdRangeRequestsLeaveThePoolWhenCanceled(t *testing.T) {
	c, first := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(1, 0)
	st.log = &coldFetchLog{memLog: log}
	n.sizes = sizeFor(96 << 20)
	n.connOut = 512 << 10
	n.fetching = map[int64]bool{}
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	sh := &heldFetchShell{}
	n.shell = sh
	putValue(t, st, log, "/cold", "value")
	f, err := os.CreateTemp(t.TempDir(), "values")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(log.b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	msg := appendBytesField(nil, 1, []byte("/cold"))
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, first, &req, msg)
	second := c.newStream(3, kRange)
	n.serveRange(c, second, &req, msg)
	if c.heldCounted != 2*int64(len(msg)) {
		t.Fatal("the two cold requests were not counted")
	}
	n.dropWaits(c, first.id)
	if c.heldCounted != int64(len(msg)) {
		t.Fatal("the reset kept its cold request counted")
	}
	take := sh.work[0]()
	sh.work = nil
	if err := take(); err != nil {
		t.Fatal(err)
	}
	if len(sh.work) != 1 || len(n.awayReads) != 1 || !first.open {
		t.Fatal("the canceled head's result served a call instead of planning the next")
	}
	c.dropAll()
	if c.heldCounted != 0 {
		t.Fatal("the closed client kept a cold request counted")
	}
	if err := sh.work[0]()(); err != nil {
		t.Fatal(err)
	}
	if len(n.awayReads) != 0 || n.stats.replies != 0 {
		t.Fatal("a canceled cold range stayed queued or wrote a reply")
	}
}

func TestQueuedFetchesOfAReplacedStoreAreDropped(t *testing.T) {
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	st.setBudget(1, 0)
	sh := &heldFetchShell{}
	n := &node{st: st, shell: sh, wal: &wal{}, fetching: map[int64]bool{}}
	for i := range 2 {
		st.away = []logValue{{off: int64(i * 1024), len: 64}}
		n.fetchAway()
	}
	if len(sh.work) != 1 || len(n.fetchQueue) != 1 {
		t.Fatal("the test did not leave a batch queued")
	}
	next, _ := newTestStore(t)
	defer func() { _ = next.close() }()
	next.setBudget(1, 0)
	n.st = next
	n.pumpFetch()
	if len(n.fetchQueue) != 0 || len(sh.work) != 1 {
		t.Fatal("a stale-store batch stayed queued, or began reading")
	}
	if err := sh.work[0]()(); err != nil {
		t.Fatal(err)
	}
	if next.fetchedBytes != 0 || len(next.fetched) != 0 {
		t.Fatal("the old result reached the replacement store")
	}
}
