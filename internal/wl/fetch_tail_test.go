package wl

import (
	"fmt"
	"os"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// heldFetchShell keeps both halves of offLoop under the test's control.
type heldFetchShell struct {
	shell
	work []func() func() error
}

func (s *heldFetchShell) offLoop(work func() func() error) { s.work = append(s.work, work) }

// coldFetchLog never serves a value from the page cache. The fetch reads
// the same bytes through the node's log file, not this reader.
type coldFetchLog struct{ *memLog }

func (*coldFetchLog) readNoWait([]byte, int64) error { return errAway }

// Completed fetches count too: until the loop takes a result, no next
// batch allocates its values. Unrelated ranges are not read again on a
// completion, and every range completes even with no page cache at all.
func TestValueFetchesWaitForTheLoopToTakeThePreviousBatch(t *testing.T) {
	c, firstStream := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(1, 0) // no record survives a read, nor a fetched value a trim
	st.log = &coldFetchLog{log}
	n.sizes = sizeFor(96 << 20)
	n.connOut = 512 << 10
	n.fetching = map[int64]bool{}
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	c.flow.conn = 0
	sh := &heldFetchShell{}
	n.shell = sh
	for i := range 6 {
		putValue(t, st, log, fmt.Sprintf("/f/%d/%d", i/2, i%2), strings.Repeat("v", 16<<10))
	}
	f, err := os.CreateTemp(t.TempDir(), "values")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(log.b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	for i := range 3 {
		s := firstStream
		if i > 0 {
			s = c.newStream(uint32(2*i+1), kRange)
		}
		msg := appendBytesField(appendBytesField(nil, 1, fmt.Appendf(nil, "/f/%d/", i)), 2, fmt.Appendf(nil, "/f/%d0", i))
		var req rangeReq
		if err := parseRange(msg, &req); err != nil {
			t.Fatal(err)
		}
		n.serveRange(c, s, &req, msg)
	}
	for i := range 3 {
		if len(sh.work) != 1 {
			t.Fatalf("%d fetches started, want only one before the loop takes it", len(sh.work))
		}
		take := sh.work[0]() // read finished, but the loop has not taken it
		sh.work = nil
		if st.loads != uint64(2*i) || len(n.awayReads) != 3-i {
			t.Fatal("a range advanced before the loop took its values")
		}
		if err := take(); err != nil {
			t.Fatal(err)
		}
		if st.loads != uint64(2*(i+1)) || len(n.awayReads) != 2-i {
			t.Fatalf("completion %d: %d loads and %d waiting ranges", i, st.loads, len(n.awayReads))
		}
		if st.fetchedBytes > fetchedMax(st.budget) {
			t.Fatal("the fetched values passed their share")
		}
	}
	if len(sh.work) != 0 || len(n.fetching) != 0 || n.stats.fetched != 6 {
		t.Fatal("the ranges fetched values again or left a fetch behind")
	}
}

// Fetches whose bytes fit in the share still run together. A batch past
// it runs alone, and releasing a result on the loop starts the next.
func TestValueFetchesUseTheirShareAndLetOneLargerBatchThrough(t *testing.T) {
	for name, large := range map[string]bool{"within": false, "larger": true} {
		t.Run(name, func(t *testing.T) {
			st, _ := newTestStore(t)
			defer func() { _ = st.close() }()
			st.setBudget(32*128, 0) // 128 bytes of fetched values
			sh := &heldFetchShell{}
			n := &node{st: st, wal: &wal{}, shell: sh, fetching: map[int64]bool{}}
			for i := range 3 {
				size := uint32(64)
				if large && i == 0 {
					size = 256
				}
				st.away = []logValue{{off: int64(i * 1024), len: size}}
				n.fetchAway()
			}
			want, bytes := 2, int64(128)
			if large {
				want, bytes = 1, 256
			}
			if len(sh.work) != want || n.fetchBytes != bytes || len(n.fetchQueue) != 3-want {
				t.Fatalf("%d fetches of %d bytes, %d queued", len(sh.work), n.fetchBytes, len(n.fetchQueue))
			}
			for len(sh.work) > 0 {
				take := sh.work[0]()
				sh.work = sh.work[1:]
				if err := take(); err != nil {
					t.Fatal(err)
				}
				if st.fetchedBytes > fetchedMax(st.budget) {
					t.Fatal("a completed batch was kept past the share")
				}
			}
			if n.fetchBytes != 0 || len(n.fetchQueue) != 0 || len(n.fetching) != 0 {
				t.Fatal("a fetch stayed queued or counted after every result was taken")
			}
		})
	}
}

// A range that waits for reply room does not read its values again for
// every small return of room. Eighty ranges of 100 values did that in
// TestFetchedValuesStayWithinTheirShare (#833): thousands of reads of
// the same ranges, and gigabytes of records built only to be thrown away.
// The store here keeps no record, so each rebuild counts in loads.
func TestARangeWaitingForRoomDoesNotReadItsValuesAgain(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(1, 0)
	n.sizes = sizeFor(96 << 20)
	n.connOut = 512 << 10
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	c.flow.conn, s.win = 0, 0
	const keys = 100
	value := strings.Repeat("v", 16<<10)
	for i := range keys {
		putValue(t, st, log, fmt.Sprintf("/f/%04d", i), value)
	}
	n.reserved = n.sizes.reserve() // other clients hold it all
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/f/")), 2, []byte("/f0"))
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, s, &req, msg)
	if len(n.roomWait) != 1 || st.loads != keys {
		t.Fatalf("%d waiting ranges and %d loads, want one and %d", len(n.roomWait), st.loads, keys)
	}
	before := st.loads
	counted := c.heldCounted
	for range 32 {
		n.reserved -= 1024 // too little for the waiting reply
		n.roomFreed = true
		n.serveRoomWait()
	}
	if st.loads != before {
		t.Fatalf("the range read %d values again without room for its reply", st.loads-before)
	}
	if len(n.roomWait) != 1 || c.roomWaiters != 1 || c.heldCounted != counted {
		t.Fatal("the range no longer waits with just its request counted")
	}
	n.reserved = 0
	n.roomFreed = true
	n.serveRoomWait()
	if len(n.roomWait) != 0 || c.roomWaiters != 0 || c.heldCounted != 0 || st.loads != before+keys {
		t.Fatalf("after room came back: %d waiting, %d waiters, %d bytes counted, %d loads", len(n.roomWait), c.roomWaiters, c.heldCounted, st.loads-before)
	}
	if len(c.flow.held) == 0 {
		t.Fatal("the range sent no reply")
	}
	var reply pb.RangeResponse
	if err := proto.Unmarshal(c.flow.held[0].data[5:], &reply); err != nil {
		t.Fatal(err)
	}
	if len(reply.Kvs) != keys {
		t.Fatalf("the range returned %d keys, want %d", len(reply.Kvs), keys)
	}
	for i, kv := range reply.Kvs {
		if string(kv.Key) != fmt.Sprintf("/f/%04d", i) || string(kv.Value) != value {
			t.Fatalf("record %d is not its key and whole value", i)
		}
	}
}

// A waiting range's old size must not hide a smaller reply after writes
// or a store install, a compacted-revision error, or a reply that must go
// in pieces after its client's share shrinks.
func TestAWaitingRangeChecksItsSizeAgainWhenTheStoreChanges(t *testing.T) {
	for _, change := range []string{"write", "compact", "install", "share"} {
		t.Run(change, func(t *testing.T) {
			c, s := flowClient(t)
			n := c.node
			st, log := newTestStore(t)
			defer func() { _ = st.close() }()
			n.st = st
			st.setBudget(1, 0)
			n.sizes = sizeFor(96 << 20)
			n.connOut = 512 << 10
			c.idx, c.gen = 0, 1
			n.socks, n.gens = []any{c}, []uint16{1}
			c.flow.conn, s.win = 0, 0
			keys, size := 100, 16<<10
			if change == "share" {
				keys, size, n.connOut = 2, 1400<<10, 1<<20
			}
			for i := range keys {
				putValue(t, st, log, fmt.Sprintf("/f/%04d", i), strings.Repeat("v", size))
			}
			n.reserved = n.sizes.reserve()
			msg := appendBytesField(appendBytesField(nil, 1, []byte("/f/")), 2, []byte("/f0"))
			if change == "compact" {
				msg = appendVarintField(msg, 4, uint64(st.rev-1))
			}
			var req rangeReq
			if err := parseRange(msg, &req); err != nil {
				t.Fatal(err)
			}
			n.serveRange(c, s, &req, msg)
			if len(n.roomWait) != 1 {
				t.Fatal("the range did not wait for room")
			}
			switch change {
			case "write":
				for i := range 100 {
					putValue(t, st, log, fmt.Sprintf("/f/%04d", i), "small")
				}
			case "compact":
				st.compact(st.rev) // no change to st.rev
			case "install":
				next, nextLog := newTestStore(t)
				defer func() { _ = next.close() }()
				for i := range 100 {
					putValue(t, next, nextLog, fmt.Sprintf("/f/%04d", i), "small")
				}
				n.st = next // the same revision, different values
			case "share":
				n.connOut = 1024 // the reply can no longer fit, even alone
			}
			n.roomFreed = true
			n.serveRoomWait()
			if len(n.roomWait) != 0 || c.roomWaiters != 0 || c.heldCounted != 0 {
				t.Fatal("the range still waits for its old size")
			}
			if change == "share" && len(n.bigs) != 1 {
				t.Fatal("the reply past its new share and the reserve did not go in pieces")
			}
		})
	}
}
