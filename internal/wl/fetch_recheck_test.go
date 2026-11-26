package wl

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// partialPageLog lets a test change which value is resident between the
// disk read and the loop's handoff, without a sleep or a real page cache.
type partialPageLog struct {
	*memLog
	hot int64
}

func (l *partialPageLog) readNoWait(p []byte, off int64) error {
	if off != l.hot {
		return errAway
	}
	return l.read(p, off)
}

// Neither alternating resident pages nor writes to the current key may
// invalidate a head's completed work forever. Both scenarios left the
// head and an unrelated read queued after 80 handoffs on 8c7a2bf9.
func TestAColdHeadKeepsItsRevisionAndCompletePlan(t *testing.T) {
	for name, mode := range map[string]int{"pages": 0, "writes": 1, "reply room": 2} {
		t.Run(name, func(t *testing.T) {
			writes := mode > 0
			c, first := flowClient(t)
			n := c.node
			st, log := newTestStore(t)
			defer func() { _ = st.close() }()
			n.st = st
			st.setBudget(96<<20, 0)
			cache := &partialPageLog{memLog: log}
			st.log = cache
			n.sizes = sizeFor(st.budget)
			n.connOut = n.sizes.connOut(1)
			c.idx, c.gen = 0, 1
			n.socks, n.gens = []any{c}, []uint16{1}
			c.flow.conn = 0
			sh := &heldFetchShell{}
			n.shell = sh
			for _, k := range []string{"a", "b", "z"} {
				putValue(t, st, log, k, strings.Repeat(k, 65536))
			}
			rev := st.rev
			current := "new"
			if mode == 2 {
				current += strings.Repeat("n", 128<<10)
				n.connOut, n.reserved = 64<<10, n.sizes.reserve()
			}
			for _, k := range []string{"a", "b", "z"} {
				putValue(t, st, log, k, current)
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
			serve := func(s *stream, key, end string, at int64) {
				msg := appendBytesField(nil, 1, []byte(key))
				if end != "" {
					msg = appendBytesField(msg, 2, []byte(end))
				}
				if at > 0 {
					msg = appendVarintField(msg, 4, uint64(at))
				}
				var req rangeReq
				if err := parseRange(msg, &req); err != nil {
					t.Fatal(err)
				}
				n.serveRange(c, s, &req, msg)
			}
			if writes {
				dropRecords(st)
				cache.hot = -1
				serve(first, "a", "", 0)
			} else {
				serve(first, "a", "c", rev)
			}
			serve(c.newStream(3, kRange), "z", "", rev)
			handoffs := 0
			for range 80 {
				if len(sh.work) == 0 {
					break
				}
				take := sh.work[0]()
				sh.work = sh.work[1:]
				if writes && (mode != 2 || handoffs == 0) {
					putValue(t, st, log, "a", fmt.Sprintf("update-%d", handoffs))
					if mode != 2 {
						dropRecords(st)
					}
					if _, err := f.WriteAt(log.b, 0); err != nil {
						t.Fatal(err)
					}
				} else if !writes {
					cache.hot = 65536 - cache.hot
				}
				if err := take(); err != nil {
					t.Fatal(err)
				}
				handoffs++
			}
			if mode == 2 {
				if len(n.roomWait) != 2 {
					t.Fatal("the two cold replies did not wait for reply room")
				}
				// Enough for the newer tiny reply, not its old 128 KiB
				// size. Its hint must not strand the original current read.
				n.reserved, n.roomFreed = n.sizes.reserve()-1024, true
				n.serveRoomWait()
			}
			if len(n.awayReads) != 0 || handoffs > 4 || st.heldReads() != 0 {
				t.Fatalf("%d handoffs left %d ranges and %d revision holds", handoffs, len(n.awayReads), st.heldReads())
			}
			replies := 0
			for _, h := range c.flow.held {
				if len(h.data) == 0 {
					continue
				}
				var r pb.RangeResponse
				if err := proto.Unmarshal(h.data[5:], &r); err != nil {
					t.Fatal(err)
				}
				want := 1
				if h.sid == first.id && !writes {
					want = 2
				}
				if len(r.Kvs) != want {
					t.Fatalf("stream %d returned %d keys, want %d", h.sid, len(r.Kvs), want)
				}
				for _, kv := range r.Kvs {
					value := strings.Repeat(string(kv.Key), 65536)
					if h.sid == first.id && writes {
						value = current // the head's revision, not a moving latest value
						if mode == 2 {
							value = "update-0" // the unbuilt room wait restarts the original current request
						}
					}
					if string(kv.Value) != value {
						t.Fatal("the range did not return its held revision's whole value")
					}
				}
				replies++
			}
			if replies != 2 {
				t.Fatalf("%d replies, want both the head and unrelated read", replies)
			}
		})
	}
}

// An actual location change, unlike a page-cache change, can require a
// replan. Bound it too: a repeatedly moved head must release the FIFO.
func TestARepeatedlyMovedColdHeadReleasesTheFIFO(t *testing.T) {
	c, first := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(96<<20, 0)
	st.log = &coldFetchLog{memLog: log}
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	sh := &heldFetchShell{}
	n.shell = sh
	for _, k := range []string{"a", "z"} {
		putValue(t, st, log, k, "v")
	}
	dropRecords(st)
	f, err := os.CreateTemp(t.TempDir(), "values")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(log.b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	for i, key := range []string{"a", "z"} {
		s := first
		if i > 0 {
			s = c.newStream(3, kRange)
		}
		msg := appendBytesField(nil, 1, []byte(key))
		var req rangeReq
		if err := parseRange(msg, &req); err != nil {
			t.Fatal(err)
		}
		n.serveRange(c, s, &req, msg)
	}
	id, _ := st.find([]byte("a"))
	handoffs := 0
	for range 80 {
		if len(sh.work) == 0 {
			break
		}
		take := sh.work[0]()
		sh.work = sh.work[1:]
		st.keys[id].voff = log.add("v") // as a checkpoint moves the same state
		if _, err := f.WriteAt(log.b, 0); err != nil {
			t.Fatal(err)
		}
		if err := take(); err != nil {
			t.Fatal(err)
		}
		handoffs++
	}
	if len(n.awayReads) != 0 || handoffs > 4 || st.heldReads() != 0 {
		t.Fatalf("a moving head used %d handoffs and left %d ranges, %d holds", handoffs, len(n.awayReads), st.heldReads())
	}
}

type installFetchShell struct{ heldFetchShell }

func (*installFetchShell) membersChanged()     {}
func (*installFetchShell) logf(string, ...any) {}

// A real install invalidates every fetch wait, even when the old result
// arrives after it. The bytes are stale; the watch's wakeup is not.
func TestAnInstallWakesAWatchWhoseFetchIsDiscarded(t *testing.T) {
	n, sn := installFixture(t)
	defer func() { _ = n.st.close() }()
	n.st.log = &coldFetchLog{memLog: n.st.log.(*memLog)}
	dropRecords(n.st)
	sh := &installFetchShell{}
	n.shell = sh
	n.sizes = sizeFor(96 << 20)
	n.connOut = n.sizes.connOut(1)
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(1, kWatch)
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, key: []byte("/"), end: []byte("0"), syncRev: n.st.rev})
	n.w.unsynced = append(n.w.unsynced, i)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if !n.w.ws[i].fetchWait || len(sh.work) != 1 {
		t.Fatal("the fixture did not start a watch fetch")
	}
	take := sh.work[0]()
	sh.work = nil
	if err := n.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	// installFixture's values live in a memLog, not its empty real WAL.
	// Back the new watch's legitimate fetches with those same bytes.
	f, err := os.CreateTemp(t.TempDir(), "installed-values")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(n.st.log.(*coldFetchLog).b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	if err := take(); err != nil {
		t.Fatal(err)
	}
	for range 80 {
		if len(sh.work) > 0 {
			work := sh.work[0]
			sh.work = sh.work[1:]
			if err := work()(); err != nil {
				t.Fatal(err)
			}
		}
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
	}
	if n.w.ws[i].fetchWait && len(sh.work) == 0 {
		t.Fatal("the installed watch is stranded waiting for a discarded fetch")
	}
}

func TestAnInstallReleasesAHeldReplyWhoseFetchIsDiscarded(t *testing.T) {
	n, sn := installFixture(t)
	defer func() { _ = n.st.close() }()
	log := n.st.log.(*memLog)
	n.st.log = &coldFetchLog{memLog: log}
	sh := &installFetchShell{}
	n.shell = sh
	n.sizes = sizeFor(96 << 20)
	n.connOut = n.sizes.connOut(1)
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(1, kPut)
	id, _ := n.st.find([]byte("/follower"))
	rec, err := n.st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	n.holdPut(c, s, rec)
	dropRecords(n.st)
	n.serveHeld()
	if len(sh.work) != 1 || len(n.heldReplies) != 1 {
		t.Fatal("the fixture did not start a held-reply fetch")
	}
	n.serveRoomWait() // consume its earlier room wake while it still fetches
	take := sh.work[0]()
	sh.work = nil
	if err := n.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	if err := take(); err != nil {
		t.Fatal(err)
	}
	for range 80 {
		n.serveRoomWait()
	}
	if len(n.heldReplies) != 0 {
		t.Fatal("the installed node retains a held reply and the old store")
	}
}

// A held write's already-applied result must not allocate unbounded
// per-value work either. A plan past the cap aborts its stream (outcome
// unknown), never reports a refusal that would invite repeating a write.
func TestAHeldReplyCapsItsPerValueFetchWork(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(96<<20, 0)
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	st.log = &coldFetchLog{memLog: log}
	sh := &heldFetchShell{}
	n.shell = sh
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	var recs [][]byte
	for i := range 8193 {
		key := fmt.Sprintf("a%05d", i)
		putValue(t, st, log, key, "v")
		id, _ := st.find([]byte(key))
		rec, err := st.live(id)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec)
	}
	n.holdDel(c, s, int64(len(recs)), recs)
	dropRecords(st)
	n.serveHeld()
	if len(sh.work) != 0 || len(n.heldReplies) != 0 || cap(st.away) != 0 || c.heldCounted != 0 {
		t.Fatal("an oversized per-value plan was allocated or left held after its stream ended")
	}
}

// One revision is indivisible on the wire, not in its fetch allocation.
// Thousands of tiny delete-prev events must use bounded big-watch pieces
// before hitting the metadata cap, not fail the node at the cap.
func TestTinyValuesInOneWatchRevisionUseBoundedPieces(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(96<<20, 0)
	st.log = &coldFetchLog{memLog: log}
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s.kind = kWatch
	c.flow.conn, s.win = 10<<20, 10<<20
	sh := &heldFetchShell{}
	n.shell = sh
	for i := range 8193 {
		putValue(t, st, log, fmt.Sprintf("a%05d", i), "v")
	}
	st.rev++
	rev := st.rev
	for i := range 8193 {
		id, _ := st.find(fmt.Appendf(nil, "a%05d", i))
		if _, _, err := st.del(id, rev, false); err != nil {
			t.Fatal(err)
		}
	}
	st.endApply()
	dropRecords(st)
	f, err := os.CreateTemp(t.TempDir(), "values")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(log.b); err != nil {
		t.Fatal(err)
	}
	n.wal = &wal{rfds: []int{int(f.Fd())}}
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, key: []byte("a"), end: []byte("b"), syncRev: rev, prevKV: true})
	n.w.unsynced = append(n.w.unsynced, i)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if !n.w.ws[i].big {
		t.Fatal("the tiny-value revision did not plan bounded watch pieces")
	}
	for range 80 {
		n.pumpBigWatches()
		if n.fetchBytes > fetchWorkMax(st.budget) {
			t.Fatal("a tiny-value watch piece passed its work allowance")
		}
		if len(sh.work) > 0 {
			work := sh.work[0]
			sh.work = sh.work[1:]
			if err := work()(); err != nil {
				t.Fatal(err)
			}
		}
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
		if len(n.bigWatches) == 0 && n.w.ws[i].syncRev == 0 {
			break
		}
	}
	if len(n.bigWatches) != 0 || n.w.ws[i].syncRev != 0 || !n.w.ws[i].active {
		t.Fatal("the bounded tiny-value watch failed to finish its revision")
	}
}

// The handoff's live allocation includes places, results, payload and
// retained scratch. A range of 98,304 one-byte values charged just 96 KiB
// on 8c7a2bf9, though those four parts alone held 9,846,784 bytes.
func TestTinyValuesHaveABoundedChargedHandoff(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.setBudget(96<<20, 0)
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	st.log = &coldFetchLog{memLog: log}
	sh := &heldFetchShell{}
	n.shell = sh
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	for i := range 98304 {
		putValue(t, st, log, fmt.Sprintf("a%05d", i), "v")
	}
	dropRecords(st)
	msg := appendBytesField(appendBytesField(nil, 1, []byte("a")), 2, []byte("b"))
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, s, &req, msg)
	if len(sh.work) != 1 {
		t.Fatalf("%d fetches, want one charged handoff", len(sh.work))
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
	take := sh.work[0]()
	defer runtime.KeepAlive(take)
	entries := int64(n.stats.fetched)
	lower := entries*(1+int64(unsafe.Sizeof(logValue{}))+int64(unsafe.Sizeof(fetchedValue{}))) + int64(cap(st.away))*int64(unsafe.Sizeof(logValue{}))
	if n.fetchBytes < lower {
		t.Fatalf("%d charged bytes do not cover %d live bytes of plan/results/payload/scratch", n.fetchBytes, lower)
	}
	if entries > 8192 || n.fetchBytes > fetchWorkMax(st.budget) || cap(st.away) != 0 {
		t.Fatalf("%d entries charge %d bytes, with %d scratch entries retained", entries, n.fetchBytes, cap(st.away))
	}
	t.Logf("%d entries: %d bytes charged, %d bytes in the original allocation lower bound", entries, n.fetchBytes, lower)
}

// Cache bookkeeping must remain bounded after replacement and expiry,
// not retain an ever-growing order or an empty high-water map.
func TestFetchedCacheChargesAndReleasesItsMetadata(t *testing.T) {
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	at := time.Unix(1, 0)
	for range 64 {
		st.keepFetched(1, fetchedValue{val: []byte("v"), at: at})
		st.trimFetched(at, 3<<20)
	}
	if len(st.fetchedOrder) > 2*len(st.fetched)+16 {
		t.Fatal("replaced same-turn values retain an unbounded order")
	}
	if st.fetchedBytes <= 1+int64(unsafe.Sizeof(fetchedValue{})) {
		t.Fatal("the cache counts payload alone, without its allocation metadata")
	}
	st.trimFetched(at.Add(time.Second), 3<<20)
	if st.fetched != nil || st.fetchedOrder != nil || st.fetchedBytes != 0 {
		t.Fatal("the empty cache retains its map or order allocation")
	}
}

// Queued metadata is work too. Keep at most one generic plan and the
// FIFO head's plan behind active work; dropped generic plans retry on
// that work's wake, rather than accumulating offset arrays indefinitely.
func TestQueuedFetchMetadataIsBounded(t *testing.T) {
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	st.setBudget(1, 0)
	sh := &heldFetchShell{}
	n := &node{st: st, shell: sh, wal: &wal{}, fetching: map[int64]bool{}}
	for i := range 16 {
		st.away = []logValue{{off: int64(i * 1024), len: 64}}
		n.fetchAway()
	}
	n.queueFetch([]logValue{{off: 65536, len: 64}}, true)
	if len(sh.work) != 1 || len(n.fetchQueue) > 2 || len(n.fetching) > 2 {
		t.Fatalf("%d active, %d queued, %d indexed; want one active and at most two queued plans", len(sh.work), len(n.fetchQueue), len(n.fetching))
	}
	for len(sh.work) > 0 {
		work := sh.work[0]
		sh.work = sh.work[1:]
		if err := work()(); err != nil {
			t.Fatal(err)
		}
	}
	if len(n.fetchQueue) != 0 || len(n.fetching) != 0 || n.fetchBytes != 0 {
		t.Fatal("a bounded queued plan did not finish and release its index")
	}
}

func TestAStreamErrorReleasesItsReplyRoomRequestCharge(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes = sizeFor(96 << 20)
	n.connOut = 64 << 10
	n.reserved = n.sizes.reserve()
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	putValue(t, st, log, "k", strings.Repeat("v", 128<<10))
	msg := appendBytesField(nil, 1, []byte("k"))
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, s, &req, msg)
	if len(n.roomWait) != 1 || c.heldCounted != int64(len(msg)) {
		t.Fatal("the fixture did not leave a counted request waiting for room")
	}
	n.replyError(c, s.id, codeInternal, "request error")
	n.roomFreed = true
	n.serveRoomWait()
	c.dropAll()
	if len(n.roomWait) != 0 || c.heldCounted != 0 || c.roomWaiters != 0 {
		t.Fatal("the ended stream left a reply-room request charged")
	}
}

func TestAStreamErrorReleasesItsColdRequestCharge(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, log := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	st.log = &coldFetchLog{memLog: log}
	st.setBudget(96<<20, 0)
	n.sizes = sizeFor(st.budget)
	n.connOut = n.sizes.connOut(1)
	sh := &heldFetchShell{}
	n.shell = sh
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	putValue(t, st, log, "k", "v")
	dropRecords(st)
	msg := appendBytesField(nil, 1, []byte("k"))
	var req rangeReq
	if err := parseRange(msg, &req); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, s, &req, msg)
	if len(sh.work) != 1 || c.heldCounted != int64(len(msg)) {
		t.Fatal("the fixture did not start a counted cold request")
	}
	n.wal = &wal{}
	take := sh.work[0]()
	n.replyError(c, s.id, codeInternal, "request error")
	if err := take(); err != nil {
		t.Fatal(err)
	}
	c.dropAll()
	if len(n.awayReads) != 0 || c.heldCounted != 0 || st.heldReads() != 0 {
		t.Fatalf("after error/completion/close: %d queued, %d bytes charged, %d holds", len(n.awayReads), c.heldCounted, st.heldReads())
	}
}
