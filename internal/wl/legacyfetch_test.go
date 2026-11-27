package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

type corruptCursorLog struct{ *memLog }

func (*corruptCursorLog) readNoWait([]byte, int64) error { return errAway }
func (l *corruptCursorLog) read(dst []byte, off int64) error {
	if err := l.memLog.read(dst, off); err != nil {
		return err
	}
	if len(dst) > 0 {
		dst[0] ^= 1
	}
	return nil
}

func TestFetchCursorsRejectCorruptionAndMarkTheStoreBad(t *testing.T) {
	for _, mode := range []string{"cursor", "legacy", "material"} {
		t.Run(mode, func(t *testing.T) {
			c, s, st, log, l := fillRangeFixture(t)
			n := c.node
			count := 1
			switch mode {
			case "legacy":
				count = 2
			case "material":
				count = 3
			}
			for i := range count {
				putValue(t, st, log, fmt.Sprintf("/%c", 'a'+i), string(bytes.Repeat([]byte{byte('a' + i)}, 900<<10)))
			}
			for i := range st.keys[:st.nkeys] {
				st.keys[i].rec = 0
			}
			st.log = &corruptCursorLog{memLog: log}
			msg := appendBytesField(nil, 1, []byte("/a"))
			if mode != "cursor" {
				msg = appendBytesField(msg, 2, []byte("/d"))
				msg = appendVarintField(msg, 3, 1)
				msg = appendVarintField(msg, 5, 1)
				msg = appendVarintField(msg, 6, sortByValue)
				msg = appendVarintField(msg, 8, 1)
			}
			serveRoomRange(t, c, s, msg)
			for step := 0; len(n.fills) > 0 || n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
				if step > 8 {
					t.Fatal("corruption did not end the operation")
				}
				n.pumpFills()
				if n.fillActive != nil || n.legacyRunning != nil {
					awaitFetchResult(t, l)
					if err := l.runPosts(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !errors.Is(st.bad, ErrBadValue) || len(fillQueuedBytes(t, c)) != 0 {
				t.Fatal("corruption was published or did not stop the store")
			}
		})
	}
}

func TestLegacyPagedControlAndEntryCharges(t *testing.T) {
	if unsafe.Sizeof(legacyEntry{}) != 24 || unsafe.Sizeof(legacyPage{}) != stageUnit || unsafe.Sizeof(legacyBatch{})+unsafe.Sizeof(materialReply{})+unsafe.Sizeof(fetchWorkLease{}) > stageUnit {
		t.Fatal("legacy roots/pages no longer fit their charged capacities")
	}
}

func TestLegacyOvershootUsesItsFundedMaterializationView(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	for _, key := range []string{"/a", "/b", "/c"} {
		putValue(t, st, log, key, string(bytes.Repeat([]byte(key[1:]), 900<<10)))
	}
	view := st.rev
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	msg := appendBytesField(nil, 1, []byte("/a"))
	msg = appendBytesField(msg, 2, []byte("/d"))
	msg = appendVarintField(msg, 3, 1)
	msg = appendVarintField(msg, 5, 1)
	msg = appendVarintField(msg, 6, sortByValue)
	msg = appendVarintField(msg, 8, 1)
	serveRoomRange(t, c, s, msg)
	n := c.node
	b := n.legacyPending
	if b == nil || !b.material || !b.prepared || b.materialCharge == 0 || n.fillBook.used != fetchCoreWork+stageUnit+b.materialIndexCharge {
		t.Fatal("overshoot was not a single funded materialization view")
	}
	n.pumpFills()
	awaitFetchResult(t, l)
	putValue(t, st, log, "/a", "overwritten")
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		n.pumpFills()
	}
	data := fillQueuedBytes(t, c)
	var response pb.RangeResponse
	if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Key) != "/a" || len(response.Kvs[0].Value) != 0 || response.Header.Revision != view {
		t.Fatal("materialized sort changed its admitted ordering/view")
	}
	if b.data != nil || b.materialCharge != 0 || b.materialIndex != nil || st.heldReads() != 0 || n.fillBook.used != fetchCoreWork || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool {
		t.Fatal("small selected result was not funded/copied before retiring the all-input view")
	}
	id, _ := st.find([]byte("/b"))
	if st.keys[id].rec == 0 {
		t.Fatal("materialized current record lost normal slab reuse")
	}
}

func TestWholeLegacyMaterializationCommitsWithoutCopyOverlap(t *testing.T) {
	for _, kind := range []uint8{kRange, kRangeStream} {
		for _, plain := range []bool{true, false} {
			t.Run(strconv.Itoa(int(kind))+"/plain="+strconv.FormatBool(plain), func(t *testing.T) {
				c, s, st, log, l := fillRangeFixture(t)
				s.kind = kind
				c.plain = plain
				n := c.node
				n.connOut = n.sizes.connOut(1)
				for _, key := range []string{"/a", "/b", "/c"} {
					putValue(t, st, log, key, string(bytes.Repeat([]byte(key[1:]), 900<<10)))
				}
				view := st.rev
				for _, key := range []string{"/a", "/b", "/c"} {
					putValue(t, st, log, key, "later")
				}
				header := st.rev
				fd := roomSocket(t, c)
				l.epfd = -1
				msg := appendBytesField(nil, 1, []byte("/a"))
				msg = appendBytesField(msg, 2, []byte("/d"))
				msg = appendVarintField(msg, 4, uint64(view))
				msg = appendVarintField(msg, 5, 1)
				msg = appendVarintField(msg, 6, sortByValue)
				serveRoomRange(t, c, s, msg)
				for step := 0; n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
					if step > 8 {
						t.Fatal("whole main-admitted materialization did not complete")
					}
					n.pumpFills()
					if n.legacyRunning != nil {
						awaitFetchResult(t, l)
						if err := l.runPosts(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if c.waiting() != 0 || len(c.flow.held) == 0 || c.flow.held[0].material == nil || st.heldReads() != 0 || n.fillBook.used != fetchCoreWork+c.flow.held[0].material.batch.materialIndexCharge {
					t.Fatal("complete reply was not independently queued before DATA")
				}
				// Destroy its former store before any DATA publication. Queue
				// ownership, not store/history lifetime, now protects this reply.
				replacement, _ := newTestStore(t)
				replacement.setBudget(83<<20, 0)
				n.st = replacement
				if err := st.close(); err != nil {
					t.Fatal(err)
				}
				beforePhysical, beforeSocket := n.cout, cap(c.out)
				beforeW := n.fillBook.used
				c.flow.conn = 1
				c.flow.streams[s.id] = 1
				c.drain()
				if n.cout-beforePhysical != int64(cap(c.out)-beforeSocket) || n.fillBook.used != beforeW {
					t.Fatal("partial scatter drain freed live materialization")
				}
				var wire []byte
				for step := 0; len(c.flow.held) > 0 || c.waiting() > 0; step++ {
					if step > 4096 {
						t.Fatal("complete materialization stopped draining")
					}
					c.flow.conn = 1 << 30
					c.flow.streams[s.id] = 1 << 30
					c.drain()
					n.markDirty(&c.sock)
					l.flush()
					readFillSocket(t, fd, &wire)
					if cap(c.out) > 64<<10 {
						t.Fatal("materialized output grew unbounded socket backing")
					}
				}
				if !plain {
					receiver := sock{rd: c.rd, in: wire, iw: len(wire), pt: make([]byte, 64<<10)}
					if err := receiver.open(); err != nil {
						t.Fatal(err)
					}
					wire = receiver.pt[:receiver.pw]
				}
				data := fillData(t, wire)
				var replies []*pb.RangeResponse
				if kind == kRangeStream {
					replies = decodeFillStream(t, data)
				} else {
					var response pb.RangeResponse
					if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil {
						t.Fatal("invalid materialized response")
					}
					replies = []*pb.RangeResponse{&response}
				}
				seen := 0
				for _, response := range replies {
					for _, kv := range response.Kvs {
						if !bytes.Equal(kv.Value, bytes.Repeat([]byte{byte('a' + seen)}, 900<<10)) || kv.ModRevision > view {
							t.Fatal("queued materialization changed historical values")
						}
						seen++
					}
				}
				last := replies[len(replies)-1]
				if seen != 3 || last.Header.Revision != header || last.Count != 3 || last.More || n.stagePromises != 0 || c.stages != 0 || n.fillBook.used != fetchCoreWork {
					t.Fatal("materialized wire/header/count/ownership changed")
				}
			})
		}
	}
}

func TestLegacyUnstartedRetryUsesReplacementStoreToken(t *testing.T) {
	for _, records := range []int{2, 3} {
		for _, kind := range []uint8{kRange, kRangeStream} {
			t.Run(fmt.Sprintf("%d/%d", records, kind), func(t *testing.T) {
				c, s, st, log, l := fillRangeFixture(t)
				s.kind = kind
				n := c.node
				for i := range records {
					putValue(t, st, log, fmt.Sprintf("/%c", 'a'+i), string(bytes.Repeat([]byte{byte('a' + i)}, 900<<10)))
				}
				for i := range st.keys[:st.nkeys] {
					st.keys[i].rec = 0
				}
				msg := appendBytesField(nil, 1, []byte("/a"))
				msg = appendBytesField(msg, 2, []byte("/d"))
				msg = appendVarintField(msg, 3, 1)
				msg = appendVarintField(msg, 5, 1)
				msg = appendVarintField(msg, 6, sortByValue)
				msg = appendVarintField(msg, 8, 1)
				serveRoomRange(t, c, s, msg)
				n.pumpFills()
				old := n.legacyRunning
				if old == nil {
					t.Fatal("legacy source did not start")
				}
				before := n.fillBook.used
				replacement, nextLog := newTestStore(t)
				replacement.setBudget(83<<20, 0)
				for i := range records {
					value := byte('b' + i)
					if i == 1 {
						value = 'a'
					}
					putValue(t, replacement, nextLog, fmt.Sprintf("/%c", 'a'+i), string(bytes.Repeat([]byte{value}, 900<<10)))
				}
				for i := range replacement.keys[:replacement.nkeys] {
					replacement.keys[i].rec = 0
				}
				replacement.log = &fillColdLog{memLog: nextLog}
				n.invalidateFills(st)
				n.st = replacement
				if err := st.close(); err != nil {
					t.Fatal(err)
				}
				if n.fillBook.used != before {
					t.Fatal("install released worker-owned storage before ACK")
				}
				for step := 0; n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
					if step > 12 {
						t.Fatal("unstarted replacement retry did not progress")
					}
					if n.legacyRunning != nil {
						awaitFetchResult(t, l)
						if err := l.runPosts(); err != nil {
							t.Fatal(err)
						}
					}
					n.pumpFills()
					if n.legacyRunning != nil && n.legacyRunning != old {
						current := n.legacyRunning
						used := n.fillBook.used
						if err := n.finishLegacyPiece(old, fillToken{op: old.op, piece: old.piece}, nil); err != nil {
							t.Fatal(err)
						}
						if n.legacyRunning != current || n.fillBook.used != used {
							t.Fatal("stale legacy ACK changed the replacement owner")
						}
					}
				}
				data := fillQueuedBytes(t, c)
				var response *pb.RangeResponse
				if kind == kRangeStream {
					parts := decodeFillStream(t, data)
					response = parts[len(parts)-1]
				} else {
					response = &pb.RangeResponse{}
					if len(data) < 5 || proto.Unmarshal(data[5:], response) != nil {
						t.Fatal("invalid replacement response")
					}
				}
				if len(response.Kvs) != 1 || string(response.Kvs[0].Key) != "/b" || response.Header.Revision != replacement.rev {
					t.Fatal("old store values escaped with the replacement header")
				}
			})
		}
	}
}

func TestLegacyDenseMetadataUsesBoundedMaterializationLookup(t *testing.T) {
	const value = "v"
	// #908 excludes only main's two scratch arrays, with an actual
	// same-state reference. All materialization/lookup/output still counts.
	baseline := mainDenseSortScratch(t, value)
	c, s, st, log, l := fillRangeFixture(t)
	var scratch int64
	c.node.measureSort = func(a, b int) { scratch = max(scratch, int64(a)*24+int64(b)*96) }
	const count = 115000
	for i := range count {
		key := []byte{byte(i >> 16), byte(i >> 8), byte(i)}
		putValue(t, st, log, string(key), value)
	}
	for i := range st.keys[:st.nkeys] {
		st.keys[i].rec = 0
	}
	msg := appendBytesField(nil, 1, []byte{0})
	msg = appendBytesField(msg, 2, []byte{2})
	msg = appendVarintField(msg, 3, 1)
	msg = appendVarintField(msg, 5, 1)
	msg = appendVarintField(msg, 6, sortByValue)
	msg = appendVarintField(msg, 8, 1)
	serveRoomRange(t, c, s, msg)
	n := c.node
	for step := 0; n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
		if step > 1200 {
			t.Fatal("dense view did not make finite progress")
		}
		n.pumpFills()
		if n.fillBook.used > n.fillBook.limit {
			t.Fatal("dense view exceeded W")
		}
		if n.cpool+n.cout+n.legacyPromises+n.stagePromises+int64(n.clients)*idleConn > n.sizes.clientPool+n.sizes.reserve() || n.stageCredits.used+n.stageOther(0) > n.sizes.clientPool {
			t.Fatal("dense materialization exceeded client credit")
		}
		if n.legacyRunning != nil {
			awaitFetchResult(t, l)
			n.now = n.now.Add(2 * time.Second)
			n.dropFetched()
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
	}
	data := fillQueuedBytes(t, c)
	var response pb.RangeResponse
	if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil || len(response.Kvs) != 1 || response.Count != count || len(response.Kvs[0].Value) != 0 {
		t.Fatal("dense legacy lookup changed main's admitted result")
	}
	if scratch == 0 || scratch > baseline {
		t.Fatalf("#897 enlarged #908 scratch: %d > %d", scratch, baseline)
	}
	t.Logf("scratch=%d main=%d; all new storage independently funded", scratch, baseline)
	n.pumpFills()
	if n.fillBook.used != fetchCoreWork || len(st.fetched) > 0 || len(st.away) > 0 {
		t.Fatal("dense view leaked metadata or built another cache")
	}
}

func mainDenseSortScratch(t *testing.T, value string) int64 {
	t.Helper()
	c, s, st, log, _ := fillRangeFixture(t)
	c.node.cursorFetch = false
	st.log = log
	var scratch int64
	c.node.measureSort = func(a, b int) { scratch = max(scratch, int64(a)*24+int64(b)*96) }
	for i := range 115000 {
		putValue(t, st, log, string([]byte{byte(i >> 16), byte(i >> 8), byte(i)}), value)
	}
	msg := appendBytesField(nil, 1, []byte{0})
	msg = appendBytesField(msg, 2, []byte{2})
	msg = appendVarintField(msg, 3, 1)
	msg = appendVarintField(msg, 5, 1)
	msg = appendVarintField(msg, 6, sortByValue)
	msg = appendVarintField(msg, 8, 1)
	serveRoomRange(t, c, s, msg)
	r := roomRangeReply(t, c)
	if r.Count != 115000 || len(r.Kvs) != 1 {
		t.Fatal("main dense reference selection")
	}
	return scratch
}

func TestSharedLegacyCursorRuntimeBudgetMatrix(t *testing.T) {
	for _, budget := range []int64{83 << 20, 137 << 20, 191 << 20} {
		for _, first := range []string{"cursor", "legacy"} {
			for _, cancelRunning := range []bool{false, true} {
				t.Run(fmt.Sprintf("%dMiB/%s-first/cancel=%t", budget>>20, first, cancelRunning), func(t *testing.T) {
					c, s, st, log, l := fillRangeFixture(t)
					n := c.node
					n.sizes = sizeFor(budget)
					st.setBudget(budget, 0)
					other := partClient(t, n, 1)
					other.flow.conn = 0
					n.socks, n.gens = []any{c, other}, []uint16{1, 1}
					n.connOut = n.sizes.connOut(2)
					t.Cleanup(func() { other.dropAll(); n.pumpFills() })
					putValue(t, st, log, "/a", string(bytes.Repeat([]byte("a"), 1280<<10)))
					putValue(t, st, log, "/b", string(bytes.Repeat([]byte("b"), 1280<<10)))
					putValue(t, st, log, "/z", string(bytes.Repeat([]byte("z"), 64<<10)))
					for i := range st.keys[:st.nkeys] {
						st.keys[i].rec = 0
					}
					legacy := other.newStream(1, kRange)
					legacyMsg := appendBytesField(nil, 1, []byte("/a"))
					legacyMsg = appendBytesField(legacyMsg, 2, []byte("/c"))
					legacyMsg = appendVarintField(legacyMsg, 3, 1)
					legacyMsg = appendVarintField(legacyMsg, 5, 1)
					legacyMsg = appendVarintField(legacyMsg, 6, sortByValue)
					legacyMsg = appendVarintField(legacyMsg, 8, 1)
					cursorMsg := appendBytesField(nil, 1, []byte("/z"))
					if first == "cursor" {
						serveRoomRange(t, c, s, cursorMsg)
						n.pumpFills()
						if n.fillActive == nil {
							t.Fatal("cursor did not start")
						}
						serveRoomRange(t, other, legacy, legacyMsg)
					} else {
						serveRoomRange(t, other, legacy, legacyMsg)
						n.pumpFills()
						if n.legacyRunning == nil {
							t.Fatal("legacy did not start")
						}
						serveRoomRange(t, c, s, cursorMsg)
					}
					putStream := other.newStream(3, kPut)
					previous := appendKV(nil, []byte("/a"), bytes.Repeat([]byte("a"), 1280<<10), 2, 2, 1, 0)
					n.holdPut(other, putStream, previous)
					n.serveHeld()
					if len(n.heldReplies) != 1 || !n.heldReplies[0].fetching {
						t.Fatal("main held-reply consumer was not queued")
					}
					peak := n.fillBook.used
					peakP := n.stageCredits.used + n.stageOther(0)
					if cancelRunning {
						before := n.fillBook.used
						if first == "cursor" {
							n.dropWaits(c, s.id)
						} else {
							n.dropWaits(other, legacy.id)
						}
						if n.fillBook.used != before {
							t.Fatal("running cancellation released work before ACK")
						}
					}
					blocked := false
					for step := 0; len(n.fills) > 0 || len(n.heldReplies) > 0 || n.legacyAgain || n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
						if step > 64 {
							t.Fatal("shared consumers did not finish")
						}
						peak = max(peak, n.fillBook.peak)
						peakP = max(peakP, n.stageCredits.used+n.stageOther(0))
						if peakP > n.sizes.clientPool {
							t.Fatalf("combined paths overspent independently backed P: %d > %d", peakP, n.sizes.clientPool)
						}
						if n.fillBook.used > n.fillBook.limit {
							t.Fatal("shared W exceeded")
						}
						if budget == 83<<20 && (n.fillActive != nil && n.legacyPending != nil && !n.legacyPending.prepared || n.legacyRunning != nil && len(n.fills) > 0 && n.fillActive == nil) {
							blocked = true
						}
						if n.cpool+n.cout+n.stagePromises+n.legacyPromises+int64(n.clients+n.stageOrphans)*idleConn > n.sizes.clientPool+n.sizes.reserve() {
							t.Fatal("changed storage exceeded the client pool plus reserve")
						}
						if n.fillActive != nil || n.legacyRunning != nil {
							awaitFetchResult(t, l)
							n.now = n.now.Add(2 * time.Second)
							n.dropFetched()
							if err := l.runPosts(); err != nil {
								t.Fatal(err)
							}
						}
						n.pumpFills()
					}
					if budget == 83<<20 && !blocked {
						t.Fatal("83 MiB silently admitted the combined footprint")
					}
					if !cancelRunning && budget > 83<<20 && peak < 2960<<10 {
						t.Fatalf("fitting paths never coexisted: peak=%d", peak)
					}
					data := fillQueuedBytes(t, other)
					heldData := data
					if !cancelRunning || first != "legacy" {
						var result pb.RangeResponse
						if len(data) < 5 {
							t.Fatal("missing legacy response")
						}
						size := int(binary.BigEndian.Uint32(data[1:5]))
						if len(data) < 5+size || proto.Unmarshal(data[5:5+size], &result) != nil || len(result.Kvs) != 1 || string(result.Kvs[0].Key) != "/a" || len(result.Kvs[0].Value) != 0 {
							t.Fatal("legacy value ordering/keys-only projection changed")
						}
						heldData = data[5+size:]
					}
					var put pb.PutResponse
					if len(heldData) < 5 || proto.Unmarshal(heldData[5:], &put) != nil || put.PrevKv == nil || !bytes.Equal(put.PrevKv.Value, bytes.Repeat([]byte("a"), 1280<<10)) {
						t.Fatal("held-reply materialization did not independently finish")
					}
					cursorData := fillQueuedBytes(t, c)
					if cancelRunning && first == "cursor" {
						if len(cursorData) != 0 {
							t.Fatal("cancelled cursor published")
						}
					} else {
						var got pb.RangeResponse
						if len(cursorData) < 5 || proto.Unmarshal(cursorData[5:], &got) != nil || len(got.Kvs) != 1 || !bytes.Equal(got.Kvs[0].Value, bytes.Repeat([]byte("z"), 64<<10)) {
							t.Fatal("cursor did not independently complete")
						}
					}
					t.Logf("W peak=%d limit=%d P peak=%d limit=%d blocked=%v", peak, n.fillBook.limit, peakP, n.sizes.clientPool, blocked)
				})
			}
		}
	}
}

func captureParityFixture(t *testing.T, cursor bool) (*node, *store, *memLog, *client, *heldReply, int64) {
	t.Helper()
	st, log := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	st.setBudget(83<<20, sizeFor(83<<20).mapped())
	for i := range 65536 {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(i))
		putValue(t, st, log, string(key[:]), "x")
	}
	putValue(t, st, log, "/room/a", string(bytes.Repeat([]byte("a"), 1300<<10)))
	putValue(t, st, log, "/room/b", string(bytes.Repeat([]byte("b"), 1300<<10)))
	n, _ := testNode(t)
	n.st = st
	n.sizes = sizeFor(83 << 20)
	n.cursorFetch = cursor
	a, b := partClient(t, n, 0), partClient(t, n, 1)
	n.socks, n.gens = []any{a, b}, []uint16{1, 1}
	n.connOut = n.sizes.connOut(2)
	a.flow.conn = 0
	b.flow.conn = 0
	sa := a.newStream(1, kRange)
	msg := appendBytesField(nil, 1, []byte("/room/"))
	msg = appendBytesField(msg, 2, prefixEnd([]byte("/room/")))
	serveRoomRange(t, a, sa, msg)
	sb := b.newStream(1, kDel)
	req := appendBytesField(nil, 1, []byte{0})
	req = appendBytesField(req, 2, []byte{0, 1})
	req = appendVarintField(req, 3, 1)
	before := n.cout
	if err := n.applyDel(req, b, sb); err != nil {
		t.Fatal(err)
	}
	if len(n.heldReplies) != 1 {
		t.Fatal("main-admitted delete was not held")
	}
	hr := n.heldReplies[0]
	charge := n.cout - before
	a.dropAll()
	t.Cleanup(func() { b.dropAll(); n.pumpFills() })
	return n, st, log, b, hr, charge
}

func TestMainHeldCaptureParityAndBoundedMaterialization904(t *testing.T) {
	const mainCount = 8388608
	for _, cursor := range []bool{false, true} {
		t.Run(strconv.FormatBool(cursor), func(t *testing.T) {
			n, st, log, c, hr, charge := captureParityFixture(t, cursor)
			if hr.counted != mainCount || c.heldCounted != mainCount || charge != mainCount {
				t.Fatalf("main capture changed: hr=%d client=%d growOut=%d", hr.counted, c.heldCounted, charge)
			}
			// MARKED LIMIT-PROOF SPOT: #904 owns main's unchanged capture.
			// Do NOT assert its client-pool ceiling here. The equality above
			// is unconditional, and every #897 byte below still has a limit.
			if !cursor {
				return
			}
			st.log = &fillColdLog{memLog: log}
			l := &loop{n: n, hoFD: -1, epfd: -1}
			n.shell = l
			t.Cleanup(func() {
				if l.fetchWorker != nil {
					l.fetchWorker.stop()
					_ = l.runFetchCompletion()
				}
				n.pumpFills()
			})
			if !n.stepHeld(hr) || !hr.fetching || n.legacyPending == nil {
				t.Fatal("admitted capture did not use shared W")
			}
			if hr.counted != mainCount || c.heldCounted != mainCount {
				t.Fatal("fetching enlarged the original capture")
			}
			for steps := 0; len(n.heldReplies) > 0 || n.legacyRunning != nil || n.legacyPending != nil; steps++ {
				if steps > 1024 {
					t.Fatal("materialization did not make bounded progress")
				}
				n.pumpFills()
				if n.fillBook.used > n.fillBook.limit {
					t.Fatal("materialization exceeded shared W")
				}
				changed := n.cpool + n.cout - n.mainCaptureCount() + n.stagePromises + n.legacyPromises + int64(n.clients+n.stageOrphans)*idleConn
				if changed > n.sizes.clientPool+n.sizes.reserve() {
					t.Fatal("changed storage exceeded its ceiling")
				}
				if n.legacyRunning != nil {
					awaitFetchResult(t, l)
					n.now = n.now.Add(2 * time.Second)
					n.dropFetched()
					if err := l.runPosts(); err != nil {
						t.Fatal(err)
					}
				}
			}
			data := fillQueuedBytes(t, c)
			var response pb.DeleteRangeResponse
			if len(data) < 5 || proto.Unmarshal(data[5:], &response) != nil || response.Deleted != 65536 || len(response.PrevKvs) != 65536 {
				t.Fatal("materialized delete changed its result")
			}
			for _, kv := range response.PrevKvs {
				if len(kv.Key) != 4 || string(kv.Value) != "x" {
					t.Fatal("materialization changed previous values")
				}
			}
			beforePhysical, beforeSocket, beforeLogical := n.cout, cap(c.out), c.flow.bytes
			c.windowUpdate(0, 1)
			if c.flow.bytes != beforeLogical-1 || n.cout-beforePhysical != int64(cap(c.out)-beforeSocket) {
				t.Fatal("a partial flat drain returned live backing")
			}
		})
	}
}
