package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestLegacyPagedControlAndEntryCharges(t *testing.T) {
	if unsafe.Sizeof(legacyEntry{}) != 24 || unsafe.Sizeof(legacyPage{}) != stageUnit || unsafe.Sizeof(legacyBatch{}) > stageUnit {
		t.Fatal("legacy roots/pages no longer fit their charged capacities")
	}
}

func TestSharedLegacyCursorRuntimeBudgetMatrix(t *testing.T) {
	for _, budget := range []int64{83 << 20, 137 << 20, 191 << 20} {
		for _, first := range []string{"cursor", "legacy"} {
			t.Run(fmt.Sprintf("%dMiB/%s-first", budget>>20, first), func(t *testing.T) {
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
				blocked := false
				for step := 0; len(n.fills) > 0 || len(n.heldReplies) > 0 || n.legacyAgain || n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed; step++ {
					if step > 64 {
						t.Fatal("shared consumers did not finish")
					}
					peak = max(peak, n.fillBook.peak)
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
				if budget > 83<<20 && peak < 2960<<10 {
					t.Fatalf("fitting paths never coexisted: peak=%d", peak)
				}
				data := fillQueuedBytes(t, other)
				var result pb.RangeResponse
				if len(data) < 5 {
					t.Fatal("missing legacy response")
				}
				size := int(binary.BigEndian.Uint32(data[1:5]))
				if len(data) < 5+size || proto.Unmarshal(data[5:5+size], &result) != nil || len(result.Kvs) != 1 || string(result.Kvs[0].Key) != "/a" || len(result.Kvs[0].Value) != 0 {
					t.Fatal("legacy value ordering/keys-only projection changed")
				}
				heldData := data[5+size:]
				var put pb.PutResponse
				if len(heldData) < 5 || proto.Unmarshal(heldData[5:], &put) != nil || put.PrevKv == nil || !bytes.Equal(put.PrevKv.Value, bytes.Repeat([]byte("a"), 1280<<10)) {
					t.Fatal("held-reply materialization did not independently finish")
				}
				cursorData := fillQueuedBytes(t, c)
				var got pb.RangeResponse
				if len(cursorData) < 5 || proto.Unmarshal(cursorData[5:], &got) != nil {
					t.Fatal("invalid cursor response")
				}
				if len(got.Kvs) != 1 || !bytes.Equal(got.Kvs[0].Value, bytes.Repeat([]byte("z"), 64<<10)) {
					t.Fatal("cursor did not independently complete")
				}
				t.Logf("W peak=%d limit=%d blocked=%v", peak, n.fillBook.limit, blocked)
			})
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
