package wl

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// The reserve's defects the re-check of 5d361913 found (#689, area B),
// each on the node's structures alone: no node runs. They began as GPT-6
// Astra's probes.

// A held Txn takes room for its whole reply, its put's value before
// included (B3): sized after the value moved into its records, it took
// room for 16 bytes and wrote 1 MiB.
func TestAHeldTxnTakesRoomForItsWholeReply(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	putValue(t, st, l, "/a", strings.Repeat("a", 1<<20))
	id, _ := st.find([]byte("/a"))
	prev, err := st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	res := []opRes{{kind: kPut, rev: st.rev + 1, prev: prev}}
	want := n.txnReplySize(st.rev, true, res, 0, 1, nil)
	n.holdTxn(c, s, true, res, 0, 1, nil)
	hr := n.heldReplies[0]
	if got := sizeBytesField(1, n.hdrSizeAt(hr.hdrRev)) + hr.body; got != want {
		t.Fatalf("the held Txn takes room for %d bytes, its reply is %d", got, want)
	}
}

// A stream reset gives back the room of the reserve its dropped bytes
// took (B4), though a lone RST_STREAM writes nothing that a flush would
// give it back at.
func TestAResetGivesBackTheReservesRoom(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	n.sizes = sizeFor(96 << 20)
	n.connOut = 384 << 10
	c.preface, c.lastSid = true, s.id
	c.flow.conn, s.win = 0, 0
	if !n.msgRoom(c, 1<<20+5) {
		t.Fatal("the reserve has no room for the reply")
	}
	sendMsg(c, s, 1<<20)
	n.giveRoom(c, false)
	c.pr, c.pw = 0, 13
	putFrameHeader(c.pt, 4, frRST, 0, s.id)
	clear(c.pt[9:13])
	if err := c.frames(n); err != nil {
		t.Fatal(err)
	}
	if c.flow.bytes != 0 || c.get(s.id) != nil {
		t.Fatal("the reset kept the stream or its bytes")
	}
	if n.reserved != 0 || c.roomHeld != 0 {
		t.Fatalf("after the reset the reserve holds %d bytes, the client %d", n.reserved, c.roomHeld)
	}
}

// A stream reset lets go of the write reply it held, and of the revision
// the reply held (B4).
func TestAResetLetsGoOfAHeldReply(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes = sizeFor(96 << 20)
	n.connOut = 384 << 10
	n.reserved = n.sizes.reserve()
	c.preface, c.lastSid = true, s.id
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	putValue(t, st, l, "/a", strings.Repeat("a", 1<<20))
	id, _ := st.find([]byte("/a"))
	rec, err := st.live(id)
	if err != nil {
		t.Fatal(err)
	}
	n.holdDel(c, s, 1, [][]byte{rec})
	c.pr, c.pw = 0, 13
	putFrameHeader(c.pt, 4, frRST, 0, s.id)
	clear(c.pt[9:13])
	if err := c.frames(n); err != nil {
		t.Fatal(err)
	}
	if len(n.heldReplies) != 0 || len(st.readHolds) != 0 || c.roomWaiters != 0 {
		t.Fatalf("after the reset %d replies, %d revisions and %d waiters are held", len(n.heldReplies), len(st.readHolds), c.roomWaiters)
	}
}

// A message that waited for room behind its own client's bytes, with no
// room of the reserve held, is tried again once those bytes leave (B2).
func TestAWaitBehindItsOwnClientIsTriedAgain(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	n.sizes = sizeFor(4 << 30)
	n.connOut = n.sizes.connOut(1)
	c.flow.bytes = 32 << 20
	if n.msgRoom(c, 41<<20) {
		t.Fatal("the reply fits")
	}
	n.waitRoom(c, s, nil)
	c.flow.bytes = 0 // the bytes before it left
	n.giveRoom(c, false)
	n.serveRoomWait()
	if len(n.roomWait) != 0 {
		t.Fatalf("the waiting range was not tried again: reserved %d, held %d, freed %v", n.reserved, c.roomHeld, n.roomFreed)
	}
}

// A watch's catch-up that finds the reserve spent waits, though its
// client holds nothing (B7): it built 1 MiB past its share with no room.
func TestAWatchCatchUpWaitsForTheReserve(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes = sizeFor(96 << 20)
	n.connOut = 384 << 10
	putValue(t, st, l, "/a", strings.Repeat("a", 1<<20))
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s.kind = kWatch
	c.flow.conn, s.win = 0, 0
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, id: 1, key: []byte("/a"), syncRev: st.rev})
	n.w.unsynced = append(n.w.unsynced, i)
	n.reserved = n.sizes.reserve() // other clients hold all of it
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if c.flow.bytes != 0 {
		t.Fatalf("the catch-up built %d bytes with the reserve spent", c.flow.bytes)
	}
}

// A range reply that passes its client's share by more than the whole
// reserve goes in pieces: waiting for room, it waited for good (B1). Two
// values of 1.4 MiB on a 96 MiB node with a share of 512 KiB make a reply
// of 2.8 MiB, within oneShot plus its last record, past the share and the
// reserve of 2 MiB together.
func TestAReplyPastTheReserveGoesInPieces(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes = sizeFor(96 << 20)
	n.connOut = 512 << 10
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	for _, k := range []string{"/p/a", "/p/b"} {
		putValue(t, st, l, k, strings.Repeat("v", 1400<<10))
	}
	req := appendBytesField(appendBytesField(nil, 1, []byte("/p/")), 2, []byte("/p0"))
	var r rangeReq
	if err := parseRange(req, &r); err != nil {
		t.Fatal(err)
	}
	n.serveRange(c, s, &r, req)
	if len(n.roomWait) != 0 || len(n.bigs) != 1 {
		t.Fatalf("the reply past the share and the reserve: %d waiting for room, %d in pieces; want it in pieces", len(n.roomWait), len(n.bigs))
	}
}

// A client's held replies count against its share: past it, its new
// requests are refused, so that held replies' keys never grow past the
// pool (B6). Each held Txn here keeps 1.75 MiB of keys of a keys-only
// range while the reserve is spent.
func TestHeldRepliesCountAgainstTheShare(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes = sizeFor(96 << 20)
	n.connOut = n.sizes.connOut(1)
	n.reserved = n.sizes.reserve()
	var kvs [][]byte
	for _, prefix := range []string{"a", "b"} {
		key := prefix + strings.Repeat("k", 896<<10)
		putValue(t, st, l, key, "v")
		id, _ := st.find([]byte(key))
		kvs = append(kvs, stripValue(st.keyAt(id, 0), false))
	}
	res := []opRes{{kind: kRange, rev: st.rev, kvs0: 0, kvs1: 2, count: 2}}
	n.replyTxn(c, s, true, res, 0, 1, kvs)
	if len(n.heldReplies) != 1 {
		t.Fatalf("%d replies held, want the one", len(n.heldReplies))
	}
	if !c.holdsTooMuch() {
		t.Fatalf("a client holding %d bytes of held replies past its share of %d is not refused", c.heldCounted, n.connOut)
	}
}

// The second re-check of #689 (R2, Opus's probes).

// A range waiting for room whose client closes no longer counts its kept
// request in the pool (B9's leak).
func TestAClosedClientsWaitingRangeLeavesThePool(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	before := n.cout
	req := appendBytesField(appendBytesField(nil, 1, []byte("/p/")), 2, []byte("/p0"))
	n.waitRoom(c, s, req)
	c.dropAll()
	n.gens[0]++
	n.serveRoomWait()
	if n.cout != before || len(n.roomWait) != 0 {
		t.Fatalf("after its client closed, its waiting range keeps %d bytes in the pool, %d waiting", n.cout-before, len(n.roomWait))
	}
}

// A watch stream that ends with an error ends its watches.
func TestAWatchStreamEndedWithAnErrorEndsItsWatches(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(3, kWatch)
	s.watch = true
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, id: 1, key: []byte("/a")})
	s.wids = append(s.wids, i)
	n.replyError(c, s.id, codeResourceExhausted, "etcdserver: too many requests")
	if c.get(s.id) != nil || n.w.ws[i].active {
		t.Fatal("the watch stream ended with an error keeps its watcher")
	}
}

// A watch stream ended while a large response is half written in it is
// reset: what waited behind the response never goes out inside it.
func TestAStreamEndedInsideALargeResponseIsReset(t *testing.T) {
	c, _ := flowClient(t)
	n := c.node
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	s := c.newStream(3, kWatch)
	s.watch = true
	s.big, c.bigs = true, 1 // a large response's message is half written
	c.hold(s, make([]byte, 100), nil)
	n.replyError(c, s.id, codeResourceExhausted, "etcdserver: too many requests")
	c.drain()
	if len(c.flow.held) != 0 || c.bigs != 0 || c.get(s.id) != nil {
		t.Fatalf("after the end: %d held, %d large streams, stream open %v; want it reset with nothing held", len(c.flow.held), c.bigs, c.get(s.id) != nil)
	}
	if !bytes.Contains(c.out, []byte{0, 0, 4, frRST}) {
		t.Fatal("the stream was not reset")
	}
}

// A catch-up of deletes of large keys, each at its own revision, counts
// their keys and goes out a revision or so at a time (D4): counted as
// nothing, the three made one message of 3 MiB, past a 384 KiB share and
// the reserve together, and the watch never moved on.
func TestACatchUpOfDeletesOfLargeKeysMovesOn(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes, n.connOut = sizeFor(96<<20), 384<<10
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s.kind, s.watch = kWatch, true
	c.flow.conn, s.win = 1<<30, 1<<30
	var ids []uint32
	for _, prefix := range []string{"a", "b", "c"} {
		key := prefix + strings.Repeat("k", 1<<20)
		putValue(t, st, l, key, "v")
		id, _ := st.find([]byte(key))
		ids = append(ids, id)
	}
	start := st.rev + 1
	for _, id := range ids {
		st.rev++
		if _, ok, err := st.del(id, st.rev, false); err != nil || !ok {
			t.Fatalf("delete: %v", err)
		}
		st.endApply()
	}
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, id: 1, key: []byte{0}, end: []byte{0}, syncRev: start})
	n.w.unsynced = append(n.w.unsynced, i)
	for range 3 {
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
		c.ow = len(c.out) // the client read it
		n.giveRoom(c, false)
	}
	if x := &n.w.ws[i]; x.syncRev != 0 {
		t.Fatalf("the catch-up stays at revision %d of %d..%d; reserve %d of %d, %d waits", x.syncRev, start, st.rev, n.reserved, n.sizes.reserve(), n.stats.roomWaits)
	}
}

// A held reply holds its records' least revision, for trimming, and each
// record's own state, so that a checkpoint takes in the state each was
// read in (store.takeStates), and lets go of them all when it ends (the
// re-check of e710ebb9: held at the least revision alone, a newer record
// was seen by no hold).
func TestAHeldReplyHoldsEachRecordsState(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/b", "b1") // 3
	var kvs [][]byte
	for _, key := range []string{"/a", "/b"} {
		id, _ := st.find([]byte(key))
		rec, err := st.live(id)
		if err != nil {
			t.Fatal(err)
		}
		kvs = append(kvs, rec)
	}
	res := []opRes{{kind: kRange, rev: st.rev, kvs0: 0, kvs1: 2, count: 2}}
	n.holdTxn(c, s, true, res, 0, 1, kvs)
	hr := n.heldReplies[0]
	if hr.hold != 2 || st.heldReads() != 1 || !st.stateHeld(hr.refs[0].id, 2) || !st.stateHeld(hr.refs[1].id, 3) {
		t.Fatalf("the held reply holds revision %d, the store %d reads, states %v; want 2, 1, and /a at 2 and /b at 3", hr.hold, st.heldReads(), st.heldStates)
	}
	n.endHeld(hr)
	if st.heldReads() != 0 || len(st.heldStates) != 0 {
		t.Fatalf("after the reply, the store holds %d reads, %d states", st.heldReads(), len(st.heldStates))
	}
}

// A held reply whose records were put at many revisions makes a checkpoint
// take in its records' own states, not every state of every key a read at
// those revisions sees: 20 keys put at each of 500 revisions, beside the
// reply's records, give three states each (the re-check of e710ebb9,
// Astra's probe at scale: 27,000 revisions behind 100 keys made millions
// of entries).
func TestAHeldReplyOfManyRevisionsTakesInItsOwnStates(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	var hot, marks []uint32
	for i := range 20 {
		id, err := st.intern(fmt.Appendf(nil, "/hot/%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		hot = append(hot, id)
	}
	for r := range 500 {
		st.rev++
		for _, id := range hot {
			if _, err := st.put(id, []byte("h"), 0, st.rev, l.add("h"), false); err != nil {
				t.Fatal(err)
			}
		}
		id, err := st.intern(fmt.Appendf(nil, "/mark/%03d", r))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.put(id, []byte("m"), 0, st.rev, l.add("m"), false); err != nil {
			t.Fatal(err)
		}
		marks = append(marks, id)
		st.endApply()
	}
	var kvs [][]byte
	for _, id := range marks {
		rec, err := st.live(id)
		if err != nil {
			t.Fatal(err)
		}
		kvs = append(kvs, rec)
	}
	res := []opRes{{kind: kRange, rev: st.rev, kvs0: 0, kvs1: len(kvs), count: int64(len(kvs))}}
	n.holdTxn(c, s, true, res, 0, 1, kvs)
	st.compact(st.rev)
	st.trimSteps(1 << 20)
	for _, id := range hot {
		if got, _ := st.takeStates(id, st.rev, st.ckptRev(), nil); len(got) > 3 {
			t.Fatalf("a checkpoint takes in %d states of %s", len(got), st.key(id))
		}
	}
	for _, id := range marks {
		if got, _ := st.takeStates(id, st.rev, st.ckptRev(), nil); len(got) != 1 {
			t.Fatalf("a checkpoint takes in %d states of %s, want its one", len(got), st.key(id))
		}
	}
	n.endHeld(n.heldReplies[0])
	if st.heldReads() != 0 || len(st.heldStates) != 0 {
		t.Fatalf("after the reply, %d reads and %d states are held", st.heldReads(), len(st.heldStates))
	}
}

// A watch from the compacted revision, at which a Txn put a key and
// deleted it, gets the deletion alone, as etcd keeps it: trimming dropped
// the put. Before, sizing the revision found the put without a state and
// the error stopped the node (the re-check of 9b538554, Opus's probe).
func TestAWatchFromACompactedPutAndDeleteGetsTheDeletion(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	st, l := newTestStore(t)
	defer func() { _ = st.close() }()
	n.st = st
	n.sizes, n.connOut = sizeFor(96<<20), 384<<10
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	s.kind, s.watch = kWatch, true
	c.flow.conn, s.win = 1<<30, 1<<30
	putValue(t, st, l, "/a", "a1") // 2
	id, _ := st.find([]byte("/a"))
	st.rev++
	if _, err := st.put(id, []byte("a3"), 0, st.rev, l.add("a3"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.del(id, st.rev, false); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	st.compact(3)
	st.endApply() // the compaction's apply trims
	i := n.w.add(watcher{sock: 0, gen: 1, sid: s.id, id: 1, key: []byte("/a"), syncRev: 3})
	n.w.unsynced = append(n.w.unsynced, i)
	if err := n.syncWatchers(); err != nil {
		t.Fatalf("the watch's catch-up failed, which stops the node: %v", err)
	}
	if x := &n.w.ws[i]; x.syncRev != 0 || len(n.bigWatches) != 0 {
		t.Fatalf("the watch is at %d, %d large responses; want it caught up", x.syncRev, len(n.bigWatches))
	}
	if !bytes.Contains(c.out, []byte("/a")) {
		t.Fatal("no event of /a went out")
	}
}

// The maps of held reads and states let go of their memory once empty: a
// map never shrinks, and a million states held and let go kept 56 MB that
// nothing counted (the re-check of 9b538554, Opus).
func TestHeldStatesLetGoOfTheirMemory(t *testing.T) {
	st, _ := newTestStore(t)
	defer func() { _ = st.close() }()
	for i := range 1000 {
		st.holdState(uint32(i), 2)
	}
	st.holdRead(2)
	for i := range 1000 {
		st.releaseState(uint32(i), 2)
	}
	st.releaseRead(2)
	if st.heldStates != nil || st.readHolds != nil {
		t.Fatalf("empty, the maps are still kept: states %v, reads %v", st.heldStates != nil, st.readHolds != nil)
	}
}
