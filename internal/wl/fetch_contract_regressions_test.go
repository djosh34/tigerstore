package wl

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestCursorEightTinyValueWaitersRetainBoundedOwnership(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	n := c.node
	const count = 65536
	for i := range count {
		putValue(t, st, log, string([]byte{0, byte(i >> 16), byte(i >> 8), byte(i)}), "v")
	}
	dropRecords(st)
	clients := []*client{c}
	fds := []int{}
	wire := make([][]byte, 8)
	n.socks, n.gens = make([]any, 8), make([]uint16, 8)
	n.clients = 8
	for i := range 8 {
		var peer *client
		if i == 0 {
			peer = c
		} else {
			peer = partClient(t, n, i)
			clients = append(clients, peer)
			t.Cleanup(peer.dropAll)
		}
		peer.plain = true
		peer.flow.conn = 1 << 30
		n.socks[i], n.gens[i] = peer, uint16(1)
		fds = append(fds, roomSocket(t, peer))
	}
	for i, peer := range clients {
		var s *stream
		if i == 0 {
			s = peer.get(1)
		} else {
			s = peer.newStream(1, kRange)
		}
		s.win = 1 << 30
		msg := appendBytesField(nil, 1, []byte{0})
		msg = appendBytesField(msg, 2, []byte{1})
		serveRoomRange(t, peer, s, msg)
	}
	jobs := 0
	for step := 0; len(n.fills) > 0 || len(n.roomWait) > 0 || func() bool {
		for _, p := range clients {
			if p.waiting() > 0 || len(p.flow.held) > 0 {
				return true
			}
		}
		return false
	}(); step++ {
		if step > 4000 {
			t.Fatalf("tiny-value waiters stopped: jobs=%d fills=%d waits=%d P=%d/%d stages=%d promises=%d cpp=%d cout=%d covered=%d reserved=%d freed=%v wake=%v", jobs, len(n.fills), len(n.roomWait), n.stageOther(0)+n.stageCredits.used, n.sizes.clientPool, n.stageCredits.used, n.stagePromises, n.cpool, n.cout, n.coveredRoom, n.reserved, n.roomFreed, n.fillCreditWake)
		}
		n.pumpFills()
		if n.fillActive != nil {
			jobs++
			awaitFetchResult(t, l)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
		for _, p := range clients {
			n.markDirty(&p.sock)
		}
		l.flush()
		for i, fd := range fds {
			readFillSocket(t, fd, &wire[i])
		}
		if n.fillBook != nil && n.fillBook.used > n.fillBook.limit || n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool {
			t.Fatalf("tiny-value waiters overspent: P=%d/%d cpp=%d cout=%d stages=%d promises=%d headers=%d active=%d W=%d/%d", n.stageOther(0)+n.stageCredits.used, n.sizes.clientPool, n.cpool, n.cout, n.stageCredits.used, n.stagePromises, n.headerSpare, n.active, n.fillBook.used, n.fillBook.limit)
		}
	}
	if jobs > 8*34 {
		t.Fatal("tiny-value replies regressed to per-value handoffs")
	}
	for _, data := range wire {
		data = fillData(t, data)
		var r pb.RangeResponse
		if len(data) < 5 || proto.Unmarshal(data[5:], &r) != nil || len(r.Kvs) != count || r.Count != count {
			t.Fatal("tiny-value waiter lost its complete reply")
		}
		for _, kv := range r.Kvs {
			if string(kv.Value) != "v" {
				t.Fatal("tiny value changed")
			}
		}
	}
}

func TestCursorRounded32769ByteValuesUseChargedDestinations(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	n.connOut = 64 << 10
	c.plain = true
	c.flow.conn, s.win = 1<<30, 1<<30
	value := bytes.Repeat([]byte("v"), 32769)
	for i := range 84 {
		putValue(t, st, log, fmt.Sprintf("/r/%03d", i), string(value))
	}
	dropRecords(st)
	fd := roomSocket(t, c)
	msg := appendBytesField(nil, 1, []byte("/r/"))
	msg = appendBytesField(msg, 2, []byte("/s"))
	serveRoomRange(t, c, s, msg)
	data := fillData(t, driveLargeFills(t, n, l, c, fd, nil))
	var r pb.RangeResponse
	if len(data) < 5 || proto.Unmarshal(data[5:], &r) != nil || len(r.Kvs) != 84 || r.Count != 84 || st.fetchedBytes != 0 {
		t.Fatal("rounded values used a legacy cache or changed selection")
	}
	for _, kv := range r.Kvs {
		if !bytes.Equal(kv.Value, value) {
			t.Fatal("rounded value changed")
		}
	}
}

func TestCursorAdjacentFourAndFiveThousandRevisionGroups(t *testing.T) {
	c, s, st, log, l := fillRangeFixture(t)
	n := c.node
	for i := range 9000 {
		putValue(t, st, log, fmt.Sprintf("/a/%05d", i), "v")
	}
	start := st.rev + 1
	for _, bounds := range [][2]int{{0, 4000}, {4000, 9000}} {
		st.rev++
		for i := bounds[0]; i < bounds[1]; i++ {
			id, _ := st.find(fmt.Appendf(nil, "/a/%05d", i))
			if _, _, err := st.del(id, st.rev, false); err != nil {
				t.Fatal(err)
			}
		}
		st.endApply()
	}
	dropRecords(st)
	s.kind = kWatch
	wi := n.w.add(watcher{sock: c.idx, gen: c.gen, sid: s.id, key: []byte("/a/"), end: []byte("/b"), syncRev: start, prevKV: true})
	n.w.unsynced = append(n.w.unsynced, wi)
	for turn := 0; n.w.ws[wi].syncRev != 0; turn++ {
		if turn > 12 {
			t.Fatal("adjacent revision groups stopped progressing")
		}
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
		runFillWork(t, n, l)
	}
	got := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if len(got) != 2 || len(got[0].Events) != 4000 || len(got[1].Events) != 5000 {
		t.Fatal("individually fitting revisions were refused, combined or fragmented")
	}
	for i, m := range got {
		if m.Header.Revision != st.rev {
			t.Fatal("watch header changed")
		}
		for _, ev := range m.Events {
			if ev.Kv.ModRevision != start+int64(i) || ev.PrevKv == nil || string(ev.PrevKv.Value) != "v" {
				t.Fatal("adjacent revision lost previous state")
			}
		}
	}
}

func TestCursorHot8193RecordValueSortKeepsMainAdmission(t *testing.T) {
	c, s, st, log, _ := fillRangeFixture(t)
	for i := range 8193 {
		putValue(t, st, log, fmt.Sprintf("/a/%05d", i), "v")
	}
	msg := appendBytesField(nil, 1, []byte("/a/"))
	msg = appendBytesField(msg, 2, []byte("/b"))
	msg = appendVarintField(msg, 5, sortAscend)
	msg = appendVarintField(msg, 6, sortByValue)
	serveRoomRange(t, c, s, msg)
	response := roomRangeReply(t, c)
	if len(response.Kvs) != 8193 || response.Count != 8193 {
		t.Fatal("hot sort acquired an entry-count refusal or changed selection")
	}
}

func TestCursorRealCheckpointMovementOnEveryHandoff(t *testing.T) {
	n, log := ckptNode(t)
	st := n.st
	t.Cleanup(func() { _ = st.close() })
	n.sizes, n.connOut, n.cursorFetch = sizeFor(83<<20), 512<<10, true
	st.setBudget(83<<20, 0)
	for i := range 3000 {
		putValue(t, st, log, fmt.Sprintf("/a/%05d", i), "v")
	}
	dropRecords(st)
	st.log = &fillColdLog{memLog: log}
	header := st.rev
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
	c.flow.conn = 0
	s := c.newStream(1, kRange)
	l := &loop{n: n, hoFD: -1}
	n.shell = l
	t.Cleanup(func() {
		if l.fetchWorker != nil {
			l.fetchWorker.stop()
			_ = l.runFetchCompletion()
		}
		c.dropAll()
		n.pumpFills()
	})
	msg := appendBytesField(nil, 1, []byte("/a/"))
	msg = appendBytesField(msg, 2, []byte("/b"))
	serveRoomRange(t, c, s, msg)
	jobs := 0
	for len(n.fills) > 0 {
		n.pumpFills()
		if n.fillActive == nil {
			continue
		}
		jobs++
		if jobs > 6 {
			t.Fatal("checkpoint movement caused an unbounded replan")
		}
		awaitFetchResult(t, l)
		putValue(t, st, log, "/busy", strconv.Itoa(jobs))
		n.shell = nil
		writeCkptAt(t, n, uint64(30+jobs))
		n.shell = l
		if err := l.runPosts(); err != nil {
			t.Fatal(err)
		}
	}
	response := stagedRangeResponse(t, c)
	if jobs == 0 || response.Header.Revision != header || response.Count != 3000 || len(response.Kvs) != 3000 {
		t.Fatal("checkpoint movement mixed the range's admitted view")
	}
	for _, kv := range response.Kvs {
		if !bytes.Equal(kv.Value, []byte("v")) {
			t.Fatal("checkpoint movement changed a value")
		}
	}
}
