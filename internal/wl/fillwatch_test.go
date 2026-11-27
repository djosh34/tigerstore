package wl

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestCursorWatchFirstPutHasNoPrevious(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/first", "first")
	id, _ := st.find([]byte("/first"))
	st.keys[id].rec = 0
	n := c.node
	establishFillWatch(t, n, c, "/first", 2)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	runFillWork(t, n, l)
	messages := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if len(messages) != 1 || len(messages[0].Events) != 1 || messages[0].Events[0].PrevKv != nil || string(messages[0].Events[0].Kv.Value) != "first" {
		t.Fatal("first put invented or lost a previous value")
	}
}

func establishFillWatch(t *testing.T, n *node, c *client, key string, from int64) (*stream, int32) {
	t.Helper()
	c.plain = true
	c.flow.conn = 1 << 30
	s := c.newStream(3, kWatch)
	s.win = 1 << 30
	create := appendBytesField(nil, 1, []byte(key))
	create = appendVarintField(create, 3, uint64(from))
	create = appendVarintField(create, 6, 1)
	if err := n.watchRequest(c, s, appendBytesField(nil, 1, create)); err != nil {
		t.Fatal(err)
	}
	// The peer consumed the established stream's create acknowledgement.
	c.seal()
	c.out = c.out[:0]
	c.ow = 0
	c.rec = -1
	c.flow.conn = 0
	return s, s.wids[0]
}

func runFillWork(t *testing.T, n *node, l *loop) int {
	t.Helper()
	jobs := 0
	for len(n.fills) > 0 {
		n.pumpFills()
		if n.fillActive != nil {
			jobs++
			if jobs > 32 {
				t.Fatal("watch fill did not make bounded progress")
			}
			awaitFetchResult(t, l)
			n.now = n.now.Add(2 * time.Second)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return jobs
}

func fillQueuedBytes(t *testing.T, c *client) []byte {
	t.Helper()
	var data []byte
	for _, h := range c.flow.held {
		if h.material != nil {
			copy := *h.material
			for p := copy.front(); len(p) > 0; p = copy.front() {
				data = append(data, p...)
				copy.consume(len(p))
			}
			continue
		}
		if h.stage == nil {
			data = append(data, h.data...)
			continue
		}
		s := h.stage
		for at := s.read; at < s.size; {
			p := s.page(at)
			take := min(s.size-at, stagePayload-at%stagePayload)
			data = append(data, p[stageHead+at%stagePayload:stageHead+at%stagePayload+take]...)
			at += take
		}
	}
	return data
}

func decodeWatchMessages(t *testing.T, data []byte) []*pb.WatchResponse {
	t.Helper()
	var out []*pb.WatchResponse
	for len(data) > 0 {
		if len(data) < 5 {
			t.Fatal("partial watch prefix")
		}
		size := int(binary.BigEndian.Uint32(data[1:5]))
		if size > len(data)-5 {
			t.Fatal("partial watch response")
		}
		r := &pb.WatchResponse{}
		if err := proto.Unmarshal(data[5:5+size], r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
		data = data[5+size:]
	}
	return out
}

func TestSmallWatchCommitsToQueueBeforeDataAndSurvivesInstall(t *testing.T) {
	n, snapshot := installFixture(t)
	t.Cleanup(func() { _ = n.st.close() })
	log := n.st.log.(*memLog)
	putValue(t, n.st, log, "/follower", "second")
	n.st.log = &fillColdLog{memLog: log}
	id, _ := n.st.find([]byte("/follower"))
	n.st.keys[id].rec = 0
	n.sizes, n.connOut, n.cursorFetch = sizeFor(83<<20), 64<<10, true
	c := partClient(t, n, 0)
	n.socks, n.gens = []any{c}, []uint16{1}
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
	s, wi := establishFillWatch(t, n, c, "/follower", 3)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if n.st.heldReads() != 0 {
		t.Fatal("small unpublished watch pinned history")
	}
	runFillWork(t, n, l)
	if n.w.ws[wi].syncRev != 0 || c.waiting() != 0 || len(c.flow.held) == 0 {
		t.Fatal("queue move did not atomically commit the watch cursor at zero DATA window")
	}
	queued := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if len(queued) != 1 || len(queued[0].Events) != 1 {
		t.Fatal("no complete committed event")
	}
	original := queued[0]
	// Install a real, later checkpoint containing that already-queued
	// revision, plus an unrelated intervening write.
	leader, ll := ckptNode(t)
	putValue(t, leader.st, ll, "/follower", "v")
	putValue(t, leader.st, ll, "/follower", "second")
	putValue(t, leader.st, ll, "/other", "later")
	writeCkptAt(t, leader, 9)
	data, err := os.ReadFile(filepath.Join(leader.ckptDir, ckptName(9)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(string(snapshot.GetData()), data, 0o600); err != nil {
		t.Fatal(err)
	}
	n.shell = nil
	if err := n.installSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	n.shell = l
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	c.flow.conn = 1 << 20
	s.win = 1 << 20
	c.drain()
	var payload []byte
	for p := c.out; len(p) > 0; {
		if len(p) < 9 {
			t.Fatal("partial frame")
		}
		size := int(p[0])<<16 | int(p[1])<<8 | int(p[2])
		if size > len(p)-9 {
			t.Fatal("partial DATA")
		}
		if p[3] == frData {
			payload = append(payload, p[9:9+size]...)
		}
		p = p[9+size:]
	}
	got := decodeWatchMessages(t, payload)
	if len(got) != 1 || !proto.Equal(got[0], original) {
		t.Fatal("install retracted or replayed a committed small watch message")
	}
	ev := got[0].Events[0]
	if got[0].Header.Revision != 3 || string(ev.Kv.Value) != "second" || ev.Kv.ModRevision != 3 || ev.PrevKv == nil || string(ev.PrevKv.Value) != "v" || ev.PrevKv.ModRevision != 2 {
		t.Fatalf("changed committed values/header: %v", got[0])
	}
}

func TestWatchCursorExpiryProgressAndRecreatedMemberIdentity(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	putValue(t, st, log, "/k", "first")
	putValue(t, st, log, "/k", "second")
	putValue(t, st, log, "/k", "third")
	id, _ := st.find([]byte("/k"))
	st.keys[id].rec = 0
	n := c.node
	s, wi := establishFillWatch(t, n, c, "/k", 3)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	n.pumpFills()
	awaitFetchResult(t, l)
	oldInc := n.w.ws[wi].inc
	n.w.remove(wi)
	newi := n.w.add(watcher{sock: c.idx, gen: c.gen, sid: s.id, id: 1, key: []byte("/k"), syncRev: 3, prevKV: true})
	if newi != wi || n.w.ws[wi].inc == oldInc {
		t.Fatal("fixture did not recreate the member")
	}
	n.w.unsynced = []int32{wi}
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if !n.w.ws[wi].fetchWait {
		t.Fatal("replacement has no independent operation")
	}
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if !n.w.ws[wi].fetchWait {
		t.Fatal("old member acknowledgement cleared the recreated member's wait")
	}
	runFillWork(t, n, l)
	messages := decodeWatchMessages(t, fillQueuedBytes(t, c))
	if len(messages) != 1 || len(messages[0].Events) != 2 {
		t.Fatalf("recreated watch replayed or lost events: %v", messages)
	}
	for i, ev := range messages[0].Events {
		want := []string{"second", "third"}[i]
		prev := []string{"first", "second"}[i]
		if string(ev.Kv.Value) != want || ev.PrevKv == nil || string(ev.PrevKv.Value) != prev {
			t.Fatal("expiry changed current/previous dependencies")
		}
	}
	if n.w.ws[wi].syncRev != 0 || len(st.fetched) != 0 || st.heldReads() != 0 {
		t.Fatal("watch cursor retained cache dependencies or history")
	}
}

func TestWatchCursorBatchesAThousandRevisions(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	for i := range 1001 {
		putValue(t, st, log, "/k", fmt.Sprintf("value-%04d", i))
	}
	id, _ := st.find([]byte("/k"))
	st.keys[id].rec = 0
	n := c.node
	_, wi := establishFillWatch(t, n, c, "/k", 3)
	jobs := 0
	for n.w.ws[wi].syncRev != 0 {
		if err := n.syncWatchers(); err != nil {
			t.Fatal(err)
		}
		jobs += runFillWork(t, n, l)
	}
	events := 0
	for _, m := range decodeWatchMessages(t, fillQueuedBytes(t, c)) {
		events += len(m.Events)
	}
	if events != 1000 || jobs > 4 {
		t.Fatalf("catch-up did not batch complete revisions: %d events, %d handoffs", events, jobs)
	}
}
