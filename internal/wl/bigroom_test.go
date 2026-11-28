package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/proto"
)

// bigValue is value i of 2 MiB less 256 bytes, each byte from its place:
// under the request limit, and past a client's share at a small budget.
func bigValue(i int) []byte {
	v := make([]byte, maxMsg-256)
	for j := range v {
		v[j] = byte(j*7 + j/251 + i*13)
	}
	return v
}

// bigReaders is a node of budget bytes with k client connections, each
// with one stream of kind whose HTTP/2 windows are 1,024 bytes, and the
// keys /b/0 to /b/3 put with bigValue(i) at revision rev. With history,
// the puts of rev overwrote others before them, and are overwritten
// since: a read of rev reads past revisions only.
func bigReaders(t *testing.T, budget int64, k int, history bool, kind uint8) (n *node, cs []*client, rev int64) {
	t.Helper()
	n, _ = testNode(t)
	st, l := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	n.st = st
	st.setBudget(1, 0)
	n.sizes = sizeFor(budget)
	revision := func(values func(i int) []byte) {
		st.rev++
		for i := range 4 {
			id, err := st.intern(fmt.Appendf(nil, "/b/%d", i))
			if err != nil {
				t.Fatal(err)
			}
			v := values(i)
			if _, err := st.put(id, v, 0, st.rev, l.add(string(v)), false); err != nil {
				t.Fatal(err)
			}
		}
		st.endApply()
	}
	if history {
		revision(func(i int) []byte { return bigValue(i + 4) })
	}
	revision(bigValue)
	rev = st.rev
	if history {
		revision(func(i int) []byte { return []byte{byte(i)} })
	}
	for i := range k {
		c := newClient(&handoff{fd: -1, plain: true}, n)
		c.takeBufs()
		c.idx, c.gen = i, 1
		n.socks, n.gens = append(n.socks, c), append(n.gens, 1)
		s := c.newStream(1, kind)
		c.flow.conn, s.win = 1024, 1024
		cs = append(cs, c)
	}
	n.clients = k
	n.connOut = n.sizes.connOut(n.sending())
	return n, cs, rev
}

// sent takes what waits to be sent to client c, as its socket would, and
// returns the DATA it carries.
func sent(t *testing.T, c *client) []byte {
	t.Helper()
	c.seal()
	fr := http2.NewFramer(nil, &readOnly{c.out[c.ow:]})
	var data []byte
	for {
		f, err := fr.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("a frame sent to the client: %v", err)
		}
		if d, ok := f.(*http2.DataFrame); ok {
			data = append(data, d.Data()...)
		}
	}
	c.out, c.ow = c.out[:0], 0
	return data
}

// readOnly reads p, then io.EOF: a frame cut short is
// io.ErrUnexpectedEOF.
type readOnly struct{ p []byte }

func (r *readOnly) Read(b []byte) (int, error) {
	if len(r.p) == 0 {
		return 0, io.EOF
	}
	k := copy(b, r.p)
	r.p = r.p[k:]
	return k, nil
}

// windowSteps are what the fast reader opens its windows by, turn after
// turn: from one byte, which splits a record inside its tag, to a few
// hundred KiB.
var windowSteps = []int64{1, 3, 9, 100, 1000, 16385, 100_003, 300_007}

// largeReplies are the large replies of a node under test: how each
// starts on a client's stream, the loop's pump of them and its check of
// whether one could move on (bigReady), and how many are under way.
type largeReplies struct {
	start func(c *client, s *stream)
	pump  func()
	ready func() bool
	under func() int
}

// readLarge drives readers' large replies as the loop drives them: it
// pumps only while r.ready says they could move on, which it must say
// once the fast reader, the first, opens its windows, and may not say
// while it could not. The slow readers start in turn, each its windows opened
// by a share's worth, which it takes; then they read no more, their
// sockets drain and they go idle, so that the share of the readers left
// grows; then they read on all at once, a share's worth each, and stop
// again. What the clients hold stays within the client pool throughout,
// and the fast reader's message, which it returns, comes whole.
func readLarge(t *testing.T, n *node, cs []*client, r largeReplies) []byte {
	t.Helper()
	fast := cs[0]
	l := &loop{n: n}
	most := int64(0)
	var alloc uint64
	settle := func() {
		for i := 0; r.ready(); i++ {
			if i > 100 {
				t.Fatal("the large replies stay ready without moving on")
			}
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			r.pump()
			runtime.ReadMemStats(&m1)
			alloc += m1.TotalAlloc - m0.TotalAlloc
			used := n.cpool + n.cout + int64(n.clients)*idleConn
			most = max(most, used)
			if used > n.sizes.clientPool {
				for i, c := range cs {
					t.Logf("reader %d: buffers %v, send buffer %d bytes, %d of them waiting", i, c.in != nil, cap(c.out), c.waiting())
				}
				t.Fatalf("%d readers hold %d bytes of the client pool's %d: read %d, sent %d, %d active, share %d", len(cs), used, n.sizes.clientPool, n.cpool, n.cout, n.active, n.connOut)
			}
		}
	}
	r.start(fast, fast.get(1))
	settle()
	var got []byte
	for turn := 0; turn < 20000 && (turn < len(cs) || r.under() == len(cs)); turn++ {
		if i := turn + 1; i < len(cs) {
			c := cs[i]
			r.start(c, c.get(1))
			c.flow.conn, c.get(1).win = n.connOut, n.connOut
			settle()
		}
		if turn == len(cs)+1 {
			// The slow readers, idle since, read on all at once: each
			// takes its buffers to read its WINDOW_UPDATEs, and a share's
			// worth of window.
			for _, c := range cs[1:] {
				if c.in == nil {
					c.takeBufs()
				}
			}
			for _, c := range cs[1:] {
				c.windowUpdate(0, n.connOut)
				c.windowUpdate(1, n.connOut)
			}
			settle()
		}
		got = append(got, sent(t, fast)...)
		for _, c := range cs[1:] {
			sent(t, c)
		}
		// A tick: the clients idle give their buffers back, and the shares
		// are made again. The fast reader, whose WINDOW_UPDATEs it reads,
		// is not idle.
		l.tick += 2
		fast.busy = l.tick
		l.clientTick()
		settle()
		if fs := fast.get(1); r.under() == len(cs) && fast.flow.conn > 0 && fs.win > 0 {
			t.Fatal("the fast reader's reply is not ready with its windows open and its socket drained")
		}
		step := windowSteps[turn%len(windowSteps)]
		fast.windowUpdate(0, step)
		fast.windowUpdate(1, step)
		if r.under() == len(cs) && !r.ready() {
			t.Fatal("the fast reader's reply is not ready once its windows opened")
		}
		settle()
	}
	got = append(got, sent(t, fast)...)
	t.Logf("P %d: the clients held at most %d, the fast reader got %d bytes; the pumps allocated %d KiB", n.sizes.clientPool, most, len(got), alloc>>10)
	// A part reads only the bytes of its value it sends. Built whole for
	// each part, a past revision's record was copied whole for each of the
	// fast reader's about 160 parts: 320 MiB.
	if alloc > 64<<20 {
		t.Fatalf("the pumps allocated %d MiB", alloc>>20)
	}
	if r.under() != len(cs)-1 {
		t.Fatalf("%d large replies under way, want the %d slow readers'", r.under(), len(cs)-1)
	}
	if len(got) < 5 || int(binary.BigEndian.Uint32(got[1:5])) != len(got)-5 {
		t.Fatalf("the fast reader got %d bytes, not one whole message", len(got))
	}
	return got[5:]
}

// readerCases are #919's three readers at 83 MiB and the most the node
// admits at 83, 137 and 191 MiB (#919's correction), each with values
// live and resident, and with values of past revisions only.
func readerCases(t *testing.T, f func(t *testing.T, budget int64, readers int, history bool)) {
	for _, tc := range []struct {
		budget  int64
		readers int
	}{{83 << 20, 3}, {83 << 20, 5}, {137 << 20, 9}, {191 << 20, 13}} {
		for _, history := range []bool{false, true} {
			t.Run(fmt.Sprintf("%dMiB/%d/history=%v", tc.budget>>20, tc.readers, history), func(t *testing.T) {
				f(t, tc.budget, tc.readers, history)
			})
		}
	}
}

// A large range reply to a slow reader holds no more than the reader's
// share: a piece is built only as the windows take it, straight into the
// socket, and a record past that goes in parts, each reading only the
// bytes of its value it sends (#919). Main built a piece of one whole
// 2 MiB record for every slow reader, and counted it only after: three
// of them at 83 MiB held 7.5 MB of the client pool's 5.4 MB. A reader
// that reads gets its whole reply, while the others read nothing.
func TestSlowReadersOfLargeRangesStayWithinTheClientPool(t *testing.T) {
	readerCases(t, func(t *testing.T, budget int64, readers int, history bool) {
		n, cs, rev := bigReaders(t, budget, readers, history, kRange)
		msg := appendVarintField(appendBytesField(appendBytesField(nil, 1, []byte("/b/")), 2, []byte("/b0")), 4, uint64(rev))
		got := readLarge(t, n, cs, largeReplies{
			start: func(c *client, s *stream) { n.startBig(c, s, msg) },
			pump:  n.pumpBig,
			ready: n.bigReady,
			under: func() int { return len(n.bigs) },
		})
		var reply pb.RangeResponse
		if err := proto.Unmarshal(got, &reply); err != nil {
			t.Fatal(err)
		}
		if len(reply.Kvs) != 4 || reply.Count != 4 {
			t.Fatalf("the fast reader got %d records and a count of %d, want 4", len(reply.Kvs), reply.Count)
		}
		for i, kv := range reply.Kvs {
			if string(kv.Key) != fmt.Sprintf("/b/%d", i) || string(kv.Value) != string(bigValue(i)) {
				t.Fatalf("record %d: key %q, a value of %d bytes not the one put", i, kv.Key, len(kv.Value))
			}
		}
	})
}

// A large watch response to a slow watcher holds no more than its share,
// as a large range reply does (#919), with the values before too, which
// lie in past revisions only.
func TestSlowWatchersOfLargeRevisionsStayWithinTheClientPool(t *testing.T) {
	readerCases(t, func(t *testing.T, budget int64, readers int, history bool) {
		n, cs, rev := bigReaders(t, budget, readers, history, kWatch)
		got := readLarge(t, n, cs, largeReplies{
			start: func(c *client, s *stream) {
				s.watch = true
				wi := n.w.add(watcher{sock: c.idx, gen: 1, sid: s.id, id: 1, key: []byte("/b/"), end: []byte("/b0"), prevKV: history})
				s.wids = append(s.wids, wi)
				if !n.startBigWatch(wi, &n.w.ws[wi], c, s, revStart(n.st, rev)) {
					t.Fatal("the large watch response did not start")
				}
			},
			pump:  n.pumpBigWatches,
			ready: n.bigWatchReady,
			under: func() int { return len(n.bigWatches) },
		})
		var resp pb.WatchResponse
		if err := proto.Unmarshal(got, &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Events) != 4 {
			t.Fatalf("the fast watcher got %d events, want 4", len(resp.Events))
		}
		for i, ev := range resp.Events {
			if string(ev.Kv.Key) != fmt.Sprintf("/b/%d", i) || string(ev.Kv.Value) != string(bigValue(i)) {
				t.Fatalf("event %d: key %q, a value of %d bytes not the one put", i, ev.Kv.Key, len(ev.Kv.Value))
			}
			if history && (ev.PrevKv == nil || string(ev.PrevKv.Value) != string(bigValue(i+4))) {
				t.Fatalf("event %d: not the value before", i)
			}
		}
	})
}

// rangeAt is a range request of /b/ at revision rev.
func rangeAt(rev int64) []byte {
	return appendVarintField(appendBytesField(appendBytesField(nil, 1, []byte("/b/")), 2, []byte("/b0")), 4, uint64(rev))
}

// drain sends client c's large range reply to the end, its windows open,
// and returns the message it got.
func drain(t *testing.T, n *node, c *client) []byte {
	t.Helper()
	got := sent(t, c)
	c.windowUpdate(0, 1<<30)
	c.windowUpdate(1, 1<<30)
	for i := 0; n.bigReady(); i++ {
		if i > 10000 {
			t.Fatal("the large reply does not end")
		}
		n.pumpBig()
		got = append(got, sent(t, c)...)
	}
	return got
}

// A value a checkpoint moves between the parts of its record is summed
// over the bytes sent, from both copies: a byte the old copy read wrong
// fails the value's sum, and the reply ends unfinished, as a value read
// whole fails (#919's re-check). Summed again from its new copy, the sum
// passed, and the client got the wrong byte in a whole reply.
func TestAValueMovedBetweenPartsIsSummedOverTheBytesSent(t *testing.T) {
	n, cs, rev := bigReaders(t, 83<<20, 1, true, kRange)
	st, c := n.st, cs[0]
	l := st.log.(*memLog)
	id, _ := st.find([]byte("/b/0"))
	var e *histEnt
	for h := st.keys[id].hist; h != 0; h = st.hist[h].prev {
		if st.hist[h].mod == rev {
			e = &st.hist[h]
		}
	}
	if e == nil {
		t.Fatal("no past revision of /b/0")
	}
	l.b[e.voff+100] ^= 1 // the log reads one byte wrong
	n.startBig(c, c.get(1), rangeAt(rev))
	if len(n.bigs) != 1 || n.bigs[0].part == 0 {
		t.Fatal("the first record is not under way in parts")
	}
	e.voff = l.add(string(bigValue(0))) // a checkpoint copies it, right, and moves it there
	got := drain(t, n, c)
	if st.bad == nil || len(n.bigs) != 0 {
		t.Fatalf("a value sent with a byte read wrong passed its sum: store bad %v, %d large replies under way", st.bad, len(n.bigs))
	}
	if len(got) >= 5 && int(binary.BigEndian.Uint32(got[1:5])) == len(got)-5 {
		t.Fatal("the client got the whole reply")
	}
}

// A put of a key with ignore_value between the parts of its record keeps
// the value where it is, in a record whose revisions may be wider: the
// parts find the value in the record by its own fields (#919's re-check).
// Found by those of the revision read, a put at revision 128 shifted it
// by a byte, failed the value's sum and stopped the node.
func TestAPutWithIgnoreValueBetweenPartsLeavesTheValueWhole(t *testing.T) {
	n, cs, rev := bigReaders(t, 83<<20, 1, false, kRange)
	st, c := n.st, cs[0]
	n.startBig(c, c.get(1), rangeAt(rev))
	if len(n.bigs) != 1 || n.bigs[0].part == 0 {
		t.Fatal("the first record is not under way in parts")
	}
	st.setBudget(0, 0) // the records put stay in the slab
	st.rev = 127
	for i := range 4 {
		id, _ := st.find(fmt.Appendf(nil, "/b/%d", i))
		k := &st.keys[id]
		st.rev++
		if _, err := st.put(id, bigValue(i), k.lease, st.rev, k.voff, false); err != nil {
			t.Fatal(err)
		}
		st.endApply()
		if k.rec == 0 {
			t.Fatal("the record put is not in the slab")
		}
	}
	got := drain(t, n, c)
	if st.bad != nil {
		t.Fatalf("the store went bad: %v", st.bad)
	}
	if len(got) < 5 || int(binary.BigEndian.Uint32(got[1:5])) != len(got)-5 {
		t.Fatalf("the client got %d bytes, not one whole message", len(got))
	}
	var reply pb.RangeResponse
	if err := proto.Unmarshal(got[5:], &reply); err != nil {
		t.Fatal(err)
	}
	for i, kv := range reply.Kvs {
		if string(kv.Value) != string(bigValue(i)) || kv.ModRevision != rev {
			t.Fatalf("record %d: a value of %d bytes not the one put, at revision %d", i, len(kv.Value), kv.ModRevision)
		}
	}
}

// Clients idle at a tick that read again in one turn share what waits to
// be sent from then on, not from the next tick (#919's re-check): three
// at 83 MiB that sent large ranges each wrote a lone client's share, 7.4
// MB of the client pool's 5.4 MB.
func TestIdleClientsThatReadOnInOneTurnShareWhatWaits(t *testing.T) {
	n, cs, rev := bigReaders(t, 83<<20, 3, false, kRange)
	for _, c := range cs {
		c.giveBufs()
	}
	l := &loop{n: n}
	l.tick += 2
	l.clientTick() // the share of a lone client
	for _, c := range cs {
		c.takeBufs() // each reads its range
		c.flow.conn, c.get(1).win = 1<<30, 1<<30
	}
	for _, c := range cs {
		n.startBig(c, c.get(1), rangeAt(rev))
		for i := 0; n.bigReady(); i++ {
			if i > 100 {
				t.Fatal("the large replies stay ready without moving on")
			}
			n.pumpBig()
		}
		if used := n.cpool + n.cout + int64(n.clients)*idleConn; used > n.sizes.clientPool {
			t.Fatalf("3 clients hold %d bytes of the client pool's %d, with a share of %d", used, n.sizes.clientPool, n.connOut)
		}
	}
}
