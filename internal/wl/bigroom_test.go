package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/proto"
)

// bigValue is a value of 2 MiB less 256 bytes: under the request limit,
// and past a client's share at a small budget.
var bigValue = strings.Repeat("v", maxMsg-256)

// rangeReaders starts, on a node of budget bytes, k connections that each
// read a range of four values of bigValue, past one piece, with HTTP/2
// windows of 1,024 bytes.
func rangeReaders(t *testing.T, budget int64, k int) (*node, []*client) {
	t.Helper()
	n, _ := testNode(t)
	st, log := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	n.st = st
	st.setBudget(1, 0)
	n.sizes = sizeFor(budget)
	n.clients = k
	n.connOut = n.sizes.connOut(k)
	for i := range 4 {
		putValue(t, st, log, fmt.Sprintf("/b/%d", i), bigValue)
	}
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/b/")), 2, []byte("/b0"))
	cs := make([]*client, k)
	for i := range cs {
		c := newClient(&handoff{fd: -1, plain: true}, n)
		c.takeBufs()
		c.idx, c.gen = i, 1
		n.socks, n.gens = append(n.socks, c), append(n.gens, 1)
		s := c.newStream(1, kRange)
		c.flow.conn, s.win = 1024, 1024
		n.startBig(c, s, msg)
		cs[i] = c
	}
	return n, cs
}

// sent takes what waits to be sent to client c, as its socket would,
// and returns the DATA it carries.
func sent(t *testing.T, c *client) []byte {
	t.Helper()
	fr := http2.NewFramer(nil, bytes.NewReader(c.out[c.ow:]))
	var data []byte
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			break
		}
		if d, ok := f.(*http2.DataFrame); ok {
			data = append(data, d.Data()...)
		}
	}
	c.out, c.ow = c.out[:0], 0
	return data
}

// A large range reply to a slow reader holds no more than the reader's
// share: a piece is built only as the windows take it, and a record past
// that goes in parts (#919). Main built a piece of one whole 2 MiB record
// for every slow reader and counted it only after: three of them at 83
// MiB held 6 MiB of pieces and 7.5 MB of the client pool's 5.4 MB. What
// the clients hold stays within the client pool for the three readers of
// #919 and for the most the node admits at 83, 137 and 191 MiB (5, 9 and
// 13, #919's correction). Among them a reader that reads gets its whole
// reply, the records sent in parts as etcd sends them whole, while the
// others read nothing.
func TestSlowReadersOfLargeRangesStayWithinTheClientPool(t *testing.T) {
	for _, tc := range []struct {
		budget  int64
		readers int
	}{{83 << 20, 3}, {83 << 20, 5}, {137 << 20, 9}, {191 << 20, 13}} {
		t.Run(fmt.Sprintf("%dMiB/%d", tc.budget>>20, tc.readers), func(t *testing.T) {
			n, cs := rangeReaders(t, tc.budget, tc.readers)
			fast := cs[0]
			var got []byte
			most := int64(0)
			for turn := 0; len(n.bigs) == tc.readers && turn < 10000; turn++ {
				// The fast reader reads what was sent, and opens its
				// windows by an odd amount, so that records go in parts.
				got = append(got, sent(t, fast)...)
				fast.flow.conn, fast.get(1).win = 100_003, 100_003
				n.pumpBig()
				most = max(most, n.cpool+n.cout+int64(n.clients)*idleConn)
			}
			got = append(got, sent(t, fast)...)
			t.Logf("P %d: the clients held at most %d, the fast reader got %d bytes", n.sizes.clientPool, most, len(got))
			if most > n.sizes.clientPool {
				t.Fatalf("%d readers, %d of them slow, held %d bytes of the client pool's %d", tc.readers, tc.readers-1, most, n.sizes.clientPool)
			}
			if len(n.bigs) != tc.readers-1 {
				t.Fatalf("%d large replies under way, want the %d slow readers'", len(n.bigs), tc.readers-1)
			}
			if len(got) < 5 || int(binary.BigEndian.Uint32(got[1:5])) != len(got)-5 {
				t.Fatalf("the fast reader got %d bytes, not one whole message", len(got))
			}
			var reply pb.RangeResponse
			if err := proto.Unmarshal(got[5:], &reply); err != nil {
				t.Fatal(err)
			}
			if len(reply.Kvs) != 4 || reply.Count != 4 {
				t.Fatalf("the fast reader got %d records and a count of %d, want 4", len(reply.Kvs), reply.Count)
			}
			for i, kv := range reply.Kvs {
				if string(kv.Key) != fmt.Sprintf("/b/%d", i) || string(kv.Value) != bigValue {
					t.Fatalf("record %d: key %q, a value of %d bytes", i, kv.Key, len(kv.Value))
				}
			}
		})
	}
}

// watchReaders has, on a node of budget bytes, k connections each watch a
// revision of four puts of bigValue, past one piece, with HTTP/2 windows
// of 1,024 bytes.
func watchReaders(t *testing.T, budget int64, k int) (*node, []*client) {
	t.Helper()
	n, _ := testNode(t)
	st, l := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	n.st = st
	st.setBudget(1, 0)
	n.sizes = sizeFor(budget)
	n.clients = k
	n.connOut = n.sizes.connOut(k)
	st.rev++
	for i := range 4 {
		id, err := st.intern(fmt.Appendf(nil, "/b/%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.put(id, []byte(bigValue), 0, st.rev, l.add(bigValue), false); err != nil {
			t.Fatal(err)
		}
	}
	st.endApply()
	cs := make([]*client, k)
	for i := range cs {
		c := newClient(&handoff{fd: -1, plain: true}, n)
		c.takeBufs()
		c.idx, c.gen = i, 1
		n.socks, n.gens = append(n.socks, c), append(n.gens, 1)
		s := c.newStream(1, kWatch)
		s.watch = true
		c.flow.conn, s.win = 1024, 1024
		wi := n.w.add(watcher{sock: i, gen: 1, sid: s.id, id: 1, key: []byte("/b/"), end: []byte("/b0"), syncRev: st.rev})
		s.wids = append(s.wids, wi)
		if !n.startBigWatch(wi, &n.w.ws[wi], c, s, revStart(st, st.rev)) {
			t.Fatal("the large watch response did not start")
		}
		cs[i] = c
	}
	return n, cs
}

// A large watch response to a slow watcher holds no more than its share,
// as a large range reply does (#919): main built a piece of one whole
// 2 MiB event for every slow watcher. Among them a watcher that reads
// gets the whole response.
func TestSlowWatchersOfLargeRevisionsStayWithinTheClientPool(t *testing.T) {
	for _, tc := range []struct {
		budget  int64
		readers int
	}{{83 << 20, 3}, {83 << 20, 5}, {137 << 20, 9}, {191 << 20, 13}} {
		t.Run(fmt.Sprintf("%dMiB/%d", tc.budget>>20, tc.readers), func(t *testing.T) {
			n, cs := watchReaders(t, tc.budget, tc.readers)
			fast := cs[0]
			var got []byte
			most := int64(0)
			for turn := 0; len(n.bigWatches) == tc.readers && turn < 10000; turn++ {
				got = append(got, sent(t, fast)...)
				fast.flow.conn, fast.get(1).win = 100_003, 100_003
				n.pumpBigWatches()
				most = max(most, n.cpool+n.cout+int64(n.clients)*idleConn)
			}
			got = append(got, sent(t, fast)...)
			t.Logf("P %d: the clients held at most %d, the fast watcher got %d bytes", n.sizes.clientPool, most, len(got))
			if most > n.sizes.clientPool {
				t.Fatalf("%d watchers, %d of them slow, held %d bytes of the client pool's %d", tc.readers, tc.readers-1, most, n.sizes.clientPool)
			}
			if len(n.bigWatches) != tc.readers-1 {
				t.Fatalf("%d large watch responses under way, want the %d slow watchers'", len(n.bigWatches), tc.readers-1)
			}
			if len(got) < 5 || int(binary.BigEndian.Uint32(got[1:5])) != len(got)-5 {
				t.Fatalf("the fast watcher got %d bytes, not one whole message", len(got))
			}
			var resp pb.WatchResponse
			if err := proto.Unmarshal(got[5:], &resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Events) != 4 {
				t.Fatalf("the fast watcher got %d events, want 4", len(resp.Events))
			}
			for i, ev := range resp.Events {
				if string(ev.Kv.Key) != fmt.Sprintf("/b/%d", i) || string(ev.Kv.Value) != bigValue {
					t.Fatalf("event %d: key %q, a value of %d bytes", i, ev.Kv.Key, len(ev.Kv.Value))
				}
			}
		})
	}
}
