package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// watchReader reads one watch stream of a raw connection a field at a
// time, so that a response of hundreds of MiB is never held whole: each
// event goes to ev as it is complete, with the number of the response it
// is in and whether that response is a fragment.
type watchReader struct {
	data     []byte
	msgLeft  int // bytes of the response being read still to come; 0 between responses
	msgs     int // responses begun
	fragment bool
	created  bool
	canceled string
	ev       func(msg int, ev *mvccpb.Event) error
	// fragments counts the responses marked as fragments.
	fragments int
}

// add takes the stream's DATA bytes p.
func (r *watchReader) add(p []byte) error {
	r.data = append(r.data, p...)
	for {
		if r.msgLeft == 0 {
			if len(r.data) < 5 {
				return nil
			}
			r.msgLeft = int(binary.BigEndian.Uint32(r.data[1:5]))
			r.data = r.data[5:]
			r.msgs++
			r.fragment = false
			if r.msgLeft == 0 {
				continue
			}
		}
		num, typ, n := protowire.ConsumeTag(r.data)
		if n < 0 {
			return nil // more to come
		}
		var m int
		switch typ {
		case protowire.VarintType:
			v, k := protowire.ConsumeVarint(r.data[n:])
			if k < 0 {
				return nil
			}
			m = n + k
			if num == 3 {
				r.created = v != 0
			}
			if num == 7 && v != 0 {
				r.fragment = true
				r.fragments++
			}
		case protowire.BytesType:
			p, k := protowire.ConsumeBytes(r.data[n:])
			if k < 0 {
				return nil
			}
			m = n + k
			if num == 11 {
				var ev mvccpb.Event
				if err := proto.Unmarshal(p, &ev); err != nil {
					return err
				}
				if err := r.ev(r.msgs, &ev); err != nil {
					return err
				}
			}
			if num == 6 {
				r.canceled = string(p)
			}
		case protowire.Fixed32Type, protowire.Fixed64Type, protowire.StartGroupType, protowire.EndGroupType:
			return fmt.Errorf("a field of wire type %d in a WatchResponse", typ)
		}
		if m > r.msgLeft {
			return fmt.Errorf("a field of %d bytes past its response's %d left", m, r.msgLeft)
		}
		r.msgLeft -= m
		r.data = r.data[m:]
	}
}

// openRawWatch opens a Watch of prefix with prev_kv, and fragment if
// asked, on a raw connection of its own, and reads its created response.
func openRawWatch(t *testing.T, tc *testCluster, prefix string, fragment bool, r *watchReader) net.Conn {
	t.Helper()
	conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
	fr := h2Start(t, conn)
	h2Call(t, fr, 1, "/etcdserverpb.Watch/Watch")
	req, err := proto.Marshal(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)), PrevKv: true, Fragment: fragment,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	writeMsg(t, fr, 1, req, false)
	readWatchStream(t, conn, r, func() bool { return !r.created })
	return conn
}

// readWatchStream hands conn's watch stream to r until more reports false.
func readWatchStream(t *testing.T, conn net.Conn, r *watchReader, more func() bool) {
	t.Helper()
	readFrames(t, conn, func(f http2.Frame) bool {
		switch f := f.(type) {
		case *http2.DataFrame:
			if err := r.add(f.Data()); err != nil {
				t.Fatal(err)
			}
		case *http2.RSTStreamFrame:
			t.Fatalf("the watch stream was reset: %v", f.ErrCode)
		case *http2.MetaHeadersFrame:
			if f.StreamEnded() {
				t.Fatalf("the watch stream ended: %v", f.RegularFields())
			}
		}
		return more()
	})
}

// A revision whose events pass one message goes out in pieces (#728,
// ruled: as etcd sends it, within the budget). Here a DeleteRange with
// prev_kv over 384 values of 1 MiB, four times the node's budget and
// taken out of memory and the page cache, is watched with prev_kv, with
// fragment and without. The watch without gets every event and value
// before it in one response, as etcd sends it; the other gets them in
// etcd's fragments; and the clients' send buffers and the fetches stay
// within their bounds throughout.
func TestALargeRevisionsEventsGoOutInPieces(t *testing.T) {
	const budget = 96 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const keys, size = 384, 1 << 20
	value := func(i int) string { return fmt.Sprintf("%04d", i) + strings.Repeat("v", size-4) }
	for i := range keys {
		if _, err := c.Put(ctx, fmt.Sprintf("/big/%04d", i), value(i)); err != nil {
			t.Fatal(err)
		}
	}
	l := loopOf(tc.ids[0])
	z := sizeFor(budget)
	// As TestALargeRangeAndCatchUpStayWithinTheBudget bounds them.
	sendLimit := z.clientBufs() + z.reserve() + 2*maxMsg + 4*outBuf
	var most int64
	sample := func(what string) {
		t.Helper()
		s := seeNode(l)
		most = max(most, s.cout)
		if s.cout > sendLimit {
			t.Fatalf("%s: the clients' send buffers and what waits for them hold %d KiB, past the bound of %d KiB", what, s.cout>>10, sendLimit>>10)
		}
	}
	check := func(want string) func(msg int, ev *mvccpb.Event) error {
		got := 0
		return func(msg int, ev *mvccpb.Event) error {
			if want == "one" && msg != 2 {
				return fmt.Errorf("event %d came in response %d, want all in the one after the created one", got, msg)
			}
			k := fmt.Sprintf("/big/%04d", got)
			if ev.Type != mvccpb.DELETE || string(ev.Kv.Key) != k || ev.PrevKv == nil || string(ev.PrevKv.Key) != k || string(ev.PrevKv.Value) != value(got) {
				return fmt.Errorf("event %d: %v of %s, its value before %d bytes; want the delete of %s with its value", got, ev.Type, ev.Kv.Key, len(ev.PrevKv.GetValue()), k)
			}
			got++
			if got%32 == 0 {
				sample(fmt.Sprintf("after %d events", got))
			}
			return nil
		}
	}
	var one, frags watchReader
	one.ev, frags.ev = check("one"), check("fragments")
	whole := openRawWatch(t, tc, "/big/", false, &one)
	split := openRawWatch(t, tc, "/big/", true, &frags)

	dropped := make(chan struct{})
	l.post(func() error {
		dropRecords(l.n.st)
		l.n.stats.fetchMost = 0
		close(dropped)
		return nil
	})
	<-dropped
	syncFiles(t, tc.dir)
	dropPageCache(t, tc.dir)
	// The delete's own values before pass one message: refused, the keys
	// deleted (#721's second ruling).
	if _, err := c.Delete(ctx, "/big/", clientv3.WithPrefix(), clientv3.WithPrevKV()); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("the delete with its values before: %v, want ResourceExhausted", err)
	}
	if r, err := c.Get(ctx, "/big/", clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || r.Count != 0 {
		t.Fatalf("after the delete %v keys are left: %v", r, err)
	}
	time.Sleep(time.Second) // the node writes what it may before the clients read
	sample("before the clients read")
	n := 0
	countOne := one.ev
	one.ev = func(msg int, ev *mvccpb.Event) error { n++; return countOne(msg, ev) }
	readWatchStream(t, whole, &one, func() bool { return n < keys || one.msgLeft > 0 })
	if one.msgs != 2 || one.fragments != 0 {
		t.Fatalf("the watch without fragment got %d responses, %d fragments; want the created one and one with every event", one.msgs, one.fragments)
	}
	m := 0
	countFrags := frags.ev
	frags.ev = func(msg int, ev *mvccpb.Event) error { m++; return countFrags(msg, ev) }
	readWatchStream(t, split, &frags, func() bool { return m < keys || frags.msgLeft > 0 })
	if frags.msgs < 3 || frags.fragments != frags.msgs-2 || frags.fragment {
		t.Fatalf("the watch with fragment got %d responses, %d of them fragments; want several, all but the last", frags.msgs, frags.fragments)
	}
	got := make(chan [2]int64, 1)
	l.post(func() error {
		got <- [2]int64{l.n.stats.fetchMost, int64(l.n.stats.bigWatches)}
		return nil
	})
	r := <-got
	t.Logf("the largest fetch read %d KiB, the send buffers held at most %d KiB; %d watch responses in pieces; the fragmented watch got %d fragments", r[0]>>10, most>>10, r[1], frags.fragments)
	if r[0] == 0 || r[0] > fetchedMax(budget) {
		t.Fatalf("the largest fetch read %d KiB, want some, within the loop's share of %d KiB", r[0]>>10, fetchedMax(budget)>>10)
	}
	if r[1] < 2 {
		t.Fatalf("%d watch responses went in pieces, want both", r[1])
	}
}

// One put of 1 MiB over a value of 1 MiB, watched with prev_kv as
// kube-apiserver watches, is an event of 2 MiB, past one message's room:
// it comes whole, in one response, its value before with it (#728). At
// 96 MiB the read half does not take two requests of 1 MiB with a
// client's buffers; at 256 MiB it does.
func TestAnEventPastOneMessageComesWhole(t *testing.T) {
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = 256 << 20 })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	value := func(i int) string { return fmt.Sprintf("%04d", i) + strings.Repeat("v", 1<<20-4) }
	if _, err := c.Put(ctx, "/one/k", value(1)); err != nil {
		t.Fatal(err)
	}
	var single watchReader
	var got *mvccpb.Event
	single.ev = func(_ int, ev *mvccpb.Event) error {
		if got != nil {
			return errors.New("a second event")
		}
		got = ev
		return nil
	}
	conn := openRawWatch(t, tc, "/one/", false, &single)
	if _, err := c.Put(ctx, "/one/k", value(2)); err != nil {
		t.Fatal(err)
	}
	readWatchStream(t, conn, &single, func() bool { return got == nil || single.msgLeft > 0 })
	if got.Type != mvccpb.PUT || string(got.Kv.Value) != value(2) || got.PrevKv == nil || string(got.PrevKv.Value) != value(1) || single.msgs != 2 {
		t.Fatalf("the event of 2 MiB came as %v with %d and %d bytes, in %d responses", got.Type, len(got.Kv.Value), len(got.PrevKv.GetValue()), single.msgs)
	}
	l := loopOf(tc.ids[0])
	n := make(chan uint64, 1)
	l.post(func() error { n <- l.n.stats.bigWatches; return nil })
	if k := <-n; k != 1 {
		t.Fatalf("%d watch responses went in pieces, want the one", k)
	}
}
