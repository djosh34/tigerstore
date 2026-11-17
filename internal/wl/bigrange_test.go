package wl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// A range with no limit that matches four times the node's budget
// returns every key and value, as etcd's does, while the node stays
// within its budget (#721, ruled: keep etcd's semantics and keep the
// budget); and a watch that catches up through as much does too
// (finding 22 of #689's merge review). The values are taken out of
// memory and the page cache first, so that every one is fetched off the
// loop. Built at once, the range's reply took 384 MiB of send buffer in
// one turn, and its fetch read every value at once; the watch's catch-up
// fetched every value of the 384 changes at once too.
func TestALargeRangeAndCatchUpStayWithinTheBudget(t *testing.T) {
	const budget = 96 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const keys, size = 384, 1 << 20 // four times the budget
	value := func(i int) string { return fmt.Sprintf("%04d", i) + strings.Repeat("v", size-4) }
	for i := range keys {
		if _, err := c.Put(ctx, fmt.Sprintf("/big/%04d", i), value(i)); err != nil {
			t.Fatal(err)
		}
	}
	l := loopOf(tc.ids[0])
	z := sizeFor(budget)
	// What the clients' send buffers, what waits in them for window and
	// what the replies in pieces hold, together (node.cout), may take:
	// every client within its share, as TestASlowWatcherStaysWithinItsShare
	// bounds it, and a message over it each. The fetches: the loop's share
	// of values fetched (fetchedMax).
	sendLimit := z.clientBufs() + 2*maxMsg
	fetchLimit := fetchedMax(budget)
	away := func() {
		t.Helper()
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
	}
	var most int64
	sample := func(t *testing.T, what string) {
		t.Helper()
		s := seeNode(l)
		most = max(most, s.cout)
		if s.cout > sendLimit {
			t.Fatalf("%s: the clients' send buffers and what waits for them hold %d KiB (share %d KiB, %d clients), past the bound of %d KiB",
				what, s.cout>>10, s.connOut>>10, s.clients, sendLimit>>10)
		}
	}
	checkFetches := func(t *testing.T, what string) {
		t.Helper()
		got := make(chan [2]int64, 1)
		l.post(func() error {
			got <- [2]int64{l.n.stats.fetchMost, int64(l.n.stats.bigRanges)}
			return nil
		})
		r := <-got
		t.Logf("%s: the largest fetch read %d KiB, the send buffers held at most %d KiB; %d replies in pieces", what, r[0]>>10, most>>10, r[1])
		if r[0] == 0 {
			t.Fatalf("%s: nothing was fetched off the loop: the test did not take the values away", what)
		}
		if r[0] > fetchLimit {
			t.Fatalf("%s: one fetch read %d MiB of values, past the loop's share of %d MiB", what, r[0]>>20, fetchLimit>>20)
		}
	}

	t.Run("range", func(t *testing.T) {
		away()
		conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
		fr := h2Start(t, conn)
		h2Call(t, fr, 1, "/etcdserverpb.KV/Range")
		req, err := proto.Marshal(&pb.RangeRequest{Key: []byte("/big/"), RangeEnd: prefixEnd([]byte("/big/")), Serializable: true})
		if err != nil {
			t.Fatal(err)
		}
		writeMsg(t, fr, 1, req, true)
		time.Sleep(time.Second) // the node writes what it may before the client reads
		sample(t, "before the client read")
		var p rangeParser
		got := 0
		p.kv = func(kv *mvccpb.KeyValue) error {
			if want := fmt.Sprintf("/big/%04d", got); string(kv.Key) != want || string(kv.Value) != value(got) {
				return fmt.Errorf("record %d is %s with %d bytes, want %s with its value", got, kv.Key, len(kv.Value), want)
			}
			got++
			if got%32 == 0 {
				sample(t, fmt.Sprintf("after %d records", got))
			}
			return nil
		}
		status := readCall(t, conn, 1, p.add)
		if status != "0" || got != keys || p.count != keys || p.more || p.left != 0 {
			t.Fatalf("grpc-status %s, %d records of %d, count %d, more %v, %d bytes short", status, got, keys, p.count, p.more, p.left)
		}
		checkFetches(t, "range")
	})

	t.Run("watch", func(t *testing.T) {
		away()
		most = 0
		conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
		fr := h2Start(t, conn)
		h2Call(t, fr, 1, "/etcdserverpb.Watch/Watch")
		req, err := proto.Marshal(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
			Key: []byte("/big/"), RangeEnd: prefixEnd([]byte("/big/")), StartRevision: 1,
		}}})
		if err != nil {
			t.Fatal(err)
		}
		writeMsg(t, fr, 1, req, false)
		time.Sleep(time.Second)
		sample(t, "before the client read")
		got := 0
		var data []byte
		readFrames(t, conn, func(f http2.Frame) bool {
			df, ok := f.(*http2.DataFrame)
			if !ok {
				return true
			}
			data = append(data, df.Data()...)
			for len(data) >= 5 {
				k := int(binary.BigEndian.Uint32(data[1:]))
				if len(data) < 5+k {
					break
				}
				var resp pb.WatchResponse
				if err := proto.Unmarshal(data[5:5+k], &resp); err != nil {
					t.Fatal(err)
				}
				data = data[5+k:]
				for _, ev := range resp.Events {
					if want := fmt.Sprintf("/big/%04d", got); string(ev.Kv.Key) != want || string(ev.Kv.Value) != value(got) {
						t.Fatalf("event %d is %s with %d bytes, want %s with its value", got, ev.Kv.Key, len(ev.Kv.Value), want)
					}
					got++
					if got%32 == 0 {
						sample(t, fmt.Sprintf("after %d events", got))
					}
				}
			}
			return got < keys
		})
		checkFetches(t, "watch")
	})
}

// writeMsg sends msg as one gRPC message on stream sid, ending the
// client's side with end.
func writeMsg(t *testing.T, fr *http2.Framer, sid uint32, msg []byte, end bool) {
	t.Helper()
	b := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(b[1:], uint32(len(msg)))
	if err := fr.WriteData(sid, end, append(b, msg...)); err != nil {
		t.Fatal(err)
	}
}

// readFrames reads conn's frames and hands each to f until it returns
// false.
func readFrames(t *testing.T, conn net.Conn, f func(http2.Frame) bool) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(nil, conn)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	fr.MaxHeaderListSize = 1 << 20
	for {
		x, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if !f(x) {
			return
		}
	}
}

// readCall reads a unary call's response on stream sid, handing its DATA
// to data, and returns its grpc-status.
func readCall(t *testing.T, conn net.Conn, sid uint32, data func([]byte) error) string {
	t.Helper()
	status := ""
	readFrames(t, conn, func(f http2.Frame) bool {
		switch f := f.(type) {
		case *http2.DataFrame:
			if f.StreamID == sid {
				if err := data(f.Data()); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.MetaHeadersFrame:
			if f.StreamID == sid && f.StreamEnded() {
				status = f.PseudoValue("status")
				for _, h := range f.RegularFields() {
					if h.Name == "grpc-status" {
						status = h.Value
					}
				}
				return false
			}
		case *http2.RSTStreamFrame:
			t.Fatalf("stream %d reset: %v", f.StreamID, f.ErrCode)
		}
		return true
	})
	return status
}

// rangeParser takes a RangeResponse's message as it comes and hands on
// each record as it is whole, holding no more than one.
type rangeParser struct {
	pend  []byte
	begun bool
	left  int // message bytes still to come
	count int64
	more  bool
	kv    func(*mvccpb.KeyValue) error
}

func (p *rangeParser) add(b []byte) error {
	p.pend = append(p.pend, b...)
	if !p.begun {
		if len(p.pend) < 5 {
			return nil
		}
		p.left = int(binary.BigEndian.Uint32(p.pend[1:]))
		p.pend, p.begun = p.pend[5:], true
	}
	for len(p.pend) > 0 {
		num, typ, n := protowire.ConsumeTag(p.pend)
		if n < 0 {
			return nil
		}
		var m int
		switch typ { //nolint:exhaustive // a RangeResponse holds no other
		case protowire.VarintType:
			v, k := protowire.ConsumeVarint(p.pend[n:])
			if k < 0 {
				return nil
			}
			switch num { //nolint:exhaustive // its varint fields
			case 3:
				p.more = v != 0
			case 4:
				p.count = int64(v)
			}
			m = n + k
		case protowire.BytesType:
			v, k := protowire.ConsumeBytes(p.pend[n:])
			if k < 0 {
				return nil
			}
			if num == 2 {
				var kv mvccpb.KeyValue
				if err := proto.Unmarshal(v, &kv); err != nil {
					return err
				}
				if err := p.kv(&kv); err != nil {
					return err
				}
			}
			m = n + k
		default:
			return errors.New("a field of an unknown type")
		}
		p.left -= m
		p.pend = append(p.pend[:0], p.pend[m:]...)
	}
	return nil
}

// A range whose reply is sent in pieces answers as etcd's steps answer it
// (executeRange): filtered, sorted with etcd's sort, ties in its order,
// cut to the limit, with the count of the whole range and more; keys
// only; at a past revision while writes go on; and a RangeStream in
// etcd's chunks. Each reply here is past what the node builds at once on
// a budget of 96 MiB.
func TestALargeRangeAnswersAsEtcdDoes(t *testing.T) {
	const budget = 96 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const keys, size = 48, 96 << 10
	put := func(i, round int) {
		t.Helper()
		v := fmt.Sprintf("%02d-%d-", i, round) + strings.Repeat("v", size)
		if _, err := c.Put(ctx, fmt.Sprintf("/eq/%02d", i), v); err != nil {
			t.Fatal(err)
		}
	}
	for i := range keys {
		put(i, 0)
	}
	// Some keys again, out of order: versions, mods and creates differ,
	// and some tie.
	for _, i := range []int{7, 3, 40, 3, 22, 7, 7, 15} {
		put(i, 1)
	}
	if _, err := c.Delete(ctx, "/eq/30"); err != nil {
		t.Fatal(err)
	}
	put(30, 2)
	l := loopOf(tc.ids[0])
	bigs := func() uint64 {
		got := make(chan uint64, 1)
		l.post(func() error {
			got <- l.n.stats.bigRanges
			return nil
		})
		return <-got
	}
	kv := pb.NewKVClient(c.ActiveConnection())
	big := grpc.MaxCallRecvMsgSize(64 << 20)
	full, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/eq/"), RangeEnd: prefixEnd([]byte("/eq/")), Serializable: true}, big)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Kvs) != keys {
		t.Fatalf("%d keys, want %d", len(full.Kvs), keys)
	}
	rev := full.Header.Revision
	// etcd's steps on the records in key order.
	want := func(r *pb.RangeRequest) *pb.RangeResponse {
		var kvs []*mvccpb.KeyValue
		for _, x := range full.Kvs {
			if r.MaxModRevision != 0 && x.ModRevision > r.MaxModRevision || r.MinModRevision != 0 && x.ModRevision < r.MinModRevision ||
				r.MaxCreateRevision != 0 && x.CreateRevision > r.MaxCreateRevision || r.MinCreateRevision != 0 && x.CreateRevision < r.MinCreateRevision {
				continue
			}
			kvs = append(kvs, x)
		}
		order := r.SortOrder
		if r.SortTarget != pb.RangeRequest_KEY && order == pb.RangeRequest_NONE {
			order = pb.RangeRequest_ASCEND
		}
		if order != pb.RangeRequest_NONE && (r.SortTarget != pb.RangeRequest_KEY || order != pb.RangeRequest_ASCEND) {
			var s sort.Interface = pbKVSort{kvs, r.SortTarget}
			if order == pb.RangeRequest_DESCEND {
				s = sort.Reverse(s)
			}
			sort.Sort(s)
		}
		resp := &pb.RangeResponse{Count: keys}
		if r.Limit > 0 && int64(len(kvs)) > r.Limit {
			kvs, resp.More = kvs[:r.Limit], true
		}
		for _, x := range kvs {
			if r.KeysOnly {
				x = &mvccpb.KeyValue{Key: x.Key, CreateRevision: x.CreateRevision, ModRevision: x.ModRevision, Version: x.Version}
			}
			resp.Kvs = append(resp.Kvs, x)
		}
		return resp
	}
	same := func(what string, got, want *pb.RangeResponse) {
		t.Helper()
		if got.Count != want.Count || got.More != want.More || len(got.Kvs) != len(want.Kvs) {
			t.Fatalf("%s: count %d, more %v, %d records; want %d, %v, %d", what, got.Count, got.More, len(got.Kvs), want.Count, want.More, len(want.Kvs))
		}
		for i := range got.Kvs {
			if !proto.Equal(got.Kvs[i], want.Kvs[i]) {
				t.Fatalf("%s: record %d is %s at mod %d, want %s at mod %d", what, i, got.Kvs[i].Key, got.Kvs[i].ModRevision, want.Kvs[i].Key, want.Kvs[i].ModRevision)
			}
		}
	}
	base := func() *pb.RangeRequest {
		return &pb.RangeRequest{Key: []byte("/eq/"), RangeEnd: prefixEnd([]byte("/eq/")), Serializable: true, Revision: rev}
	}
	// Writes go on meanwhile: the ranges read at rev all the same.
	stop := make(chan struct{})
	writing := make(chan struct{})
	go func() {
		defer close(writing)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := c.Put(ctx, fmt.Sprintf("/eq/%02d", i%keys), "later"); err != nil {
				return
			}
		}
	}()
	before := bigs()
	for what, r := range map[string]func(*pb.RangeRequest){
		"all":                     func(*pb.RangeRequest) {},
		"limited":                 func(r *pb.RangeRequest) { r.Limit = 30 },
		"keys only":               func(r *pb.RangeRequest) { r.KeysOnly = true; r.Serializable = false },
		"by mod descending":       func(r *pb.RangeRequest) { r.SortTarget, r.SortOrder = pb.RangeRequest_MOD, pb.RangeRequest_DESCEND },
		"by version, limited":     func(r *pb.RangeRequest) { r.SortTarget, r.Limit = pb.RangeRequest_VERSION, 40 },
		"by create descending":    func(r *pb.RangeRequest) { r.SortTarget, r.SortOrder = pb.RangeRequest_CREATE, pb.RangeRequest_DESCEND },
		"by key descending":       func(r *pb.RangeRequest) { r.SortTarget, r.SortOrder = pb.RangeRequest_KEY, pb.RangeRequest_DESCEND },
		"filtered by mod":         func(r *pb.RangeRequest) { r.MinModRevision = full.Kvs[5].ModRevision },
		"filtered by create, cut": func(r *pb.RangeRequest) { r.MaxCreateRevision, r.Limit = full.Kvs[40].CreateRevision, 35 },
		"by version, keys only":   func(r *pb.RangeRequest) { r.SortTarget, r.KeysOnly = pb.RangeRequest_VERSION, true },
	} {
		req := base()
		r(req)
		got, err := kv.Range(ctx, req, big)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		same(what, got, want(req))
	}
	if n := bigs() - before; n < 8 {
		t.Fatalf("%d of the ranges were sent in pieces: the test's replies were not large", n)
	}
	// A RangeStream: its chunks put together are the range, the first of
	// 10 keys and each next one as etcd's adjustChunkLimit sizes it.
	before = bigs()
	st, err := kv.RangeStream(ctx, base(), big)
	if err != nil {
		t.Fatal(err)
	}
	var got []*mvccpb.KeyValue
	var last *pb.RangeResponse
	limit, rest := int64(initialStreamChunk), int64(keys)
	for {
		resp, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		r := resp.RangeResponse
		if int64(len(r.Kvs)) != min(limit, rest) {
			t.Fatalf("a chunk of %d keys after %d, want %d", len(r.Kvs), len(got), min(limit, rest))
		}
		sz := sizeBytesField(1, sizeVarintField(3, uint64(full.Header.Revision))) + 2 + sizeVarintField(4, uint64(min(rest, limit+1)))
		for _, x := range r.Kvs {
			b, _ := proto.Marshal(x)
			sz += sizeBytesField(2, len(b))
		}
		got = append(got, r.Kvs...)
		rest -= int64(len(r.Kvs))
		last = r
		limit = nextChunk(limit, sz, maxRequestBytes)
	}
	close(stop)
	<-writing
	same("a RangeStream", &pb.RangeResponse{Kvs: got, Count: last.Count, More: last.More}, want(base()))
	if last.Header == nil || bigs() == before {
		t.Fatalf("the RangeStream's last chunk has header %v; %d sent in pieces", last.Header, bigs()-before)
	}
}

// pbKVSort is etcd's kvSortBy*, by target.
type pbKVSort struct {
	kvs    []*mvccpb.KeyValue
	target pb.RangeRequest_SortTarget
}

func (s pbKVSort) Len() int      { return len(s.kvs) }
func (s pbKVSort) Swap(i, j int) { s.kvs[i], s.kvs[j] = s.kvs[j], s.kvs[i] }

func (s pbKVSort) Less(i, j int) bool {
	a, b := s.kvs[i], s.kvs[j]
	switch s.target { //nolint:exhaustive // by key below
	case pb.RangeRequest_VERSION:
		return a.Version-b.Version < 0
	case pb.RangeRequest_CREATE:
		return a.CreateRevision-b.CreateRevision < 0
	case pb.RangeRequest_MOD:
		return a.ModRevision-b.ModRevision < 0
	case pb.RangeRequest_VALUE:
		return bytes.Compare(a.Value, b.Value) < 0
	}
	return bytes.Compare(a.Key, b.Key) < 0
}
