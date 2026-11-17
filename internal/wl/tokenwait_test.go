package wl

import (
	"bytes"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// tokenCall is the frames of a call of method on stream sid with token:
// its headers, then its request m and the end of its side in one DATA
// frame, then a PING.
func tokenCall(t *testing.T, enc *hpack.Encoder, sid uint32, method, token string, m proto.Message, extra ...hpack.HeaderField) []byte {
	t.Helper()
	var buf, hb bytes.Buffer
	*enc = *hpack.NewEncoder(&hb)
	for _, f := range append([]hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: method},
		{Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
		{Name: "token", Value: token},
	}, extra...) {
		if err := enc.WriteField(f); err != nil {
			t.Fatal(err)
		}
	}
	fr := http2.NewFramer(&buf, nil)
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: sid, BlockFragment: hb.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := fr.WriteData(sid, true, append([]byte{0, byte(len(b) >> 24), byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}, b...)); err != nil {
		t.Fatal(err)
	}
	if err := fr.WritePing(false, [8]byte{1}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// tokenNode is a node with auth on, 10 entries applied and its log to
// 20, and a client of it; feed has the client's frames read, and refused
// counts the calls of a kind refused for their token.
func tokenNode(t *testing.T) (*node, *client, *loop, func([]byte), func(uint8) int64) {
	t.Helper()
	n := authNode(t)
	c, _ := flowClient(t)
	c.node = n
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.now = time.Now()
	n.applied = 10
	n.storage.snapIndex = 20 // the log holds entries to 20
	n.sizes, n.connOut, n.maxStreams = sizeFor(256<<20), 64<<10, defaultMaxStreams
	feed := func(b []byte) {
		t.Helper()
		c.pt, c.pr, c.pw, c.preface = append(c.pt[:0], b...), 0, len(b), true
		if err := c.frames(n); err != nil {
			t.Fatal(err)
		}
	}
	refused := func(kind uint8) int64 { return n.gs.calls[kind].handled[codes.Unauthenticated] }
	return n, c, &loop{n: n}, feed, refused
}

// A call whose simple token names an entry in the node's log that it has
// not applied waits for it, as etcd's isValidSimpleToken does: the node
// reads none of its connection's frames past its headers until the entry
// applies, then reads on, its request and its stream's end in their
// order. A watch create whose token's entry does not apply in
// requestTimeout is canceled as any with an unknown token, its stream
// open. Refused at once: a token of an applied entry that the node does
// not know, of an entry past its log, of no dot or two; and calls etcd
// checks no token of do not wait. Refused at once, a lagging node refused
// a valid token: a watch was canceled as "invalid auth token" (#689's CI
// loop for #733).
func TestACallWaitsForTheEntryOfItsToken(t *testing.T) {
	n, c, l, feed, refused := tokenNode(t)
	var enc hpack.Encoder

	feed(tokenCall(t, &enc, 3, "/etcdserverpb.KV/Range", "abc.12", &etcdpb.RangeRequest{Key: []byte("b")}))
	if c.tokenWait != 12 || len(n.tokenWaits) != 1 || len(n.reads) != 0 || c.pr == c.pw || refused(kRange) != 0 {
		t.Fatalf("a call whose token names entry 12, 10 applied: waits for %d, %d clients wait, %d reads, %d bytes unread, %d refused", c.tokenWait, len(n.tokenWaits), len(n.reads), c.pw-c.pr, refused(kRange))
	}
	l.resumeTokenWaits()
	if c.tokenWait != 12 || len(n.reads) != 0 {
		t.Fatal("the connection read on before the token's entry applied")
	}
	if _, err := n.authenticated(&etcdpb.InternalAuthenticateRequest{Name: "alice", SimpleToken: "abc"}, 12, &etcdpb.ResponseHeader{}); err != nil {
		t.Fatal(err)
	}
	n.applied = 12
	l.resumeTokenWaits()
	if c.tokenWait != 0 || len(n.tokenWaits) != 0 || len(n.reads) != 1 || c.pr != c.pw || refused(kRange) != 0 {
		t.Fatalf("once entry 12 applied: waits for %d, %d clients wait, %d reads, %d bytes unread, %d refused", c.tokenWait, len(n.tokenWaits), len(n.reads), c.pw-c.pr, refused(kRange))
	}
	if s := c.get(3); s == nil || !s.ended {
		t.Fatal("the call's stream did not take its end, which came behind its request")
	}

	// A watch create whose token's entry does not come: after
	// requestTimeout it is canceled, and its stream stays open.
	feed(tokenCall(t, &enc, 5, "/etcdserverpb.Watch/Watch", "abc.15", &etcdpb.WatchRequest{RequestUnion: &etcdpb.WatchRequest_CreateRequest{CreateRequest: &etcdpb.WatchCreateRequest{Key: []byte("b")}}}))
	if c.tokenWait != 15 {
		t.Fatalf("a watch whose token names entry 15 waits for %d", c.tokenWait)
	}
	n.now = n.now.Add(requestTimeout - time.Millisecond)
	l.resumeTokenWaits()
	if c.tokenWait != 15 {
		t.Fatal("the connection read on before requestTimeout")
	}
	n.now = n.now.Add(time.Millisecond)
	l.resumeTokenWaits()
	if s := c.get(5); c.tokenWait != 0 || s == nil || !s.watch || len(s.wids) != 0 || refused(kWatch) != 0 {
		t.Fatalf("at requestTimeout: waits for %d, stream %v, refused %d; want the create canceled and its stream open", c.tokenWait, s != nil, refused(kWatch))
	}

	// A token of an entry past the log: refused at once. Waited for, a
	// made-up token stalled its connection, the node's own client among
	// them (the L re-check of d33d3711).
	feed(tokenCall(t, &enc, 15, "/etcdserverpb.KV/Range", "abc.99", &etcdpb.RangeRequest{Key: []byte("b")}))
	if c.tokenWait != 0 || refused(kRange) != 1 {
		t.Fatalf("a token of an entry past the log: waits for %d, %d refused", c.tokenWait, refused(kRange))
	}
	// An unknown method, answered with no token checked, does not wait.
	feed(tokenCall(t, &enc, 17, "/etcdserverpb.KV/NoSuchMethod", "abc.15", &etcdpb.RangeRequest{}))
	if c.tokenWait != 0 {
		t.Fatalf("an unknown method waits for %d", c.tokenWait)
	}
	if !healthKind(paths["/grpc.health.v1.Health/Check"]) || healthKind(paths["/v3lockpb.Lock/Lock"]) {
		t.Fatal("Health's methods are not told from the other side calls")
	}

	// A token of more than one dot names no entry: refused at once, as
	// etcd's.
	feed(tokenCall(t, &enc, 19, "/etcdserverpb.KV/Range", "a.b.15", &etcdpb.RangeRequest{Key: []byte("b")}))
	if c.tokenWait != 0 || refused(kRange) != 2 {
		t.Fatalf("a token of two dots: waits for %d, %d refused", c.tokenWait, refused(kRange))
	}

	// An Authenticate with a token that names an entry not applied
	// neither waits nor is refused for it: etcd checks no token of one.
	s13 := c.newStream(21, kAuthenticate)
	s13.token = "abc.99"
	if err := n.request(c, s13, appendBytesField(appendBytesField(nil, 1, []byte("alice")), 2, []byte("pw"))); err != nil {
		t.Fatal(err)
	}
	if c.tokenWait != 0 || refused(kAuthenticate) != 0 {
		t.Fatalf("an Authenticate with a stale token: waits for %d, %d refused as an invalid token", c.tokenWait, refused(kAuthenticate))
	}

	// A token of an applied entry, unknown: refused at once.
	feed(tokenCall(t, &enc, 23, "/etcdserverpb.KV/Range", "abc.11", &etcdpb.RangeRequest{Key: []byte("b")}))
	if c.tokenWait != 0 || refused(kRange) != 3 {
		t.Fatalf("a token of an applied entry, unknown: waits for %d, %d refused", c.tokenWait, refused(kRange))
	}
}

// A token of an entry this node's log does not hold yet is refused at
// once, and the connection's next call is read with it: the node waits
// only for entries in its log, where etcd waits for any (#736's first
// limit). A client refused so authenticates again and retries, as
// clientv3 does.
func TestATokenOfAnEntryNotInTheLogIsRefusedAtOnce(t *testing.T) {
	n, c, _, feed, refused := tokenNode(t)
	if _, err := n.authenticated(&etcdpb.InternalAuthenticateRequest{Name: "alice", SimpleToken: "ok"}, 10, &etcdpb.ResponseHeader{}); err != nil {
		t.Fatal(err)
	}
	var enc hpack.Encoder
	feed(append(tokenCall(t, &enc, 3, "/etcdserverpb.KV/Range", "abc.21", &etcdpb.RangeRequest{Key: []byte("b")}),
		tokenCall(t, &enc, 5, "/etcdserverpb.KV/Range", "ok.10", &etcdpb.RangeRequest{Key: []byte("b")})...))
	if c.tokenWait != 0 || len(n.tokenWaits) != 0 || refused(kRange) != 1 || len(n.reads) != 1 || c.pr != c.pw {
		t.Fatalf("a token of entry 21, the log to 20: waits for %d, %d clients wait, %d refused, %d reads, %d bytes unread; want it refused at once and the next call read",
			c.tokenWait, len(n.tokenWaits), refused(kRange), len(n.reads), c.pw-c.pr)
	}
}

// A call whose token's entry is in the log but never applies holds its
// connection for requestTimeout and no longer: then it is refused as any
// call of an unknown token, and the connection's other calls, behind it,
// go on (#736's bound).
func TestACallWhoseEntryNeverAppliesIsRefusedAfterRequestTimeout(t *testing.T) {
	n, c, l, feed, refused := tokenNode(t)
	if _, err := n.authenticated(&etcdpb.InternalAuthenticateRequest{Name: "alice", SimpleToken: "ok"}, 10, &etcdpb.ResponseHeader{}); err != nil {
		t.Fatal(err)
	}
	var enc hpack.Encoder
	feed(append(tokenCall(t, &enc, 3, "/etcdserverpb.KV/Range", "abc.15", &etcdpb.RangeRequest{Key: []byte("b")}),
		tokenCall(t, &enc, 5, "/etcdserverpb.KV/Range", "ok.10", &etcdpb.RangeRequest{Key: []byte("b")})...))
	if c.tokenWait != 15 || len(n.reads) != 0 || refused(kRange) != 0 {
		t.Fatalf("a token of entry 15, 10 applied: waits for %d, %d reads, %d refused", c.tokenWait, len(n.reads), refused(kRange))
	}
	n.now = n.now.Add(requestTimeout - time.Millisecond)
	l.resumeTokenWaits()
	if c.tokenWait != 15 || len(n.reads) != 0 {
		t.Fatal("the connection read on before requestTimeout")
	}
	n.now = n.now.Add(time.Millisecond)
	l.resumeTokenWaits()
	if c.tokenWait != 0 || len(n.tokenWaits) != 0 || refused(kRange) != 1 || len(n.reads) != 1 || c.pr != c.pw {
		t.Fatalf("at requestTimeout: waits for %d, %d clients wait, %d refused, %d reads, %d bytes unread; want the call refused and the next one read",
			c.tokenWait, len(n.tokenWaits), refused(kRange), len(n.reads), c.pw-c.pr)
	}
}

// A call that waits for its token's entry stops waiting once the entry
// leaves the log, cut by Raft or replaced by a snapshot, and is refused;
// a call of an encoding the node cannot read is answered Unimplemented
// with no wait, as etcd checks no token of it (#689's M re-check).
func TestATokenWaitEndsWithItsEntryAndSkipsAnUnreadableCall(t *testing.T) {
	n, c, l, feed, refused := tokenNode(t)
	var enc hpack.Encoder
	feed(tokenCall(t, &enc, 3, "/etcdserverpb.KV/Range", "abc.15", &etcdpb.RangeRequest{Key: []byte("b")}))
	if c.tokenWait != 15 {
		t.Fatalf("a token of entry 15 waits for %d", c.tokenWait)
	}
	n.storage.snapIndex = 11 // Raft cut the log after 11
	l.resumeTokenWaits()
	if c.tokenWait != 0 || refused(kRange) != 1 {
		t.Fatalf("the log cut to 11: waits for %d, %d refused", c.tokenWait, refused(kRange))
	}
	n.storage.snapIndex = 20
	feed(tokenCall(t, &enc, 5, "/etcdserverpb.KV/Range", "abc.15", &etcdpb.RangeRequest{Key: []byte("b")}, hpack.HeaderField{Name: "grpc-encoding", Value: "snappy"}))
	if c.tokenWait != 0 || refused(kRange) != 1 {
		t.Fatalf("a call of an encoding the node cannot read: waits for %d, %d refused for its token", c.tokenWait, refused(kRange))
	}
}
