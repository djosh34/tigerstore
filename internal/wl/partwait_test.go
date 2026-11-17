package wl

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
)

// partClient is a client of n that has read its preface.
func partClient(t *testing.T, n *node, idx int) *client {
	t.Helper()
	b, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(&handoff{fd: -1, wr: aeadDir{aead: g}, rd: aeadDir{aead: g}}, n)
	c.idx, c.gen, c.preface = idx, 1, true
	c.takeBufs()
	return c
}

// putPart is the frames of the start of a Put of partSize bytes on
// stream sid, its headers and first part bytes, or of part more bytes of
// it when first is false, and a PING behind them.
func putPart(t *testing.T, sid uint32, first bool) []byte {
	t.Helper()
	var buf, hb bytes.Buffer
	fr := http2.NewFramer(&buf, nil)
	var p []byte
	if first {
		enc := hpack.NewEncoder(&hb)
		for _, f := range []hpack.HeaderField{
			{Name: ":method", Value: "POST"},
			{Name: ":scheme", Value: "https"},
			{Name: ":path", Value: "/etcdserverpb.KV/Put"},
			{Name: ":authority", Value: "localhost"},
			{Name: "content-type", Value: "application/grpc"},
			{Name: "te", Value: "trailers"},
		} {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: sid, BlockFragment: hb.Bytes(), EndHeaders: true}); err != nil {
			t.Fatal(err)
		}
		p = binary.BigEndian.AppendUint32([]byte{0}, partSize)
	}
	p = append(p, make([]byte, part-len(p))...)
	if err := fr.WriteData(sid, false, p); err != nil {
		t.Fatal(err)
	}
	if err := fr.WritePing(false, [8]byte{1}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// partNode is a node of a 128 MiB budget with clients a and b, whose read
// half of the client buffers other clients fill; feed has a client's
// frames read, and tooMany counts the Puts refused as too many requests.
func partNode(t *testing.T) (n *node, a, b *client, l *loop, feed func(*client, []byte), tooMany func() int64) {
	t.Helper()
	n, _ = testNode(t)
	n.now = time.Now()
	n.sizes, n.connOut, n.maxStreams = sizeFor(128<<20), 64<<10, defaultMaxStreams
	a, b = partClient(t, n, 0), partClient(t, n, 1)
	n.socks, n.gens = []any{a, b}, []uint16{1, 1}
	n.connIn = n.sizes.connIn(n.active)
	n.cpool = n.sizes.clientInBufs() // other clients hold the rest
	feed = func(c *client, p []byte) {
		t.Helper()
		c.pt, c.pr, c.pw = append(c.pt[:0], p...), 0, len(p)
		if err := c.frames(n); err != nil {
			t.Fatal(err)
		}
	}
	tooMany = func() int64 { return n.gs.calls[kPut].handled[codes.ResourceExhausted] }
	return n, a, b, &loop{n: n}, feed, tooMany
}

const partSize, part = 64 << 10, 16 << 10

// A part of a request sent in parts that finds the read half of the
// client buffers full waits its turn for room instead of being refused
// (#735): its connection reads nothing past it until the buffers have
// room. When none comes, the first in line takes the turn, with room of
// the reserve for what its share has left, and goes on past the read
// half; the turn ends once its client holds no request in part, and its
// room goes back. Another that waits requestTimeout while the reserve is
// spent is refused as before, and its connection reads on.
func TestAPartWaitsItsTurnForRoom(t *testing.T) {
	n, a, b, l, feed, tooMany := partNode(t)
	full := n.sizes.clientInBufs()

	feed(a, putPart(t, 1, true))
	if a.roomNeed != part || len(n.waiting) != 1 || a.pw-a.pr != 9+part+17 || a.partial != 0 || tooMany() != 0 || n.stats.partsWaited != 1 {
		t.Fatalf("a part, the buffers full: needs %d, %d wait, %d bytes unread, %d held in part, %d refused", a.roomNeed, len(n.waiting), a.pw-a.pr, a.partial, tooMany())
	}
	// The buffers get room: the part goes on, and the rest behind it.
	n.cpool -= 2 * part
	l.serveWaiting()
	if a.roomNeed != 0 || len(n.waiting) != 0 || a.pr != a.pw || a.partial != part || n.inTurn != nil {
		t.Fatalf("with room: needs %d, %d wait, %d bytes unread, %d held in part, turn %v", a.roomNeed, len(n.waiting), a.pw-a.pr, a.partial, n.inTurn != nil)
	}

	// The buffers fill, and no room comes: the first in line takes the
	// turn, and goes on past the read half.
	n.cpool += 2*part + part
	feed(a, putPart(t, 1, false))
	feed(b, putPart(t, 3, true))
	if a.roomNeed != part || b.roomNeed != part || len(n.waiting) != 2 {
		t.Fatalf("the buffers full: a needs %d, b %d, %d wait", a.roomNeed, b.roomNeed, len(n.waiting))
	}
	l.serveWaiting()
	if n.inTurn != a || n.turnRoom != n.connIn-part || n.reserved != n.turnRoom || a.roomNeed != 0 || a.partial != 2*part || n.cpool <= full || b.roomNeed != part || len(n.waiting) != 1 {
		t.Fatalf("no room: turn %v with %d of the reserve's %d, a needs %d and holds %d in part, the buffers hold %d of %d, b needs %d, %d wait",
			n.inTurn == a, n.turnRoom, n.reserved, a.roomNeed, a.partial, n.cpool, full, b.roomNeed, len(n.waiting))
	}
	// The turn's client goes on within its room.
	feed(a, putPart(t, 1, false))
	if a.roomNeed != 0 || a.partial != 3*part || tooMany() != 0 {
		t.Fatalf("the turn's client: needs %d, holds %d in part, %d refused", a.roomNeed, a.partial, tooMany())
	}
	// It holds no request in part: the turn and its room go to b.
	a.endStream(a.get(1))
	l.serveWaiting()
	if n.inTurn != b || n.reserved != n.turnRoom || b.roomNeed != 0 || b.partial != part || len(n.waiting) != 0 {
		t.Fatalf("a's requests done: turn to b %v, %d of the reserve's %d, b needs %d and holds %d, %d wait", n.inTurn == b, n.turnRoom, n.reserved, b.roomNeed, b.partial, len(n.waiting))
	}
	b.endStream(b.get(3))
	l.serveWaiting()
	if n.inTurn != nil || n.reserved != 0 {
		t.Fatalf("the turn outlived its client's requests in part: turn %v, %d of the reserve taken", n.inTurn != nil, n.reserved)
	}

	// The reserve spent on replies: no turn, and a part that waits
	// requestTimeout is refused, as before, and its connection reads on.
	n.reserved = n.sizes.reserve()
	feed(a, putPart(t, 5, true))
	n.now = n.now.Add(requestTimeout - time.Millisecond)
	l.serveWaiting()
	if a.roomNeed != part || n.inTurn != nil || tooMany() != 0 {
		t.Fatalf("before requestTimeout: needs %d, turn %v, %d refused", a.roomNeed, n.inTurn != nil, tooMany())
	}
	n.now = n.now.Add(time.Millisecond)
	l.serveWaiting()
	if a.roomNeed != 0 || len(n.waiting) != 0 || a.pr != a.pw || a.partial != 0 || tooMany() != 1 {
		t.Fatalf("at requestTimeout: needs %d, %d wait, %d bytes unread, %d held in part, %d refused", a.roomNeed, len(n.waiting), a.pw-a.pr, a.partial, tooMany())
	}
}

// A turn lasts requestTimeout at most: a client that took it and sends
// no more of its requests in part has them refused, and the room goes
// back to the reserve, whose replies may wait for it.
func TestATurnEndsAtRequestTimeout(t *testing.T) {
	n, a, _, l, feed, tooMany := partNode(t)
	feed(a, putPart(t, 1, true))
	l.serveWaiting()
	if n.inTurn != a || a.partial != part || n.reserved == 0 {
		t.Fatalf("no room: turn %v, a holds %d in part, %d of the reserve taken", n.inTurn == a, a.partial, n.reserved)
	}
	feed(a, putPart(t, 3, true))
	if a.partial != 2*part {
		t.Fatalf("the turn's client holds %d in part", a.partial)
	}
	n.now = n.now.Add(requestTimeout)
	l.serveWaiting()
	if n.inTurn != nil || n.reserved != 0 || a.partial != 0 || tooMany() != 2 || a.get(1) != nil || a.get(3) != nil {
		t.Fatalf("at requestTimeout: turn %v, %d of the reserve taken, a holds %d in part, %d refused", n.inTurn != nil, n.reserved, a.partial, tooMany())
	}
}

// A part that can never fit, past the connection's share of requests
// sent in part, is refused at once and does not wait.
func TestAPartPastTheShareIsRefusedAtOnce(t *testing.T) {
	n, _ := testNode(t)
	n.now = time.Now()
	n.sizes, n.connOut, n.maxStreams = sizeFor(128<<20), 64<<10, defaultMaxStreams
	a := partClient(t, n, 0)
	n.socks, n.gens = []any{a}, []uint16{1}
	n.connIn = n.sizes.connIn(n.active)
	n.cpool = n.sizes.clientInBufs()
	a.partial = n.connIn - 1024 // its streams hold its share, near enough
	p := putPart(t, 1, true)
	a.pt, a.pr, a.pw = append(a.pt[:0], p...), 0, len(p)
	if err := a.frames(n); err != nil {
		t.Fatal(err)
	}
	if a.roomNeed != 0 || len(n.waiting) != 0 || a.pr != a.pw || n.gs.calls[kPut].handled[codes.ResourceExhausted] != 1 {
		t.Fatalf("a part past the share: needs %d, %d wait, %d bytes unread, %d refused", a.roomNeed, len(n.waiting), a.pw-a.pr, n.gs.calls[kPut].handled[codes.ResourceExhausted])
	}
}

// partTail is what a stream's buffer keeps of a message not yet whole.
func TestPartTail(t *testing.T) {
	msg := func(k int) []byte {
		return append(binary.BigEndian.AppendUint32([]byte{0}, uint32(k)), make([]byte, k)...)
	}
	whole := msg(10)
	for _, tc := range []struct {
		a, b []byte
		want int64
	}{
		{nil, whole, 0},
		{whole[:3], whole[3:], 0},
		{whole[:3], nil, 3},
		{nil, append(append([]byte{}, whole...), msg(20)[:7]...), 7},
		{whole[:7], append(append([]byte{}, whole[7:]...), msg(20)[:2]...), 2},
		{nil, binary.BigEndian.AppendUint32([]byte{0}, maxMsg+1), 0},
	} {
		if got := partTail(tc.a, tc.b); got != tc.want {
			t.Errorf("partTail(%d bytes, %d bytes) = %d, want %d", len(tc.a), len(tc.b), got, tc.want)
		}
	}
}
