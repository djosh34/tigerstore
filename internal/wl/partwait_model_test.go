package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

// TestTheReadHalfModel checks the invariant of partwait.go against a
// randomized model: clients that keep HTTP/2's flow control as grpc-go's
// do, each sending Puts and side calls (Lock) of every size up to the
// largest message, in parts, on many streams at once; some stall within
// their windows for longer than requestTimeout, some reset streams, and
// replies hold the reserve at times. The real client code reads their
// frames, in pieces as a socket gives them, with the loop's line and
// turns, and with no cluster. After every step:
//
//   - the read half holds what its clients hold, no more and no less, and
//     stays within its half and the turn's room (rule 1);
//   - a request is refused only when its client stalled (rule 2);
//
// and once the stalls end and no new requests start, every request comes
// whole, each within a bound set by the stalls alone (rules 2 and 3).
// TIGERSTORE_MODEL_CASES sets how many seeds it runs; a failing seed is
// rerun with TIGERSTORE_MODEL_SEED.
func TestTheReadHalfModel(t *testing.T) {
	cases := 24
	if v, err := strconv.Atoi(os.Getenv("TIGERSTORE_MODEL_CASES")); err == nil {
		cases = v
	}
	first := uint64(1)
	if v, err := strconv.ParseUint(os.Getenv("TIGERSTORE_MODEL_SEED"), 10, 64); err == nil {
		first, cases = v, 1
	}
	for seed := first; seed < first+uint64(cases); seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runReadHalfModel(t, seed) })
	}
}

// modelSeeds are seeds that once failed, kept as cases of their own.
var modelSeeds = []uint64{}

func TestTheReadHalfModelSeeds(t *testing.T) {
	for _, seed := range modelSeeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runReadHalfModel(t, seed) })
	}
}

// mStream is a model client's stream: a call whose one message is msg
// bytes long, 5+msg of DATA.
type mStream struct {
	id      uint32
	side    bool
	msg     int
	sent    int   // DATA sent of 5+msg
	granted int64 // stream WINDOW_UPDATEs the node gave
	endSent bool
	// done says the node took the request whole (answered it, or kept a
	// side call's and took it at its end); refused, that it answered
	// "too many requests".
	done, refused, reset bool
	start, end        time.Time
}

// mClient is a model client and the node's client of it.
type mClient struct {
	c       *client
	enc     *hpack.Encoder
	encBuf  bytes.Buffer
	dec     *hpack.Decoder
	status  string
	streams []*mStream
	byID    map[uint32]*mStream
	nextID  uint32
	connWin int64 // the connection window the node gave, less what was sent
	init    int64 // the initial stream window, as the node last set it
	outbox  []byte
	// stalled until stallEnd: it sends nothing; stalledTo is when its
	// last stall ended.
	stalled             bool
	stallEnd, stalledTo time.Time
	stalls              int
}

func (m *mClient) window(s *mStream) int64 { return m.init + s.granted - int64(s.sent) }

type readHalfModel struct {
	t       *testing.T
	rng     *rand.Rand
	n       *node
	l       *loop
	clients []*mClient
	// lent is reserve room the model's replies hold.
	lent      int64
	stallTime time.Duration
	lastTurn  *client
	most      time.Duration
	steps     int
}

func runReadHalfModel(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	n, _ := testNode(t)
	n.now = time.Unix(1_700_000_000, 0)
	budgets := []int64{128 << 20, 256 << 20, 1 << 30}
	n.sizes, n.maxStreams = sizeFor(budgets[rng.IntN(len(budgets))]), defaultMaxStreams
	n.sideReqs, n.sideCalls = map[handle][]byte{}, nil
	m := &readHalfModel{t: t, rng: rng, n: n, l: &loop{n: n}}
	k := 2 + rng.IntN(10)
	for i := range k {
		c := newClient(&handoff{fd: -1, plain: true}, n)
		c.idx, c.gen = i, 1
		n.socks, n.gens = append(n.socks, c), append(n.gens, 1)
		mc := &mClient{c: c, byID: map[uint32]*mStream{}, nextID: 1, connWin: initialWindow, init: 65535}
		mc.enc = hpack.NewEncoder(&mc.encBuf)
		mc.dec = hpack.NewDecoder(4096, func(f hpack.HeaderField) {
			if f.Name == "grpc-status" {
				mc.status = f.Value
			}
		})
		mc.outbox = append(mc.outbox, clientPreface...)
		mc.outbox = appendFrame(mc.outbox, frSettings, 0, 0, nil)
		m.clients = append(m.clients, mc)
	}
	n.connOut = n.sizes.connOut(1)
	steps := 3000 + rng.IntN(3000)
	for m.steps = 0; m.steps < steps; m.steps++ {
		m.step(true)
	}
	// The stalls end, the reserve comes back, and no new calls start:
	// every call comes whole.
	for _, mc := range m.clients {
		if mc.stalled {
			mc.stalled, mc.stalledTo = false, m.n.now
		}
	}
	m.n.reserved -= m.lent
	m.lent = 0
	for i := 0; ; i++ {
		if m.allDone() {
			break
		}
		if i > 20000 {
			m.dump()
			t.Fatalf("seed %d: calls not whole %d steps after the stalls ended", seed, i)
		}
		m.step(false)
	}
	bound := time.Duration(2*m.stallsTotal()+2)*requestTimeout + 2*time.Second
	if m.most > bound {
		t.Fatalf("seed %d: a call of a client that never stalled took %v, past the bound %v of %d stalls", seed, m.most, bound, m.stallsTotal())
	}
	t.Logf("seed %d: %d clients, budget %d MiB, %d steps; %d stalls, %d turns, %d waits for window; the slowest call of a client that never stalled took %v",
		seed, k, n.sizes.clientPool<<4>>20, m.steps, m.stallsTotal(), n.stats.partTurns, n.stats.windowsWaited, m.most)
}

func (m *readHalfModel) stallsTotal() int {
	k := 0
	for _, mc := range m.clients {
		k += mc.stalls
	}
	return k
}

func (m *readHalfModel) allDone() bool {
	for _, mc := range m.clients {
		if len(mc.streams) > 0 {
			return false
		}
	}
	return true
}

func (m *readHalfModel) dump() {
	n := m.n
	m.t.Logf("cpool %d of %d, reserved %d, turn %v room %d, waiting %d", n.cpool, n.sizes.clientInBufs(), n.reserved, n.inTurn != nil, n.turnRoom, len(n.waiting))
	for i, mc := range m.clients {
		c := mc.c
		m.t.Logf("client %d: in %v cwin %d connWin %d partial %d inLine %v frozen %v cold %v starved %v paused %v named %d init %d outbox %d pt %d",
			i, c.in != nil, c.cwin, mc.connWin, c.partial, c.inLine, c.frozen, c.cold, c.starved, c.paused, c.named, mc.init, len(mc.outbox), c.pw-c.pr)
		for _, s := range mc.streams {
			var ns *stream
			if c.slots != nil {
				ns = c.get(s.id)
			}
			named, recvd := false, 0
			if ns != nil {
				named, recvd = ns.named, ns.recvd
			}
			m.t.Logf("  stream %d side %v msg %d sent %d win %d endSent %v named %v recvd %d", s.id, s.side, s.msg, s.sent, mc.window(s), s.endSent, named, recvd)
		}
	}
}

// step is one turn of the loop and of the clients.
func (m *readHalfModel) step(live bool) {
	n, rng := m.n, m.rng
	n.now = n.now.Add(time.Duration(rng.IntN(20)) * time.Millisecond)
	if live {
		m.events()
	}
	if at := os.Getenv("DBG739AT"); at != "" {
		d, _ := time.ParseDuration(at)
		if t0 := time.Unix(1_700_000_000, 0).Add(d); !n.now.Before(t0) && n.now.Add(-20*time.Millisecond).Before(t0) {
			m.dump()
		}
	}
	for _, i := range rng.Perm(len(m.clients)) {
		mc := m.clients[i]
		if live && !mc.stalled {
			m.open(mc)
		}
		if !mc.stalled {
			m.send(mc)
		}
		m.over("before read", mc)
		m.read(mc)
		m.over("read", mc)
		m.drain(mc)
		m.over("drain", mc)
		m.check()
	}
	if m.steps%5 == 0 || !live {
		m.l.tick++
		m.l.clientTick()
	} else if len(n.waiting) > 0 || n.inTurn != nil {
		m.l.serveWaiting()
	}
	for _, mc := range m.clients {
		m.drain(mc)
	}
	m.check()
	if os.Getenv("DBG739") != "" && n.inTurn != m.lastTurn {
		who := -1
		if n.inTurn != nil {
			who = n.inTurn.idx
		}
		fmt.Printf("%v turn %d named %d room %d cpool %d\n", n.now.Sub(time.Unix(1_700_000_000, 0)), who, func() int {
			if n.inTurn != nil {
				return n.inTurn.named
			}
			return 0
		}(), n.turnRoom, n.cpool)
		m.lastTurn = n.inTurn
	}
}

// events starts and ends stalls, and has replies take and give back
// room of the reserve.
func (m *readHalfModel) events() {
	n, rng := m.n, m.rng
	for _, mc := range m.clients {
		switch {
		case mc.stalled && n.now.After(mc.stallEnd):
			mc.stalled, mc.stalledTo = false, n.now
		case !mc.stalled && rng.IntN(4000) == 0:
			mc.stalled, mc.stallEnd = true, n.now.Add(time.Duration(1+rng.IntN(12))*time.Second)
			mc.stalls++
		}
	}
	switch r := rng.IntN(500); {
	case r == 0 && m.lent == 0:
		m.lent = min(int64(rng.IntN(int(n.sizes.reserve())))+1, n.sizes.reserve()-n.reserved)
		n.reserved += m.lent
	case r < 3 && m.lent > 0:
		n.reserved -= m.lent
		m.lent = 0
	}
}

// open starts calls on mc's connection now and then.
func (m *readHalfModel) open(mc *mClient) {
	rng := m.rng
	if len(mc.streams) >= 12 || rng.IntN(6) != 0 {
		return
	}
	s := &mStream{id: mc.nextID, side: rng.IntN(6) == 0, start: m.n.now}
	mc.nextID += 2
	switch r := rng.IntN(10); {
	case r < 6:
		s.msg = 1 + rng.IntN(7000)
	case r < 9:
		s.msg = 16<<10 + rng.IntN(300<<10)
	default:
		s.msg = 300<<10 + rng.IntN(maxMsg-300<<10+1)
	}
	path := "/etcdserverpb.KV/Put"
	if s.side {
		path = "/v3lockpb.Lock/Lock"
	}
	mc.encBuf.Reset()
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":path", Value: path},
		{Name: ":authority", Value: "model"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
	} {
		if err := mc.enc.WriteField(f); err != nil {
			m.t.Fatal(err)
		}
	}
	mc.outbox = appendFrame(mc.outbox, frHeaders, flEndHeaders, s.id, mc.encBuf.Bytes())
	mc.streams = append(mc.streams, s)
	mc.byID[s.id] = s
}

// send writes DATA of mc's streams, round robin as grpc-go's writer
// does, within the connection's window and each stream's; resets a
// stream now and then; and ends a side call's side once its request is
// out.
func (m *readHalfModel) send(mc *mClient) {
	rng := m.rng
	for range 1 + rng.IntN(8) {
		sent := false
		for _, s := range mc.streams {
			if s.reset {
				continue
			}
			if rng.IntN(3000) == 0 {
				mc.outbox = appendFrame(mc.outbox, frRST, 0, s.id, []byte{0, 0, 0, 8})
				s.reset = true
				continue
			}
			left := 5 + s.msg - s.sent
			if left == 0 {
				if s.side && !s.endSent && rng.IntN(4) == 0 {
					mc.outbox = appendFrame(mc.outbox, frData, flEndStream, s.id, nil)
					s.endSent = true
				}
				continue
			}
			k := min(int64(left), mc.connWin, mc.window(s), int64(1+rng.IntN(16<<10)))
			if k <= 0 {
				continue
			}
			p := make([]byte, k)
			for i := range p {
				if at := s.sent + i; at >= 1 && at < 5 {
					p[i] = byte(uint32(s.msg) >> (8 * (4 - at)))
				}
			}
			var fl byte
			if int(k) == left && !s.side {
				fl, s.endSent = flEndStream, true
			}
			mc.outbox = appendFrame(mc.outbox, frData, fl, s.id, p)
			s.sent += int(k)
			mc.connWin -= k
			sent = true
		}
		if !sent {
			return
		}
	}
}

// read has the node read what mc sent, in a piece as a socket gives it,
// if it reads mc now.
func (m *readHalfModel) read(mc *mClient) {
	c := mc.c
	if len(mc.outbox) == 0 && c.pr == c.pw {
		return
	}
	if !m.l.admit(c, false) {
		return
	}
	if c.paused || c.tokenWait != 0 || c.dead {
		return
	}
	if c.pr > 0 {
		c.pw = copy(c.pt, c.pt[c.pr:c.pw])
		c.pr = 0
	}
	if os.Getenv("DBG739") != "" && m.n.inTurn == c {
		fmt.Printf("%v read turn client %d: pr %d pw %d outbox %d cwin %d\n", m.n.now.Sub(m.n.turnAt), c.idx, c.pr, c.pw, len(mc.outbox), c.cwin)
	}
	k := min(len(c.pt)-c.pw, len(mc.outbox), 1+m.rng.IntN(48<<10))
	c.pw += copy(c.pt[c.pw:], mc.outbox[:k])
	mc.outbox = mc.outbox[k:]
	err := c.readFrames(m.n)
	m.over("readFrames", mc)
	if err == nil {
		m.n.giveCredit(c)
		m.over("giveCredit", mc)
	}
	if err != nil {
		m.dump()
		m.t.Fatalf("the node broke a client that keeps flow control: %v", err)
	}
}

// drain hands mc what the node wrote it, as a flush would, and has mc
// take it in: windows, settings and answers.
func (m *readHalfModel) drain(mc *mClient) {
	c := mc.c
	c.seal()
	out := bytes.Clone(c.out[c.ow:])
	c.out, c.ow = c.out[:0], 0
	if c.paused && c.waiting() <= m.n.connOut/2 {
		c.paused = false
		if err := c.frames(m.n); err != nil {
			m.t.Fatal(err)
		}
	}
	for len(out) >= 9 {
		l := int(out[0])<<16 | int(out[1])<<8 | int(out[2])
		typ, flags, sid := out[3], out[4], binary.BigEndian.Uint32(out[5:])&(1<<31-1)
		p := out[9 : 9+l]
		out = out[9+l:]
		switch typ {
		case frSettings:
			if flags&flAck != 0 {
				continue
			}
			for ; len(p) >= 6; p = p[6:] {
				if binary.BigEndian.Uint16(p) == 0x4 {
					mc.init = int64(binary.BigEndian.Uint32(p[2:]))
				}
			}
			mc.outbox = appendFrame(mc.outbox, frSettings, flAck, 0, nil)
		case frWindowUpdate:
			inc := int64(binary.BigEndian.Uint32(p) & (1<<31 - 1))
			if sid == 0 {
				mc.connWin += inc
			} else if s := mc.byID[sid]; s != nil {
				s.granted += inc
			}
		case frHeaders:
			mc.status = ""
			if _, err := mc.dec.Write(p); err != nil {
				m.t.Fatal(err)
			}
			if flags&flEndStream != 0 {
				m.finish(mc, sid, mc.status == "8")
			}
		case frRST:
			m.finish(mc, sid, true)
		case frGoAway:
			m.dump()
			m.t.Fatalf("the node went away: code %d", binary.BigEndian.Uint32(p[4:]))
		}
	}
	// A side call is whole once the node took its request at its end; it
	// has no server here to answer it, so its client resets it.
	for _, s := range mc.streams {
		if s.side && s.endSent && !s.done && c.slots != nil {
			if ns := c.get(s.id); ns != nil && ns.ended && !m.n.keepsSideReq(c, ns) {
				mc.outbox = appendFrame(mc.outbox, frRST, 0, s.id, []byte{0, 0, 0, 8})
				m.finish(mc, s.id, false)
			}
		}
	}
	for _, s := range mc.streams {
		if s.reset && !s.done {
			m.finish(mc, s.id, false)
		}
	}
}

// finish ends model stream sid of mc, answered or refused.
func (m *readHalfModel) finish(mc *mClient, sid uint32, refused bool) {
	s := mc.byID[sid]
	if s == nil || s.done {
		return
	}
	s.done, s.refused, s.end = true, refused, m.n.now
	if refused && !s.reset && !mc.stalled && !mc.stalledTo.After(s.start) {
		m.dump()
		m.t.Fatalf("stream %d (side %v, %d of %d sent) of a client that did not stall was refused", sid, s.side, s.sent, 5+s.msg)
	}
	if !refused && !s.reset && mc.stalls == 0 {
		if d := s.end.Sub(s.start); d > m.most {
			m.most = d
			if os.Getenv("DBG739") != "" {
				fmt.Printf("slowest so far: client %d stream %d msg %d took %v, from %v\n", mc.c.idx, sid, s.msg, d, s.start.Sub(time.Unix(1_700_000_000, 0)))
			}
		}
	}
	delete(mc.byID, sid)
	for i, x := range mc.streams {
		if x == s {
			mc.streams = append(mc.streams[:i], mc.streams[i+1:]...)
			break
		}
	}
}

func (m *readHalfModel) over(what string, mc *mClient) {
	if os.Getenv("DBG739") != "" && m.n.cpool > m.n.sizes.clientInBufs()+m.n.turnRoom {
		fmt.Printf("over after %s of client %d: cpool %d limit %d turn %v\n", what, mc.c.idx, m.n.cpool, m.n.sizes.clientInBufs()+m.n.turnRoom, m.n.inTurn != nil)
	}
}

// check holds rule 1 of partwait.go: the read half holds what its
// clients hold, and stays within its half and the turn's room.
func (m *readHalfModel) check() {
	n := m.n
	var sum int64
	for _, mc := range m.clients {
		c := mc.c
		if c.dead {
			continue
		}
		var held int64
		if c.in != nil {
			held += int64(cap(c.in)+cap(c.pt)) + c.heldWindow()
		}
		if c.slots != nil {
			held += int64(slotBytes)
		}
		var part int64
		c.eachStream(func(s *stream) {
			part += s.counted
			if req, ok := n.sideReqs[mkHandle(c, s.id)]; ok {
				part += int64(len(req))
			}
		})
		if part != c.partial {
			m.dump()
			m.t.Fatalf("client %d counts %d in part, its streams hold %d", c.idx, c.partial, part)
		}
		if c.cwin < 0 || c.cwin < mc.connWin {
			m.dump()
			m.t.Fatalf("client %d: the node counts a window of %d, the client may send %d", c.idx, c.cwin, mc.connWin)
		}
		sum += held + c.partial + int64(cap(c.block))
	}
	if sum != n.cpool {
		m.dump()
		m.t.Fatalf("the read half counts %d, its clients hold %d", n.cpool, sum)
	}
	if n.cpool > n.sizes.clientInBufs()+n.turnRoom {
		m.dump()
		m.t.Fatalf("the read half holds %d, past its %d and the turn's room %d", n.cpool, n.sizes.clientInBufs(), n.turnRoom)
	}
	if n.reserved > n.sizes.reserve() || n.reserved != n.turnRoom+m.lent {
		m.t.Fatalf("the reserve lends %d of %d: the turn %d, replies %d", n.reserved, n.sizes.reserve(), n.turnRoom, m.lent)
	}
}

// appendFrame appends an HTTP/2 frame to b.
func appendFrame(b []byte, typ, flags byte, sid uint32, p []byte) []byte {
	h := make([]byte, 9)
	putFrameHeader(h, len(p), typ, flags, sid)
	return append(append(b, h...), p...)
}
