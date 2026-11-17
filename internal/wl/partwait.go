package wl

import "encoding/binary"

// The read half of the client buffers (sizes.clientInBufs) is shared out
// with HTTP/2's flow control (#739): a client sends DATA only within the
// window the node gave its connection, and the node gives window only as
// it reserves room of the read half for it. A request that finds the
// buffers full waits at its window, its connection read all the while,
// instead of being refused (#735), as etcd refuses "too many requests"
// only past its apply gap, never for memory. Before, a part stopped the
// reading of its whole connection; a client that pipelined large writes
// on one connection then kept the turn below for its full 7 s (#739).
//
// The invariant, which TestTheReadHalfModel checks against a randomized
// model of clients, streams (side calls among them), parts and turns:
//
//  1. Room is conserved and never above the budget. What a reading
//     client may still send, its connection window (client.cwin), counts
//     in the read half (node.cpool) with its buffers, its requests in
//     part, its kept side requests and its header block, and cpool is
//     always the sum of what the clients hold. A DATA frame moves its
//     bytes out of the window, into a request in part or, once the
//     message is whole, out of the read half: it never adds to cpool.
//     Window is given only with room for it: within the read half, or
//     within the turn's room of the reserve for the turn's client
//     (roomFor). So cpool stays within clientInBufs, the turn's room and
//     what is taken with no check of its room (node.looseGrown: a new
//     connection's handshake leftovers, a read buffer grown for a frame,
//     a header block; HEADERS are not flow-controlled). A client that
//     sends past its window breaks its connection with
//     FLOW_CONTROL_ERROR (RFC 9113 §6.9.1).
//  2. Every request that fits completes. A message of at most maxMsg
//     that its client sends within its windows is taken whole. The read
//     path refuses only a message past maxMsg, at its header, as
//     grpc-go's server does, and the requests in part of a turn's client
//     that has not sent them within requestTimeout (3). A side call's
//     one request (side.go) is a stream like any other: it came within
//     its window, and it is kept until its client ends its side.
//  3. A waiting stream is served within a bound that does not depend on
//     another client's behaviour, save a stalled client's. A client whose
//     window ran low waits in line (node.waiting) with the clients that
//     wait for their buffers, and is served in order as room comes back:
//     as messages come whole, and as idle clients give their buffers
//     and window back (a client not read holds no room; its window is
//     taken again with its buffers). When the first in line finds no
//     room, the client whose requests in part went longest without one
//     coming whole takes the turn (takeTurn): room of the reserve, for
//     its streams that hold a request in part, named when it takes it.
//     For the turn its connection's new streams get no window (its
//     SETTINGS_INITIAL_WINDOW_SIZE goes to 0, and every stream's window
//     with it), and each named stream gets window for the rest of its
//     request, in turn as room allows, while the connection keeps being
//     read. The turn ends when the named streams' requests are whole,
//     and the initial window goes back. A client that stops sending
//     within the window it was given holds the turn requestTimeout at
//     most: its named requests still in part are then refused, the only
//     refusal of a request that fits, and its room goes back.
//
// With plenty of room nothing waits: a connection's window is topped up
// to its credit (sizes.connCredit) once half of it is spent, in one
// WINDOW_UPDATE, and its streams keep the 1 GiB initial window they
// always had, so a request needs no WINDOW_UPDATE of its stream. The
// credit is many times the largest request of the bench's Kubernetes
// recordings (about 6.5 KiB).

// initialWindow is HTTP/2's initial connection window, which a client
// may send in before any WINDOW_UPDATE (RFC 9113 §6.9.2).
const initialWindow = 65535

// streamWindow is the initial window of the client's streams the node
// announces (client.start) outside a turn; within a turn it is 0.
const streamWindow = 1 << 30

// roomFor reports whether the read half has room for k more of client
// c's: within the read half, or within the turn's limit for the turn's
// client.
func (n *node) roomFor(c *client, k int64) bool {
	in := n.sizes.clientInBufs()
	return n.cpool+k <= in || n.inTurn == c && n.cpool+k <= in+n.turnRoom+n.looseGrown-n.turnLoose
}

// takeWindow takes DATA of l bytes of stream s, padding and all, out of
// c's window, and its bytes out of the read half; data then counts what
// of them stays in part. A cold client's window is not held: what it
// sends must be within its stream's window, which a turn gave it with
// room (-recvd, as its initial window is 0), or on a stream closed, whose
// bytes go.
func (c *client) takeWindow(s *stream, l int) error {
	if int64(l) > c.cwin || c.cold && s != nil && s.recvd+l > 0 {
		return errFlowControl
	}
	c.cwin -= int64(l)
	if !c.cold {
		c.grow(-l)
	}
	return nil
}

// heldWindow is what of c's window the read half holds room for while it
// reads.
func (c *client) heldWindow() int64 {
	if c.cold {
		return 0
	}
	return c.cwin
}

// giveWindow gives c k more of window, with room the caller found.
func (c *client) giveWindow(n *node, sid uint32, k int64) {
	b := c.frame(9 + 4)
	putFrameHeader(b, 4, frWindowUpdate, 0, sid)
	binary.BigEndian.PutUint32(b[9:], uint32(k))
	n.markDirty(&c.sock)
	if sid == 0 {
		c.cwin += k
		c.grow(int(k))
	}
}

// credit is the window c is topped up by, or 0 while half its credit is
// left.
func (n *node) credit(c *client) int64 {
	full := n.sizes.connCredit()
	if c.cwin > full/2 {
		return 0
	}
	return full - c.cwin
}

// topUp gives c the window it lacks of its credit if the read half has
// room for it, and reports whether it lacks none now.
func (n *node) topUp(c *client) bool {
	k := n.credit(c)
	if k == 0 {
		return true
	}
	if !n.roomFor(c, k) {
		return false
	}
	c.giveWindow(n, 0, k)
	return true
}

// giveCredit runs after each read of client c: the turn's client gets
// window for its named streams; another tops its window up, in line
// behind any that wait.
func (n *node) giveCredit(c *client) {
	if c.dead || c.in == nil {
		return
	}
	if n.inTurn == c {
		n.turnWindows(c)
		return
	}
	if c.inLine || !c.frozen && (n.credit(c) == 0 || len(n.waiting) == 0 && n.topUp(c)) {
		return
	}
	if c.frozen && len(n.waiting) == 0 && n.thaw(c) && n.topUp(c) {
		return
	}
	c.inLine = true
	n.waiting = append(n.waiting, c)
	n.stats.windowsWaited++
}

// need is what stream s has yet to send of the request it holds in part
// or awaits: the rest of its message, or of its message's prefix while
// that is not whole; none for a side call's request, which is whole.
func (s *stream) need() int64 {
	switch {
	case len(s.buf) == 0 && s.awaits:
		return 5
	case len(s.buf) == 0:
		return 0
	case len(s.buf) < 5:
		return int64(5 - len(s.buf))
	}
	return int64(5+binary.BigEndian.Uint32(s.buf[1:5])) - int64(len(s.buf))
}

// takeTurn gives the turn, when none holds it and the reserve has room,
// to the client whose requests in part went longest without one coming
// whole: room of the reserve for what its streams that hold a request in
// part have yet to send, or for the largest request at least, so that
// they are finished one after another. Its new streams get no window for
// the turn.
func (l *loop) takeTurn() {
	n := l.n
	if n.inTurn != nil {
		return
	}
	var c *client
	for _, x := range n.socks {
		if v, ok := x.(*client); ok && v.reads() && !v.frozen && v.underWay() && (c == nil || v.partAt.Before(c.partAt)) {
			c = v
		}
	}
	if c == nil {
		return
	}
	var need int64
	c.eachStream(func(s *stream) {
		switch {
		case len(s.buf) >= 5:
			need += s.need()
		case s.counted > 0 || s.awaits:
			need += maxMsg + 5 - s.counted
		}
	})
	// The read half may be over its half already, by what an earlier
	// turn's room still holds; the new turn's room comes on top.
	over := max(n.cpool-n.sizes.clientInBufs(), 0)
	room := min(need, n.sizes.reserve()-(n.reserved-n.turnRoom)-over)
	if room < min(need, maxMsg+5) {
		return
	}
	c.eachStream(func(s *stream) {
		if n.underWay(c, s) {
			s.named = true
			c.named++
		}
	})
	n.reserved += over + room - n.turnRoom
	n.inTurn, n.turnRoom, n.turnLoose, n.turnAt = c, over+room, n.looseGrown, n.now
	n.stats.partTurns++
	c.initialWindow(0)
	n.turnWindows(c)
}

// initialWindow sends c a SETTINGS_INITIAL_WINDOW_SIZE of w, which moves
// the window of each of its streams by the change, as RFC 9113 §6.9.2
// has it.
func (c *client) initialWindow(w uint32) {
	var p [6]byte
	binary.BigEndian.PutUint16(p[0:], 0x4)
	binary.BigEndian.PutUint32(p[2:], w)
	c.writeFrame(frSettings, 0, p[:])
	c.node.markDirty(&c.sock)
	c.settingsSent++
	c.zeroAt = 0
	if w == 0 {
		c.zeroAt = c.settingsSent
	}
}

// cool lets go of the room of a frozen client's window once it acked the
// SETTINGS that gave its streams no initial window: the bytes it sent
// before came before that ACK, and since then it may send only within
// its streams' windows, and none of its open streams has any.
func (n *node) cool(c *client) {
	if !c.frozen || c.cold || c.zeroAt == 0 || c.settingsAcked < c.zeroAt {
		return
	}
	if c.in != nil {
		c.grow(-int(c.cwin))
	}
	c.cold = true
	n.lendBack()
}

// thaw gives frozen client c its streams' initial window back, and the
// read half room for its window again, once it has room for it; it
// reports whether c is thawed. It waits for none in line: a client that
// stopped sending holds up no other.
func (n *node) thaw(c *client) bool {
	if !c.cold || c.in == nil || n.cpool+c.cwin > n.sizes.clientInBufs() {
		return false
	}
	c.grow(int(c.cwin))
	c.frozen, c.cold = false, false
	c.initialWindow(streamWindow)
	return true
}

// turnWindows gives the turn's client window for its named streams, one
// after another while room allows: each stream the rest of its request,
// and the connection what those streams have yet to send. A stream's
// window for the turn is what it was given past what came (-recvd), as
// its initial window is 0. Bytes it sent before it took the new initial
// window in come all the same, within the window it had, and lower what
// it needs.
func (n *node) turnWindows(c *client) {
	var used int64
	full := false
	c.eachStream(func(s *stream) {
		need := s.need()
		if !s.named || need <= 0 || full {
			return
		}
		if !n.roomFor(c, max(used+need-c.cwin, 0)) {
			// No room for it: the streams after it wait for it.
			full = true
			return
		}
		if win := -int64(s.recvd); win < need {
			c.giveWindow(n, s.id, need-win)
			s.recvd -= int(need - win)
		}
		used += need
	})
	if used > c.cwin {
		c.giveWindow(n, 0, used-c.cwin)
	}
}

// underWay reports whether client c holds a request in part, or awaits
// one (stream.awaits).
func (c *client) underWay() bool { return c.partial > 0 || c.awaits > 0 }

// underWay reports whether stream s of c holds a request in part, keeps a
// side call's, or awaits one.
func (n *node) underWay(c *client, s *stream) bool {
	return s.counted > 0 || s.awaits || n.keepsSideReq(c, s)
}

// arrived marks stream s's request come whole, or its client's side
// ended.
func (c *client) arrived(s *stream) {
	if s.awaits {
		s.awaits = false
		c.awaits--
	}
}

// unname takes stream s of c out of the turn's named streams.
func (c *client) unname(s *stream) {
	if s.named {
		s.named = false
		c.named--
	}
}

// reads reports whether client c reads its socket: it holds buffers, and
// waits for nothing, so that a turn it took goes on.
func (c *client) reads() bool {
	return !c.dead && c.in != nil && !c.paused && c.tokenWait == 0
}

// endTurn ends the turn of a client that closed or whose named streams'
// requests are whole, and its streams' initial window goes back; or of
// one that held it requestTimeout, whose requests in part are refused:
// it stopped sending within its windows. That one stays frozen, its new
// streams with no window, until the read half has room for the window
// it was given and did not use (cool, thaw). The room of the reserve the
// read half is still over its half by stays lent until the read half
// comes back within it (lendBack).
func (l *loop) endTurn() {
	n := l.n
	c := n.inTurn
	if c == nil {
		return
	}
	late := !c.dead && n.now.Sub(n.turnAt) >= requestTimeout
	if !c.dead && !late && c.named > 0 {
		return
	}
	n.inTurn = nil
	if !c.dead {
		c.eachStream(func(s *stream) {
			c.unname(s)
			if !late || !n.underWay(c, s) {
				return
			}
			if sideKind(s.kind) {
				n.cancelSide(mkHandle(c, s.id))
			}
			n.reject(c, s, errTooManyRequests)
		})
		if late {
			c.frozen = true
			n.cool(c)
		} else {
			c.initialWindow(streamWindow)
		}
		n.giveCredit(c)
	}
	n.lendBack()
}

// lendBack gives the reserve back the room the read half no longer takes
// past its half, once no turn holds it; the ranges and replies that wait
// for the reserve take it first.
func (n *node) lendBack() {
	if n.inTurn != nil || n.turnRoom == 0 {
		return
	}
	over := max(n.cpool-n.sizes.clientInBufs(), 0)
	if over >= n.turnRoom {
		return
	}
	n.reserved -= n.turnRoom - over
	n.turnRoom = over
	n.roomFreed = n.roomFreed || len(n.roomWait) > 0 || len(n.heldReplies) > 0
	n.roomShort = false
	n.serveRoomWait()
}

// eachStream calls f for each open stream of c.
func (c *client) eachStream(f func(*stream)) {
	for i := range c.slots {
		if c.slots[i].open {
			f(&c.slots[i])
		}
	}
	for _, s := range c.over {
		f(s)
	}
}
