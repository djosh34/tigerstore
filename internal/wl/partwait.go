package wl

import "slices"

// A request sent in parts whose next part finds the read half of the
// client buffers full waits its turn for room, as a connection waits for
// its buffers, instead of being refused (#735). Refused, a client's large
// writes were refused on every try while many small clients kept the
// buffers near full, and etcd refuses "too many requests" only past its
// apply gap, never for memory.
//
// The part's DATA frame stops the reading of its connection: the node
// reads none of its frames past it, and nothing more of its socket, so
// TCP holds the sender, until the buffers have room for what the part
// adds. The connection waits in the same line as the connections that
// wait for their buffers (node.waiting, loop.serveWaiting), and no
// connection that comes after it takes buffers before it is served. A
// part past the connection's own share (connIn) can never fit and is
// refused, as before.
//
// Waiting connections hold what they hold, their buffers and stream
// slots and requests in part: if every one that holds the buffers waited
// for more, none would go on. So the first in line that finds no room
// takes the turn (node.inTurn), with room of the reserve (#727, which
// holds at least one request) for what its share of requests in part
// has left, as much as the reserve has free: its connection may take the
// buffers past the read half by that much. While many connections read,
// a share is one request (connIn), so the turn is all it needs to finish
// what it holds. The turn is for the requests it holds in part and the
// one it waits at (node.turnSids), and ends once they are whole, or
// after requestTimeout: what its client still holds past the read half
// stays charged to the reserve (node.inDebt) until it is whole too, and
// the next turn takes only what the reserve has free. So a client that
// keeps starting requests does not keep the turn, and none is refused
// for the turn's end. A waiting connection's buffers are not taken by one
// that waits for buffers, as those come after it (serveWaiting), and the
// replies that wait for the reserve take its room back before another
// turn does.
//
// A part that waits requestTimeout is refused, and its connection reads
// on: the reserve was spent on replies, or the turn's room did not finish
// a share of more than the reserve.

// roomFor reports whether the client buffers have room for client c to
// hold k more of requests in part: within the read half, or past it by
// what the reserve holds for it, the turn's room and the debt of turns
// before, for the turn's client.
func (n *node) roomFor(c *client, k int64) bool {
	in := n.sizes.clientInBufs()
	return n.cpool+k <= in || n.inTurn == c && n.cpool+k <= in+n.inDebt+n.turnRoom
}

// waitsForRoom reports whether DATA p of stream s, which does not fit
// the read half as it is, waits for room in the client buffers; if so,
// c's reading stops before it, and c waits in line. A part that fits
// once the whole messages in it are taken, or that can never fit and is
// refused, does not wait (client.data).
func (c *client) waitsForRoom(n *node, s *stream, p []byte) bool {
	if c.roomLate && s.id == c.roomSid {
		// It waited requestTimeout: the part is refused (client.data).
		return false
	}
	grow := partTail(s.buf, p) - s.counted
	if grow <= 0 || c.partial+grow > n.connIn || n.roomFor(c, grow) {
		return false
	}
	c.roomNeed, c.roomSid, c.roomAt = grow, s.id, n.now
	n.stats.partsWaited++
	c.linePass = n.waitPass
	n.waiting = append(n.waiting, c)
	n.markDirty(&c.sock)
	return true
}

// partTail is what of a stream's buffer a and DATA b stays in the buffer
// once the whole gRPC messages in them are taken (client.data): none for
// a message past the limit, which is refused.
func partTail(a, b []byte) int64 {
	at := func(i int) byte {
		if i < len(a) {
			return a[i]
		}
		return b[i-len(a)]
	}
	total, off := len(a)+len(b), 0
	for total-off >= 5 {
		l := int(at(off+1))<<24 | int(at(off+2))<<16 | int(at(off+3))<<8 | int(at(off+4))
		if l > maxMsg {
			return 0
		}
		if total-off-5 < l {
			break
		}
		off += 5 + l
	}
	return int64(total - off)
}

// takeTurn gives c, the first in line for room, the turn when the read
// half has no room for its part and the reserve has free room for it, as
// much as its share has left, unless another holds it. The turn is for
// the requests c holds in part and the one it waits at.
func (n *node) takeTurn(c *client) {
	in := n.sizes.clientInBufs()
	if n.inTurn != nil || n.cpool+c.roomNeed <= in {
		return
	}
	room := min(n.connIn-c.partial, n.sizes.reserve()-n.reserved)
	if n.cpool+c.roomNeed > in+n.inDebt+room {
		return
	}
	n.reserved += room
	n.inTurn, n.turnRoom, n.turnAt = c, room, n.now
	n.turnSids = append(n.turnSids[:0], c.roomSid)
	c.eachStream(func(s *stream) {
		if s.counted > 0 && s.id != c.roomSid {
			n.turnSids = append(n.turnSids, s.id)
		}
	})
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

// turnDone reports whether the requests the turn is for are whole, or
// gone: none waits for room or holds a part.
func (n *node) turnDone(c *client) bool {
	if c.roomNeed != 0 && slices.Contains(n.turnSids, c.roomSid) {
		return false
	}
	for _, sid := range n.turnSids {
		if s := c.get(sid); s != nil && s.counted > 0 {
			return false
		}
	}
	return true
}

// endTurn ends the turn of a client that closed, whose requests the turn
// is for are whole, or that held it requestTimeout. Its room stays
// charged to the reserve as debt while the buffers hold more than the
// read half (settle).
func (l *loop) endTurn() {
	n := l.n
	c := n.inTurn
	if c == nil {
		n.settle()
		return
	}
	if !c.dead && n.now.Sub(n.turnAt) < requestTimeout && !n.turnDone(c) && n.cpool > n.sizes.clientInBufs() {
		return
	}
	n.inTurn = nil
	n.inDebt += n.turnRoom
	n.turnRoom = 0
	n.turnSids = n.turnSids[:0]
	n.settle()
}

// settle gives back to the reserve what the debt of turns no longer
// needs, as the client buffers come down to the read half; the ranges
// and replies that wait for the reserve take it first, before the next
// turn.
func (n *node) settle() {
	if n.inTurn != nil || n.inDebt == 0 {
		return
	}
	if debt := min(n.inDebt, max(n.cpool-n.sizes.clientInBufs(), 0)); debt < n.inDebt {
		n.reserved -= n.inDebt - debt
		n.inDebt = debt
		n.roomFreed = n.roomFreed || len(n.roomWait) > 0 || len(n.heldReplies) > 0
		n.roomShort = false
		n.serveRoomWait()
	}
}

// serveRoom lets c, which waited for room, read on from the part it
// stopped at: with room, or refused when it has none (it waited
// requestTimeout, or its turn ended so).
func (l *loop) serveRoom(c *client) {
	n := l.n
	if !n.roomFor(c, c.roomNeed) {
		c.roomLate = true
	}
	c.roomNeed = 0
	c.busy = l.tick
	n.markDirty(&c.sock)
	if err := c.frames(n); err != nil {
		l.closeClient(c.idx, err)
	}
	c.roomLate = false
}
