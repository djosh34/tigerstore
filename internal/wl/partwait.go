package wl

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
// has left, at most the whole reserve: its requests in part may grow by
// that much past what they were, the buffers full or not, until it holds
// none, or the buffers have room again. While many connections read, a
// share is one request (connIn), so the turn is all it needs to finish
// what it holds. A waiting connection's buffers are not taken by one that
// waits for buffers, as those come after it (serveWaiting).
//
// A turn lasts requestTimeout at most: then the requests its client
// holds in part are refused, as etcd refuses too many requests, and the
// room goes back to the reserve, whose replies may wait for it. A part
// that waits requestTimeout is refused too, and its connection reads on:
// the reserve was spent on replies, or the turn's room did not finish a
// share of more than the reserve.

// roomFor reports whether the client buffers have room for client c to
// hold k more of requests in part: within the read half, or within the
// turn's room for the turn's client.
func (n *node) roomFor(c *client, k int64) bool {
	return n.cpool+k <= n.sizes.clientInBufs() || n.inTurn == c && c.partial+k <= n.turnBase+n.turnRoom
}

// waitsForRoom reports whether DATA p of stream s waits for room in the
// client buffers; if so, c's reading stops before it, and c waits in
// line. A part that fits, or that can never fit and is refused, does
// not wait (client.data).
func (c *client) waitsForRoom(n *node, s *stream, p []byte) bool {
	k := int64(len(p))
	if c.partial+k <= n.connIn && n.cpool+k <= n.sizes.clientInBufs() {
		c.roomLate = false
		return false
	}
	if c.roomLate {
		// It waited requestTimeout: the part is refused (client.data).
		c.roomLate = false
		return false
	}
	grow := partTail(s.buf, p) - s.counted
	if grow <= 0 || c.partial+grow > n.connIn || n.roomFor(c, grow) {
		return false
	}
	c.roomNeed, c.roomAt = grow, n.now
	n.stats.partsWaited++
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
// half has no room for its part and the reserve has room for what its
// share has left, unless another holds it.
func (n *node) takeTurn(c *client) {
	if n.inTurn != nil || n.cpool+c.roomNeed <= n.sizes.clientInBufs() {
		return
	}
	room := min(n.connIn-c.partial, n.sizes.reserve())
	if n.reserved+room > n.sizes.reserve() {
		return
	}
	n.reserved += room
	n.inTurn, n.turnBase, n.turnRoom, n.turnAt = c, c.partial, room, n.now
}

// endTurn ends the turn of a client that closed, that does not wait and
// holds no request in part or finds the buffers with room again, or that
// held it requestTimeout, whose requests in part are refused and which
// reads on; its room goes back to the reserve.
func (l *loop) endTurn() {
	n := l.n
	c := n.inTurn
	if c == nil {
		return
	}
	late := !c.dead && n.now.Sub(n.turnAt) >= requestTimeout
	if !c.dead && !late && (c.roomNeed != 0 || c.partial > 0 && n.cpool > n.sizes.clientInBufs()) {
		return
	}
	n.inTurn = nil
	n.reserved -= n.turnRoom
	n.turnRoom = 0
	n.roomFreed = n.roomFreed || len(n.roomWait) > 0 || len(n.heldReplies) > 0
	n.roomShort = false
	if late {
		c.refuseParts()
		if c.roomNeed != 0 {
			// The part it stopped at goes on with room, or is refused.
			l.serveRoom(c)
		}
	}
}

// refuseParts refuses the requests client c holds in part, as etcd
// refuses too many requests.
func (c *client) refuseParts() {
	n := c.node
	refuse := func(s *stream) {
		if s.open && s.counted > 0 {
			n.reject(c, s, errTooManyRequests)
		}
	}
	for i := range c.slots {
		refuse(&c.slots[i])
	}
	for _, s := range c.over {
		refuse(s)
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
