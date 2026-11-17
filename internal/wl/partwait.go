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
// has left: while it holds the turn, the buffers may hold that much past
// what they held when it took it (node.turnLimit), for its connection
// alone, and past what they take with no check of their room meanwhile
// (node.looseGrown: a new connection's handshake leftovers, a read buffer
// grown for a frame, a header block). Taken from the turn's room, those
// left its client waiting at a part its turn could not serve, first in
// line, and every client behind it for requestTimeout (#735's loop on
// 38cf23e4, as clients connected). While many connections
// read, a share is one request (connIn), so the turn is all it needs to
// finish what it holds. The turn ends once its client holds no request
// in part and waits for none, or the buffers are back within the read
// half, and its room goes back to the reserve, whose waiting ranges and
// replies take it before the next turn. A waiting connection's buffers
// are not taken by one that waits for buffers, as those come after it
// (serveWaiting).
//
// A turn lasts requestTimeout at most: then the requests its client still
// holds in part are refused, as etcd refuses too many requests, and the
// room goes back, so a client that stops sending does not hold the
// reserve for good. A client that keeps starting requests in its turn
// keeps it that long (a known limit). A part that waits requestTimeout is
// refused too, and its connection reads on: the reserve was spent on
// replies, or the turn's room did not finish a share of more than the
// reserve. Both the turn and a debt of what a turn left past the read
// half were tried: the client of a debt stopped at its next part, its
// requests in part never finished, and the debt held the reserve until
// every waiting part was refused (CI on 86a91c73).

// roomFor reports whether the client buffers have room for client c to
// hold k more of requests in part: within the read half, or within the
// turn's limit for the turn's client.
func (n *node) roomFor(c *client, k int64) bool {
	return n.cpool+k <= n.sizes.clientInBufs() || n.inTurn == c && n.cpool+k <= n.turnLimit+n.looseGrown-n.turnLoose
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
// half has no room for its part and the reserve has room for what its
// share has left, at most the whole reserve, unless another holds it. A
// turn of less room did not finish its client's requests, and waited for
// its end though the reserve came free (#689's O re-check, Opus O2).
func (n *node) takeTurn(c *client) {
	in := n.sizes.clientInBufs()
	if n.inTurn != nil || n.cpool+c.roomNeed <= in {
		return
	}
	room := min(n.connIn-c.partial, n.sizes.reserve())
	if n.reserved+room > n.sizes.reserve() {
		return
	}
	limit := max(n.cpool, in) + room
	n.reserved += room
	n.inTurn, n.turnRoom, n.turnLimit, n.turnLoose, n.turnAt = c, room, limit, n.looseGrown, n.now
}

// endTurn ends the turn of a client that closed, that holds no request in
// part and waits for none, or that finds the buffers back within the read
// half; or that held it requestTimeout, whose requests in part are
// refused and which reads on. Its room goes back to the reserve, and the
// ranges and replies that wait for the reserve take it first.
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
	n.turnRoom, n.turnLimit = 0, 0
	n.roomFreed = n.roomFreed || len(n.roomWait) > 0 || len(n.heldReplies) > 0
	n.roomShort = false
	if late {
		c.eachStream(func(s *stream) {
			if s.counted > 0 {
				n.reject(c, s, errTooManyRequests)
			}
		})
		if c.roomNeed != 0 {
			// The part it stopped at goes on with room, or is refused.
			l.serveRoom(c)
		}
	}
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
