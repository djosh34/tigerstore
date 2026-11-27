package wl

import "encoding/binary"

// HTTP/2 flow control of what the server sends (RFC 9113, 6.9). A gRPC
// message that both its stream's window and the connection's take goes
// out at once, the fast path. One that does not is held, on the heap, in
// order behind anything its stream holds already, and goes out as the
// client's WINDOW_UPDATEs open the windows; the stream's trailers follow
// it.

// defaultWindow is a window before the client's SETTINGS say otherwise.
const defaultWindow = 65535

// held is a stream's message, or its trailers, waiting for window.
type held struct {
	sid uint32
	// Main's padding holds either a rounded flat charge (low bits zero),
	// the inherited control marker, or a funded output handle. #912:
	// keep this entry at main's 56 bytes, including empty high water.
	flatCharge uint32
	data       []byte // the gRPC message bytes still to send
	trailer    []byte // a header block that ends the stream once data is out
}

// flow is a client connection's send windows and what it holds.
type flow struct {
	conn    int64            // the connection's window
	initial int64            // the client's SETTINGS_INITIAL_WINDOW_SIZE
	streams map[uint32]int64 // the windows of streams that hold something
	counts  map[uint32]int   // how many held entries each such stream has
	held    []held
	bytes   int64 // the data and trailers held
}

func newFlow() flow { return flow{conn: defaultWindow, initial: defaultWindow} }

// take reports whether a message of n bytes on stream s may go out now,
// and if so takes its window.
func (c *client) take(s *stream, n int) bool {
	f := &c.flow
	if s.holds || s.big || f.conn < int64(n) || s.win < int64(n) {
		return false
	}
	f.conn -= int64(n)
	s.win -= int64(n)
	return true
}

// hold keeps stream s's message data, or its trailers, until the windows
// take it, and sends what they take now.
func (c *client) hold(s *stream, data, trailer []byte) {
	c.holdOutput(s, data, trailer, false)
}

func (c *client) holdOutput(s *stream, data, trailer []byte, control bool) {
	f := &c.flow
	if !s.holds {
		s.holds = true
		if f.streams == nil {
			f.streams, f.counts = map[uint32]int64{}, map[uint32]int{}
		}
		f.streams[s.id] = s.win
	}
	h := held{sid: s.id, data: data, trailer: trailer}
	if control {
		h.flatCharge = heldControl
		c.controlHeld += int64(len(data) + len(trailer))
		c.node.controlHeld += int64(len(data) + len(trailer))
	}
	if len(data) > 0 && !control && c.node.cursorFetch && c.node.legacyRetry != nil {
		h.flatCharge = uint32(roundedFetch(int64(cap(data))))
		promise := int64(0)
		if c.stages == 0 {
			promise = stageUnit
			if c.fundedSocketCapacity() < 64<<10 {
				promise += 64 << 10
				c.socketPromise = 64 << 10
			}
		}
		c.takeOutputPromise(promise)
		c.stagePromise += promise
		c.node.stagePromises += promise
		c.stages++
		c.dropOut = true
	}
	f.held = append(f.held, h)
	f.bytes += int64(len(data) + len(trailer))
	physical := int64(len(data))
	if charge := h.flatBytes(); charge > 0 {
		physical = charge
	}
	c.growOut(int(physical) + len(trailer))
	f.counts[s.id]++
	// Behind another of its stream's entries, or with the connection's
	// window shut, it cannot go out yet.
	if f.counts[s.id] == 1 && f.conn > 0 {
		c.drain()
	}
}

// holdFirst puts the message stream sid held last before its others,
// none of which is under way while a large response is written in the
// stream (drain): the rest keep their order.
func (c *client) holdFirst(sid uint32) {
	f := &c.flow
	first, last := -1, -1
	for i, h := range f.held {
		if h.sid == sid {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first == last {
		return
	}
	h := f.held[last]
	copy(f.held[first+1:last+1], f.held[first:last])
	f.held[first] = h
}

// windowUpdate adds inc to stream sid's window, or the connection's for
// sid 0, and sends what the windows take now.
func (c *client) windowUpdate(sid uint32, inc int64) {
	f := &c.flow
	switch w, ok := f.streams[sid]; {
	case sid == 0:
		f.conn += inc
	case ok:
		f.streams[sid] = w + inc
	default:
		if s := c.get(sid); s != nil {
			s.win += inc
		}
	}
	c.drain()
}

// windowOver reports whether inc takes stream sid's window, or the
// connection's for sid 0, past 2^31-1 (RFC 9113 §6.9.1).
func (c *client) windowOver(sid uint32, inc int64) bool {
	f := &c.flow
	w := f.conn
	if sid != 0 {
		var ok bool
		if w, ok = f.streams[sid]; !ok {
			s := c.get(sid)
			if s == nil {
				return false
			}
			w = s.win
		}
	}
	return w+inc > 1<<31-1
}

// setInitial applies a new SETTINGS_INITIAL_WINDOW_SIZE to every stream.
func (c *client) setInitial(v int64) {
	f := &c.flow
	d := v - f.initial
	f.initial = v
	for i := range c.slots {
		if c.slots[i].open {
			c.slots[i].win += d
		}
	}
	for _, s := range c.over {
		s.win += d
	}
	for sid := range f.streams {
		f.streams[sid] += d
	}
	c.drain()
}

// drain sends what the windows take of what is held, stream by stream in
// order.
func (c *client) drain() {
	f := &c.flow
	n := c.node
	if len(f.held) == 0 {
		return
	}
	var blocked map[uint32]bool
	k := 0
	for i := range f.held {
		h := &f.held[i]
		if blocked[h.sid] || c.bigs > 0 && c.isBig(h.sid) {
			f.held[k] = *h
			k++
			continue
		}
		if stage := c.heldStage(h); stage != nil {
			c.drainStage(h, stage)
		}
		if material := c.heldMaterial(h); material != nil {
			c.drainMaterial(h, material)
		}
		limit := int64(len(h.data))
		if h.flatBytes() > 0 {
			c.stageSocket()
			limit = min(limit, int64(max(0, rawSocketRoom(c))))
		}
		if m := min(limit, f.conn, f.streams[h.sid]); m > 0 {
			before := cap(c.out)
			w := msgW{s: &c.sock, sid: h.sid, left: int(m)}
			w.write(h.data[:m])
			if h.flatCharge == heldControl {
				c.controlSocketGrew(before)
			}
			h.data = h.data[m:]
			f.bytes -= m
			if h.flatBytes() == 0 {
				c.growOut(-int(m))
				if h.flatCharge == heldControl {
					c.controlHeld -= m
					n.controlHeld -= m
				}
			}
			f.conn -= m
			f.streams[h.sid] -= m
			n.markDirty(&c.sock)
		}
		stage := c.heldStage(h)
		if len(h.data) > 0 || c.heldMaterial(h) != nil || stage != nil && stage.read < stage.size {
			if blocked == nil {
				blocked = map[uint32]bool{}
			}
			blocked[h.sid] = true
			f.held[k] = *h
			k++
			continue
		}
		if h.flatBytes() > 0 {
			c.releaseFlatOutput(h)
		} else if h.flatCharge&3 >= 2 {
			c.unlinkOutput(h)
		}
		if h.trailer != nil {
			b := c.frame(9 + len(h.trailer))
			putFrameHeader(b, len(h.trailer), frHeaders, flEndHeaders|flEndStream, h.sid)
			copy(b[9:], h.trailer)
			f.bytes -= int64(len(h.trailer))
			c.growOut(-len(h.trailer))
			if h.flatCharge == heldControl {
				c.controlHeld -= int64(len(h.trailer))
				n.controlHeld -= int64(len(h.trailer))
			}
			n.markDirty(&c.sock)
		}
		f.counts[h.sid]--
		if f.counts[h.sid] == 0 {
			// The stream holds nothing more: its window goes back to it.
			if s := c.get(h.sid); s != nil {
				s.win, s.holds = f.streams[h.sid], false
			}
			delete(f.streams, h.sid)
			delete(f.counts, h.sid)
		}
	}
	clear(f.held[k:])
	f.held = f.held[:k]
}

// isBig reports whether a watch response is written into stream sid in
// pieces, which its held messages wait behind (bigwatch.go).
func (c *client) isBig(sid uint32) bool {
	s := c.get(sid)
	return s != nil && s.big
}

// streamWin is stream s's send window, held in its client's flow while
// it holds messages.
func (c *client) streamWin(s *stream) int64 {
	if s.holds {
		return c.flow.streams[s.id]
	}
	return s.win
}

// spendWin takes m bytes of the windows of stream s and its connection.
func (c *client) spendWin(s *stream, m int64) {
	c.flow.conn -= m
	if s.holds {
		c.flow.streams[s.id] -= m
		return
	}
	s.win -= m
}

// dropHeld drops what stream sid holds: the client reset it.
func (c *client) dropHeld(sid uint32) {
	f := &c.flow
	if _, ok := f.streams[sid]; !ok {
		return
	}
	k := 0
	for _, h := range f.held {
		if h.sid != sid {
			f.held[k] = h
			k++
			continue
		}
		f.bytes -= int64(len(h.data) + len(h.trailer))
		if material := c.heldMaterial(&h); material != nil {
			f.bytes -= material.remaining
			c.releaseMaterial(&h, material)
		}
		if h.flatBytes() > 0 {
			c.releaseFlatOutput(&h)
			c.growOut(-len(h.trailer))
		} else {
			c.growOut(-len(h.data) - len(h.trailer))
		}
		if stage := c.heldStage(&h); stage != nil {
			f.bytes -= int64(stage.size - stage.read)
			stage.discard()
			c.unlinkOutput(&h)
		}
		if h.flatCharge == heldControl {
			bytes := int64(len(h.data) + len(h.trailer))
			c.controlHeld -= bytes
			c.node.controlHeld -= bytes
		}
	}
	clear(f.held[k:])
	f.held = f.held[:k]
	delete(f.streams, sid)
	delete(f.counts, sid)
	if s := c.get(sid); s != nil {
		s.holds = false
	}
}

// settings reads a SETTINGS frame's payload.
func (c *client) settings(p []byte) error {
	if len(p)%6 != 0 {
		return errProtocol
	}
	for ; len(p) > 0; p = p[6:] {
		id, v := binary.BigEndian.Uint16(p), binary.BigEndian.Uint32(p[2:])
		switch id {
		case 0x2: // SETTINGS_ENABLE_PUSH
			if v > 1 {
				return errProtocol
			}
		case 0x4: // SETTINGS_INITIAL_WINDOW_SIZE
			if v > 1<<31-1 {
				return errProtocol
			}
			c.setInitial(int64(v))
		case 0x5: // SETTINGS_MAX_FRAME_SIZE
			if v < maxFrame || v > 1<<24-1 {
				return errProtocol
			}
		}
	}
	return nil
}
