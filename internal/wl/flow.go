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
	sid     uint32
	data    []byte // the gRPC message bytes still to send
	trailer []byte // a header block that ends the stream once data is out
}

// flow is a client connection's send windows and what it holds.
type flow struct {
	conn    int64            // the connection's window
	initial int64            // the client's SETTINGS_INITIAL_WINDOW_SIZE
	streams map[uint32]int64 // the windows of streams that hold something
	held    []held
}

func newFlow() flow { return flow{conn: defaultWindow, initial: defaultWindow} }

// take reports whether a message of n bytes on stream s may go out now,
// and if so takes its window.
func (c *client) take(s *stream, n int) bool {
	f := &c.flow
	if s.holds || f.conn < int64(n) || s.win < int64(n) {
		return false
	}
	f.conn -= int64(n)
	s.win -= int64(n)
	return true
}

// hold keeps stream s's message data, or its trailers, until the windows
// take it, and sends what they take now.
func (c *client) hold(s *stream, data, trailer []byte) {
	f := &c.flow
	if !s.holds {
		s.holds = true
		if f.streams == nil {
			f.streams = map[uint32]int64{}
		}
		f.streams[s.id] = s.win
	}
	f.held = append(f.held, held{sid: s.id, data: data, trailer: trailer})
	c.drain()
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
		if blocked[h.sid] {
			f.held[k] = *h
			k++
			continue
		}
		if m := min(int64(len(h.data)), f.conn, f.streams[h.sid]); m > 0 {
			w := msgW{s: &c.sock, sid: h.sid, left: int(m)}
			w.write(h.data[:m])
			h.data = h.data[m:]
			f.conn -= m
			f.streams[h.sid] -= m
			n.markDirty(&c.sock)
		}
		if len(h.data) > 0 {
			if blocked == nil {
				blocked = map[uint32]bool{}
			}
			blocked[h.sid] = true
			f.held[k] = *h
			k++
			continue
		}
		if h.trailer != nil {
			b := c.frame(9 + len(h.trailer))
			putFrameHeader(b, len(h.trailer), frHeaders, flEndHeaders|flEndStream, h.sid)
			copy(b[9:], h.trailer)
			n.markDirty(&c.sock)
		}
		if !c.holdsMore(i, h.sid) {
			// The stream holds nothing more: its window goes back to it.
			if s := c.get(h.sid); s != nil {
				s.win, s.holds = f.streams[h.sid], false
			}
			delete(f.streams, h.sid)
		}
	}
	clear(f.held[k:])
	f.held = f.held[:k]
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
		}
	}
	clear(f.held[k:])
	f.held = f.held[:k]
	delete(f.streams, sid)
	if s := c.get(sid); s != nil {
		s.holds = false
	}
}

// holdsMore reports whether a held entry after i belongs to stream sid.
func (c *client) holdsMore(i int, sid uint32) bool {
	for _, h := range c.flow.held[i+1:] {
		if h.sid == sid {
			return true
		}
	}
	return false
}

// settings reads a SETTINGS frame's payload.
func (c *client) settings(p []byte) error {
	if len(p)%6 != 0 {
		return errProtocol
	}
	for ; len(p) > 0; p = p[6:] {
		id, v := binary.BigEndian.Uint16(p), binary.BigEndian.Uint32(p[2:])
		if id == 0x4 { // SETTINGS_INITIAL_WINDOW_SIZE
			if v > 1<<31-1 {
				return errProtocol
			}
			c.setInitial(int64(v))
		}
	}
	return nil
}
