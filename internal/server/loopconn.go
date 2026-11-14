package server

import (
	"errors"
	"math"
	"slices"
	"time"

	"golang.org/x/net/http2"

	"github.com/djosh34/tigerstore/internal/loop"
)

// An HTTP/2 connection runs on the node's event loop once its TLS
// handshake, on a goroutine of its own, is done. The loop reads what
// arrived, decrypts it, reads its frames and hands the unary calls of the
// node to the core in one batch; the core's replies come back on the loop
// in the same turn (Complete), which encodes them and writes them with
// one write for all. Streaming calls, calls the server answers itself and
// HTTP requests still run on goroutines of their own: they queue their
// output under c.mu and signal the loop.

// maxReadFrame is the largest frame payload a client may send: HTTP/2's
// default SETTINGS_MAX_FRAME_SIZE, which the server does not raise. So a
// whole frame always fits the input buffer.
const maxReadFrame = 16384

// connBuffer is the size of the buffers a connection borrows from the
// server (Server.bufs) while it holds bytes: the bridge's read buffer under
// TLS, the decrypted input, which a whole frame always fits, and the
// sealed output that waits for the socket, which grows past it for a long
// pass.
const connBuffer = 32 << 10

// passBuffer is the size of the buffers that hold a write pass sealed for
// the socket, maxTake and room for the control frames and TLS's record
// overhead; keepBuffers and keepPassBuffers are how many of each the
// server keeps for the next connection that needs one.
const (
	passBuffer      = maxTake + 64<<10
	keepBuffers     = 64
	keepPassBuffers = 4
)

// connBytes bounds what one HTTP/2 connection holds besides what its
// calls hold (maxHeld) and its output that waits for the socket, which
// counts in the server's waiting output (Server.maxOut): an idle connection
// (idleBytes); while it has bytes in them, a read buffer under TLS and an
// input buffer (connBuffer each); the control frames queued (maxCtrl of
// 48 bytes, 24 KiB); a header block that waits for its CONTINUATION
// (maxHeaderList); and the header blocks it keeps decoded (maxBlocks,
// under 40 KiB). Server.setLimits sizes the number of connections from it.
const connBytes = idleBytes + 2*connBuffer + 24<<10 + maxHeaderList + 40<<10

// idleBytes bounds what an idle HTTP/2 connection holds.
// TestIdleConnectionMemory measures it, with its client, at about 112 KB.
const idleBytes = 128 << 10

// Phases of a connection's input.
const (
	phasePreface = iota
	phaseSettings
	phaseFrames
)

// frameDst is what the Framer writes: the server's write buffer, which
// write points at the connection for the pass.
type frameDst struct{ c *conn }

func (d frameDst) Write(p []byte) (int, error) { return d.c.s.bw.Write(p) }

// attach hands the connection to the loop. It runs on the loop.
func (c *conn) attach() {
	c.attached.Store(true)
	if c.isClosed() {
		c.release()
		return
	}
	sock, token, err := c.s.loop.Attach(c.bridge.Conn, c)
	if err != nil {
		c.close()
		c.release()
		return
	}
	c.bridge.Attach(sock, c.s.bufs, c.s.bigBufs)
	c.token = token
	if c.traffic == TrafficGRPC {
		go c.keepalive()
	}
	c.prefaceBy = time.Now().Add(handshakeTimeout)
	c.s.loop.After(c, c.prefaceBy)
}

// Ready reads what arrived and runs every whole frame. A batch of calls
// for the node goes when it is full, and once nothing more has arrived.
func (c *conn) Ready() {
	if c.gone {
		return
	}
	c.reading.Store(true)
	err := c.readFrames()
	// Calls read before an error still go to the node.
	if ferr := c.flush(); err == nil {
		err = ferr
	}
	c.reading.Store(false)
	// What the frames queued, and what goroutines queued while reading
	// held off their signals, goes out at the end of the turn.
	c.s.loop.Dirty(c)
	if err != nil {
		c.readFailed(err)
	}
}

// readFrames reads into an input buffer that the connection borrows from
// the server's buffers while it holds bytes: a whole frame always fits it
// (maxReadFrame), and an idle connection holds none. It stops once the
// node took a turn's worth of calls (Node.TurnRoom), between two frames,
// and goes on at the next turn with the frames it holds.
func (c *conn) readFrames() error {
	defer c.releaseIn()
	if c.ending || c.isClosed() {
		// The input ended: what is left of it goes, with how it ended.
		c.inR, c.inW, c.readErr = 0, 0, nil
		return nil
	}
	start := c.turnRoom()
	if c.inR < c.inW {
		if err := c.process(); err != nil {
			return err
		}
	}
	if err := c.readErr; err != nil && (c.inR == c.inW || c.turnRoom() > 0) {
		// The frames read before the read failed are done, or the rest
		// is not a whole frame.
		c.readErr = nil
		return err
	}
	for !c.ending && !c.isClosed() {
		if room := c.turnRoom(); room <= 0 {
			// The node took a turn's worth of calls: the rest waits for
			// the next turn, ahead of the connections that took some of
			// this one if this one took none.
			if c.s.cfg.Deferred != nil && c.holdsCallFrame() {
				c.s.cfg.Deferred()
			}
			if room < start {
				c.s.loop.Again(c.token)
			} else {
				c.s.loop.AgainFirst(c.token)
			}
			return nil
		}
		if c.in == nil {
			c.in = c.s.bufs.Get()
		}
		if c.inW == len(c.in) {
			c.compactIn()
		}
		n, rerr := c.nc.Read(c.in[c.inW:])
		c.inW += n
		if err := c.process(); err != nil {
			return err
		}
		if c.inR < c.inW && c.turnRoom() <= 0 {
			// Frames this turn had no room for wait for the next, and so
			// does how the read ended.
			if rerr != nil && !errors.Is(rerr, loop.ErrWouldBlock) {
				c.readErr = rerr
			}
			continue
		}
		if rerr != nil {
			if errors.Is(rerr, loop.ErrWouldBlock) {
				return nil
			}
			return rerr
		}
	}
	return nil
}

// process runs the whole frames in the buffer: first the client's preface
// and its SETTINGS, then any frame.
func (c *conn) process() error {
	for {
		if c.turnRoom() <= 0 {
			// The node took a turn's worth of calls: the rest of the
			// frames wait for the next turn (readFrames), which comes at
			// once. Frames that carry no call, such as PING and
			// WINDOW_UPDATE, wait with them, in their order.
			return nil
		}
		have := c.inW - c.inR
		switch {
		case c.phase == phasePreface:
			if have < len(http2.ClientPreface) {
				return nil
			}
			if string(c.in[c.inR:c.inR+len(http2.ClientPreface)]) != http2.ClientPreface {
				return http2.ConnectionError(http2.ErrCodeProtocol)
			}
			c.inR += len(http2.ClientPreface)
			c.phase = phaseSettings
			continue
		case have < frameHeaderLen:
			return nil
		}
		h := c.in[c.inR:]
		n := frameHeaderLen + (int(h[0])<<16 | int(h[1])<<8 | int(h[2]))
		if n > frameHeaderLen+maxReadFrame {
			// The server never raises SETTINGS_MAX_FRAME_SIZE above its
			// default, which a client must not exceed (RFC 9113 4.2).
			return http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if have < n {
			if n > len(c.in)-c.inR {
				c.compactIn()
			}
			return nil
		}
		b := c.in[c.inR : c.inR+n]
		c.inR += n
		if err := c.readFrame(b); err != nil {
			return err
		}
		if c.inR == c.inW {
			c.inR, c.inW = 0, 0
		}
		if len(c.batch.reqs) >= maxBatchOps || c.batch.bytes >= maxBatchBytes {
			if err := c.flush(); err != nil {
				return err
			}
		}
	}
}

// holdsCallFrame reports whether the next frame the connection's input
// holds, one it has not run, is whole and a call's: HEADERS or DATA.
func (c *conn) holdsCallFrame() bool {
	have := c.inW - c.inR
	if c.phase == phasePreface || have < frameHeaderLen {
		return false
	}
	h := c.in[c.inR:]
	t := http2.FrameType(h[3])
	whole := have >= frameHeaderLen+(int(h[0])<<16|int(h[1])<<8|int(h[2]))
	return whole && (t == http2.FrameHeaders || t == http2.FrameData)
}

// turnRoom is how many more calls the node takes in this turn besides
// those in the connection's batch: unbounded for a server with no node,
// as in tests.
func (c *conn) turnRoom() int {
	if c.s.n == nil {
		return math.MaxInt
	}
	return c.s.n.TurnRoom() - len(c.batch.reqs)
}

// readFrame runs one frame, which b holds whole: its payload stays in
// the input buffer while it runs.
func (c *conn) readFrame(b []byte) error {
	c.frames.Add(1)
	f, err := parseFrame(b, c.contID)
	if err != nil {
		se, ok := errors.AsType[http2.StreamError](err)
		if !ok {
			return err
		}
		c.resetStream(se.StreamID, se.Code)
		return nil
	}
	if f.typ == http2.FrameHeaders || f.typ == http2.FrameContinuation {
		c.contID = f.id
		if f.has(http2.FlagHeadersEndHeaders) {
			c.contID = 0
		}
	}
	if c.phase == phaseSettings {
		if f.typ != http2.FrameSettings || f.has(http2.FlagSettingsAck) {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		c.phase = phaseFrames
		return c.onSettings(&f)
	}
	return c.onFrame(&f)
}

// compactIn moves the unread bytes to the front of the input buffer.
func (c *conn) compactIn() {
	c.inW = copy(c.in, c.in[c.inR:c.inW])
	c.inR = 0
}

// releaseIn returns the input buffer once it holds no bytes.
func (c *conn) releaseIn() {
	if c.in != nil && c.inR == c.inW {
		c.s.bufs.Put(c.in)
		c.in, c.inR, c.inW = nil, 0, 0
	}
}

// readFailed ends the connection's input. A protocol error or GOAWAY
// queues a GOAWAY and gives the streams in flight up to a second to end;
// anything else, such as the client closing, closes the connection.
func (c *conn) readFailed(err error) {
	ga, ok := errors.AsType[goAwayError](err)
	if ce, isCE := errors.AsType[http2.ConnectionError](err); isCE {
		ga, ok = goAwayError{code: http2.ErrCode(ce)}, true
	}
	if !ok {
		c.close()
		c.release()
		return
	}
	c.ending = true
	c.queueCtrl(ctrlFrame{kind: ctrlGoAway, id: c.lastID(), val: uint32(ga.code), debug: ga.debug})
	c.mu.Lock()
	c.goingAway = true
	c.mu.Unlock()
	c.endBy = time.Now().Add(time.Second)
	c.s.loop.After(c, c.endBy)
	c.s.loop.Dirty(c)
}

// signal makes the loop write the connection's output at its next turn.
// While the loop reads the connection, it writes it afterwards anyway, and
// before the loop has it, attach makes the loop read and write it first,
// so only the acceptor's hand-over can end it until then.
func (c *conn) signal() {
	if c.reading.Load() || !c.attached.Load() {
		return
	}
	if c.signaled.CompareAndSwap(false, true) && !c.s.loop.Post(c.flushLater) {
		c.signaled.Store(false)
	}
}

// flushLater runs on the loop for a signal.
func (c *conn) flushLater() {
	c.signaled.Store(false)
	c.s.loop.Dirty(c)
}

// Flush encodes the replies the node handed over, and writes the output
// that the flow-control windows allow, as far as the socket takes it.
func (c *conn) Flush() {
	if c.gone {
		return
	}
	defer func() {
		if !c.gone {
			c.chargeOut(c.bridge.Retained())
		}
	}()
	if c.isClosed() {
		c.release()
		return
	}
	now := time.Now()
	if c.phase != phaseFrames && !now.Before(c.prefaceBy) {
		c.close()
		c.release()
		return
	}
	drained, err := c.write()
	if err != nil {
		c.close()
		c.release()
		return
	}
	pending := c.bridge.Pending()
	if drained && pending == 0 || c.ending && !now.Before(c.endBy) {
		c.close()
		c.release()
		return
	}
	if pending == 0 {
		c.waitSince = time.Time{}
		c.bridge.SendBy(time.Time{})
		return
	}
	if c.waitSince.IsZero() {
		c.waitSince = now
	}
	// The socket's own deadline fails the send the loop tries then.
	at := c.waitSince.Add(writeTimeout)
	c.bridge.SendBy(at)
	if now.Before(at) {
		c.s.loop.After(c, at)
		return
	}
	// The client stopped reading from its socket.
	c.close()
	c.release()
}

// write moves queued output into the socket until nothing more is
// allowed or the socket is full. Bytes already encoded go first, and
// nothing new is taken while the socket holds some back, so what waits
// below the streams' queues is at most one pass of take. It reports
// whether the connection is going away, has no stream left and sent
// everything.
func (c *conn) write() (bool, error) {
	c.completeWaiting()
	for {
		if before := c.bridge.Pending(); before > 0 {
			if err := c.flushBridge(); err != nil {
				return false, err
			}
			if pending := c.bridge.Pending(); pending > 0 {
				if pending < before {
					c.waitSince = time.Now()
				}
				return false, nil
			}
			// The output that waited went: its deadline does not bind the
			// next pass.
			c.waitSince = time.Time{}
			c.bridge.SendBy(time.Time{})
		}
		if !c.outRoom() {
			// Other connections' output fills the server's room for it.
			return false, nil
		}
		c.mu.Lock()
		c.s.ctrlOut = append(c.s.ctrlOut[:0], c.ctrl...)
		clear(c.ctrl)
		c.ctrl = c.ctrl[:0]
		// The control frames spend the pass's budget first.
		budget := maxTake
		for i := range c.s.ctrlOut {
			budget -= ctrlBytes(&c.s.ctrlOut[i])
		}
		c.s.framesOut = c.take(c.s.framesOut[:0], budget)
		drained := c.goingAway && len(c.streams) == 0
		c.mu.Unlock()
		if len(c.s.ctrlOut) == 0 && len(c.s.framesOut) == 0 {
			return drained, nil
		}
		// Every connection writes through the server's one write buffer,
		// which each pass flushes.
		c.s.bw.Reset(c.nc)
		for i := range c.s.ctrlOut {
			if err := c.writeCtrl(&c.s.ctrlOut[i]); err != nil {
				return false, err
			}
		}
		for i, f := range c.s.framesOut {
			if f.ctrl != nil {
				if err := c.writeCtrl(f.ctrl); err != nil {
					return false, err
				}
			} else {
				c.wrote.Store(true)
				c.s.hdrOut = frameHeader(c.s.hdrOut[:0], len(f.b), f.typ, f.flags, f.id)
				_, _ = c.s.bw.Write(c.s.hdrOut)
				_, _ = c.s.bw.Write(f.b)
			}
			c.s.framesOut[i] = wframe{}
		}
		// bufio.Writer keeps the first write error and returns it here.
		if err := c.s.bw.Flush(); err != nil {
			return false, err
		}
	}
}

// flushBridge sends what the bridge holds, as far as the socket takes it,
// after sealing the TLS record being written.
func (c *conn) flushBridge() error {
	if c.records != nil {
		return c.records.Flush()
	}
	return c.bridge.Flush()
}

// ctrlBytes is what a control frame takes on the wire.
func ctrlBytes(f *ctrlFrame) int {
	switch f.kind {
	case ctrlSettings:
		return frameHeaderLen + 12
	case ctrlPingAck, ctrlPing:
		return frameHeaderLen + 8
	case ctrlGoAway:
		return frameHeaderLen + 8 + len(f.debug)
	case ctrlSettingsAck:
		return frameHeaderLen
	case ctrlWindowUpdate, ctrlRST:
		return frameHeaderLen + 4
	}
	return frameHeaderLen + 4
}

// release drops a closed connection from the loop and the server. It runs
// on the loop, once.
func (c *conn) release() {
	if c.gone {
		return
	}
	c.gone = true
	if c.token != 0 {
		c.s.loop.Forget(c.token)
		// The socket closes too: a pump's goroutines end with it.
		_ = c.bridge.Close()
	}
	c.chargeOut(0)
	c.s.untrack(c)
	c.s.ended()
}

// chargeOut counts pending, the memory of the connection's output that
// waits for its socket (Bridge.Retained), in the server's waiting output
// (Server.outAll), in place of what it counted before. Once that falls below its bound, the connections that
// waited for room write again. Loop only.
func (c *conn) chargeOut(pending int) {
	s := c.s
	d := pending - c.outCharged
	if d == 0 {
		return
	}
	c.outCharged = pending
	s.outAll += d
	if d < 0 && len(s.outWaiting) > 0 && s.outAll < s.maxOut {
		for i, w := range s.outWaiting {
			s.loop.Dirty(w)
			s.outWaiting[i] = nil
		}
		s.outWaiting = s.outWaiting[:0]
	}
}

// outRoom reports whether the server's waiting output has room for
// another write pass of the connection, and lists it to write again once
// it has. Loop only.
func (c *conn) outRoom() bool {
	s := c.s
	if s.maxOut == 0 || s.outAll < s.maxOut {
		return true
	}
	if !slices.Contains(s.outWaiting, c) {
		s.outWaiting = append(s.outWaiting, c)
	}
	return false
}

// Stopped fails the calls the node still held once it stopped; the calls
// that come after fail as they reach it.
func (c *conn) Stopped() {
	c.nodeStopped()
	c.s.loop.Dirty(c)
}
