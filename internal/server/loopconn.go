package server

import (
	"errors"
	"io"
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

// Phases of a connection's input.
const (
	phasePreface = iota
	phaseSettings
	phaseFrames
)

// frameSrc is what the Framer reads: the decrypted bytes of the
// connection that the loop read, of which the reader only asks for whole
// frames.
type frameSrc struct{ c *conn }

func (s frameSrc) Read(p []byte) (int, error) {
	c := s.c
	n := copy(p, c.in[c.inR:c.inW])
	c.inR += n
	if n == 0 && len(p) > 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return n, nil
}

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
	c.bridge.Attach(sock)
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

func (c *conn) readFrames() error {
	for !c.ending && !c.isClosed() {
		if c.inW == len(c.in) {
			// process made room for a partial frame of any size, so a full
			// buffer only needs its read bytes moved out.
			c.compactIn(0)
		}
		n, rerr := c.nc.Read(c.in[c.inW:])
		c.inW += n
		if err := c.process(); err != nil {
			return err
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
			c.compactIn(frameHeaderLen)
			return nil
		}
		h := c.in[c.inR:]
		n := frameHeaderLen + (int(h[0])<<16 | int(h[1])<<8 | int(h[2]))
		if have < n {
			c.compactIn(n)
			return nil
		}
		if err := c.readFrame(); err != nil {
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

// readFrame reads and runs one frame, which is buffered whole.
func (c *conn) readFrame() error {
	fh, err := c.fr.ReadFrameHeader()
	if err != nil {
		return err
	}
	c.frames.Add(1)
	f, err := c.fr.ReadFrameForHeader(fh)
	if err != nil {
		se, ok := errors.AsType[http2.StreamError](err)
		switch {
		case !ok:
			return err
		case fh.Type == http2.FrameHeaders:
			// A header block that cannot be decoded leaves the HPACK
			// state behind the client's (RFC 9113 6.2).
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		c.resetStream(se.StreamID, se.Code)
		return nil
	}
	if c.phase == phaseSettings {
		sf, ok := f.(*http2.SettingsFrame)
		if !ok || sf.IsAck() {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		c.phase = phaseFrames
		return c.onSettings(sf)
	}
	return c.onFrame(f)
}

// compactIn moves the unread bytes to the front of the input buffer, and
// makes it hold at least need bytes, for a frame larger than it. A buffer
// grown for a large frame shrinks back once it is empty.
func (c *conn) compactIn(need int) {
	if c.inR == c.inW && len(c.in) > ioBuffer && need <= ioBuffer {
		c.in, c.inR, c.inW = make([]byte, ioBuffer), 0, 0
		return
	}
	if need > len(c.in) {
		in := make([]byte, need)
		c.inW = copy(in, c.in[c.inR:c.inW])
		c.in, c.inR = in, 0
		return
	}
	if c.inR > 0 && (c.inW == len(c.in) || need > len(c.in)-c.inR) {
		c.inW = copy(c.in, c.in[c.inR:c.inW])
		c.inR = 0
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
// While the loop reads the connection, it writes it afterwards anyway.
func (c *conn) signal() {
	if c.reading.Load() {
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
		return
	}
	if c.waitSince.IsZero() {
		c.waitSince = now
	}
	if at := c.waitSince.Add(writeTimeout); now.Before(at) {
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
			if err := c.bridge.Flush(); err != nil {
				return false, err
			}
			if pending := c.bridge.Pending(); pending > 0 {
				if pending < before {
					c.waitSince = time.Now()
				}
				return false, nil
			}
			c.waitSince = time.Time{}
		}
		c.mu.Lock()
		c.ctrlOut = append(c.ctrlOut[:0], c.ctrl...)
		clear(c.ctrl)
		c.ctrl = c.ctrl[:0]
		c.framesOut = c.take(c.framesOut[:0])
		drained := c.goingAway && len(c.streams) == 0
		c.mu.Unlock()
		if len(c.ctrlOut) == 0 && len(c.framesOut) == 0 {
			return drained, nil
		}
		for i := range c.ctrlOut {
			if err := c.writeCtrl(&c.ctrlOut[i]); err != nil {
				return false, err
			}
		}
		for i, f := range c.framesOut {
			if f.ctrl != nil {
				if err := c.writeCtrl(f.ctrl); err != nil {
					return false, err
				}
			} else {
				c.wrote.Store(true)
				c.hdrOut = frameHeader(c.hdrOut[:0], len(f.b), f.typ, f.flags, f.id)
				_, _ = c.bw.Write(c.hdrOut)
				_, _ = c.bw.Write(f.b)
			}
			c.framesOut[i] = wframe{}
		}
		// bufio.Writer keeps the first write error and returns it here.
		if err := c.bw.Flush(); err != nil {
			return false, err
		}
	}
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
	c.s.untrack(c)
	c.s.wg.Done()
}

// Stopped fails the calls the node still held once it stopped; the calls
// that come after fail as they reach it.
func (c *conn) Stopped() {
	c.nodeStopped()
	c.s.loop.Dirty(c)
}
