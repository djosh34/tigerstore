package server

import (
	"time"

	"golang.org/x/net/http2"
)

// wframe is one frame the writer sends.
type wframe struct {
	typ   http2.FrameType
	flags http2.Flags
	id    uint32
	b     []byte
	owner *allocSlot // payload storage pinned through Flush (including abort)
	ctrl  *ctrlFrame // set for frames other than HEADERS and DATA
}

// maxTake bounds the data bytes of one write, so flow-controlled streams
// take turns and one flush stays bounded.
const maxTake = 1 << 20

// list puts a stream with output on the ready list. c.mu is held.
func (c *conn) list(st *stream) {
	if !st.listed && len(st.out) > 0 {
		st.listed = true
		c.ready = append(c.ready, st)
	}
}

// writeLoop sends what the reader and the handlers queued, within the
// client's flow-control windows. It is the only goroutine that writes to
// the connection.
func (c *conn) writeLoop() error {
	var frames []wframe
	var ctrl []ctrlFrame
	var hdr []byte
	if c.s.alloc != nil {
		frames = make([]wframe, 0, 4*allocationQuota)
		hdr = make([]byte, 0, frameHeaderLen)
	}
	defer func() { c.retireFrames(frames) }()
	nodeDone := c.s.n.Done()
	for {
		if c.s.alloc != nil {
			if head := c.takeManagedCompletions(); head != nil {
				c.completeManaged(head)
			}
		}
		if comps := c.takeCompletions(); comps != nil {
			c.complete(comps)
		}
		c.mu.Lock()
		ctrl = append(ctrl[:0], c.ctrl...)
		clear(c.ctrl)
		c.ctrl = c.ctrl[:0]
		frames = c.take(frames[:0])
		drained := c.goingAway && len(c.streams) == 0
		c.mu.Unlock()
		if len(ctrl) == 0 && len(frames) == 0 {
			if drained {
				return nil
			}
			select {
			case <-c.wake:
				continue
			case <-nodeDone:
				nodeDone = nil
				c.nodeStopped()
				continue
			case <-c.ctx.Done():
				return nil
			}
		}
		if err := c.nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return err
		}
		for i := range ctrl {
			if err := c.writeCtrl(&ctrl[i]); err != nil {
				return err
			}
		}
		for _, f := range frames {
			if f.ctrl != nil {
				if err := c.writeCtrl(f.ctrl); err != nil {
					return err
				}
			} else {
				hdr = frameHeader(hdr[:0], len(f.b), f.typ, f.flags, f.id)
				_, _ = c.bw.Write(hdr)
				_, _ = c.bw.Write(f.b)
			}
		}
		// bufio.Writer keeps the first write error and returns it here.
		if err := c.bw.Flush(); err != nil {
			return err
		}
		c.retireFrames(frames)
		frames = frames[:0]
		if drained {
			return nil
		}
	}
}

// take moves the output that the windows allow into frames. Each pass
// gives every ready stream at most one frame of data, so one large reply
// does not hold up the others. c.mu is held.
func (c *conn) take(frames []wframe) []wframe {
	budget := maxTake
	for budget > 0 && len(c.ready) > 0 {
		before := len(frames)
		kept := c.ready[:0]
		for _, st := range c.ready {
			if !st.reset {
				quantum := min(budget, c.maxFrame)
				left := quantum
				frames = c.takeStream(st, frames, &left)
				budget -= quantum - left
			}
			if !st.reset && len(st.out) > 0 {
				kept = append(kept, st)
			} else {
				st.listed = false
				if st.slot != nil {
					c.retireSlotLocked(st.slot)
				}
			}
		}
		clear(c.ready[len(kept):])
		c.ready = kept
		if len(frames) == before {
			// Every stream left waits for a window.
			break
		}
	}
	return frames
}

// takeStream moves one stream's output, in order, until it is empty or
// blocked by a window.
func (c *conn) takeStream(st *stream, frames []wframe, budget *int) []wframe {
	queued := st.queued
	defer func() {
		if st.queued < queued {
			select {
			case st.space <- struct{}{}:
			default:
			}
		}
	}()
	for len(st.out) > 0 {
		it := &st.out[0]
		switch it.kind {
		case outHeaders:
			frames = append(frames, wframe{typ: http2.FrameHeaders, flags: http2.FlagHeadersEndHeaders, id: st.id, b: it.b, owner: c.frameOwner(st)})
		case outTrailers:
			frames = append(frames, wframe{typ: http2.FrameHeaders, flags: http2.FlagHeadersEndHeaders | http2.FlagHeadersEndStream, id: st.id, b: it.b, owner: c.frameOwner(st)})
			frames = c.endStream(st, frames)
		case outData:
			for len(it.b) > 0 || it.end {
				n := 0
				if len(it.b) > 0 {
					n = int(min(int64(len(it.b)), int64(c.maxFrame), c.sendWin, st.sendWin, int64(*budget)))
					if n <= 0 {
						return frames
					}
				}
				last := n == len(it.b)
				var flags http2.Flags
				if last && it.end {
					flags = http2.FlagDataEndStream
				}
				frames = append(frames, wframe{typ: http2.FrameData, flags: flags, id: st.id, b: it.b[:n], owner: c.frameOwner(st)})
				it.b = it.b[n:]
				c.sendWin -= int64(n)
				st.sendWin -= int64(n)
				st.queued -= n
				*budget -= n
				if last {
					if it.end {
						frames = c.endStream(st, frames)
					}
					break
				}
			}
		}
		st.out[0] = outItem{}
		st.out = st.out[1:]
	}
	st.out = nil
	return frames
}

// endStream closes the server's side of a stream. If the client has not
// ended its side, a RST_STREAM tells it to stop sending, as grpc-go does.
func (c *conn) endStream(st *stream, frames []wframe) []wframe {
	st.ended = true
	delete(c.streams, st.id)
	if st.slot != nil {
		st.slot.mapRef = false
	}
	if st.cancel != nil {
		// Calls that ended without a handler, such as unknown methods,
		// release their context here.
		st.cancel()
	}
	c.stopDeadlineLocked(st)
	c.releaseLocked(st)
	if !st.inEOF {
		frames = append(frames, wframe{ctrl: &ctrlFrame{kind: ctrlRST, id: st.id, val: uint32(http2.ErrCodeNo)}})
	}
	return frames
}

func (c *conn) writeCtrl(f *ctrlFrame) error {
	switch f.kind {
	case ctrlSettings:
		return c.fr.WriteSettings(
			http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: maxStreams},
			http2.Setting{ID: http2.SettingInitialWindowSize, Val: streamWindow},
		)
	case ctrlSettingsAck:
		return c.fr.WriteSettingsAck()
	case ctrlPingAck:
		return c.fr.WritePing(true, f.ping)
	case ctrlWindowUpdate:
		return c.fr.WriteWindowUpdate(f.id, f.val)
	case ctrlRST:
		return c.fr.WriteRSTStream(f.id, http2.ErrCode(f.val))
	case ctrlGoAway:
		return c.fr.WriteGoAway(f.id, http2.ErrCode(f.val), nil)
	}
	return nil
}
