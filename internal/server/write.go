package server

import (
	"golang.org/x/net/http2"
)

// wframe is one frame the writer sends.
type wframe struct {
	typ   http2.FrameType
	flags http2.Flags
	id    uint32
	b     []byte
	ctrl  *ctrlFrame // set for frames other than HEADERS and DATA
}

// maxTake bounds the bytes of the frames one write pass takes, headers,
// trailers and frame headers included, and maxPassFrames their number, so
// flow-controlled streams take turns and what one pass leaves waiting for
// a socket stays bounded (Server.maxOut).
const (
	maxTake       = 256 << 10
	maxPassFrames = 1024
)

// list puts a stream with output on the ready list. c.mu is held.
func (c *conn) list(st *stream) {
	if !st.listed && len(st.out) > 0 {
		st.listed = true
		c.ready = append(c.ready, st)
	}
}

// take moves the output that the windows allow into frames. Each pass
// gives every ready stream at most one frame of data, so one large reply
// does not hold up the others. c.mu is held.
func (c *conn) take(frames []wframe) []wframe {
	budget := maxTake
	for budget > 0 && len(frames) < maxPassFrames && len(c.ready) > 0 {
		before := len(frames)
		kept := c.ready[:0]
		for _, st := range c.ready {
			if !st.reset && budget > 0 && len(frames) < maxPassFrames {
				quantum := min(budget, c.maxFrame)
				left := quantum
				frames = c.takeStream(st, frames, &left)
				budget -= quantum - left
			}
			if !st.reset && len(st.out) > 0 {
				kept = append(kept, st)
			} else {
				st.listed = false
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

// takeStream moves one stream's output, in order, until it is empty, or
// blocked by a window or by budget, which each frame's bytes and header
// spend.
func (c *conn) takeStream(st *stream, frames []wframe, budget *int) []wframe {
	queued := st.queued
	defer func() {
		if st.queued < queued {
			st.wake()
		}
	}()
	for len(st.out) > 0 {
		it := &st.out[0]
		if *budget <= frameHeaderLen {
			return frames
		}
		switch it.kind {
		case outHeaders:
			frames = append(frames, wframe{typ: http2.FrameHeaders, flags: http2.FlagHeadersEndHeaders, id: st.id, b: it.b})
			*budget -= frameHeaderLen + len(it.b)
		case outTrailers:
			frames = append(frames, wframe{typ: http2.FrameHeaders, flags: http2.FlagHeadersEndHeaders | http2.FlagHeadersEndStream, id: st.id, b: it.b})
			*budget -= frameHeaderLen + len(it.b)
			frames = c.endStream(st, frames, budget)
		case outData:
			for len(it.b) > 0 || it.end {
				n := 0
				if len(it.b) > 0 {
					n = int(min(int64(len(it.b)), int64(c.maxFrame), c.sendWin, st.sendWin, int64(*budget-frameHeaderLen)))
					if n <= 0 {
						return frames
					}
				}
				last := n == len(it.b)
				var flags http2.Flags
				if last && it.end {
					flags = http2.FlagDataEndStream
				}
				frames = append(frames, wframe{typ: http2.FrameData, flags: flags, id: st.id, b: it.b[:n]})
				it.b = it.b[n:]
				it.started = true
				c.sendWin -= int64(n)
				st.sendWin -= int64(n)
				st.queued -= n
				*budget -= frameHeaderLen + n
				if last {
					if it.end {
						frames = c.endStream(st, frames, budget)
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
// ended its side, a RST_STREAM tells it to stop sending, as grpc-go does,
// which spends budget.
func (c *conn) endStream(st *stream, frames []wframe, budget *int) []wframe {
	st.ended = true
	delete(c.streams, st.id)
	if st.cancel != nil {
		// Calls that ended without a handler, such as unknown methods,
		// release their context here.
		st.cancel()
	}
	if st.deadline != nil {
		st.deadline.Stop()
	}
	c.releaseLocked(&st.replyHeld)
	if !st.inEOF {
		frames = append(frames, wframe{ctrl: &ctrlFrame{kind: ctrlRST, id: st.id, val: uint32(http2.ErrCodeNo)}})
		*budget -= frameHeaderLen + 4
	}
	return frames
}

func (c *conn) writeCtrl(f *ctrlFrame) error {
	switch f.kind {
	case ctrlSettings:
		return c.fr.WriteSettings(
			http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: c.s.maxStreams},
			http2.Setting{ID: http2.SettingInitialWindowSize, Val: uint32(c.s.streamWindow)})
	case ctrlSettingsAck:
		return c.fr.WriteSettingsAck()
	case ctrlPingAck:
		return c.fr.WritePing(true, f.ping)
	case ctrlWindowUpdate:
		return c.fr.WriteWindowUpdate(f.id, f.val)
	case ctrlRST:
		return c.fr.WriteRSTStream(f.id, http2.ErrCode(f.val))
	case ctrlGoAway:
		return c.fr.WriteGoAway(f.id, http2.ErrCode(f.val), []byte(f.debug))
	case ctrlPing:
		return c.fr.WritePing(false, f.ping)
	}
	return nil
}
