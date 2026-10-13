package batchfront

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/node"
)

// encoded retains only response-owned wire bytes. No protobuf, descriptor,
// request body, or log slice crosses the writer queue.
type encoded struct {
	id       uint32
	wire     []byte
	reserved int
}
type control struct {
	id                                         uint32
	open, reset, ackSettings, setInitial, ping bool
	receive                                    uint32
	delta, initial                             int64
	maxFrame                                   uint32
	pingData                                   [8]byte
}

func headerBlock(fields ...hpack.HeaderField) ([]byte, error) {
	var b bytes.Buffer
	e := hpack.NewEncoder(&b)
	for _, f := range fields {
		// Each response is independent of connection HPACK encoder state.
		f.Sensitive = true
		if err := e.WriteField(f); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func grpcMessage(s string) string {
	const hex = "0123456789ABCDEF"
	var b bytes.Buffer
	for i := 0; i < min(len(s), 1024); i++ {
		v := s[i]
		if v < 0x20 || v > 0x7e || v == '%' {
			b.WriteByte('%')
			b.WriteByte(hex[v>>4])
			b.WriteByte(hex[v&15])
		} else {
			b.WriteByte(v)
		}
	}
	return b.String()
}

func (c *conn) reserve(id uint32, n int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.budgets[id]
	if b == nil {
		return false
	}
	c.inputBytes -= b.input
	b.input = 0
	b.ready = true
	if b.reset {
		delete(c.budgets, id)
		return false
	}
	if n > MaxResponseBytes || c.outputBytes+n > MaxOutputBytes {
		c.close()
		return false
	}
	c.outputBytes += n
	h := uint64(c.outputBytes)
	for old := c.s.stats.highwater.Load(); h > old; old = c.s.stats.highwater.Load() {
		if c.s.stats.highwater.CompareAndSwap(old, h) {
			break
		}
	}
	return true
}
func (c *conn) release(n int) { c.mu.Lock(); c.outputBytes -= n; c.mu.Unlock() }

// Complete executes on the owner for batch modes. Size is inspected before
// allocation, so even a huge Range/prev_kv response cannot overshoot the cap.
// No operation here waits for the writer, even when flow control is zero.
func (c *conn) Complete(replies []node.Completion) {
	if c.ctx.Err() != nil {
		return
	}
	c.s.stats.completions.Add(1)
	c.s.stats.replies.Add(uint64(len(replies)))
	batch := make([]encoded, 0, len(replies))
	for _, r := range replies {
		h, t := c.s.successHeader, c.s.successTrailer
		if r.Err != nil {
			fields := []hpack.HeaderField{{Name: "grpc-status", Value: strconv.Itoa(int(status.Code(r.Err)))}, {Name: "grpc-message", Value: grpcMessage(status.Convert(r.Err).Message())}}
			var err error
			t, err = headerBlock(fields...)
			if err != nil {
				c.close()
				break
			}
		}
		size := 0
		if r.Err == nil && r.Resp != nil {
			size = proto.Size(r.Resp) + 5
		}
		n := 9 + len(h) + 9 + len(t) + size + 9*((size+16383)/16384)
		if !c.reserve(r.Tag, n) {
			if c.ctx.Err() != nil {
				break
			}
			continue // reset stream
		}
		e, err := encode(r.Tag, r.Resp, r.Err, h, t, n)
		if err != nil {
			c.release(n)
			c.close()
			break
		}
		batch = append(batch, e)
	}
	if len(batch) == 0 {
		return
	}
	select {
	case c.out <- batch:
		return
	default:
		for _, e := range batch {
			c.release(e.reserved)
		}
		c.close()
	}
}

func frameHeader(b []byte, size int, kind http2.FrameType, flags byte, id uint32) {
	b[0], b[1], b[2] = byte(size>>16), byte(size>>8), byte(size)
	b[3], b[4] = byte(kind), flags
	binary.BigEndian.PutUint32(b[5:9], id)
}

func encode(id uint32, resp proto.Message, replyErr error, h, t []byte, n int) (encoded, error) {
	// Marshal straight into the reserved response buffer, then insert DATA
	// headers backwards in that same buffer. No second large scratch copy.
	wire := make([]byte, 9+len(h), n)
	frameHeader(wire, len(h), http2.FrameHeaders, byte(http2.FlagHeadersEndHeaders), id)
	copy(wire[9:], h)
	start := len(wire)
	if replyErr == nil && resp != nil {
		wire = append(wire, make([]byte, 5)...)
		var err error
		wire, err = proto.MarshalOptions{}.MarshalAppend(wire, resp)
		if err != nil {
			return encoded{}, err
		}
		size := len(wire) - start
		binary.BigEndian.PutUint32(wire[start+1:start+5], uint32(size-5))
		frames := (size + 16383) / 16384
		wire = wire[:start+size+9*frames]
		for i := frames - 1; i >= 0; i-- {
			count := min(16384, size-i*16384)
			dest := start + i*(9+16384)
			copy(wire[dest+9:dest+9+count], wire[start+i*16384:start+i*16384+count])
			frameHeader(wire[dest:], count, http2.FrameData, 0, id)
		}
	}
	start = len(wire)
	wire = wire[:start+9+len(t)]
	frameHeader(wire[start:], len(t), http2.FrameHeaders, byte(http2.FlagHeadersEndHeaders|http2.FlagHeadersEndStream), id)
	copy(wire[start+9:], t)
	return encoded{id: id, wire: wire, reserved: n}, nil
}

type pending struct {
	window    int64
	response  *encoded
	pos, part int
}

func (c *conn) writeLoop() error {
	w := bufio.NewWriterSize(c.Conn, 64<<10)
	f := http2.NewFramer(w, nil)
	timeout := c.writeTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	deadline := func() error { return c.SetWriteDeadline(time.Now().Add(timeout)) }
	if err := deadline(); err != nil {
		return err
	}
	if err := f.WriteSettings(http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: MaxStreams}, http2.Setting{ID: http2.SettingInitialWindowSize, Val: maxMessage + 5}); err != nil {
		return err
	}
	if err := f.WriteWindowUpdate(0, MaxInputBytes-65535); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	streams := make(map[uint32]*pending)
	var window int64 = 65535
	var initial int64 = 65535
	applyControl := func(o control) error {
		if o.ackSettings {
			if err := f.WriteSettingsAck(); err != nil {
				return err
			}
		}
		if o.setInitial {
			diff := o.initial - initial
			initial = o.initial
			for _, p := range streams {
				p.window += diff
			}
		}
		if o.ping {
			if err := f.WritePing(true, o.pingData); err != nil {
				return err
			}
		}
		if o.receive > 0 {
			if err := f.WriteWindowUpdate(o.id, o.receive); err != nil {
				return err
			}
		}
		if o.reset {
			if p := streams[o.id]; p != nil {
				if p.response != nil {
					c.release(p.response.reserved)
				}
				delete(streams, o.id)
			}
		}
		if o.open && streams[o.id] == nil && c.live(o.id) {
			streams[o.id] = &pending{window: initial}
		}
		if o.delta > 0 {
			if o.id == 0 {
				window += o.delta
				if window > 1<<31-1 {
					return errors.New("connection window overflow")
				}
			} else if p := streams[o.id]; p != nil {
				p.window += o.delta
				if p.window > 1<<31-1 {
					return errors.New("stream window overflow")
				}
			}
		}
		return nil
	}
	applyBatch := func(batch []encoded) {
		for i := range batch {
			e := batch[i]
			if !c.live(e.id) {
				c.release(e.reserved)
				continue
			}
			p := streams[e.id]
			if p == nil {
				p = &pending{window: initial}
				streams[e.id] = p
			}
			p.response = &e
		}
	}
	for {
		var nextControl *control
		var nextBatch []encoded
		select {
		case o := <-c.control:
			nextControl = &o
		case nextBatch = <-c.out:
		case <-c.ctx.Done():
			return nil
		}
		// An idle connection must not inherit a deadline set before select.
		if err := deadline(); err != nil {
			return err
		}
		if nextControl != nil {
			if err := applyControl(*nextControl); err != nil {
				return err
			}
		}
		if nextBatch != nil {
			applyBatch(nextBatch)
		}
		// Drain bounded channels without a fill timer.
	controls:
		for range 256 {
			select {
			case o := <-c.control:
				if err := applyControl(o); err != nil {
					return err
				}
			default:
				break controls
			}
		}
	batches:
		for range 64 {
			select {
			case batch := <-c.out:
				applyBatch(batch)
			default:
				break batches
			}
		}
		var finished []encoded
		for id, p := range streams {
			if p.response == nil {
				continue
			}
			e := p.response
			if !c.live(id) {
				c.release(e.reserved)
				delete(streams, id)
				continue
			}
			for p.pos < len(e.wire) {
				frame := e.wire[p.pos:]
				n := int(frame[0])<<16 | int(frame[1])<<8 | int(frame[2])
				if frame[3] == byte(http2.FrameData) {
					m := int(min(int64(n-p.part), window, p.window))
					if m <= 0 {
						break
					}
					if p.part == 0 && m == n {
						if _, err := w.Write(frame[:9+n]); err != nil {
							return err
						}
					} else {
						if err := f.WriteData(id, false, frame[9+p.part:9+p.part+m]); err != nil {
							return err
						}
					}
					window -= int64(m)
					p.window -= int64(m)
					p.part += m
					if p.part < n {
						break
					}
					p.part = 0
				} else {
					if _, err := w.Write(frame[:9+n]); err != nil {
						return err
					}
				}
				p.pos += 9 + n
			}
			if p.pos == len(e.wire) {
				finished = append(finished, *e)
				delete(streams, id)
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
		// Keep bytes reserved through the actual Flush, not just queue removal.
		for _, e := range finished {
			c.release(e.reserved)
			c.finish(e.id)
		}
	}
}
