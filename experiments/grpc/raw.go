package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxMessage = 4 << 20

// Reader owns incoming stream state. Writer owns all framing, HPACK and credit.
// No per-unary goroutine, no custom TLS or HPACK implementation.
type rawStream struct {
	path     string
	input    []byte
	watch    *subscription
	received uint32
}
type output struct {
	id       uint32
	payload  []byte
	end      bool
	code     int
	reset    bool
	control  func(*http2.Framer) error
	delta    int64
	initial  *int64
	maxFrame uint32
}
type rawConn struct {
	net.Conn
	b    *backend
	out  chan output
	done chan struct{}
	stop chan struct{}
}

func (c *rawConn) enqueue(o output) {
	select {
	case c.out <- o:
	case <-c.done:
	case <-c.stop:
	}
}
func (c *rawConn) serve() error {
	defer c.Close()
	r := bufio.NewReaderSize(c.Conn, 64<<10)
	pre := make([]byte, len(http2.ClientPreface))
	if _, e := io.ReadFull(r, pre); e != nil {
		return e
	}
	if string(pre) != http2.ClientPreface {
		return errors.New("bad preface")
	}
	c.out = make(chan output, 4096)
	c.done = make(chan struct{})
	c.stop = make(chan struct{})
	go func() {
		defer close(c.done)
		defer c.Close()
		if e := c.writeLoop(); e != nil && !errors.Is(e, net.ErrClosed) { /* caller's disconnect is expected */
		}
	}()
	f := http2.NewFramer(io.Discard, r)
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	f.MaxHeaderListSize = 16 << 10
	f.SetMaxReadFrameSize(16 << 10)
	streams := make(map[uint32]*rawStream)
	defer func() {
		close(c.stop)
		c.Close()
		for _, s := range streams {
			if s.watch != nil {
				c.b.unsubscribe(s.watch)
			}
		}
	}()
	var received uint32
	for {
		frame, e := f.ReadFrame()
		if e != nil {
			return e
		}
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			if v.IsAck() {
				continue
			}
			var initial *int64
			var mf uint32
			if e := v.ForeachSetting(func(s http2.Setting) error {
				if e := s.Valid(); e != nil {
					return e
				}
				switch s.ID {
				case http2.SettingInitialWindowSize:
					n := int64(s.Val)
					initial = &n
				case http2.SettingMaxFrameSize:
					mf = s.Val
				}
				return nil
			}); e != nil {
				return e
			}
			c.enqueue(output{initial: initial, maxFrame: mf, control: func(f *http2.Framer) error { return f.WriteSettingsAck() }})
		case *http2.MetaHeadersFrame:
			if v.Truncated {
				return errors.New("headers too large")
			}
			s := &rawStream{}
			for _, h := range v.Fields {
				if h.Name == ":path" {
					s.path = h.Value
				}
			}
			streams[v.StreamID] = s
			if v.StreamEnded() {
				c.enqueue(output{id: v.StreamID, end: true, code: 3})
				delete(streams, v.StreamID)
			}
		case *http2.DataFrame:
			s := streams[v.StreamID]
			if s == nil {
				continue
			}
			if len(s.input)+len(v.Data()) > maxMessage+5 {
				return errors.New("message too large")
			}
			s.input = append(s.input, v.Data()...)
			received += v.Header().Length
			s.received += v.Header().Length
			// Return credit in chunks, not one WINDOW_UPDATE syscall per Put.
			if received >= 64<<10 {
				n := received
				received = 0
				c.enqueue(output{control: func(f *http2.Framer) error { return f.WriteWindowUpdate(0, n) }})
			}
			if s.received >= 64<<10 {
				n := s.received
				s.received = 0
				id := v.StreamID
				c.enqueue(output{control: func(f *http2.Framer) error { return f.WriteWindowUpdate(id, n) }})
			}
			for len(s.input) >= 5 {
				n := int(binary.BigEndian.Uint32(s.input[1:5]))
				if n > maxMessage {
					return errors.New("message limit")
				}
				if len(s.input) < n+5 {
					break
				}
				if s.input[0] != 0 {
					return errors.New("compression unsupported")
				}
				body := s.input[5 : 5+n]
				if s.path == "/etcdserverpb.Watch/Watch" {
					req := new(pb.WatchRequest)
					if e := proto.Unmarshal(body, req); e != nil {
						return e
					}
					if cr := req.GetCreateRequest(); cr != nil {
						if s.watch != nil {
							return errors.New("multiple watch IDs unsupported")
						}
						id := v.StreamID
						s.watch = c.b.subscribe(cr, 1, func(r *pb.WatchResponse) {
							p, e := proto.Marshal(r)
							if e == nil {
								c.enqueue(output{id: id, payload: p})
							}
						})
					} else if req.GetCancelRequest() != nil && s.watch != nil {
						c.b.unsubscribe(s.watch)
						s.watch = nil
						c.enqueue(output{id: v.StreamID, end: true})
					}
				} else {
					p, e := c.b.unary(s.path, body)
					code := 0
					if e != nil {
						code = int(status.Code(e))
					}
					c.enqueue(output{id: v.StreamID, payload: p, end: true, code: code})
				}
				s.input = s.input[5+n:]
			}
			if v.StreamEnded() && s.watch == nil {
				delete(streams, v.StreamID)
			}
		case *http2.WindowUpdateFrame:
			c.enqueue(output{id: v.StreamID, delta: int64(v.Increment)})
		case *http2.RSTStreamFrame:
			if s := streams[v.StreamID]; s != nil && s.watch != nil {
				c.b.unsubscribe(s.watch)
			}
			delete(streams, v.StreamID)
			c.enqueue(output{id: v.StreamID, reset: true})
		case *http2.PingFrame:
			if !v.IsAck() {
				data := v.Data
				c.enqueue(output{control: func(f *http2.Framer) error { return f.WritePing(true, data) }})
			}
		case *http2.GoAwayFrame:
			return nil
		}
	}
}

type pendingStream struct {
	window   int64
	headers  bool
	messages []output
	offset   int
	framed   []byte
}

func (c *rawConn) writeLoop() error {
	w := bufio.NewWriterSize(c.Conn, 64<<10)
	f := http2.NewFramer(w, nil)
	if e := f.WriteSettings(http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 1024}, http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 20}); e != nil {
		return e
	}
	if e := f.WriteWindowUpdate(0, (16<<20)-65535); e != nil {
		return e
	}
	if e := w.Flush(); e != nil {
		return e
	}
	var header bytes.Buffer
	enc := hpack.NewEncoder(&header)
	headers := func(id uint32, end bool, fields ...hpack.HeaderField) error {
		header.Reset()
		for _, h := range fields {
			if e := enc.WriteField(h); e != nil {
				return e
			}
		}
		return f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: header.Bytes(), EndHeaders: true, EndStream: end})
	}
	streams := make(map[uint32]*pendingStream)
	var connWindow int64 = 65535
	var initial int64 = 65535
	maxFrame := uint32(16384)
	apply := func(o output) error {
		if o.control != nil {
			if e := o.control(f); e != nil {
				return e
			}
		}
		if o.initial != nil {
			diff := *o.initial - initial
			initial = *o.initial
			for _, s := range streams {
				s.window += diff
			}
		}
		if o.maxFrame != 0 {
			maxFrame = o.maxFrame
		}
		if o.reset {
			delete(streams, o.id)
			return nil
		}
		if o.delta != 0 {
			if o.id == 0 {
				connWindow += o.delta
			} else if s := streams[o.id]; s != nil {
				s.window += o.delta
			}
			return nil
		}
		if o.id == 0 {
			return nil
		}
		s := streams[o.id]
		if s == nil {
			s = &pendingStream{window: initial}
			streams[o.id] = s
		}
		s.messages = append(s.messages, o)
		return nil
	}
	for {
		select {
		case o := <-c.out:
			if e := apply(o); e != nil {
				return e
			}
		case <-c.stop:
			return nil
		}
		// Coalesce whatever is ready without a batching timer.
		for len(c.out) > 0 {
			if e := apply(<-c.out); e != nil {
				return e
			}
		}
		for id, s := range streams {
			if !s.headers {
				if e := headers(id, false, hpack.HeaderField{Name: ":status", Value: "200"}, hpack.HeaderField{Name: "content-type", Value: "application/grpc"}); e != nil {
					return e
				}
				s.headers = true
			}
			for len(s.messages) > 0 {
				o := s.messages[0]
				if s.framed == nil && o.payload != nil {
					s.framed = make([]byte, 5+len(o.payload))
					binary.BigEndian.PutUint32(s.framed[1:5], uint32(len(o.payload)))
					copy(s.framed[5:], o.payload)
				}
				for s.offset < len(s.framed) && connWindow > 0 && s.window > 0 {
					n := min(int64(len(s.framed)-s.offset), int64(maxFrame), connWindow, s.window)
					if e := f.WriteData(id, false, s.framed[s.offset:s.offset+int(n)]); e != nil {
						return e
					}
					s.offset += int(n)
					connWindow -= n
					s.window -= n
				}
				if s.offset < len(s.framed) {
					break
				}
				s.framed = nil
				s.offset = 0
				s.messages = s.messages[1:]
				if o.end {
					if e := headers(id, true, hpack.HeaderField{Name: "grpc-status", Value: strconv.Itoa(o.code)}); e != nil {
						return e
					}
					delete(streams, id)
					break
				}
			}
		}
		if e := w.Flush(); e != nil {
			return fmt.Errorf("flush: %w", e)
		}
	}
}
