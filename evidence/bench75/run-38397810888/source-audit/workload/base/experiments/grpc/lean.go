package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// A closed-loop Put-only load generator. One reader and one owner per connection.
// It verifies response protobufs and status trailers, and reuses request bytes.
type leanEvent struct {
	id       uint32
	data     []byte
	end      bool
	code     string
	delta    int64
	initial  int64
	settings bool
	ping     *[8]byte
	err      error
}
type leanClient struct {
	conn                  *tls.Conn
	w                     *bufio.Writer
	f                     *http2.Framer
	events                chan leanEvent
	stop                  chan struct{}
	firstHeaders, headers []byte
	first                 bool
	next                  uint32
	window                int64
	streamWindow          int64
	requests              [][]byte
}

func newLeanClient(index int) (*leanClient, error) {
	cfg, e := tlsConfig(false)
	if e != nil {
		return nil, e
	}
	c, e := tls.Dial("tcp", *addr, cfg)
	if e != nil {
		return nil, e
	}
	if c.ConnectionState().Version != tls.VersionTLS13 || c.ConnectionState().NegotiatedProtocol != "h2" {
		c.Close()
		return nil, errors.New("TLS13/h2 not negotiated")
	}
	l := &leanClient{conn: c, w: bufio.NewWriterSize(c, 64<<10), events: make(chan leanEvent, 4096), stop: make(chan struct{}), first: true, next: 1, window: 65535, streamWindow: 65535}
	l.f = http2.NewFramer(l.w, nil)
	var h bytes.Buffer
	enc := hpack.NewEncoder(&h)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: "/etcdserverpb.KV/Put"}, {Name: ":authority", Value: "localhost"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}, {Name: "user-agent", Value: "grpcbench-lean/0"}}
	for _, v := range fields {
		if e := enc.WriteField(v); e != nil {
			c.Close()
			return nil, e
		}
	}
	l.firstHeaders = bytes.Clone(h.Bytes())
	h.Reset()
	for _, v := range fields {
		if e := enc.WriteField(v); e != nil {
			c.Close()
			return nil, e
		}
	}
	l.headers = bytes.Clone(h.Bytes())
	l.requests = make([][]byte, *streams)
	for slot := range l.requests {
		req := &pb.PutRequest{Key: []byte(fmt.Sprintf("/registry/configmaps/bench/worker-%06d%s", index**streams+slot, strings.Repeat("x", 20))), Value: []byte(strings.Repeat("v", 1024))}
		p, e := proto.Marshal(req)
		if e != nil {
			c.Close()
			return nil, e
		}
		wire := make([]byte, 5+len(p))
		binary.BigEndian.PutUint32(wire[1:5], uint32(len(p)))
		copy(wire[5:], p)
		l.requests[slot] = wire
	}
	if _, e := l.w.WriteString(http2.ClientPreface); e != nil {
		c.Close()
		return nil, e
	}
	if e := l.f.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 20}); e != nil {
		c.Close()
		return nil, e
	}
	if e := l.f.WriteWindowUpdate(0, (16<<20)-65535); e != nil {
		c.Close()
		return nil, e
	}
	if e := l.w.Flush(); e != nil {
		c.Close()
		return nil, e
	}
	go l.readLoop()
	return l, nil
}
func (l *leanClient) close() { close(l.stop); l.conn.Close() }
func (l *leanClient) event(v leanEvent) bool {
	select {
	case l.events <- v:
		return true
	case <-l.stop:
		return false
	}
}
func (l *leanClient) readLoop() {
	f := http2.NewFramer(io.Discard, bufio.NewReaderSize(l.conn, 64<<10))
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	for {
		frame, e := f.ReadFrame()
		if e != nil {
			l.event(leanEvent{err: e})
			return
		}
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			if v.IsAck() {
				continue
			}
			var initial int64 = -1
			if e := v.ForeachSetting(func(s http2.Setting) error {
				if s.ID == http2.SettingInitialWindowSize {
					initial = int64(s.Val)
				}
				return s.Valid()
			}); e != nil {
				l.event(leanEvent{err: e})
				return
			}
			if !l.event(leanEvent{settings: true, initial: initial}) {
				return
			}
		case *http2.WindowUpdateFrame:
			if !l.event(leanEvent{id: v.StreamID, delta: int64(v.Increment)}) {
				return
			}
		case *http2.MetaHeadersFrame:
			var code string
			for _, h := range v.Fields {
				if h.Name == "grpc-status" {
					code = h.Value
				}
				if h.Name == ":status" && h.Value != "200" {
					l.event(leanEvent{err: fmt.Errorf("HTTP status %s", h.Value)})
					return
				}
			}
			if !l.event(leanEvent{id: v.StreamID, end: v.StreamEnded(), code: code}) {
				return
			}
		case *http2.DataFrame:
			if !l.event(leanEvent{id: v.StreamID, data: bytes.Clone(v.Data()), end: v.StreamEnded()}) {
				return
			}
		case *http2.PingFrame:
			if !v.IsAck() {
				data := v.Data
				if !l.event(leanEvent{ping: &data}) {
					return
				}
			}
		case *http2.RSTStreamFrame:
			l.event(leanEvent{err: fmt.Errorf("RST_STREAM %d %v", v.StreamID, v.ErrCode)})
			return
		case *http2.GoAwayFrame:
			l.event(leanEvent{err: fmt.Errorf("GOAWAY %v", v.ErrCode)})
			return
		}
	}
}

type leanPending struct {
	slot  int
	start time.Time
	body  []byte
	code  string
}

func (l *leanClient) run(d time.Duration, measure bool) workerResult {
	result := workerResult{}
	if measure {
		result.hist = make([]int64, 20000)
	}
	pending := make(map[uint32]*leanPending, *streams)
	free := make([]int, *streams)
	for i := range free {
		free[i] = i
	}
	end := time.Now().Add(d)
	var received uint32
	fail := func(e error) workerResult { result.err = e; return result }
	for {
		for len(free) > 0 && time.Now().Before(end) && l.window >= int64(len(l.requests[free[len(free)-1]])) && l.streamWindow >= int64(len(l.requests[free[len(free)-1]])) {
			slot := free[len(free)-1]
			free = free[:len(free)-1]
			id := l.next
			l.next += 2
			h := l.headers
			if l.first {
				h = l.firstHeaders
				l.first = false
			}
			pending[id] = &leanPending{slot: slot, start: time.Now()}
			if e := l.f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: h, EndHeaders: true}); e != nil {
				return fail(e)
			}
			if e := l.f.WriteData(id, true, l.requests[slot]); e != nil {
				return fail(e)
			}
			l.window -= int64(len(l.requests[slot]))
		}
		if e := l.w.Flush(); e != nil {
			return fail(e)
		}
		if len(pending) == 0 && !time.Now().Before(end) {
			return result
		}
		// Drain frames already read before replenishing the request slots.
		var ev leanEvent
		select {
		case ev = <-l.events:
		case <-time.After(30 * time.Second):
			return fail(errors.New("lean client stalled"))
		}
		for {
			if ev.err != nil {
				return fail(ev.err)
			}
			switch {
			case ev.settings:
				if ev.initial >= 0 {
					l.streamWindow = ev.initial
				}
				if e := l.f.WriteSettingsAck(); e != nil {
					return fail(e)
				}
			case ev.ping != nil:
				if e := l.f.WritePing(true, *ev.ping); e != nil {
					return fail(e)
				}
			case ev.delta != 0:
				if ev.id == 0 {
					l.window += ev.delta
				}
			default:
				p := pending[ev.id]
				if p == nil {
					return fail(fmt.Errorf("unknown response stream %d", ev.id))
				}
				if ev.data != nil {
					p.body = append(p.body, ev.data...)
					received += uint32(len(ev.data))
					if received >= 64<<10 {
						if e := l.f.WriteWindowUpdate(0, received); e != nil {
							return fail(e)
						}
						received = 0
					}
				}
				if ev.code != "" {
					p.code = ev.code
				}
				if ev.end {
					if p.code != "0" {
						return fail(fmt.Errorf("grpc-status %q", p.code))
					}
					if len(p.body) < 5 || p.body[0] != 0 || int(binary.BigEndian.Uint32(p.body[1:5])) != len(p.body)-5 {
						return fail(errors.New("invalid gRPC message"))
					}
					resp := new(pb.PutResponse)
					if e := proto.Unmarshal(p.body[5:], resp); e != nil {
						return fail(e)
					}
					if resp.Header == nil || resp.Header.Revision <= 1 {
						return fail(errors.New("missing Put revision"))
					}
					result.n++
					if measure {
						ns := time.Since(p.start).Nanoseconds()
						result.latencyNS += ns
						result.maxNS = max(result.maxNS, ns)
						us := ns / 1000
						bin := us
						if us >= 10000 {
							bin = 10000 + (us-10000)/100
						}
						result.hist[min(bin, int64(len(result.hist)-1))]++
					}
					free = append(free, p.slot)
					delete(pending, ev.id)
				}
			}
			if len(l.events) == 0 {
				break
			}
			ev = <-l.events
		}
	}
}
func leanLoad() error {
	clients := make([]*leanClient, *connections)
	for i := range clients {
		l, e := newLeanClient(i)
		if e != nil {
			return e
		}
		clients[i] = l
		defer l.close()
	}
	phase := func(d time.Duration, measure bool) ([]workerResult, time.Duration) {
		var wg sync.WaitGroup
		results := make([]workerResult, len(clients))
		start := time.Now()
		for i, l := range clients {
			wg.Add(1)
			go func() { defer wg.Done(); results[i] = l.run(d, measure) }()
		}
		wg.Wait()
		return results, time.Since(start)
	}
	warm, _ := phase(*warmup, false)
	for _, r := range warm {
		if r.err != nil {
			return r.err
		}
	}
	s0, e := serverCPU()
	if e != nil {
		return e
	}
	c0 := cpuSeconds()
	results, wall := phase(*duration, true)
	c1 := cpuSeconds()
	s1, e := serverCPU()
	if e != nil {
		return e
	}
	return emitLoadResult(results, wall, s1-s0, c1-c0)
}
