// Adapted from experiments/grpc/lean.go at d2408301966b99471bfed06d4c2dc1cd1e5af462.
package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

const (
	initialWindow       = 65535
	receiveStreamWindow = 1 << 20
	receiveConnWindow   = 16 << 20
	maxResponseBytes    = 16 << 10
	maxWindow           = (1 << 31) - 1
)

type eventKind uint8

const (
	eventHeaders eventKind = iota
	eventData
	eventSettings
	eventWindow
	eventPing
	eventFailure
)

type leanEvent struct {
	kind     eventKind
	id       uint32
	data     []byte
	fields   []hpack.HeaderField
	end, ack bool
	flow     uint32
	delta    int64
	settings []http2.Setting
	ping     [8]byte
	err      error
}

type leanClient struct {
	conn                      *tls.Conn
	w                         *bufio.Writer
	f                         *http2.Framer
	events                    chan leanEvent
	stop, readDone            chan struct{}
	closeOnce                 sync.Once
	headers                   []byte
	next                      uint32
	window, streamWindow      int64
	receiveWindow             int64
	received                  uint32
	maxStreams, maxFrame      uint32
	headerListSize            uint64
	requests                  [][]byte
	slots                     []leanPending
	pending                   map[uint32]int
	free                      []int
	settingsSeen, settingsAck bool
	pingSequence              uint64
	waitingPing               [8]byte
	pingPending               bool
}

type leanPending struct {
	id                    uint32
	start                 time.Time
	request               []byte
	offset                int
	window, receiveWindow int64
	response              unaryResponse
}

func encodeRequests(keys, values [][]byte) ([][]byte, error) {
	if len(keys) != len(values) {
		return nil, errors.New("workload length mismatch")
	}
	requests := make([][]byte, len(keys))
	for i := range keys {
		body, err := proto.Marshal(&pb.PutRequest{Key: keys[i], Value: values[i]})
		if err != nil {
			return nil, err
		}
		wire := make([]byte, 5+len(body))
		if len(wire) >= 2<<10 {
			return nil, errors.New("lean Put must be smaller than 2 KiB")
		}
		binary.BigEndian.PutUint32(wire[1:5], uint32(len(body)))
		copy(wire[5:], body)
		requests[i] = wire
	}
	return requests, nil
}

func putHeaders(authority string) ([]byte, uint64, error) {
	var b bytes.Buffer
	enc := hpack.NewEncoder(&b)
	// Never reference the dynamic table. This one block remains valid after any
	// peer HEADER_TABLE_SIZE change, including zero, without measured re-encoding.
	enc.SetMaxDynamicTableSize(0)
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"}, {Name: ":authority", Value: authority},
		{Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
		{Name: "user-agent", Value: "grpcbench-lean/0"},
	}
	var size uint64
	for _, h := range fields {
		size += uint64(len(h.Name) + len(h.Value) + 32)
		h.Sensitive = true
		if err := enc.WriteField(h); err != nil {
			return nil, 0, err
		}
	}
	return bytes.Clone(b.Bytes()), size, nil
}

func newLeanClient(endpoint string, cfg *tls.Config, requests [][]byte, slots int) (*leanClient, error) {
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: ioTimeout}, "tcp", endpoint, cfg.Clone())
	if err != nil {
		return nil, err
	}
	l := &leanClient{
		conn: c, w: bufio.NewWriterSize(c, 64<<10), events: make(chan leanEvent, max(32, min(1024, slots*8+16))),
		stop: make(chan struct{}), readDone: make(chan struct{}), next: 1,
		window: initialWindow, streamWindow: initialWindow, receiveWindow: receiveConnWindow,
		maxStreams: ^uint32(0), maxFrame: 16384, requests: requests,
		slots: make([]leanPending, slots), pending: make(map[uint32]int, slots), free: make([]int, slots),
	}
	for i := range l.free {
		l.free[i] = i
	}
	// Start the reader only after all fallible preface writes below succeed.
	fail := func(err error) (*leanClient, error) { l.close(); return nil, err }
	if c.ConnectionState().Version != tls.VersionTLS13 || c.ConnectionState().NegotiatedProtocol != "h2" {
		return fail(errors.New("TLS13/h2 not negotiated"))
	}
	l.headers, l.headerListSize, err = putHeaders(endpoint)
	if err != nil {
		return fail(err)
	}
	l.f = http2.NewFramer(l.w, nil)
	if err = c.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		return fail(err)
	}
	if _, err = l.w.WriteString(http2.ClientPreface); err != nil {
		return fail(err)
	}
	if err = l.f.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: receiveStreamWindow}); err != nil {
		return fail(err)
	}
	if err = l.f.WriteWindowUpdate(0, receiveConnWindow-initialWindow); err != nil {
		return fail(err)
	}
	if err = l.w.Flush(); err != nil {
		return fail(err)
	}
	go l.readLoop()
	if err = l.handshake(); err != nil {
		l.close()
		<-l.readDone
		return nil, err
	}
	return l, nil
}

func (l *leanClient) close() {
	l.closeOnce.Do(func() {
		close(l.stop)
		// TLS Close can write close_notify and wait on a nonreading peer. Abort
		// the raw socket instead. This also releases a reader blocked in ReadFrame.
		_ = l.conn.NetConn().Close()
	})
}
func (l *leanClient) event(ev leanEvent) bool {
	select {
	case l.events <- ev:
		return true
	case <-l.stop:
		return false
	}
}
func (l *leanClient) readLoop() {
	defer close(l.readDone)
	f := http2.NewFramer(io.Discard, bufio.NewReaderSize(l.conn, 64<<10))
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	f.MaxHeaderListSize = 64 << 10
	f.SetMaxReadFrameSize(16384)
	first := true
	for {
		frame, err := f.ReadFrame()
		if err != nil {
			l.event(leanEvent{kind: eventFailure, err: err})
			return
		}
		if first {
			v, ok := frame.(*http2.SettingsFrame)
			if !ok || v.IsAck() {
				l.event(leanEvent{kind: eventFailure, err: errors.New("server preface is not SETTINGS")})
				return
			}
			first = false
		}
		var ev leanEvent
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			ev = leanEvent{kind: eventSettings, ack: v.IsAck()}
			if err = v.ForeachSetting(func(s http2.Setting) error {
				if err := s.Valid(); err != nil {
					return err
				}
				ev.settings = append(ev.settings, s)
				return nil
			}); err != nil {
				l.event(leanEvent{kind: eventFailure, err: err})
				return
			}
		case *http2.WindowUpdateFrame:
			ev = leanEvent{kind: eventWindow, id: v.StreamID, delta: int64(v.Increment)}
		case *http2.MetaHeadersFrame:
			if v.Truncated {
				l.event(leanEvent{kind: eventFailure, err: errors.New("response headers exceed limit")})
				return
			}
			ev = leanEvent{kind: eventHeaders, id: v.StreamID, fields: append([]hpack.HeaderField(nil), v.Fields...), end: v.StreamEnded()}
		case *http2.DataFrame:
			// Framer owns Data; it will overwrite it on its next ReadFrame.
			ev = leanEvent{kind: eventData, id: v.StreamID, data: bytes.Clone(v.Data()), flow: v.Header().Length, end: v.StreamEnded()}
		case *http2.PingFrame:
			ev = leanEvent{kind: eventPing, ping: v.Data, ack: v.IsAck()}
		case *http2.RSTStreamFrame:
			l.event(leanEvent{kind: eventFailure, err: fmt.Errorf("RST_STREAM %d %v", v.StreamID, v.ErrCode)})
			return
		case *http2.GoAwayFrame:
			l.event(leanEvent{kind: eventFailure, err: fmt.Errorf("GOAWAY %v", v.ErrCode)})
			return
		case *http2.PushPromiseFrame:
			l.event(leanEvent{kind: eventFailure, err: errors.New("unexpected server push")})
			return
		default:
			// PRIORITY and extension frames do not carry response messages.
			continue
		}
		if !l.event(ev) {
			return
		}
	}
}

func addCredit(window *int64, delta int64) error {
	if delta <= 0 || *window > maxWindow-delta {
		return errors.New("invalid HTTP/2 WINDOW_UPDATE")
	}
	*window += delta
	return nil
}

// All writes and protocol state changes happen on the connection owner.
func (l *leanClient) apply(ev leanEvent, result *connectionResult) error {
	switch ev.kind {
	case eventFailure:
		return ev.err
	case eventSettings:
		if ev.ack {
			if l.settingsAck {
				return errors.New("duplicate SETTINGS acknowledgement")
			}
			l.settingsAck = true
			return nil
		}
		for _, s := range ev.settings {
			switch s.ID {
			case http2.SettingInitialWindowSize:
				delta := int64(s.Val) - l.streamWindow
				for _, slot := range l.pending {
					p := &l.slots[slot]
					if p.window+delta > maxWindow {
						return errors.New("stream window overflow after SETTINGS")
					}
					p.window += delta
				}
				l.streamWindow = int64(s.Val)
			case http2.SettingMaxConcurrentStreams:
				l.maxStreams = s.Val
			case http2.SettingMaxFrameSize:
				l.maxFrame = s.Val
			case http2.SettingMaxHeaderListSize:
				if l.headerListSize > uint64(s.Val) {
					return errors.New("Put headers exceed peer header-list limit")
				}
			case http2.SettingEnablePush:
				return errors.New("server sent SETTINGS_ENABLE_PUSH")
			}
		}
		l.settingsSeen = true
		return l.f.WriteSettingsAck()
	case eventWindow:
		if ev.id == 0 {
			return addCredit(&l.window, ev.delta)
		}
		if slot, ok := l.pending[ev.id]; ok {
			return addCredit(&l.slots[slot].window, ev.delta)
		}
		// RFC 9113 permits WINDOW_UPDATE racing with a stream's closure.
		if ev.id%2 == 0 || ev.id >= l.next {
			return fmt.Errorf("WINDOW_UPDATE for idle stream %d", ev.id)
		}
		return nil
	case eventPing:
		if !ev.ack {
			return l.f.WritePing(true, ev.ping)
		}
		if !l.pingPending || ev.ping != l.waitingPing {
			return errors.New("unexpected PING acknowledgement")
		}
		l.pingPending = false
		return nil
	case eventHeaders, eventData:
		slot, ok := l.pending[ev.id]
		if !ok {
			return fmt.Errorf("unknown/duplicate response stream %d", ev.id)
		}
		p := &l.slots[slot]
		if p.offset != len(p.request) {
			return errors.New("response before complete Put request")
		}
		if ev.kind == eventHeaders {
			if err := p.response.headers(ev.fields, ev.end); err != nil {
				return err
			}
		} else {
			if int64(ev.flow) > l.receiveWindow || int64(ev.flow) > p.receiveWindow {
				return errors.New("response exceeds HTTP/2 receive window")
			}
			l.receiveWindow -= int64(ev.flow)
			p.receiveWindow -= int64(ev.flow)
			l.received += ev.flow // Includes the pad length and padding bytes.
			if err := p.response.data(ev.data); err != nil {
				return err
			}
			if l.received >= 64<<10 {
				if err := l.f.WriteWindowUpdate(0, l.received); err != nil {
					return err
				}
				l.receiveWindow += int64(l.received)
				l.received = 0
			}
		}
		if ev.end {
			h, err := p.response.finish()
			if err != nil {
				return err
			}
			result.samples = append(result.samples, measurement{time.Since(p.start).Nanoseconds(), h.Revision, h.RaftTerm})
			delete(l.pending, ev.id)
			p.id = 0
			p.request = nil
			l.free = append(l.free, slot)
		}
	}
	return nil
}

func (l *leanClient) flush() error {
	return l.w.Flush()
}
func (l *leanClient) writeDeadline() error {
	return l.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
}
func (l *leanClient) beginPing() error {
	l.pingSequence++
	binary.BigEndian.PutUint64(l.waitingPing[:], l.pingSequence)
	l.pingPending = true
	return l.f.WritePing(false, l.waitingPing)
}

// Establish HTTP/2, apply the server's initial settings, and round-trip a PING
// before ready. This setup is outside the measured window.
func (l *leanClient) handshake() error {
	timer := time.NewTimer(ioTimeout)
	defer timer.Stop()
	var ignored connectionResult
	for !l.settingsSeen || !l.settingsAck {
		select {
		case ev := <-l.events:
			if err := l.writeDeadline(); err != nil {
				return err
			}
			if err := l.apply(ev, &ignored); err != nil {
				return err
			}
			if err := l.flush(); err != nil {
				return err
			}
		case <-timer.C:
			return errors.New("HTTP/2 setup timed out")
		}
	}
	if err := l.writeDeadline(); err != nil {
		return err
	}
	if err := l.beginPing(); err != nil {
		return err
	}
	if err := l.flush(); err != nil {
		return err
	}
	for l.pingPending {
		select {
		case ev := <-l.events:
			if err := l.writeDeadline(); err != nil {
				return err
			}
			if err := l.apply(ev, &ignored); err != nil {
				return err
			}
			if err := l.flush(); err != nil {
				return err
			}
		case <-timer.C:
			return errors.New("HTTP/2 setup PING timed out")
		}
	}
	return nil
}

func (l *leanClient) admit(b *budget) error {
	for len(l.free) != 0 && uint64(len(l.pending)) < uint64(l.maxStreams) && l.window > 0 {
		i, ok := b.claim()
		if !ok {
			break
		}
		if l.next > maxWindow {
			return errors.New("HTTP/2 stream IDs exhausted")
		}
		slot := l.free[len(l.free)-1]
		l.free = l.free[:len(l.free)-1]
		p := &l.slots[slot]
		p.id, p.start = l.next, time.Now()
		p.request = l.requests[(i*2654435761)%uint64(len(l.requests))]
		p.offset, p.window, p.receiveWindow = 0, l.streamWindow, receiveStreamWindow
		p.response.reset()
		l.next += 2
		l.pending[p.id] = slot
		if err := l.f.WriteHeaders(http2.HeadersFrameParam{StreamID: p.id, BlockFragment: l.headers, EndHeaders: true}); err != nil {
			return err
		}
	}
	return nil
}

func (l *leanClient) sendData() error {
	for i := range l.slots {
		p := &l.slots[i]
		if p.id == 0 {
			continue
		}
		for p.offset < len(p.request) && p.window > 0 && l.window > 0 {
			n := min(int64(len(p.request)-p.offset), p.window, l.window, int64(l.maxFrame))
			end := p.offset + int(n)
			if err := l.f.WriteData(p.id, end == len(p.request), p.request[p.offset:end]); err != nil {
				return err
			}
			p.offset = end
			p.window -= n
			l.window -= n
		}
	}
	return nil
}

func (l *leanClient) run(b *budget, abort <-chan struct{}) (result connectionResult) {
	// Slots, maps, frames and event channels are connection-owned, not per-Put
	// goroutines or contexts. The reader and this owner are the only Put workers.
	result.samples = make([]measurement, 0, 1024)
	timer := time.NewTimer(ioTimeout)
	defer timer.Stop()
	defer func() {
		if result.err != nil {
			l.close()
		}
	}()
	idle := time.Now().Add(ioTimeout)
	var fenceDeadline time.Time
	for {
		select {
		case <-abort:
			result.err = errors.New("another connection failed")
			return
		default:
		}
		if err := l.writeDeadline(); err != nil {
			result.err = err
			return
		}
		// Coalesce controls and requests for frames already read. The finite drain
		// bound avoids an endless control-frame queue starving timeout checks.
		for n := 0; n < cap(l.events); n++ {
			select {
			case ev := <-l.events:
				previous := len(result.samples)
				if err := l.apply(ev, &result); err != nil {
					result.err = err
					return
				}
				if len(result.samples) > previous {
					idle = time.Now().Add(ioTimeout)
				}
			default:
				n = cap(l.events)
			}
		}
		if fenceDeadline.IsZero() {
			prior := len(l.pending)
			if err := l.admit(b); err != nil {
				result.err = err
				return
			}
			if len(l.pending) > prior {
				idle = time.Now().Add(ioTimeout)
			}
			if err := l.sendData(); err != nil {
				result.err = err
				return
			}
			if len(l.pending) == 0 && !b.available(time.Now()) {
				// Fail queued duplicate frames before declaring drained. The PING
				// also orders a normal server's earlier writes before its ACK.
				if err := l.beginPing(); err != nil {
					result.err = err
					return
				}
				fenceDeadline = time.Now().Add(ioTimeout)
			}
		}
		if err := l.flush(); err != nil {
			result.err = err
			return
		}
		if !fenceDeadline.IsZero() && !l.pingPending {
			return
		}
		now := time.Now()
		wake := idle
		if !fenceDeadline.IsZero() {
			wake = fenceDeadline
		}
		for _, slot := range l.pending {
			d := l.slots[slot].start.Add(ioTimeout)
			if d.Before(wake) {
				wake = d
			}
		}
		if !now.Before(wake) {
			result.err = errors.New("lean connection/request timed out")
			return
		}
		// Stop duration arrivals even when no new frame arrives to wake an owner.
		if fenceDeadline.IsZero() && b.available(now) && !b.deadline.IsZero() && b.deadline.Before(wake) {
			wake = b.deadline
		}
		timer.Reset(time.Until(wake))
		select {
		case ev := <-l.events:
			if err := l.writeDeadline(); err != nil {
				result.err = err
				return
			}
			previous := len(result.samples)
			if err := l.apply(ev, &result); err != nil {
				result.err = err
				return
			}
			if len(result.samples) > previous {
				idle = time.Now().Add(ioTimeout)
			}
		case <-timer.C:
			// The top of the loop distinguishes the arrival cutoff from a stall.
		case <-abort:
			result.err = errors.New("another connection failed")
			return
		}
	}
}

// A unary response must contain one uncompressed message, initial HTTP status
// and content type, and one terminal grpc-status. Keep the state until END_STREAM.
type unaryResponse struct {
	body                          []byte
	initial, trailers, statusSeen bool
	status                        string
}

func (r *unaryResponse) reset() {
	r.body = r.body[:0]
	r.initial, r.trailers, r.statusSeen, r.status = false, false, false, ""
}
func (r *unaryResponse) headers(fields []hpack.HeaderField, end bool) error {
	initial := !r.initial
	if r.trailers {
		return errors.New("duplicate response trailers")
	}
	var httpStatus, contentType string
	for _, h := range fields {
		switch h.Name {
		case ":status":
			if !initial || httpStatus != "" {
				return errors.New("duplicate/unexpected HTTP status")
			}
			httpStatus = h.Value
		case "content-type":
			if !initial || contentType != "" {
				return errors.New("duplicate/unexpected content-type")
			}
			contentType = h.Value
		case "grpc-status":
			if !end || r.statusSeen {
				return errors.New("nonterminal/duplicate grpc-status")
			}
			r.statusSeen, r.status = true, h.Value
		case "grpc-encoding":
			if h.Value != "identity" {
				return errors.New("compressed response not supported")
			}
		default:
			if strings.HasPrefix(h.Name, ":") {
				return errors.New("unexpected response pseudo-header")
			}
		}
	}
	if initial {
		if httpStatus != "200" {
			return fmt.Errorf("HTTP status %q", httpStatus)
		}
		if contentType != "application/grpc" && !strings.HasPrefix(contentType, "application/grpc+") {
			return fmt.Errorf("invalid content-type %q", contentType)
		}
		r.initial = true
	} else if !end {
		return errors.New("nonterminal response trailers")
	}
	if end {
		r.trailers = true
	}
	return nil
}
func (r *unaryResponse) data(data []byte) error {
	if !r.initial || r.trailers {
		return errors.New("DATA before headers or after trailers")
	}
	if len(r.body)+len(data) > maxResponseBytes {
		return errors.New("PutResponse exceeds buffer bound")
	}
	r.body = append(r.body, data...)
	if len(r.body) >= 5 {
		if r.body[0] != 0 {
			return errors.New("compressed gRPC message")
		}
		n := uint64(binary.BigEndian.Uint32(r.body[1:5])) + 5
		if n > maxResponseBytes {
			return errors.New("PutResponse length exceeds buffer bound")
		}
		if uint64(len(r.body)) > n {
			return errors.New("duplicate gRPC message or trailing bytes")
		}
	}
	return nil
}
func (r *unaryResponse) finish() (*pb.ResponseHeader, error) {
	if !r.initial || !r.trailers || !r.statusSeen || r.status != "0" {
		return nil, fmt.Errorf("missing/bad terminal grpc-status %q", r.status)
	}
	if len(r.body) < 5 || r.body[0] != 0 || uint64(binary.BigEndian.Uint32(r.body[1:5])) != uint64(len(r.body)-5) {
		return nil, errors.New("missing or malformed unary gRPC message")
	}
	var response pb.PutResponse
	if err := proto.Unmarshal(r.body[5:], &response); err != nil {
		return nil, err
	}
	if response.Header == nil || response.Header.Revision < 1 || response.Header.RaftTerm == 0 {
		return nil, errors.New("missing Put revision/term")
	}
	return response.Header, nil
}
