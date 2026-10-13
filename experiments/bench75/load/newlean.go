package main

// Adapted from exp/pre-47-grpc experiments/grpc/lean.go (read-only source
// /tmp/bench7980/lean.go). Retains prebuilt requests, warm HPACK, coalesced
// writes, and one reader + one owner per connection, not one goroutine per RPC.
import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

const leanSource = "exp/pre-47-grpc:experiments/grpc/lean.go sha256:9553545389d7ae3d246af1ecf03acc26b7a3f167a7f33883c046054c6ed1f07e; bench75/newlean.go adaptation v1"
const leanTimeout = 10 * time.Second
const leanMaxResponse = 16 << 20
const leanReceiveCredit = 64 << 10

type leanTemplate struct {
	put, compare, branches []byte
}

func grpcWire(p []byte) []byte {
	wire := make([]byte, 5, 5+len(p))
	binary.BigEndian.PutUint32(wire[1:], uint32(len(p)))
	return append(wire, p...)
}

func leanTemplates(keys, values [][]byte, op string) ([]leanTemplate, error) {
	r := make([]leanTemplate, len(keys))
	for k := range keys {
		if op == "put" {
			p, err := proto.Marshal(&pb.PutRequest{Key: keys[k], Value: values[k]})
			if err != nil {
				return nil, err
			}
			r[k].put = grpcWire(p)
			continue
		}
		// No dynamic protobuf structs on the CAS request path. The comparison
		// length and mod_revision are encoded canonically into slot-owned scratch.
		c, err := proto.Marshal(&pb.Compare{Target: pb.Compare_MOD, Key: keys[k]})
		if err != nil {
			return nil, err
		}
		r[k].compare = append(c, 0x30) // Compare.mod_revision, field 6
		txn := casRequest(keys[k], values[k], 1)
		txn.Compare = nil
		r[k].branches, err = proto.Marshal(txn)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (t *leanTemplate) casWire(dst []byte, revision int64) ([]byte, error) {
	if revision <= 0 {
		return nil, errors.New("CAS requires positive mod revision")
	}
	var v [10]byte
	n := binary.PutUvarint(v[:], uint64(revision))
	dst = append(dst[:0], 0, 0, 0, 0, 0, 0x0a)
	dst = binary.AppendUvarint(dst, uint64(len(t.compare)+n))
	dst = append(dst, t.compare...)
	dst = append(dst, v[:n]...)
	dst = append(dst, t.branches...)
	binary.BigEndian.PutUint32(dst[1:5], uint32(len(dst)-5))
	return dst, nil
}

type leanEvent struct {
	id       uint32
	data     []byte
	flow     uint32 // Includes DATA padding, not just protobuf bytes.
	end      bool
	headers  []hpack.HeaderField
	settings []http2.Setting
	ack      bool
	delta    int64
	ping     *[8]byte
	err      error
}

type leanResult struct {
	latencies        []int64
	errs             map[string]int
	terms            map[uint64]int
	passes, failures uint64
}

type leanPhase struct {
	start    <-chan struct{}
	next     atomic.Uint64
	aborted  atomic.Bool
	total    uint64
	deadline time.Time
	warm     bool
}

// Claim globally, never N per connection. A claimed operation is sent once and
// drained even if the duration expires; no failed operation is retried.
func (p *leanPhase) claim() (uint64, bool) {
	if p.aborted.Load() || (!p.deadline.IsZero() && !time.Now().Before(p.deadline)) {
		return 0, false
	}
	if p.total == 0 {
		return p.next.Add(1) - 1, true
	}
	for {
		i := p.next.Load()
		if i >= p.total {
			return 0, false
		}
		if p.next.CompareAndSwap(i, i+1) {
			return i, true
		}
	}
}

func (p *leanPhase) exhausted() bool {
	return p.aborted.Load() || (p.total > 0 && p.next.Load() >= p.total) || (!p.deadline.IsZero() && !time.Now().Before(p.deadline))
}

type leanCommand struct {
	phase   *leanPhase
	warmKey int
}

type leanClient struct {
	conn                      net.Conn
	w                         *bufio.Writer
	f                         *http2.Framer
	events                    chan leanEvent
	stop                      chan struct{}
	once                      sync.Once
	commands                  chan leanCommand
	results                   chan leanResult
	done                      chan struct{}
	templates                 []leanTemplate
	keys, values              [][]byte
	revisions                 []atomic.Int64
	op                        string
	slots                     int
	next                      uint32
	window, initial           int64
	received                  uint32
	writeDeadline             time.Time
	maxConcurrent             uint32
	maxFrame                  uint32
	settingsSeen, settingsAck bool
	headerBuf                 bytes.Buffer
	encoder                   *hpack.Encoder
	fields                    []hpack.HeaderField
	firstHeaders, headers     []byte
	first, needHeaders        bool
	pending                   map[uint32]*leanPending
	free                      []*leanPending
}

type leanPool struct {
	clients    []*leanClient
	warmWrites uint64
}

func leanSlotCounts(workers, conns int) []int {
	counts := make([]int, min(workers, conns))
	for i := range counts {
		counts[i] = workers / len(counts)
		if i < workers%len(counts) {
			counts[i]++
		}
	}
	return counts
}

func newLeanPool(endpoint string, cfg *tls.Config, op string, workers, conns int, keys, values [][]byte, revisions []atomic.Int64) (*leanPool, error) {
	templates, err := leanTemplates(keys, values, op)
	if err != nil {
		return nil, err
	}
	p := &leanPool{}
	// Workers are total outstanding, evenly spread; workers < conns means only
	// workers active connections. The JSON reports this actual count.
	for _, slots := range leanSlotCounts(workers, conns) {
		c := cfg.Clone()
		c.NextProtos = []string{"h2"}
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: leanTimeout}, "tcp", endpoint, c)
		if err != nil {
			p.close()
			return nil, err
		}
		if conn.ConnectionState().Version != tls.VersionTLS13 || conn.ConnectionState().NegotiatedProtocol != "h2" {
			conn.Close()
			p.close()
			return nil, errors.New("lean requires TLS13/h2")
		}
		l, err := newLeanConnection(conn, endpoint, op, slots, templates, keys, values, revisions)
		if err != nil {
			conn.Close()
			p.close()
			return nil, err
		}
		p.clients = append(p.clients, l)
	}
	// All settings handshakes and one valid RPC on EVERY raw connection precede
	// the caller's before probe, revision fence, CPU/IO and heap snapshots.
	start := make(chan struct{})
	close(start)
	for i, l := range p.clients {
		l.commands <- leanCommand{phase: &leanPhase{start: start, total: 1, warm: true}, warmKey: i}
	}
	for _, l := range p.clients {
		r := <-l.results
		if len(r.errs) != 0 {
			p.close()
			return nil, fmt.Errorf("lean warmup: %v", r.errs)
		}
		if op == "put" {
			p.warmWrites += uint64(len(r.latencies))
		} else {
			p.warmWrites += r.passes
		}
	}
	return p, nil
}

func newLeanConnection(conn net.Conn, authority, op string, slots int, templates []leanTemplate, keys, values [][]byte, revisions []atomic.Int64) (*leanClient, error) {
	path := "/etcdserverpb.KV/Put"
	if op == "cas" {
		path = "/etcdserverpb.KV/Txn"
	}
	l := &leanClient{conn: conn, w: bufio.NewWriterSize(conn, 64<<10), events: make(chan leanEvent, 256), stop: make(chan struct{}), commands: make(chan leanCommand, 1), results: make(chan leanResult, 1), done: make(chan struct{}), templates: templates, keys: keys, values: values, revisions: revisions, op: op, slots: slots, next: 1, window: 65535, initial: 65535, maxConcurrent: math.MaxUint32, maxFrame: 16384}
	l.f = http2.NewFramer(l.w, nil)
	l.pending = make(map[uint32]*leanPending, slots)
	l.free = make([]*leanPending, slots)
	for i := range l.free {
		l.free[i] = &leanPending{body: make([]byte, 0, 2048), scratch: make([]byte, 0, 2048)}
	}
	l.fields = []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: path}, {Name: ":authority", Value: authority}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}, {Name: "user-agent", Value: "bench75-lean/1"}}
	l.encoder = hpack.NewEncoder(&l.headerBuf)
	l.needHeaders = true
	if err := l.refreshWriteDeadline(time.Now()); err != nil {
		return nil, err
	}
	if _, err := l.w.WriteString(http2.ClientPreface); err != nil {
		return nil, err
	}
	if err := l.f.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 20}, http2.Setting{ID: http2.SettingEnablePush, Val: 0}); err != nil {
		return nil, err
	}
	if err := l.f.WriteWindowUpdate(0, (16<<20)-65535); err != nil {
		return nil, err
	}
	if err := l.w.Flush(); err != nil {
		return nil, err
	}
	go l.readLoop()
	go l.ownerLoop()
	return l, nil
}

// Keep the write-hang guard without resetting netpoll timers for every reply.
func (l *leanClient) refreshWriteDeadline(now time.Time) error {
	if l.writeDeadline.Sub(now) > leanTimeout/2 {
		return nil
	}
	deadline := now.Add(leanTimeout)
	if err := l.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	l.writeDeadline = deadline
	return nil
}

func (l *leanClient) prepareHeaders() error {
	l.headerBuf.Reset()
	for _, field := range l.fields {
		if err := l.encoder.WriteField(field); err != nil {
			return err
		}
	}
	l.firstHeaders = bytes.Clone(l.headerBuf.Bytes())
	l.headerBuf.Reset()
	for _, field := range l.fields {
		if err := l.encoder.WriteField(field); err != nil {
			return err
		}
	}
	l.headers = bytes.Clone(l.headerBuf.Bytes())
	l.first = true
	return nil
}

func (l *leanClient) close() { l.once.Do(func() { close(l.stop); l.conn.Close() }) }
func (p *leanPool) close() {
	for _, l := range p.clients {
		l.close()
	}
	for _, l := range p.clients {
		<-l.done
	}
}
func (p *leanPool) begin(phase *leanPhase) {
	for _, l := range p.clients {
		l.commands <- leanCommand{phase: phase}
	}
}
func (p *leanPool) collect() []leanResult {
	r := make([]leanResult, len(p.clients))
	for i, l := range p.clients {
		r[i] = <-l.results
	}
	return r
}
func (l *leanClient) event(e leanEvent) bool {
	select {
	case l.events <- e:
		return true
	case <-l.stop:
		return false
	}
}

func (l *leanClient) readLoop() {
	f := http2.NewFramer(io.Discard, bufio.NewReaderSize(l.conn, 64<<10))
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	f.MaxHeaderListSize = 64 << 10
	firstFrame := true
	for {
		frame, err := f.ReadFrame()
		if err != nil {
			l.event(leanEvent{err: err})
			return
		}
		if firstFrame {
			settings, ok := frame.(*http2.SettingsFrame)
			if !ok || settings.IsAck() {
				l.event(leanEvent{err: errors.New("server preface missing SETTINGS")})
				return
			}
			firstFrame = false
		}
		e := leanEvent{}
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			if v.IsAck() {
				e.ack = true
			} else {
				e.settings = make([]http2.Setting, 0, v.NumSettings())
				if err := v.ForeachSetting(func(s http2.Setting) error {
					if s.ID == http2.SettingEnablePush {
						return errors.New("server sent SETTINGS_ENABLE_PUSH")
					}
					e.settings = append(e.settings, s)
					return s.Valid()
				}); err != nil {
					l.event(leanEvent{err: err})
					return
				}
			}
		case *http2.WindowUpdateFrame:
			e.id, e.delta = v.StreamID, int64(v.Increment)
		case *http2.MetaHeadersFrame:
			if v.Truncated {
				l.event(leanEvent{err: errors.New("truncated response headers")})
				return
			}
			e.id, e.end, e.headers = v.StreamID, v.StreamEnded(), append([]hpack.HeaderField(nil), v.Fields...)
			// A non-nil empty slice distinguishes HEADERS from DATA.
			if e.headers == nil {
				e.headers = []hpack.HeaderField{}
			}
		case *http2.DataFrame:
			e.id, e.end, e.data, e.flow = v.StreamID, v.StreamEnded(), bytes.Clone(v.Data()), v.Header().Length
		case *http2.PingFrame:
			if v.IsAck() {
				continue
			}
			data := v.Data
			e.ping = &data
		case *http2.RSTStreamFrame:
			e.err = fmt.Errorf("RST_STREAM %d %v", v.StreamID, v.ErrCode)
		case *http2.GoAwayFrame:
			e.err = fmt.Errorf("GOAWAY %v", v.ErrCode)
		case *http2.PushPromiseFrame:
			e.err = errors.New("unexpected server push")
		default:
			continue
		}
		if !l.event(e) || e.err != nil {
			return
		}
	}
}

type leanPending struct {
	key                    int
	start                  time.Time
	body, scratch, request []byte
	sent                   int
	window                 int64
	received               uint32
	gotHeaders, gotStatus  bool
	code                   string
}

func (p *leanPending) responseHeaders(fields []hpack.HeaderField, end bool) error {
	initial := !p.gotHeaders
	var status, contentType string
	for _, h := range fields {
		switch h.Name {
		case ":status":
			if !initial || status != "" {
				return errors.New("duplicate/unexpected HTTP status")
			}
			status = h.Value
		case "content-type":
			if !initial || contentType != "" {
				return errors.New("duplicate/unexpected content-type")
			}
			contentType = h.Value
		case "grpc-status":
			if !end || p.gotStatus {
				return errors.New("duplicate/nonterminal grpc-status")
			}
			p.gotStatus, p.code = true, h.Value
		default:
			if strings.HasPrefix(h.Name, ":") {
				return errors.New("unexpected response pseudo-header")
			}
		}
	}
	if initial {
		if status != "200" || !(contentType == "application/grpc" || strings.HasPrefix(contentType, "application/grpc+") || strings.HasPrefix(contentType, "application/grpc;")) {
			return fmt.Errorf("invalid HTTP response %q %q", status, contentType)
		}
		p.gotHeaders = true
	}
	if end && (!p.gotStatus || p.code != "0") {
		return fmt.Errorf("grpc-status %q", p.code)
	}
	return nil
}

func leanResponse(op string, body, key, value []byte) (*pb.ResponseHeader, bool, int64, error) {
	if len(body) < 5 || body[0] != 0 || uint64(binary.BigEndian.Uint32(body[1:5])) != uint64(len(body)-5) {
		return nil, false, 0, errors.New("invalid gRPC message")
	}
	if op == "put" {
		r := new(pb.PutResponse)
		if err := proto.Unmarshal(body[5:], r); err != nil {
			return nil, false, 0, err
		}
		if r.Header == nil || r.Header.Revision <= 0 || r.Header.RaftTerm == 0 || r.PrevKv != nil {
			return nil, false, 0, errors.New("invalid Put response")
		}
		return r.Header, false, 0, nil
	}
	r := new(pb.TxnResponse)
	if err := proto.Unmarshal(body[5:], r); err != nil {
		return nil, false, 0, err
	}
	revision, err := checkCAS(r, key, value)
	return r.Header, r.Succeeded, revision, err
}

func (l *leanClient) ownerLoop() {
	defer close(l.done)
	for {
		select {
		case <-l.stop:
			return
		case cmd := <-l.commands:
			select {
			case <-cmd.phase.start:
			case <-l.stop:
				return
			}
			r := l.runPhase(cmd)
			l.results <- r
			if len(r.errs) != 0 {
				l.close()
				return
			}
		}
	}
}

func (l *leanClient) runPhase(cmd leanCommand) leanResult {
	r := leanResult{errs: map[string]int{}, terms: map[uint64]int{}}
	phase := cmd.phase
	pending, free := l.pending, l.free
	defer func() { l.free = free }()
	fail := func(err error) leanResult { r.errs[err.Error()]++; phase.aborted.Store(true); return r }
	timer := time.NewTimer(leanTimeout)
	defer timer.Stop()
	warmSent := false
	lastProgress := time.Now()
	for {
		// Check before any buffered writer can auto-flush, including after an
		// idle accounting interval. Actual renewals happen at most every 5s.
		if err := l.refreshWriteDeadline(time.Now()); err != nil {
			return fail(err)
		}
		// Consume all queued control/response events BEFORE replenishing streams.
		// In particular, SETTINGS changes apply to every already-open stream.
		for {
			select {
			case ev := <-l.events:
				if err := l.handleEvent(ev, pending, &free, &r); err != nil {
					return fail(err)
				}
				lastProgress = time.Now()
			default:
				goto drained
			}
		}
	drained:
		for l.settingsSeen && l.settingsAck && len(free) > 0 && uint64(len(pending)) < uint64(l.maxConcurrent) {
			var i uint64
			if phase.warm {
				if warmSent {
					break
				}
				warmSent = true
				i = uint64(cmd.warmKey)
			} else {
				var ok bool
				i, ok = phase.claim()
				if !ok {
					break
				}
			}
			if l.next > math.MaxInt32 {
				return fail(errors.New("HTTP2 stream IDs exhausted"))
			}
			k := int((i * 2654435761) % uint64(len(l.keys)))
			p := free[len(free)-1]
			free = free[:len(free)-1]
			p.key, p.start, p.sent, p.window = k, time.Now(), 0, l.initial
			p.body = p.body[:0]
			p.received = 0
			p.gotHeaders, p.gotStatus, p.code = false, false, ""
			p.request = l.templates[k].put
			if l.op == "cas" {
				var err error
				p.scratch, err = l.templates[k].casWire(p.scratch, l.revisions[k].Load())
				if err != nil {
					return fail(err)
				}
				p.request = p.scratch
			}
			id := l.next
			l.next += 2
			pending[id] = p
			if l.needHeaders {
				if err := l.prepareHeaders(); err != nil {
					return fail(err)
				}
				l.needHeaders = false
			}
			h := l.headers
			if l.first {
				h = l.firstHeaders
				l.first = false
			}
			if err := l.f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: h, EndHeaders: true}); err != nil {
				return fail(err)
			}
		}
		for id, p := range pending {
			for p.sent < len(p.request) && l.window > 0 && p.window > 0 {
				n := min(int64(len(p.request)-p.sent), l.window, p.window, int64(l.maxFrame))
				end := p.sent + int(n)
				if err := l.f.WriteData(id, end == len(p.request), p.request[p.sent:end]); err != nil {
					return fail(err)
				}
				p.sent = end
				l.window -= n
				p.window -= n
			}
		}
		if err := l.w.Flush(); err != nil {
			return fail(err)
		}
		if len(pending) == 0 && ((phase.warm && warmSent) || (!phase.warm && phase.exhausted())) {
			return r
		}
		until := lastProgress.Add(leanTimeout)
		for _, p := range pending {
			if d := p.start.Add(leanTimeout); d.Before(until) {
				until = d
			}
		}
		if len(pending) == 0 && !phase.deadline.IsZero() && phase.deadline.Before(until) {
			until = phase.deadline
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Until(until), 0))
		select {
		case <-l.stop:
			return fail(errors.New("lean client closed"))
		case <-timer.C:
			if len(pending) == 0 && phase.exhausted() {
				continue
			}
			return fail(errors.New("lean client stalled or RPC exceeded 10s"))
		case ev := <-l.events:
			if err := l.handleEvent(ev, pending, &free, &r); err != nil {
				return fail(err)
			}
			lastProgress = time.Now()
		}
	}
}

func (l *leanClient) handleEvent(ev leanEvent, pending map[uint32]*leanPending, free *[]*leanPending, r *leanResult) error {
	if ev.err != nil {
		return ev.err
	}
	switch {
	case ev.ack:
		if l.settingsAck {
			return errors.New("unexpected SETTINGS acknowledgement")
		}
		l.settingsAck = true
	case ev.settings != nil:
		for _, s := range ev.settings {
			switch s.ID {
			case http2.SettingInitialWindowSize:
				delta := int64(s.Val) - l.initial
				l.initial = int64(s.Val)
				for _, p := range pending {
					p.window += delta
					if p.window > math.MaxInt32 {
						return errors.New("stream window overflow")
					}
				}
			case http2.SettingMaxConcurrentStreams:
				l.maxConcurrent = s.Val
			case http2.SettingMaxFrameSize:
				l.maxFrame = s.Val
			case http2.SettingHeaderTableSize:
				l.encoder.SetMaxDynamicTableSizeLimit(s.Val)
				l.encoder.SetMaxDynamicTableSize(s.Val)
				l.needHeaders = true
			}
		}
		l.settingsSeen = true
		return l.f.WriteSettingsAck()
	case ev.ping != nil:
		return l.f.WritePing(true, *ev.ping)
	case ev.delta != 0:
		if ev.id == 0 {
			l.window += ev.delta
			if l.window > math.MaxInt32 {
				return errors.New("connection window overflow")
			}
		} else if ev.id%2 == 0 || ev.id >= l.next {
			return errors.New("WINDOW_UPDATE for an idle/invalid stream")
		} else if p := pending[ev.id]; p != nil {
			p.window += ev.delta
			if p.window > math.MaxInt32 {
				return errors.New("stream window overflow")
			}
		} // WINDOW_UPDATE on a just-closed stream is legal and is ignored.
	default:
		p := pending[ev.id]
		if p == nil {
			return fmt.Errorf("unknown response stream %d", ev.id)
		}
		if ev.headers != nil {
			if err := p.responseHeaders(ev.headers, ev.end); err != nil {
				return err
			}
		}
		if ev.data != nil || ev.flow != 0 {
			if !p.gotHeaders || p.gotStatus {
				return errors.New("DATA outside response body")
			}
			if len(p.body)+len(ev.data) > leanMaxResponse {
				return errors.New("oversized response")
			}
			p.body = append(p.body, ev.data...)
			// Match pre-47's coalesced connection credit, including padding.
			// Connection debt survives warm/measurement phases; stream debt is
			// reset on slot reuse. Small unary replies need no stream updates.
			l.received += ev.flow
			p.received += ev.flow
			if l.received >= leanReceiveCredit {
				if err := l.f.WriteWindowUpdate(0, l.received); err != nil {
					return err
				}
				l.received = 0
			}
			// Large replies still need receive credit on their open stream,
			// well before exhausting the advertised 1MiB stream window.
			if p.received >= leanReceiveCredit && !ev.end {
				if err := l.f.WriteWindowUpdate(ev.id, p.received); err != nil {
					return err
				}
				p.received = 0
			}
		}
		if ev.end {
			if !p.gotHeaders || !p.gotStatus || p.code != "0" {
				return fmt.Errorf("missing/successless trailers: grpc-status %q", p.code)
			}
			h, passed, revision, err := leanResponse(l.op, p.body, l.keys[p.key], l.values[p.key])
			if err != nil {
				return err
			}
			if l.op == "cas" {
				storeRevision(&l.revisions[p.key], revision)
				if passed {
					r.passes++
				} else {
					r.failures++
				}
			}
			r.latencies = append(r.latencies, time.Since(p.start).Nanoseconds())
			r.terms[h.RaftTerm]++
			*free = append(*free, p)
			delete(pending, ev.id)
		}
	}
	return nil
}
