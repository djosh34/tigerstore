package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

type senderSnapshot struct {
	Planned            uint64 `json:"planned_puts"`
	Attempted          uint64 `json:"attempted_puts"`
	Headers            uint64 `json:"sent_headers"`
	Data               uint64 `json:"completed_data"`
	DataBytes          uint64 `json:"completed_data_bytes"`
	MessageBytes       uint64 `json:"message_bytes"`
	Finished           bool   `json:"finished"`
	AllDataSent        bool   `json:"all_data_sent"`
	IntentionalClose   bool   `json:"intentional_close"`
	Error              string `json:"error"`
	ErrorBeforeClose   bool   `json:"error_before_close"`
	ExpectedCloseError bool   `json:"expected_close_error"`
	Stage              string `json:"stage"`
}

type slowSender struct {
	conn                                         net.Conn
	fr                                           *http2.Framer
	header, data                                 []byte
	planned                                      uint64
	attempted, headers, completedData, dataBytes atomic.Uint64
	closing                                      atomic.Bool
	done                                         chan struct{}
	// Published once before close(done); readers access only after that fence.
	err         error
	beforeClose bool
	stage       string
}

func (s *slowSender) start(timeout time.Duration) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		for i := range s.planned {
			if s.closing.Load() {
				s.err = net.ErrClosed
				return
			}
			s.attempted.Add(1)
			id := uint32(1 + 2*i)
			s.stage = "headers"
			if err := s.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: s.header, EndHeaders: true}); err != nil {
				s.err, s.beforeClose = err, !s.closing.Load()
				return
			}
			s.headers.Add(1)
			s.stage = "data"
			if err := s.fr.WriteData(id, true, s.data); err != nil {
				s.err, s.beforeClose = err, !s.closing.Load()
				return
			}
			s.completedData.Add(1)
			s.dataBytes.Add(uint64(len(s.data)))
		}
		s.stage = "complete"
	}()
	return nil
}

func (s *slowSender) snapshot() senderSnapshot {
	j := senderSnapshot{
		Planned: s.planned, Attempted: s.attempted.Load(), Headers: s.headers.Load(),
		Data: s.completedData.Load(), DataBytes: s.dataBytes.Load(), MessageBytes: uint64(len(s.data)), IntentionalClose: s.closing.Load(),
	}
	select {
	case <-s.done:
		j.Finished, j.Stage = true, s.stage
		j.ErrorBeforeClose = s.beforeClose
		if s.err != nil {
			j.Error = s.err.Error()
			j.ExpectedCloseError = !s.beforeClose && s.closing.Load() && (errors.Is(s.err, net.ErrClosed) || errors.Is(s.err, io.ErrClosedPipe))
		}
	default:
		j.Stage = "sending"
	}
	j.AllDataSent = j.Finished && j.Data == j.Planned && j.Error == ""
	return j
}

func (s *slowSender) check() error {
	select {
	case <-s.done:
		if s.err != nil {
			return fmt.Errorf("raw sender failed before intentional close: %w", s.err)
		}
	default:
	}
	return nil
}

func (s *slowSender) closeAndWait(timeout time.Duration) (senderSnapshot, error) {
	s.closing.Store(true)
	closeErr := s.conn.Close()
	if errors.Is(closeErr, net.ErrClosed) || errors.Is(closeErr, io.ErrClosedPipe) {
		closeErr = nil
	}
	select {
	case <-s.done:
		j := s.snapshot()
		if j.Error != "" && !j.ExpectedCloseError {
			return j, errors.Join(closeErr, fmt.Errorf("raw sender error: %w", s.err))
		}
		return j, closeErr
	case <-time.After(timeout):
		return s.snapshot(), errors.Join(closeErr, errors.New("raw sender did not terminate after connection close"))
	}
}

func validateSender(j senderSnapshot, committed int64, managed bool) error {
	if !j.Finished || j.Planned == 0 || j.Attempted > j.Planned || j.Headers > j.Attempted || j.Data > j.Headers ||
		j.Attempted-j.Headers > 1 || j.Headers-j.Data > 1 || j.DataBytes != j.Data*j.MessageBytes {
		return fmt.Errorf("inconsistent raw sender counts: %+v", j)
	}
	if committed < 1 || uint64(committed) > j.Planned || uint64(committed) > j.Data+1 {
		return fmt.Errorf("committed=%d exceeds complete DATA plus last in-progress frame: %+v", committed, j)
	}
	if j.ErrorBeforeClose || (j.Error != "" && !j.ExpectedCloseError) {
		return fmt.Errorf("unexpected sender error: %+v", j)
	}
	if !managed && (!j.AllDataSent || j.Error != "") {
		return fmt.Errorf("baseline did not send every planned Put: %+v", j)
	}
	if j.Data < j.Planned && !j.ExpectedCloseError {
		return fmt.Errorf("partial sender without intentional close evidence: %+v", j)
	}
	return nil
}

// Only handshake reads occur here. Clear its deadline before launching exactly
// one bounded sender; healthy work must not wait for the raw prefix to finish.
func stalled(endpoint string, cfg *tls.Config, key []byte) (*slowSender, error) {
	tcp, err := net.DialTimeout("tcp", endpoint, 5*time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*slowSender, error) { _ = tcp.Close(); return nil, e }
	if t, ok := tcp.(*net.TCPConn); ok {
		if err := t.SetReadBuffer(4096); err != nil {
			return fail(err)
		}
	}
	rawCFG := cfg.Clone()
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fail(err)
	}
	rawCFG.ServerName = host
	tc := tls.Client(tcp, rawCFG)
	if err := tc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fail(err)
	}
	if err := tc.Handshake(); err != nil {
		return fail(err)
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		return fail(errors.New("missing h2 ALPN"))
	}
	if _, err := io.WriteString(tc, http2.ClientPreface); err != nil {
		return fail(err)
	}
	fr := http2.NewFramer(tc, tc)
	if err := fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}); err != nil {
		return fail(err)
	}
	gotSettings, gotAck := false, false
	credit, streamWindow := 65535, 65535
	for !gotSettings || !gotAck || credit == 65535 {
		frame, err := fr.ReadFrame()
		if err != nil {
			return fail(err)
		}
		if s, ok := frame.(*http2.SettingsFrame); ok {
			if s.IsAck() {
				gotAck = true
			} else {
				gotSettings = true
				if err := s.ForeachSetting(func(setting http2.Setting) error {
					if setting.ID == http2.SettingInitialWindowSize {
						streamWindow = int(setting.Val)
					}
					return nil
				}); err != nil {
					return fail(err)
				}
				if err := fr.WriteSettingsAck(); err != nil {
					return fail(err)
				}
			}
		}
		if w, ok := frame.(*http2.WindowUpdateFrame); ok && w.StreamID == 0 {
			credit += int(w.Increment)
		}
	}
	request, err := proto.Marshal(&pb.PutRequest{Key: key, Value: valueOfSize(1024)})
	if err != nil {
		return fail(err)
	}
	data := make([]byte, 5+len(request))
	binary.BigEndian.PutUint32(data[1:5], uint32(len(request)))
	copy(data[5:], request)
	if len(data) > streamWindow || slowAttempts*len(data) > credit {
		return fail(fmt.Errorf("slow workload exceeds advertised credit: stream=%d connection=%d bytes=%d", streamWindow, credit, slowAttempts*len(data)))
	}
	var header bytes.Buffer
	enc := hpack.NewEncoder(&header)
	enc.SetMaxDynamicTableSize(0)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: endpoint},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		if err := enc.WriteField(field); err != nil {
			return fail(err)
		}
	}
	if err := tc.SetDeadline(time.Time{}); err != nil {
		return fail(err)
	}
	// Close the underlying TCP socket, not TLS Close (which can block while
	// sending close_notify). Framer I/O still runs through the TLS connection.
	s := &slowSender{conn: tcp, fr: fr, header: header.Bytes(), data: data, planned: slowAttempts}
	if err := s.start(20 * time.Second); err != nil {
		return fail(err)
	}
	return s, nil
}
