package server

import (
	"errors"
	"net"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/djosh34/tigerstore/internal/loop"
)

// rawFrame encodes one frame as a client sends it.
func rawFrame(typ http2.FrameType, flags http2.Flags, id uint32, payload ...byte) []byte {
	return append(frameHeader(nil, len(payload), typ, flags, id), payload...)
}

// framesConn is a connection past its preface and SETTINGS, with an
// open stream 1 that waits for its request.
func framesConn(t *testing.T) *conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	br := loop.NewBridge(a, ioBuffer)
	br.Attach(&gate{}, nil, nil)
	s := bareServer(t)
	c := newConn(s, br, br)
	c.phase = phaseFrames
	c.maxID = 1
	c.streams[1] = &stream{c: c, id: 1, rpc: methods["/etcdserverpb.KV/Range"]}
	return c
}

// runFrames hands the connection the bytes in, as the loop does after a
// read, and returns how processing them ended.
func runFrames(c *conn, in ...[]byte) error {
	c.in = c.s.bufs.Get()
	c.inR, c.inW = 0, 0
	for _, b := range in {
		c.inW += copy(c.in[c.inW:], b)
	}
	return c.process()
}

// TestFrameRules sends each frame that breaks one of RFC 9113's rules for
// frames, and checks that it ends the connection or only its stream, with
// the code the RFC asks for.
func TestFrameRules(t *testing.T) {
	const (
		data   = http2.FrameData
		hdrs   = http2.FrameHeaders
		cont   = http2.FrameContinuation
		ping   = http2.FramePing
		update = http2.FrameWindowUpdate
	)
	endHeaders := http2.FlagHeadersEndHeaders
	// A header block of one indexed field, :method GET, that starts but
	// does not end in its HEADERS frame.
	openBlock := rawFrame(hdrs, 0, 3, 0x82)
	for _, tc := range []struct {
		name   string
		in     [][]byte
		conn   http2.ErrCode // the connection's error, if it ends
		stream http2.ErrCode // the code of an RST_STREAM for stream 1, if it ends
	}{
		{"data on stream 0", [][]byte{rawFrame(data, 0, 0, 1)}, http2.ErrCodeProtocol, 0},
		{"data without its pad length", [][]byte{rawFrame(data, http2.FlagDataPadded, 1)}, http2.ErrCodeFrameSize, 0},
		{"data padded past its payload", [][]byte{rawFrame(data, http2.FlagDataPadded, 1, 2, 0)}, http2.ErrCodeProtocol, 0},
		{"headers on stream 0", [][]byte{rawFrame(hdrs, endHeaders, 0, 0x82)}, http2.ErrCodeProtocol, 0},
		{"headers without its pad length", [][]byte{rawFrame(hdrs, endHeaders|http2.FlagHeadersPadded, 3)}, http2.ErrCodeFrameSize, 0},
		{"headers padded past its payload", [][]byte{rawFrame(hdrs, endHeaders|http2.FlagHeadersPadded, 3, 1)}, http2.ErrCodeProtocol, 0},
		{"headers with a short priority", [][]byte{rawFrame(hdrs, endHeaders|http2.FlagHeadersPriority, 3, 0, 0, 0, 1)}, http2.ErrCodeFrameSize, 0},
		{"priority on stream 0", [][]byte{rawFrame(http2.FramePriority, 0, 0, 0, 0, 0, 1, 16)}, http2.ErrCodeProtocol, 0},
		{"priority of 4 bytes", [][]byte{rawFrame(http2.FramePriority, 0, 1, 0, 0, 0, 3)}, http2.ErrCodeFrameSize, 0},
		{"rst_stream of 3 bytes", [][]byte{rawFrame(http2.FrameRSTStream, 0, 1, 0, 0, 8)}, http2.ErrCodeFrameSize, 0},
		{"rst_stream on stream 0", [][]byte{rawFrame(http2.FrameRSTStream, 0, 0, 0, 0, 0, 8)}, http2.ErrCodeProtocol, 0},
		{"settings on a stream", [][]byte{rawFrame(http2.FrameSettings, 0, 1)}, http2.ErrCodeProtocol, 0},
		{"settings of 5 bytes", [][]byte{rawFrame(http2.FrameSettings, 0, 0, 0, 4, 0, 0, 1)}, http2.ErrCodeFrameSize, 0},
		{"settings ack with a payload", [][]byte{rawFrame(http2.FrameSettings, http2.FlagSettingsAck, 0, 0, 4, 0, 0, 0, 1)}, http2.ErrCodeFrameSize, 0},
		{"settings with a window past 2^31-1", [][]byte{rawFrame(http2.FrameSettings, 0, 0, 0, 4, 0x80, 0, 0, 0)}, http2.ErrCodeFlowControl, 0},
		{"ping of 7 bytes", [][]byte{rawFrame(ping, 0, 0, 1, 2, 3, 4, 5, 6, 7)}, http2.ErrCodeFrameSize, 0},
		{"ping on a stream", [][]byte{rawFrame(ping, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8)}, http2.ErrCodeProtocol, 0},
		{"goaway on a stream", [][]byte{rawFrame(http2.FrameGoAway, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeProtocol, 0},
		{"goaway of 7 bytes", [][]byte{rawFrame(http2.FrameGoAway, 0, 0, 0, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeFrameSize, 0},
		{"window_update of 3 bytes", [][]byte{rawFrame(update, 0, 0, 0, 0, 1)}, http2.ErrCodeFrameSize, 0},
		{"window_update of 0 on the connection", [][]byte{rawFrame(update, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeProtocol, 0},
		{"window_update of 0 on a stream", [][]byte{rawFrame(update, 0, 1, 0, 0, 0, 0)}, 0, http2.ErrCodeProtocol},
		{"push_promise", [][]byte{rawFrame(http2.FramePushPromise, endHeaders, 1, 0, 0, 0, 2)}, http2.ErrCodeProtocol, 0},
		{"continuation with no header block open", [][]byte{rawFrame(cont, endHeaders, 1, 0x82)}, http2.ErrCodeProtocol, 0},
		{"continuation of another stream", [][]byte{openBlock, rawFrame(cont, endHeaders, 5, 0x84)}, http2.ErrCodeProtocol, 0},
		{"ping inside a header block", [][]byte{openBlock, rawFrame(ping, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8)}, http2.ErrCodeProtocol, 0},
		{"unknown frame inside a header block", [][]byte{openBlock, rawFrame(0x20, 0, 0)}, http2.ErrCodeProtocol, 0},
		{"unknown frame", [][]byte{rawFrame(0x20, 0xff, 1, 1, 2, 3)}, 0, 0},
		{"priority", [][]byte{rawFrame(http2.FramePriority, 0, 1, 0, 0, 0, 3, 16)}, 0, 0},
		{"padded data", [][]byte{rawFrame(data, http2.FlagDataPadded, 1, 1, 0, 0)}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := framesConn(t)
			err := runFrames(c, tc.in...)
			ce, isCE := errors.AsType[http2.ConnectionError](err)
			switch {
			case tc.conn != 0 && (!isCE || http2.ErrCode(ce) != tc.conn):
				t.Fatalf("error %v, want connection error %v", err, tc.conn)
			case tc.conn == 0 && err != nil:
				t.Fatalf("error %v, want none", err)
			}
			var rst http2.ErrCode
			for _, f := range c.ctrl {
				if f.kind == ctrlRST && f.id == 1 {
					rst = http2.ErrCode(f.val)
				}
			}
			if rst != tc.stream {
				t.Fatalf("stream 1 reset with %v, want %v", rst, tc.stream)
			}
		})
	}
}

// TestFramesReadInPlace runs a header block in a HEADERS frame and its
// CONTINUATION, and a padded DATA frame, through the connection's input:
// the call is decoded from the bytes as they arrived.
func TestFramesReadInPlace(t *testing.T) {
	c := framesConn(t)
	block := headerBlock(
		hpackField(":method", "POST"), hpackField(":scheme", "https"),
		hpackField(":path", "/etcdserverpb.KV/Range"), hpackField(":authority", "x"),
		hpackField("content-type", "application/grpc"))
	// A message of 3 bytes: field 1, the key "a".
	msg := []byte{0, 0, 0, 0, 3, 0x0a, 1, 'a'}
	padded := append(append([]byte{4}, msg...), 0, 0, 0, 0)
	err := runFrames(c,
		rawFrame(http2.FrameHeaders, http2.FlagHeadersPadded|http2.FlagHeadersPriority, 3,
			append(append([]byte{2, 0, 0, 0, 0, 16}, block[:10]...), 0, 0)...),
		rawFrame(http2.FrameContinuation, http2.FlagHeadersEndHeaders, 3, block[10:]...),
		rawFrame(http2.FrameData, http2.FlagDataPadded|http2.FlagDataEndStream, 3, padded...))
	if err != nil {
		t.Fatal(err)
	}
	st := c.streams[3]
	if st == nil || st.rpc != methods["/etcdserverpb.KV/Range"] || !st.inEOF {
		t.Fatalf("stream 3: %+v", st)
	}
	if len(c.batch.reqs) != 1 {
		t.Fatalf("%d calls in the batch, want 1", len(c.batch.reqs))
	}
	if k := string(c.batch.reqs[0].Op.(interface{ GetKey() []byte }).GetKey()); k != "a" {
		t.Fatalf("key %q, want a", k)
	}
	if n := c.frames.Load(); n != 3 {
		t.Fatalf("%d frames counted, want 3", n)
	}
}

func hpackField(name, value string) hpack.HeaderField {
	return hpack.HeaderField{Name: name, Value: value}
}
