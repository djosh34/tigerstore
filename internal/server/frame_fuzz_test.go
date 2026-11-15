package server

import (
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"testing"

	"golang.org/x/net/http2"

	"github.com/djosh34/tigerstore/internal/loop"
)

// chunkSocket hands out its bytes chunk at a time, then ErrWouldBlock, as
// a socket the client wrote them to in pieces.
type chunkSocket struct {
	data  []byte
	chunk int
}

func (s *chunkSocket) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, loop.ErrWouldBlock
	}
	n := copy(p[:min(len(p), s.chunk)], s.data)
	s.data = s.data[n:]
	return n, nil
}

func (s *chunkSocket) Write(p []byte) (int, error) { return len(p), nil }
func (s *chunkSocket) Close() error                { return nil }

// FuzzFrames feeds the connection's read path (readFrames, parseFrame and
// the handlers) what a client could send after its preface and SETTINGS:
// raw bytes, then one frame built from any type, flags, stream and
// declared length, with a payload that may be shorter or longer than that
// length, all arriving in pieces of any size. The connection refuses new
// streams, so that no handler or node runs, and has one call open, stream
// 1, so DATA reaches a call. Reading must only ever end with nothing or an
// HTTP/2 connection error, never panic or read past the input, and keep
// its input within its buffer.
func FuzzFrames(f *testing.F) {
	hdrs, data, cont := byte(http2.FrameHeaders), byte(http2.FrameData), byte(http2.FrameContinuation)
	settings, update, ping := byte(http2.FrameSettings), byte(http2.FrameWindowUpdate), byte(http2.FramePing)
	u32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	setting := func(id uint16, v uint32) []byte { return append(binary.BigEndian.AppendUint16(nil, id), u32(v)...) }
	// A call's request on stream 1: a gRPC message with the key "a".
	msg := []byte{0, 0, 0, 0, 3, 0x0a, 1, 'a'}
	for _, seed := range []struct {
		raw          []byte
		typ, flags   byte
		id, length   uint32
		payload      []byte
		chunk        uint16
		truncateBody bool
	}{
		{nil, data, byte(http2.FlagDataEndStream), 1, 8, msg, 3, false},
		{nil, data, byte(http2.FlagDataPadded | http2.FlagDataEndStream), 1, 11, append(append([]byte{2}, msg...), 0, 0), 1, false},
		{nil, data, byte(http2.FlagDataPadded), 1, 1, []byte{9}, 100, false},
		{nil, data, 0, 1, 1 << 20, msg, 100, false},
		{nil, data, 0, 1, 100, msg, 7, true},
		{nil, hdrs, 0, 3, 1, []byte{0x82}, 9, false},
		{rawFrame(http2.FrameHeaders, 0, 3, 0x82), cont, byte(http2.FlagHeadersEndHeaders), 3, 1, []byte{0x84}, 4, false},
		{rawFrame(http2.FrameHeaders, 0, 3, 0x82), cont, byte(http2.FlagHeadersEndHeaders), 5, 1, []byte{0x84}, 4, false},
		{rawFrame(http2.FrameHeaders, 0, 3, 0x82), ping, 0, 0, 8, make([]byte, 8), 4, false},
		{nil, cont, byte(http2.FlagHeadersEndHeaders), 1, 1, []byte{0x82}, 9, false},
		{nil, hdrs, byte(http2.FlagHeadersEndHeaders | http2.FlagHeadersPadded | http2.FlagHeadersPriority), 3, 7, []byte{1, 0, 0, 0, 3, 16, 0x82}, 2, false},
		{nil, settings, 0, 0, 18, append(append(setting(4, 1<<31), setting(5, 1<<24)...), setting(3, 0)...), 5, false},
		{nil, settings, 0, 0, 12, append(setting(4, 1<<31-1), setting(4, 0)...), 5, false},
		{nil, settings, byte(http2.FlagSettingsAck), 0, 0, nil, 9, false},
		{nil, update, 0, 0, 4, u32(1<<31 - 1), 9, false},
		{rawFrame(http2.FrameWindowUpdate, 0, 0, u32(1<<31-1)...), update, 0, 0, 4, u32(1<<31 - 1), 9, false},
		{nil, update, 0, 1, 4, u32(0), 9, false},
		{nil, update, 0, 1, 4, u32(1 << 31), 9, false},
		{nil, byte(http2.FrameRSTStream), 0, 1, 4, u32(8), 9, false},
		{nil, byte(http2.FrameGoAway), 0, 0, 8, make([]byte, 8), 9, false},
		{nil, 0x20, 0xff, 7, 3, []byte{1, 2, 3}, 9, false},
		{nil, byte(http2.FramePushPromise), 0, 1, 4, u32(2), 9, false},
		{[]byte{0, 0x40, 1}, data, 0, 1, 0, nil, 1, false},
	} {
		f.Add(seed.raw, seed.typ, seed.flags, seed.id, seed.length, seed.payload, seed.chunk, seed.truncateBody)
	}
	f.Fuzz(func(t *testing.T, raw []byte, typ, flags byte, id, length uint32, payload []byte, chunk uint16, truncateBody bool) {
		length &= 1<<24 - 1
		in := append(slices.Clone(raw), frameHeader(nil, int(length), http2.FrameType(typ), http2.Flags(flags), id)...)
		if truncateBody && len(payload) > 0 {
			payload = payload[:len(payload)/2]
		}
		in = append(in, payload...)

		a, b := net.Pipe()
		defer a.Close() //nolint:errcheck // closing a test pipe
		defer b.Close() //nolint:errcheck // closing a test pipe
		s := bareServer(t)
		br := loop.NewBridge(a, ioBuffer)
		br.Attach(&chunkSocket{data: in, chunk: max(1, int(chunk))}, s.bufs, s.bigBufs)
		c := newConn(s, br, br)
		c.phase = phaseFrames
		c.goingAway = true
		c.maxID = 1
		c.streams[1] = &stream{c: c, id: 1, rpc: methods["/etcdserverpb.KV/Range"]}
		err := c.readFrames()
		if _, ok := errors.AsType[http2.ConnectionError](err); err != nil && !ok {
			t.Fatalf("reading frames ended with %v, not an HTTP/2 connection error", err)
		}
		if c.inR < 0 || c.inR > c.inW || (c.in != nil && c.inW > len(c.in)) {
			t.Fatalf("input from %d to %d of a buffer of %d", c.inR, c.inW, len(c.in))
		}
	})
}
