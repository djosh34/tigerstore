package server

import (
	"net"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// TestStreamAfterClose decodes a call's headers after its connection
// closed: the stream must not join the connection or arm its deadline.
func TestStreamAfterClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	c := newConn(&Server{}, a, a)
	c.close()
	f := &http2.MetaHeadersFrame{
		HeadersFrame: &http2.HeadersFrame{FrameHeader: http2.FrameHeader{StreamID: 1}},
		Fields: []hpack.HeaderField{
			{Name: ":method", Value: "POST"},
			{Name: ":path", Value: "/etcdserverpb.KV/Put"},
			{Name: "content-type", Value: "application/grpc"},
			{Name: "grpc-timeout", Value: "1H"},
		},
	}
	st := c.newStream(1, f)
	if !st.reset || st.deadline != nil || len(c.streams) != 0 {
		t.Fatalf("stream after close: reset %v, timer %v, %d streams", st.reset, st.deadline != nil, len(c.streams))
	}
}
