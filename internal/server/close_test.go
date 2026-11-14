package server

import (
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/loop"
)

// TestStreamAfterClose decodes a call's headers after its connection
// closed: the stream must not join the connection or arm its deadline.
func TestStreamAfterClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	br := loop.NewBridge(a, ioBuffer)
	c := newConn(bareServer(t), br, br)
	c.close()
	st := c.newStream(&reqHeaders{
		id: 1, method: "POST", path: "/etcdserverpb.KV/Put", contentType: "application/grpc",
		timeout: time.Hour, hasTimeout: true,
	})
	if !st.reset || st.due != 0 || len(c.deadlines) != 0 || len(c.streams) != 0 {
		t.Fatalf("stream after close: reset %v, deadline armed %v, %d streams", st.reset, st.due != 0, len(c.streams))
	}
}
