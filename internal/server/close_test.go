package server

import (
	"net"
	"testing"
	"time"
)

// TestStreamAfterClose decodes a call's headers after its connection
// closed: the stream must not join the connection or arm its deadline.
func TestStreamAfterClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	c := newConn(bareServer(t), a, a)
	c.close()
	st := c.newStream(&reqHeaders{
		id: 1, method: "POST", path: "/etcdserverpb.KV/Put", contentType: "application/grpc",
		timeout: time.Hour, hasTimeout: true,
	})
	if !st.reset || st.deadline != nil || len(c.streams) != 0 {
		t.Fatalf("stream after close: reset %v, timer %v, %d streams", st.reset, st.deadline != nil, len(c.streams))
	}
}
