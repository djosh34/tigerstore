package proxy

import (
	"io"
	"net"
	"os"
	"testing"
)

// TestRouterPicksByDialer dials a Router from this process: the Router
// must find this process as the dialer and pass the connection on through
// the Proxy that pick names.
func TestRouterPicksByDialer(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close() //nolint:errcheck // test listener
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c) }()
		}
	}()
	mine, err := Listen("127.0.0.1:0", up.Addr().String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mine.Close() //nolint:errcheck // test proxy
	found := make(chan error, 1)
	r, err := Route("127.0.0.1:0", up.Addr().String(), func(c *net.TCPConn) string {
		pid, err := Dialer(c, []int{os.Getpid()})
		found <- err
		if err != nil || pid != os.Getpid() {
			return ""
		}
		return mine.Addr()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() //nolint:errcheck // test router
	c, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck // test connection
	if err := <-found; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "ping" {
		t.Fatalf("read %q: %v", b, err)
	}
	if s := mine.Stats(); s.Conns != 1 || s.ForwardBytes != 4 {
		t.Fatalf("the picked proxy relayed %+v", s)
	}
}
