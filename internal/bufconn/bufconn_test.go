package bufconn

import (
	"io"
	"net"
	"testing"
	"time"
)

// TestBufferHoldsWritesUntilFlush checks that writes go straight out
// before Buffer and wait for Flush after it, and that reads come through.
func TestBufferHoldsWritesUntilFlush(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	c := New(a, 1024)
	got := make(chan string, 4)
	go func() {
		buf := make([]byte, 64)
		for {
			n, err := b.Read(buf)
			if err != nil {
				close(got)
				return
			}
			got <- string(buf[:n])
		}
	}()
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if s := <-got; s != "hello" {
		t.Fatalf("before Buffer: read %q", s)
	}
	c.Buffer()
	if _, err := c.Write([]byte("held")); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		t.Fatalf("a buffered write went out before Flush: %q", s)
	case <-time.After(20 * time.Millisecond):
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if s := <-got; s != "held" {
		t.Fatalf("after Flush: read %q", s)
	}
	go func() { _, _ = b.Write([]byte("back")) }()
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "back" {
		t.Fatalf("read %q, %v", buf, err)
	}
	if Under(c) != nil {
		t.Fatal("a Conn that is not under TLS was found under TLS")
	}
}
