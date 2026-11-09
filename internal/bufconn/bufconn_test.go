package bufconn

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
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

// slowConn is a connection whose writes wait out a deadline and fail, as
// a write to a peer that reads nothing does.
type slowConn struct{ net.Conn }

func (slowConn) Write([]byte) (int, error) {
	<-time.After(5 * time.Second)
	return 0, os.ErrDeadlineExceeded
}

// TestWriteBehindFlushIsDurable is #567: a flush waits out its write
// deadline while a write, such as TLS's close_notify, waits for the
// connection behind it. Under synctest the bubble's clock moves only
// while every goroutine in it is durably blocked; a wait for a
// sync.Mutex is not, so the deadline never came and the run hung. The
// wait for the connection must be durable: the clock reaches the
// deadline, the flush fails, and the write behind it returns. If it is
// not, the bubble's clock stops and the test hangs until go test's
// -timeout, whose stacks show the write waiting for the connection: no
// goroutine outside the bubble may touch the channels that would free it.
func TestWriteBehindFlushIsDurable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer func() { _ = a.Close(); _ = b.Close() }()
		c := New(slowConn{a}, 1024)
		c.Buffer()
		if _, err := c.Write([]byte("held")); err != nil {
			t.Fatal(err)
		}
		flushed := make(chan error, 1)
		go func() { flushed <- c.Flush() }()
		synctest.Wait()
		wrote := make(chan error, 1)
		go func() {
			_, err := c.Write([]byte("alert"))
			wrote <- err
		}()
		time.Sleep(10 * time.Second)
		if err := <-flushed; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("the held flush = %v, want its deadline", err)
		}
		// It goes through once the flush gives the connection up, with the
		// flush's error, which a bufio.Writer keeps.
		if err := <-wrote; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("the write behind the flush = %v, want the flush's deadline", err)
		}
	})
}
