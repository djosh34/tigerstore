//go:build linux

package epoll_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/epoll"
	"github.com/djosh34/tigerstore/internal/loop"
)

// TestSocketOverTCP attaches one end of a TCP connection and moves more
// bytes through it, both ways, than the kernel's buffers hold, so that
// both a read and a write would block and an edge-triggered event must
// bring the loop back each time.
func TestSocketOverTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // closing a test listener
	other, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close() //nolint:errcheck // closing a test connection
	mine, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	p, err := epoll.New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close() //nolint:errcheck // closing a test poller
	sock, err := p.Attach(mine, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close() //nolint:errcheck // closing a test socket

	const size = 16 << 20
	out := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	// The other end echoes what it reads, then closes its side.
	go func() {
		// Plain reads and writes: TCPConn's ReadFrom would splice the
		// socket into itself.
		_, _ = io.CopyBuffer(struct{ io.Writer }{other}, io.LimitReader(other, size), make([]byte, 64<<10))
		_ = other.(*net.TCPConn).CloseWrite()
	}()
	var got []byte
	buf := make([]byte, 256<<10)
	sent, blocked := 0, 0
	deadline := time.Now().Add(time.Minute)
	for len(got) < size {
		if time.Now().After(deadline) {
			t.Fatalf("sent %d, got %d bytes in a minute", sent, len(got))
		}
		for sent < size {
			n, err := sock.Write(out[sent:])
			sent += n
			if errors.Is(err, loop.ErrWouldBlock) {
				blocked++
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		for len(got) < size {
			n, err := sock.Read(buf)
			got = append(got, buf[:n]...)
			if errors.Is(err, loop.ErrWouldBlock) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if len(got) < size {
			if _, err := p.Wait(nil, time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !bytes.Equal(got, out) {
		t.Fatal("the bytes came back changed")
	}
	if blocked == 0 {
		t.Fatal("no write blocked; the test moved too little")
	}
	// The other end closes after the echo: the read ends with EOF.
	for time.Now().Before(deadline) {
		_, err := sock.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if !errors.Is(err, loop.ErrWouldBlock) {
			t.Fatal(err)
		}
		if _, err := p.Wait(nil, time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWakeFromAnotherGoroutine checks that a Wake ends a Wait that
// blocks in the kernel, and that one before the Wait is not lost.
func TestWakeFromAnotherGoroutine(t *testing.T) {
	p, err := epoll.New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close() //nolint:errcheck // closing a test poller
	p.Wake()
	start := time.Now()
	if _, err := p.Wait(nil, time.Minute); err != nil || time.Since(start) > 10*time.Second {
		t.Fatalf("Wait after a Wake took %v: %v", time.Since(start), err)
	}
	woke := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Wake()
		close(woke)
	}()
	start = time.Now()
	// A Wait may also end early, as on EINTR when the runtime preempts
	// the thread; it waits again until the Wake came.
	for done := false; !done; {
		if _, err := p.Wait(nil, time.Minute); err != nil || time.Since(start) > 10*time.Second {
			t.Fatalf("Wait woken from another goroutine took %v: %v", time.Since(start), err)
		}
		select {
		case <-woke:
			done = true
		default:
		}
	}
	// A Wake that came after a Wait ended is kept for the next one,
	// which this takes.
	if _, err := p.Wait(nil, 0); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if _, err := p.Wait(nil, 30*time.Millisecond); err != nil || time.Since(start) < 30*time.Millisecond {
		t.Fatalf("an idle Wait of 30 ms took %v: %v", time.Since(start), err)
	}
}
