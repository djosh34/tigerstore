package simos

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// memPair is a connection of the network in memory: the client's end and
// the listener's.
func memPair(t *testing.T) (*memConn, *memConn) {
	t.Helper()
	nw := newMemNet()
	l := nw.listen("n1:2379")
	a, err := nw.Dial(context.Background(), "n1:2379")
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return a.(*memConn), b.(*memConn)
}

// A Read that found nothing wakes for what comes while it goes to wait:
// bytes written, the other end's close, and its own end's. Each comes
// here between the Read's look and its wait. Lost, a client's reader
// slept on bytes or a close for good, and its gRPC calls with it: a put
// of the install test waited 2 minutes for a reply the node had sent, or
// on a connection the test had cut (#732).
func TestAMemConnReadWakesForWhatComesAsItGoesToWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		come func(a, b *memConn)
		want func(k int, err error) bool
	}{
		{"bytes", func(_, b *memConn) { _, _ = b.Write([]byte("x")) }, func(k int, err error) bool { return k == 1 && err == nil }},
		{"the other end's close", func(_, b *memConn) { _ = b.Close() }, func(_ int, err error) bool { return errors.Is(err, io.EOF) }},
		{"both ends' close", func(a, b *memConn) { _ = a.Close(); _ = b.Close() }, func(_ int, err error) bool { return errors.Is(err, net.ErrClosed) }},
		{"its own end's close", func(a, _ *memConn) { _ = a.Close() }, func(_ int, err error) bool { return errors.Is(err, net.ErrClosed) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := memPair(t)
			var once sync.Once
			readWaits = func(c *memConn) {
				if c == a {
					once.Do(func() { tc.come(a, b) })
				}
			}
			t.Cleanup(func() { readWaits = nil })
			type result struct {
				k   int
				err error
			}
			done := make(chan result, 1)
			go func() {
				k, err := a.Read(make([]byte, 8))
				done <- result{k, err}
			}()
			select {
			case r := <-done:
				if !tc.want(r.k, r.err) {
					t.Fatalf("the Read returned %d, %v", r.k, r.err)
				}
			case <-time.After(5 * time.Second):
				_ = a.SetDeadline(time.Now()) // lets the Read go
				t.Fatal("the Read slept through it")
			}
		})
	}
}
