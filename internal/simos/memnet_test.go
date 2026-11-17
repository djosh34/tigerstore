package simos

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// memNet is a network in memory with no delays or faults: enough to run
// a cluster on the seam, as the simulator's network does with them.
type memNet struct {
	mu  sync.Mutex
	lns map[string]*memListener
	// cut are the hosts cut off (isolate): none dials them, they dial
	// none, and their connections are closed.
	cut   map[string]bool
	conns []hostConn
}

// hostConn is a connection's two ends and the hosts they are on, "" for
// a client of the test.
type hostConn struct {
	from, to string
	a, b     *memConn
}

// host is the host of address addr.
func host(addr string) string {
	h, _, _ := strings.Cut(addr, ":")
	return h
}

func newMemNet() *memNet { return &memNet{lns: map[string]*memListener{}} }

func (nw *memNet) listen(addr string) *memListener {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	l := &memListener{nw: nw, addr: addr, conns: make(chan net.Conn, 64), done: make(chan struct{})}
	nw.lns[addr] = l
	return l
}

func (nw *memNet) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return nw.dial(ctx, "", addr)
}

func (nw *memNet) dial(ctx context.Context, from, addr string) (net.Conn, error) {
	nw.mu.Lock()
	l := nw.lns[addr]
	cut := nw.cut[from] || nw.cut[host(addr)]
	nw.mu.Unlock()
	if l == nil || cut {
		return nil, unix.ECONNREFUSED
	}
	mu := &sync.Mutex{}
	ab, ba := &half{}, &half{}
	a := &memConn{mu: mu, in: ba, out: ab, local: "client", remote: addr}
	b := &memConn{mu: mu, in: ab, out: ba, local: addr, remote: "client"}
	a.peer, b.peer = b, a
	nw.mu.Lock()
	nw.conns = append(nw.conns, hostConn{from: from, to: host(addr), a: a, b: b})
	nw.mu.Unlock()
	select {
	case l.conns <- b:
		return a, nil
	case <-l.done:
		return nil, unix.ECONNREFUSED
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type memListener struct {
	nw    *memNet
	addr  string
	conns chan net.Conn
	once  sync.Once
	done  chan struct{}
}

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.nw.mu.Lock()
		delete(l.nw.lns, l.addr)
		l.nw.mu.Unlock()
	})
	return nil
}

func (l *memListener) Addr() net.Addr { return memAddr(l.addr) }

type memAddr string

func (a memAddr) Network() string { return "mem" }
func (a memAddr) String() string  { return string(a) }

// half is one direction of a connection.
type half struct {
	buf    []byte
	closed bool
	wake   chan struct{} // closed when bytes come or the writer closes
	// total is every byte written to it, and last when the last came.
	total int64
	last  time.Time
}

type memConn struct {
	mu            *sync.Mutex // the connection's, both ends
	in, out       *half
	peer          *memConn
	closed        bool
	notify        func()
	deadline      time.Time
	local, remote string
}

func (c *memConn) TryRead(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return 0, net.ErrClosed
	case len(c.in.buf) > 0:
		k := copy(p, c.in.buf)
		c.in.buf = c.in.buf[k:]
		return k, nil
	case c.in.closed:
		return 0, io.EOF
	}
	return 0, unix.EAGAIN
}

func (c *memConn) Read(p []byte) (int, error) {
	for {
		k, err := c.TryRead(p)
		if !errors.Is(err, unix.EAGAIN) {
			return k, err
		}
		c.mu.Lock()
		if c.in.wake == nil {
			c.in.wake = make(chan struct{})
		}
		wake, deadline := c.in.wake, c.deadline
		c.mu.Unlock()
		if deadline.IsZero() {
			<-wake
			continue
		}
		t := time.NewTimer(time.Until(deadline))
		select {
		case <-wake:
			t.Stop()
		case <-t.C:
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (c *memConn) TryWrite(p []byte) (int, error) {
	c.mu.Lock()
	if c.closed || c.out.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if c.peer.closed {
		c.mu.Unlock()
		return 0, unix.EPIPE
	}
	c.out.buf = append(c.out.buf, p...)
	c.out.total, c.out.last = c.out.total+int64(len(p)), time.Now()
	wake, notify := c.out.wake, c.peer.notify
	c.out.wake = nil
	c.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	if notify != nil {
		notify()
	}
	return len(p), nil
}

func (c *memConn) Write(p []byte) (int, error) { return c.TryWrite(p) }

func (c *memConn) Ready() (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.in.buf) > 0 || c.in.closed || c.closed, true
}

func (c *memConn) OnChange(f func()) {
	c.mu.Lock()
	c.notify = f
	c.mu.Unlock()
}

func (c *memConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.closed, c.out.closed = true, true
	wake, notify := c.out.wake, c.peer.notify
	c.out.wake = nil
	c.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	if notify != nil {
		notify()
	}
	return nil
}

func (c *memConn) LocalAddr() net.Addr  { return memAddr(c.local) }
func (c *memConn) RemoteAddr() net.Addr { return memAddr(c.remote) }

func (c *memConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	wake := c.in.wake
	c.in.wake = nil
	c.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	return nil
}

func (c *memConn) SetReadDeadline(t time.Time) error { return c.SetDeadline(t) }
func (c *memConn) SetWriteDeadline(time.Time) error  { return nil }
