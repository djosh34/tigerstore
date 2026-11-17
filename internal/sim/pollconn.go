package sim

import (
	"errors"
	"math"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/simos"
)

// A connection is a simos.Conn: a wl node's loop reads and writes it
// without waiting, and its epoll learns when it may.
var _ simos.Conn = (*conn)(nil)

// changed tells c's watcher that a read or write that would wait may not
// any more. The lock is held.
func (c *conn) changed() {
	if c.notify != nil {
		c.notify()
	}
}

// notifyIn tells c's watcher after wait, when bytes on their way arrive
// or a pause or stall ends; math.MaxInt64 tells it nothing. The lock is
// held.
func (c *conn) notifyIn(wait int64) {
	if c.notify == nil || wait == math.MaxInt64 {
		return
	}
	f := c.notify
	if c.notifyAt != nil {
		c.notifyAt.Stop()
	}
	c.notifyAt = time.AfterFunc(time.Duration(max(wait, 1)), f)
}

func (c *conn) OnChange(f func()) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.notify = f
	if f == nil && c.notifyAt != nil {
		c.notifyAt.Stop()
	}
}

// TryRead reads what has arrived, unix.EAGAIN if nothing has.
func (c *conn) TryRead(b []byte) (int, error) {
	c.s.mu.Lock()
	k, wait, room, err := c.readNow(b)
	if errors.Is(err, errWouldWait) {
		c.notifyIn(wait)
		err = unix.EAGAIN
	}
	c.s.mu.Unlock()
	wakeUp(room)
	return k, err
}

// TryWrite puts on their way the bytes of b that the connection's buffer
// has room for, unix.EAGAIN if it has none, as Write does without
// waiting.
func (c *conn) TryWrite(b []byte) (int, error) {
	s := c.s
	s.mu.Lock()
	switch {
	case c.closed:
		s.mu.Unlock()
		return 0, net.ErrClosed
	case c.in.err != nil:
		err := c.in.err
		s.mu.Unlock()
		return 0, err
	case c.other.closed:
		// The peer answers with RST, as in Write.
		s.net.reset(c)
		s.mu.Unlock()
		return 0, errReset
	case s.link(c.from, c.to).halfOpenUntil > s.now():
		s.mu.Unlock()
		return len(b), nil
	}
	room := sendBuffer - c.out.size
	if room <= 0 {
		s.stats.BlockedWrites++
		s.mu.Unlock()
		return 0, unix.EAGAIN
	}
	k := min(room, len(b))
	reader := c.send(b[:k])
	s.mu.Unlock()
	wakeUp(reader)
	return k, nil
}

// Hup reports whether the other end closed or the connection ended, as
// epoll's EPOLLRDHUP.
func (c *conn) Hup() bool {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return c.closed || c.in.err != nil || c.in.eof
}

// Ready reports whether a read and a write would not wait; when a read
// would until a time, the watcher is told then.
func (c *conn) Ready() (readable, writable bool) {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	in := c.in
	writable = c.closed || in.err != nil || c.other.closed || sendBuffer-c.out.size > 0
	now := s.now()
	switch {
	case c.closed || in.err != nil:
		return true, writable
	case c.p != nil && c.p.n.pausedUntil > now:
		c.notifyIn(c.p.n.pausedUntil - now)
		return false, writable
	case len(in.chunks) > 0:
		at := max(in.chunks[0].at, s.link(c.to, c.from).stallUntil)
		if at <= now {
			return true, writable
		}
		c.notifyIn(at - now)
		return false, writable
	}
	return in.eof, writable
}
