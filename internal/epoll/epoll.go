//go:build linux

// Package epoll is the production poller of a node's event loop
// (internal/loop): epoll for the sockets and an eventfd for wake-ups. A
// TCP or Unix socket is read and written by the loop with plain system
// calls on its file descriptor; any other connection reports itself, as
// loop.ChanPoller's do. Only the tigerstore command and tests on real
// sockets import this package, so the simulator, under gosim, never meets
// its system calls.
package epoll

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/djosh34/tigerstore/internal/loop"
)

// wakeToken is the token of the eventfd; sockets' tokens start at 1.
const wakeToken = 0

// maxEvents is how many events one Wait takes from the kernel.
const maxEvents = 256

// Poller is an epoll instance and an eventfd. The goroutine that calls
// Wait first is locked to its OS thread, so the loop keeps its thread and
// its caches.
type Poller struct {
	epfd, efd int
	events    []syscall.EpollEvent
	due       loop.Due
	// sleeping is set while Wait may block in the kernel, and woken once a
	// Wake came that no Wait has returned for. Each side sets its own flag
	// and then reads the other's, so a Wake either sees the loop asleep and
	// writes the eventfd, or the loop sees the Wake and does not sleep.
	sleeping, woken atomic.Bool
	locked          bool
}

// New returns a poller.
func New() (*Poller, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll: create: %w", err)
	}
	efd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if errno != 0 {
		return nil, errors.Join(fmt.Errorf("epoll: eventfd: %w", errno), syscall.Close(epfd))
	}
	p := &Poller{epfd: epfd, efd: int(efd), events: make([]syscall.EpollEvent, maxEvents)}
	if err := p.add(p.efd, wakeToken, syscall.EPOLLIN); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

func (p *Poller) add(fd int, token uint64, events uint32) error {
	ev := syscall.EpollEvent{Events: events}
	setToken(&ev, token)
	if err := syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_ADD, fd, &ev); err != nil {
		return fmt.Errorf("epoll: add fd %d: %w", fd, err)
	}
	return nil
}

// setToken and tokenOf keep a token in the event's 8 bytes of user data,
// which syscall.EpollEvent splits into Fd and Pad.
func setToken(ev *syscall.EpollEvent, token uint64) {
	*(*uint64)(unsafe.Pointer(&ev.Fd)) = token
}

func tokenOf(ev *syscall.EpollEvent) uint64 {
	return *(*uint64)(unsafe.Pointer(&ev.Fd))
}

// Attach attaches a TCP or Unix connection by its file descriptor, edge
// triggered, and any other connection as loop.ChanPoller does.
func (p *Poller) Attach(c net.Conn, token uint64) (loop.Socket, error) {
	if token == wakeToken {
		return nil, errors.New("epoll: token 0 is the poller's own")
	}
	sc, ok := c.(syscall.Conn)
	if !ok {
		return loop.AttachNotify(c, func() { p.notify(token) }), nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, err
	}
	var aerr error
	if err := rc.Control(func(fd uintptr) {
		aerr = p.add(int(fd), token, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLET)
	}); err != nil {
		return nil, err
	}
	if aerr != nil {
		return nil, aerr
	}
	return &socket{c: c, rc: rc}, nil
}

func (p *Poller) notify(token uint64) {
	if p.due.Add(token) {
		p.Wake()
	}
}

// Wake makes the current or next Wait return.
func (p *Poller) Wake() {
	p.woken.Store(true)
	if p.sleeping.Load() {
		var one = [8]byte{1}
		// EAGAIN means the counter is full, so the loop wakes anyway.
		_, _ = syscall.Write(p.efd, one[:])
	}
}

// Wait waits in epoll_wait, at millisecond granularity, rounded up so that
// the loop never wakes before its timeout.
func (p *Poller) Wait(ready []uint64, timeout time.Duration) ([]uint64, error) {
	if !p.locked {
		runtime.LockOSThread()
		p.locked = true
	}
	ms := -1
	switch {
	case timeout == 0:
		ms = 0
	case timeout > 0:
		ms = int(min((timeout+time.Millisecond-1)/time.Millisecond, math.MaxInt32))
	}
	p.sleeping.Store(true)
	if p.woken.Load() || !p.due.Empty() {
		ms = 0
	}
	n, err := syscall.EpollWait(p.epfd, p.events, ms)
	p.sleeping.Store(false)
	p.woken.Store(false)
	if err != nil && !errors.Is(err, syscall.EINTR) {
		return ready, fmt.Errorf("epoll: wait: %w", err)
	}
	for i := range max(n, 0) {
		ev := &p.events[i]
		if t := tokenOf(ev); t != wakeToken {
			ready = append(ready, t)
			continue
		}
		var b [8]byte
		_, _ = syscall.Read(p.efd, b[:])
	}
	return p.due.Take(ready), nil
}

// Close closes the epoll instance and the eventfd.
func (p *Poller) Close() error {
	return errors.Join(syscall.Close(p.epfd), syscall.Close(p.efd))
}

// socket reads and writes a connection's file descriptor with plain system
// calls. They go through the connection's RawConn, which holds the
// descriptor open for the call, so a Close from another goroutine cannot
// let a new connection reuse the number in the middle of one.
type socket struct {
	c  net.Conn
	rc syscall.RawConn
}

func (s *socket) Read(b []byte) (int, error) {
	var n int
	var err error
	if rerr := s.rc.Read(func(fd uintptr) bool {
		for {
			n, err = syscall.Read(int(fd), b)
			if !errors.Is(err, syscall.EINTR) {
				return true
			}
		}
	}); rerr != nil {
		return 0, rerr
	}
	switch {
	case errors.Is(err, syscall.EAGAIN):
		return 0, loop.ErrWouldBlock
	case err != nil:
		return 0, err
	case n == 0 && len(b) > 0:
		return 0, io.EOF
	}
	return n, nil
}

func (s *socket) Write(b []byte) (int, error) {
	var n int
	var err error
	if werr := s.rc.Write(func(fd uintptr) bool {
		for {
			n, err = syscall.Write(int(fd), b)
			if !errors.Is(err, syscall.EINTR) {
				return true
			}
		}
	}); werr != nil {
		return 0, werr
	}
	switch {
	case errors.Is(err, syscall.EAGAIN):
		return 0, loop.ErrWouldBlock
	case err != nil:
		return 0, err
	case n < len(b):
		return n, loop.ErrWouldBlock
	}
	return n, nil
}

// Close closes the connection, which takes its descriptor out of the
// epoll instance.
func (s *socket) Close() error { return s.c.Close() }
