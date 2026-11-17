package simos

import (
	"encoding/binary"
	"slices"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// ---- eventfd

// eventfd is a counter that wakes its reader, as the system's does.
type eventfd struct {
	mu       sync.Mutex
	n        uint64
	nonblock bool
	wake     chan struct{} // closed when n becomes non-zero, for a blocking read
	watch    func()        // the epoll it is in
}

func (o *OS) Eventfd(nonblock bool) (int, error) {
	return o.add(&eventfd{nonblock: nonblock}), nil
}

func (e *eventfd) close() error { return nil }

func (e *eventfd) read(p []byte) (int, error) {
	if len(p) < 8 {
		return 0, unix.EINVAL
	}
	for {
		e.mu.Lock()
		if e.n > 0 {
			binary.LittleEndian.PutUint64(p, e.n)
			e.n = 0
			e.mu.Unlock()
			return 8, nil
		}
		if e.nonblock {
			e.mu.Unlock()
			return 0, unix.EAGAIN
		}
		if e.wake == nil {
			e.wake = make(chan struct{})
		}
		wake := e.wake
		e.mu.Unlock()
		<-wake
	}
}

func (e *eventfd) write(p []byte) (int, error) {
	if len(p) < 8 {
		return 0, unix.EINVAL
	}
	e.mu.Lock()
	e.n += binary.LittleEndian.Uint64(p)
	wake, watch := e.wake, e.watch
	e.wake = nil
	e.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	if watch != nil {
		watch()
	}
	return 8, nil
}

func (e *eventfd) ready() (bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n > 0, true
}

func (e *eventfd) hup() bool { return false }

func (e *eventfd) onChange(f func()) {
	e.mu.Lock()
	e.watch = f
	e.mu.Unlock()
}

func (s *socket) ready() (bool, bool) { return s.c.Ready() }
func (s *socket) hup() bool           { return s.c.Hup() }
func (s *socket) onChange(f func())   { s.c.OnChange(f) }

// source is what an epoll watches: an eventfd or a socket.
type source interface {
	ready() (readable, writable bool)
	hup() bool
	onChange(f func())
}

// ---- epoll

// epoll is level-triggered, as the node's loop uses it.
type epoll struct {
	o    *OS
	mu   sync.Mutex
	regs map[int]reg
	wake chan struct{} // holds a token once a source may have changed
}

type reg struct {
	events uint32
	tag    int32
}

func (o *OS) EpollCreate() (int, error) {
	return o.add(&epoll{o: o, regs: map[int]reg{}, wake: make(chan struct{}, 1)}), nil
}

func (ep *epoll) close() error { return nil }

func (ep *epoll) signal() {
	select {
	case ep.wake <- struct{}{}:
	default:
	}
}

func (o *OS) epoll(fd int) (*epoll, error) {
	e, err := o.get(fd)
	if err != nil {
		return nil, err
	}
	ep, ok := e.(*epoll)
	if !ok {
		return nil, unix.EINVAL
	}
	return ep, nil
}

func (o *OS) EpollCtl(epfd, op, fd int, events uint32, tag int32) error {
	ep, err := o.epoll(epfd)
	if err != nil {
		return err
	}
	e, err := o.get(fd)
	if err != nil {
		return err
	}
	src, ok := e.(source)
	if !ok {
		return unix.EPERM
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	switch op {
	case unix.EPOLL_CTL_ADD:
		if _, ok := ep.regs[fd]; ok {
			return unix.EEXIST
		}
		ep.regs[fd] = reg{events: events, tag: tag}
		src.onChange(ep.signal)
	case unix.EPOLL_CTL_MOD:
		if _, ok := ep.regs[fd]; !ok {
			return unix.ENOENT
		}
		ep.regs[fd] = reg{events: events, tag: tag}
	case unix.EPOLL_CTL_DEL:
		delete(ep.regs, fd)
		src.onChange(nil)
	default:
		return unix.EINVAL
	}
	ep.signal()
	return nil
}

func (o *OS) EpollWait(epfd int, events []unix.EpollEvent, ms int) (int, error) {
	ep, err := o.epoll(epfd)
	if err != nil {
		return 0, err
	}
	var timer *time.Timer
	if ms > 0 {
		timer = time.NewTimer(time.Duration(ms) * time.Millisecond)
		defer timer.Stop()
	}
	for {
		if k := ep.collect(events); k > 0 || ms == 0 {
			return k, nil
		}
		if timer == nil {
			<-ep.wake
			continue
		}
		select {
		case <-ep.wake:
		case <-timer.C:
			return ep.collect(events), nil
		}
	}
}

// collect fills events with the registered descriptors that are ready,
// in descriptor order, and drops those closed since.
func (ep *epoll) collect(events []unix.EpollEvent) int {
	ep.mu.Lock()
	fds := make([]int, 0, len(ep.regs))
	for fd := range ep.regs {
		fds = append(fds, fd)
	}
	ep.mu.Unlock()
	slices.Sort(fds)
	k := 0
	for _, fd := range fds {
		if k == len(events) {
			break
		}
		e, err := ep.o.get(fd)
		src, ok := e.(source)
		ep.mu.Lock()
		r, still := ep.regs[fd]
		if err != nil || !ok {
			delete(ep.regs, fd)
		}
		ep.mu.Unlock()
		if err != nil || !ok || !still {
			continue
		}
		readable, writable := src.ready()
		var got uint32
		// As the system's: bytes wake only a registration for EPOLLIN,
		// and EPOLLRDHUP comes of the other end's close alone. Woken
		// for bytes through EPOLLRDHUP, a node read a connection it had
		// stopped reading, paused for its send buffer or a call's token,
		// and its buffers grew past the client pool (#689's re-check of
		// d33d3711).
		if readable && r.events&unix.EPOLLIN != 0 {
			got |= unix.EPOLLIN
		}
		if r.events&unix.EPOLLRDHUP != 0 && src.hup() {
			got |= unix.EPOLLRDHUP
		}
		if writable && r.events&unix.EPOLLOUT != 0 {
			got |= unix.EPOLLOUT
		}
		if got != 0 {
			events[k] = unix.EpollEvent{Events: got, Fd: r.tag}
			k++
		}
	}
	return k
}
