package loop

import (
	"net"
	"sync"
	"time"
)

// Due is the tokens that sockets reported due by their own notice
// (Pollable, Pump), for a poller to hand out in its next Wait. Each token
// is listed once until taken.
type Due struct {
	mu     sync.Mutex
	tokens []uint64
	listed map[uint64]bool
}

// Add lists token and reports whether it was not listed yet.
func (d *Due) Add(token uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.listed[token] {
		return false
	}
	if d.listed == nil {
		d.listed = map[uint64]bool{}
	}
	d.listed[token] = true
	d.tokens = append(d.tokens, token)
	return true
}

// Take appends the listed tokens to ready, in the order they came, and
// empties the list.
func (d *Due) Take(ready []uint64) []uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	ready = append(ready, d.tokens...)
	d.tokens = d.tokens[:0]
	clear(d.listed)
	return ready
}

// Empty reports whether no token is listed.
func (d *Due) Empty() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.tokens) == 0
}

// ChanPoller is a poller over a channel. Every socket it attaches reports
// itself: a Pollable connection does, and any other connection is pumped
// by two goroutines (Pump). The simulator runs its nodes' loops on it, so
// their waits are on its virtual clock and its scheduler orders them.
type ChanPoller struct {
	due  Due
	mu   sync.Mutex
	woke bool
	// wake holds a token while a Wait may have something to return.
	wake  chan struct{}
	timer *time.Timer
}

// NewChanPoller returns a poller over a channel.
func NewChanPoller() *ChanPoller {
	return &ChanPoller{wake: make(chan struct{}, 1)}
}

// Attach attaches a Pollable connection as it is, and pumps any other.
func (p *ChanPoller) Attach(c net.Conn, token uint64) (Socket, error) {
	return AttachNotify(c, func() { p.ready(token) }), nil
}

// AttachNotify returns c as a socket that calls notify each time it may
// have become ready: c itself if it is Pollable, else a pump.
func AttachNotify(c net.Conn, notify func()) Socket {
	if pc, ok := c.(Pollable); ok {
		pc.Notify(notify)
		return pc
	}
	return NewPump(c, notify)
}

func (p *ChanPoller) ready(token uint64) {
	if p.due.Add(token) {
		p.signal()
	}
}

func (p *ChanPoller) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Wake makes the current or next Wait return.
func (p *ChanPoller) Wake() {
	p.mu.Lock()
	p.woke = true
	p.mu.Unlock()
	p.signal()
}

// Wait waits for a due token, a Wake or the timeout.
func (p *ChanPoller) Wait(ready []uint64, timeout time.Duration) ([]uint64, error) {
	p.mu.Lock()
	idle := !p.woke && p.due.Empty()
	p.woke = false
	p.mu.Unlock()
	if idle && timeout != 0 {
		p.block(timeout)
		p.mu.Lock()
		p.woke = false
		p.mu.Unlock()
	}
	// A token left in the channel by a signal whose tokens this Wait takes
	// only makes the next Wait look again.
	select {
	case <-p.wake:
	default:
	}
	return p.due.Take(ready), nil
}

// block waits for a signal or the timeout.
func (p *ChanPoller) block(timeout time.Duration) {
	if timeout < 0 {
		<-p.wake
		return
	}
	if p.timer == nil {
		p.timer = time.NewTimer(timeout)
	} else {
		p.timer.Reset(timeout)
	}
	select {
	case <-p.wake:
		p.timer.Stop()
	case <-p.timer.C:
	}
}

// Close does nothing: the sockets close on their own.
func (p *ChanPoller) Close() error { return nil }
