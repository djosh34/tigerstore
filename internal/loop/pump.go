package loop

import (
	"net"
	"sync"
)

// pumpBuffer is how many bytes a pump holds in each direction.
const pumpBuffer = 64 << 10

// Pump is a socket over a blocking connection that a poller cannot wait on
// itself, such as an in-memory pipe: one goroutine reads the connection
// into a buffer and one writes from another, and each tells the loop when
// it moved bytes. It costs two goroutines and a copy each way, so the
// connections of the hot path are attached without one.
type Pump struct {
	c      net.Conn
	notify func()
	mu     sync.Mutex
	// in holds bytes read and not yet taken, and rerr the read error that
	// follows them. out holds bytes to write, and werr the write error
	// that failed them.
	in, out    []byte
	rerr, werr error
	closed     bool
	// room wakes the reader when the loop took bytes; more wakes the
	// writer when the loop gave it some, or either at the close.
	room, more *sync.Cond
}

// NewPump starts pumping c and calls notify each time the socket may have
// become readable or writable.
func NewPump(c net.Conn, notify func()) *Pump {
	p := &Pump{c: c, notify: notify}
	p.room, p.more = sync.NewCond(&p.mu), sync.NewCond(&p.mu)
	go p.read()
	go p.write()
	return p
}

func (p *Pump) read() {
	buf := make([]byte, pumpBuffer)
	for {
		p.mu.Lock()
		for !p.closed && len(p.in) >= pumpBuffer {
			p.room.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		n, err := p.c.Read(buf)
		p.mu.Lock()
		p.in = append(p.in, buf[:n]...)
		if err != nil {
			p.rerr = err
		}
		p.mu.Unlock()
		p.notify()
		if err != nil {
			return
		}
	}
}

func (p *Pump) write() {
	var buf []byte
	for {
		p.mu.Lock()
		for !p.closed && len(p.out) == 0 {
			p.more.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		buf = append(buf[:0], p.out...)
		p.mu.Unlock()
		n, err := p.c.Write(buf)
		p.mu.Lock()
		p.out = p.out[:copy(p.out, p.out[n:])]
		if err != nil {
			p.werr = err
		}
		p.mu.Unlock()
		p.notify()
		if err != nil {
			return
		}
	}
}

// Read takes the bytes read so far.
func (p *Pump) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.closed:
		return 0, ErrClosed
	case len(p.in) > 0:
		n := copy(b, p.in)
		p.in = p.in[:copy(p.in, p.in[n:])]
		p.room.Signal()
		return n, nil
	case p.rerr != nil:
		return 0, p.rerr
	}
	return 0, ErrWouldBlock
}

// Write queues as much of b as the buffer has room for.
func (p *Pump) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.closed:
		return 0, ErrClosed
	case p.werr != nil:
		return 0, p.werr
	}
	n := min(len(b), pumpBuffer-len(p.out))
	p.out = append(p.out, b[:n]...)
	if n > 0 {
		p.more.Signal()
	}
	if n < len(b) {
		return n, ErrWouldBlock
	}
	return n, nil
}

// Close closes the connection and stops both goroutines.
func (p *Pump) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	p.closed = true
	p.room.Broadcast()
	p.more.Broadcast()
	p.mu.Unlock()
	return p.c.Close()
}
