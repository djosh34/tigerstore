package loop

import (
	"errors"
	"net"
	"time"
)

// Bridge is the connection under a crypto/tls connection whose handshake
// runs on a goroutine of its own and whose records the loop then reads
// and writes. Until Attach it reads and writes the raw connection, with
// blocking calls. After it, a read takes what has arrived on the socket
// and fails with ErrWouldBlock, which crypto/tls returns without breaking
// the connection, when nothing has; a write only queues the bytes, which
// the loop sends with Flush. Reads go through a buffer, so one system call
// takes in as many records as have arrived.
//
// The buffer also keeps the bytes that came in during the handshake behind
// its last message, such as the client's first records. A bridge attached
// with Buffers takes its read and write buffers from them only while they
// hold bytes, so an idle connection holds none.
type Bridge struct {
	net.Conn
	sock Socket
	pool *Buffers
	// in holds bytes read and not yet taken, from r to w.
	in   []byte
	r, w int
	// out holds the bytes written that are not sent, from outR on.
	out  []byte
	outR int
}

// NewBridge returns a bridge over raw that reads through a buffer of size
// bytes.
func NewBridge(raw net.Conn, size int) *Bridge {
	return &Bridge{Conn: raw, in: make([]byte, size)}
}

// Attach hands the connection to the loop through sock, which the poller
// made from the raw connection. With pool, the bridge's buffers come from
// it while they hold bytes; without, the bridge keeps its own.
func (b *Bridge) Attach(sock Socket, pool *Buffers) {
	b.sock, b.pool = sock, pool
	if pool != nil && b.r == b.w {
		// The handshake's buffer goes: the next read borrows one.
		b.in, b.r, b.w = nil, 0, 0
	}
}

// Attached reports whether the loop has the connection.
func (b *Bridge) Attached() bool { return b.sock != nil }

func (b *Bridge) Read(p []byte) (int, error) {
	if b.r == b.w {
		b.r, b.w = 0, 0
		if b.in == nil {
			b.in = b.pool.Get()
		}
		var n int
		var err error
		if b.sock == nil {
			n, err = b.Conn.Read(b.in)
		} else {
			n, err = b.sock.Read(b.in)
		}
		b.w = n
		if n == 0 {
			b.release()
			return 0, err
		}
	}
	n := copy(p, b.in[b.r:b.w])
	b.r += n
	if b.r == b.w {
		b.release()
	}
	return n, nil
}

// release returns an empty read buffer to the pool.
func (b *Bridge) release() {
	if b.pool != nil {
		b.pool.Put(b.in)
		b.in, b.r, b.w = nil, 0, 0
	}
}

// Grow makes the read buffer size bytes, keeping what it holds, as for a
// connection whose handshake read through a small buffer and that then
// carries a stream.
func (b *Bridge) Grow(size int) {
	if size <= len(b.in) {
		return
	}
	in := make([]byte, size)
	b.w = copy(in, b.in[b.r:b.w])
	b.in, b.r = in, 0
}

// Peek returns the next n bytes without taking them, reading the raw
// connection until it has them. Only before Attach; n must fit the buffer.
func (b *Bridge) Peek(n int) ([]byte, error) {
	if b.r > 0 && b.r+n > len(b.in) {
		b.w = copy(b.in, b.in[b.r:b.w])
		b.r = 0
	}
	for b.w-b.r < n {
		k, err := b.Conn.Read(b.in[b.w:])
		b.w += k
		if err != nil && b.w-b.r < n {
			return b.in[b.r:b.w], err
		}
	}
	return b.in[b.r : b.r+n], nil
}

// Buffered is how many read bytes the bridge holds.
func (b *Bridge) Buffered() int { return b.w - b.r }

func (b *Bridge) Write(p []byte) (int, error) {
	if b.sock == nil {
		return b.Conn.Write(p)
	}
	if b.out == nil && b.pool != nil {
		b.out = b.pool.Get()[:0]
	}
	b.out = append(b.out, p...)
	return len(p), nil
}

// Pending is how many written bytes wait to be sent.
func (b *Bridge) Pending() int { return len(b.out) - b.outR }

// Flush sends what was written, as far as the socket takes it now. It
// returns nil when the rest has to wait for the socket.
func (b *Bridge) Flush() error {
	for b.outR < len(b.out) {
		n, err := b.sock.Write(b.out[b.outR:])
		b.outR += n
		if err != nil {
			if errors.Is(err, ErrWouldBlock) {
				return nil
			}
			return err
		}
	}
	b.out, b.outR = b.out[:0], 0
	switch {
	case b.pool != nil:
		b.pool.Put(b.out)
		b.out = nil
	case cap(b.out) > maxIdleOut:
		b.out = nil
	}
	return nil
}

// maxIdleOut is the most write buffer a bridge keeps once it sent
// everything: a large answer's buffer goes, and the next write grows a new
// one.
const maxIdleOut = 1 << 20

// Close closes the connection, through the socket once the loop has it.
func (b *Bridge) Close() error {
	if b.sock != nil {
		return b.sock.Close()
	}
	return b.Conn.Close()
}

// The deadlines are the handshake's; the loop keeps time itself.

func (b *Bridge) SetDeadline(t time.Time) error {
	if b.sock != nil {
		return nil
	}
	return b.Conn.SetDeadline(t)
}

func (b *Bridge) SetReadDeadline(t time.Time) error {
	if b.sock != nil {
		return nil
	}
	return b.Conn.SetReadDeadline(t)
}

func (b *Bridge) SetWriteDeadline(t time.Time) error {
	if b.sock != nil {
		return nil
	}
	return b.Conn.SetWriteDeadline(t)
}
