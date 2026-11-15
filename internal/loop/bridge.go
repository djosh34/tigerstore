package loop

import (
	"encoding/binary"
	"errors"
	"net"
	"slices"
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
	sock      Socket
	pool, big *Buffers
	// in holds bytes read and not yet taken, from r to w.
	in   []byte
	r, w int
	// out holds the bytes written that are not sent, from outR on, and
	// sendBy is the write deadline set for them (SendBy). keepOut is the
	// most write buffer a bridge without Buffers keeps once it sent
	// everything (KeepOut).
	out     []byte
	outR    int
	sendBy  time.Time
	keepOut int
	// byRecord is set while crypto/tls runs its handshake on the bridge
	// (Handshake): a read then ends at the end of a TLS record, so that
	// the records after the handshake stay in the buffer, and recLeft is
	// what is left of the record a read is in.
	byRecord bool
	recLeft  int
}

// NewBridge returns a bridge over raw that reads through a buffer of size
// bytes.
func NewBridge(raw net.Conn, size int) *Bridge {
	return &Bridge{Conn: raw, in: make([]byte, size), keepOut: maxIdleOut}
}

// KeepOut sets the most write buffer a bridge without Buffers keeps once
// it sent everything, maxIdleOut unless set: a buffer larger goes, and the
// next write grows a new one by append. A connection whose writes often
// pass maxIdleOut, as a peer's appends do, sets it above its largest
// write, so that it grows its buffer once instead of for each write.
func (b *Bridge) KeepOut(n int) { b.keepOut = n }

// Attach hands the connection to the loop through sock, which the poller
// made from the raw connection. With pool, the bridge's buffers come from
// it while they hold bytes, and from big once what waits to be sent
// outgrows one of pool's; without, the bridge keeps its own.
func (b *Bridge) Attach(sock Socket, pool, big *Buffers) {
	b.sock, b.pool, b.big = sock, pool, big
	if pool != nil && b.r == b.w {
		// The handshake's buffer goes: the next read borrows one.
		b.in, b.r, b.w = nil, 0, 0
	}
}

// Attached reports whether the loop has the connection.
func (b *Bridge) Attached() bool { return b.sock != nil }

func (b *Bridge) Read(p []byte) (int, error) {
	if b.byRecord {
		return b.readRecord(p)
	}
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

// readRecord reads as Read does, but never past the end of the TLS record
// it is in, before Attach.
func (b *Bridge) readRecord(p []byte) (int, error) {
	if b.recLeft == 0 {
		if err := b.fill(recordHeader); err != nil {
			return 0, err
		}
		b.recLeft = recordHeader + int(binary.BigEndian.Uint16(b.in[b.r+3:]))
	}
	if err := b.fill(1); err != nil {
		return 0, err
	}
	n := copy(p[:min(len(p), b.recLeft)], b.in[b.r:b.w])
	b.r += n
	b.recLeft -= n
	return n, nil
}

// fill makes the read buffer hold n bytes from r on, reading the raw
// connection before Attach and the socket after it, where it fails with
// ErrWouldBlock while they have not all arrived. The buffer grows to n if
// it is smaller, and what it holds moves to its start when n does not fit
// after r.
func (b *Bridge) fill(n int) error {
	if b.in == nil {
		b.in = b.pool.Get()
	}
	if n > len(b.in) {
		in := make([]byte, n)
		b.w = copy(in, b.in[b.r:b.w])
		b.r = 0
		b.in = in
	} else if b.r+n > len(b.in) {
		b.w = copy(b.in, b.in[b.r:b.w])
		b.r = 0
	}
	for b.w-b.r < n {
		var k int
		var err error
		if b.sock == nil {
			k, err = b.Conn.Read(b.in[b.w:])
		} else {
			k, err = b.sock.Read(b.in[b.w:])
		}
		b.w += k
		if err == nil && k == 0 && b.sock != nil {
			err = ErrWouldBlock
		}
		if err != nil && b.w-b.r < n {
			if b.r == b.w {
				// An idle connection holds no buffer.
				b.release()
			}
			return err
		}
	}
	return nil
}

// buffered is what the read buffer holds from r on.
func (b *Bridge) buffered() []byte { return b.in[b.r:b.w] }

// consume takes n bytes of the read buffer; an empty one goes back to the
// pool.
func (b *Bridge) consume(n int) {
	b.r += n
	if b.r == b.w {
		b.release()
	}
}

// tail is the last n bytes written and not sent, which a record layer
// seals in place.
func (b *Bridge) tail(n int) []byte { return b.out[len(b.out)-n:] }

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
	b.room(len(p))
	b.out = append(b.out, p...)
	return len(p), nil
}

// room makes the write buffer ready to take n more bytes: borrowed from
// the pool if the bridge has none, and moved into a large buffer once it
// outgrows one of the pool's. Beyond that, append grows it.
func (b *Bridge) room(n int) {
	if b.out == nil && b.pool != nil {
		b.out = b.pool.Get()[:0]
	}
	if b.big != nil && len(b.out)+n > cap(b.out) && cap(b.out) < b.big.Size() {
		// One move into a large buffer, instead of a regrowth by append
		// at each size on the way.
		out := b.big.Get()
		k := copy(out, b.out[b.outR:])
		b.pool.Put(b.out)
		b.out, b.outR = out[:k], 0
	}
}

// Reserve returns room for n bytes at the end of the write buffer, of
// length 0 and capacity n, for the caller to write in place; Commit then
// adds what it wrote to the bytes to send. Nothing else may write between
// the two. It returns nil before Attach, when writes go straight to the
// connection.
func (b *Bridge) Reserve(n int) []byte {
	if b.sock == nil {
		return nil
	}
	b.room(n)
	b.out = slices.Grow(b.out, n)
	end := len(b.out)
	return b.out[end : end : end+n]
}

// Commit adds the first n bytes of the room Reserve returned to the bytes
// to send.
func (b *Bridge) Commit(n int) { b.out = b.out[:len(b.out)+n] }

// Retained is the write buffer's whole allocation while it holds bytes
// that wait to be sent, which a partial send does not shrink.
func (b *Bridge) Retained() int {
	if b.outR == len(b.out) {
		return 0
	}
	return cap(b.out)
}

// SendBy sets when the bytes that wait to be sent must have gone, zero for
// no limit, as the raw connection's write deadline: a send the loop tries
// after it fails, as the kernel's and the simulator's connections fail it,
// so a client or peer that stops reading cannot hold the loop's output.
func (b *Bridge) SendBy(t time.Time) {
	if !t.Equal(b.sendBy) {
		b.sendBy = t
		_ = b.Conn.SetWriteDeadline(t)
	}
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
		if b.big != nil {
			b.big.Put(b.out)
		}
		b.out = nil
	case cap(b.out) > b.keepOut:
		b.out = nil
	}
	return nil
}

// maxIdleOut is the most write buffer a bridge keeps by default once it
// sent everything: a large answer's buffer goes, and the next write grows
// a new one.
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
