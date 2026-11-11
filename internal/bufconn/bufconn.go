// Package bufconn buffers a connection under TLS. TLS reads and writes a
// record of at most 16 KiB at a time, so on a busy connection each record
// costs a system call of its own. Below TLS, one read takes in as many
// records as the kernel holds, and one write sends every record that a
// flush of the layer above TLS made. Reading records one at a time took
// 30% of the load generator's core, and writing them one at a time 9% of a
// leader's core with transactions (#506).
package bufconn

import (
	"bufio"
	"crypto/tls"
	"net"
)

// Conn is a connection that reads through a buffer, and, once Buffer was
// called, writes into one until Flush.
type Conn struct {
	net.Conn
	r    *bufio.Reader
	size int
	// mu, a slot that a holder fills, guards w and buffered: TLS may
	// write from a reading goroutine, as when it answers a key update, or
	// from a closing one, while another one flushes. A flush holds it
	// while its write waits, until the write deadline; a goroutine that
	// waits on a channel is durably blocked under synctest, and one in
	// sync.Mutex.Lock is not, which stopped the simulator's clock before
	// the deadline came (#567).
	mu       chan struct{}
	w        *bufio.Writer
	buffered bool
}

func (c *Conn) lock()   { c.mu <- struct{}{} }
func (c *Conn) unlock() { <-c.mu }

// New returns c with buffers of size bytes. Writes go straight to c until
// Buffer, so that a TLS handshake over it, which waits for the other
// side's answers, needs no flush; the write buffer is made then, so a
// connection that never buffers its writes holds only the read buffer.
func New(c net.Conn, size int) *Conn {
	return &Conn{Conn: c, r: bufio.NewReaderSize(c, size), size: size, mu: make(chan struct{}, 1)}
}

func (c *Conn) Read(p []byte) (int, error) { return c.r.Read(p) }

// GrowReads makes reads go through a buffer of size bytes from now on, for
// a connection that starts small until it is known to carry a stream of
// data. Only the goroutine that reads may call it. The bytes the old
// buffer holds are read first; once it is empty, it passes the new
// buffer's reads straight to the connection.
func (c *Conn) GrowReads(size int) {
	if size > c.r.Size() {
		c.r = bufio.NewReaderSize(c.r, size)
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	c.lock()
	defer c.unlock()
	if !c.buffered {
		return c.Conn.Write(p)
	}
	return c.w.Write(p)
}

// Buffer makes writes wait in a buffer of New's size until Flush, or until
// it is full.
func (c *Conn) Buffer() {
	c.lock()
	if c.w == nil {
		c.w = bufio.NewWriterSize(c.Conn, c.size)
	}
	c.buffered = true
	c.unlock()
}

// Flush writes what waits in the buffer.
func (c *Conn) Flush() error {
	c.lock()
	defer c.unlock()
	if c.w == nil {
		return nil
	}
	return c.w.Flush()
}

// Under returns the Conn under a TLS connection, or nil if there is none.
func Under(c net.Conn) *Conn {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return nil
	}
	b, _ := tc.NetConn().(*Conn)
	return b
}

// Listener wraps every connection it accepts in a Conn, for reads.
type Listener struct {
	net.Listener
	Size int
}

func (l Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return New(c, l.Size), nil
}
