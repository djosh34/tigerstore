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
	"sync"
)

// Conn is a connection that reads through a buffer, and, once Buffer was
// called, writes into one until Flush.
type Conn struct {
	net.Conn
	r *bufio.Reader
	// mu guards w and buffered: TLS may write from a reading goroutine,
	// as when it answers a key update, while another one flushes.
	mu       sync.Mutex
	w        *bufio.Writer
	buffered bool
}

// New returns c with buffers of size bytes. Writes go straight to c until
// Buffer, so that a TLS handshake over it, which waits for the other
// side's answers, needs no flush.
func New(c net.Conn, size int) *Conn {
	return &Conn{Conn: c, r: bufio.NewReaderSize(c, size), w: bufio.NewWriterSize(c, size)}
}

func (c *Conn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.buffered {
		return c.Conn.Write(p)
	}
	return c.w.Write(p)
}

// Buffer makes writes wait in the buffer until Flush, or until it is full.
func (c *Conn) Buffer() {
	c.mu.Lock()
	c.buffered = true
	c.mu.Unlock()
}

// Flush writes what waits in the buffer.
func (c *Conn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
