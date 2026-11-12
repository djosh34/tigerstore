// Package loop is what a node's event loop stands on: a poller that waits
// for the loop's sockets, a timeout or a wake-up from another goroutine, and
// sockets that the loop reads and writes without blocking.
//
// One goroutine, the loop, owns every socket it attaches: it alone reads,
// writes and closes them, so a socket takes no lock. Other goroutines hand
// the loop work through their own queues and call Wake. The production
// poller (internal/epoll) waits in epoll; ChanPoller waits on a channel,
// which the simulator's virtual clock and scheduler drive, so the simulator
// runs the same loop as production.
package loop

import (
	"errors"
	"net"
	"time"
)

// Poller waits for the loop's sockets. Only the loop calls Attach, Wait and
// Close; any goroutine may call Wake.
type Poller interface {
	// Attach hands c to the loop and returns the socket the loop reads and
	// writes it through. Each time c may have become readable or writable,
	// or failed, a Wait returns token. The loop then reads until the
	// socket would block and writes what it holds; a token can also come
	// when nothing changed. c must not be used other than through the
	// socket from then on.
	Attach(c net.Conn, token uint64) (Socket, error)
	// Wait blocks until a socket's token is due, Wake was called since the
	// last Wait, or timeout passed, and appends the due tokens to ready. A
	// negative timeout waits without a limit, and 0 does not wait.
	Wait(ready []uint64, timeout time.Duration) ([]uint64, error)
	// Wake makes the current or next Wait return. It never blocks.
	Wake()
	Close() error
}

// Socket is a connection that the loop reads and writes without blocking.
type Socket interface {
	// Read reads what has arrived, ErrWouldBlock if nothing has, io.EOF once
	// the other end closed.
	Read(p []byte) (int, error)
	// Write writes as much of p as fits now; ErrWouldBlock with n < len(p)
	// when the rest does not fit.
	Write(p []byte) (int, error)
	// Close closes the connection; its token no longer comes.
	Close() error
}

// Pollable is a connection that reports itself when it may have become
// readable or writable, as the simulator's connections do. A poller
// attaches it without a system call or a goroutine.
type Pollable interface {
	Socket
	// Notify sets the function the connection calls, from any goroutine
	// and without holding its locks, each time it may have become readable
	// or writable, or failed. It is set once, before the first Read.
	Notify(func())
}

// ErrWouldBlock is what a socket's Read or Write returns when it would
// have to wait. It is a net.Error that is temporary, so crypto/tls keeps a
// connection whose read it ends and the read can be tried again (Bridge).
var ErrWouldBlock error = wouldBlock{}

type wouldBlock struct{}

func (wouldBlock) Error() string   { return "loop: the operation would block" }
func (wouldBlock) Timeout() bool   { return true }
func (wouldBlock) Temporary() bool { return true }

var _ net.Error = wouldBlock{}

// ErrClosed is what a closed socket returns.
var ErrClosed = errors.New("loop: socket closed")
