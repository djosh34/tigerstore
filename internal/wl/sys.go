package wl

import (
	"context"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// Sys is the simulator's stand-in for the system calls a node makes that
// the simulator cannot run as they are: epoll, eventfd, O_DIRECT opens
// and the raw reads and writes of its sockets and files (sys_os.go). A
// real binary leaves Config.Sys nil and refuses one; a binary built with
// the tag sim or simio runs the node on it (sys_sim.go).
//
// File descriptors are the simulator's, one table for every node of a
// run: the calls that take only a descriptor go to the Sys of the first
// node started, and every node's Sys must share that table.
type Sys interface {
	EpollCreate() (int, error)
	EpollCtl(ep, op, fd int, events uint32, tag int32) error
	// EpollWait waits up to ms milliseconds, -1 for no limit, as
	// epoll_wait does, and fills events with what is ready.
	EpollWait(ep int, events []unix.EpollEvent, ms int) (int, error)
	Eventfd(nonblock bool) (int, error)
	Open(path string, flags int, mode uint32) (int, error)
	// OpenDirect opens a log segment for writes that are durable once
	// they return.
	OpenDirect(path string) (int, error)

	Read(fd int, p []byte) (int, error)
	Write(fd int, p []byte) (int, error)
	Pread(fd int, p []byte, off int64) (int, error)
	Pwrite(fd int, p []byte, off int64) (int, error)
	Fsync(fd int) error
	Close(fd int) error
	// Seek moves fd's offset as lseek does.
	Seek(fd int, off int64, whence int) (int64, error)

	// The node's files are under its data dir; their errors are the
	// system's, so that a missing file is os.ErrNotExist.
	MkdirAll(path string) error
	ReadDir(dir string) ([]string, error)
	Remove(path string) error
	Rename(oldPath, newPath string) error
	Truncate(path string, size int64) error

	// Dial connects to a peer at addr.
	Dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error)
	// ConnFD takes nc, a connection the node dialed or accepted, as a
	// non-blocking descriptor; nc is the descriptor's from then on.
	ConnFD(nc net.Conn) (int, error)
	// Blocking makes socket fd's reads and writes block, failing after
	// timeout, or never for 0; Nonblocking makes them not block again.
	Blocking(fd int, timeout time.Duration) error
	Nonblocking(fd int) error
}
