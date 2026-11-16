// Package simos is the simulator's stand-in for the system calls a wl
// node makes that the simulator cannot run as they are (wl.Sys): epoll,
// eventfd, its log's direct writes, and the raw reads and writes of its
// sockets and files. A node's files are names on its disk, a wal.FS such
// as a simfs disk with the simulator's faults; its sockets are the
// simulator's connections (Conn). One OS holds the descriptors of every
// node of a run; each node runs on a Node of its own.
package simos

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

// Conn is a connection of the simulator's network that a node's loop
// reads and writes without waiting.
type Conn interface {
	net.Conn
	// TryRead reads what has arrived, unix.EAGAIN if nothing has, and
	// io.EOF once the other end closed; TryWrite writes what fits,
	// unix.EAGAIN if nothing does.
	TryRead(p []byte) (int, error)
	TryWrite(p []byte) (int, error)
	// Ready reports whether a read and a write would not wait.
	Ready() (readable, writable bool)
	// OnChange sets f, called when Ready may have changed, nil to stop.
	OnChange(f func())
}

// Net dials the simulator's network for a node.
type Net interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// OS is the descriptor table of every node of a run.
type OS struct {
	mu   sync.Mutex
	next int
	fds  map[int]entry
}

// New returns an empty table.
func New() *OS { return &OS{next: 3, fds: map[int]entry{}} }

type entry interface{ close() error }

func (o *OS) add(e entry) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	fd := o.next
	o.next++
	o.fds[fd] = e
	return fd
}

func (o *OS) get(fd int) (entry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	e, ok := o.fds[fd]
	if !ok {
		return nil, unix.EBADF
	}
	return e, nil
}

// Node is one start of a node: its data dir, which its disk holds, and
// its network. It implements wl.Sys.
type Node struct {
	*OS
	root string
	disk wal.FS
	net  Net
}

var _ wl.Sys = (*Node)(nil)

// NewNode returns a node whose files under root lie on disk.
func (o *OS) NewNode(root string, disk wal.FS, nw Net) *Node {
	return &Node{OS: o, root: strings.TrimSuffix(root, "/"), disk: disk, net: nw}
}

// name is path's name on the disk: relative to the data dir.
func (n *Node) name(path string) (string, error) {
	if path == n.root {
		return "", nil
	}
	rel, ok := strings.CutPrefix(path, n.root+"/")
	if !ok {
		return "", &fs.PathError{Op: "open", Path: path, Err: unix.EACCES}
	}
	return rel, nil
}

// sysErr is err as the system's: a missing file is ENOENT, one that
// exists already EEXIST.
func sysErr(err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return unix.ENOENT
	case errors.Is(err, fs.ErrExist):
		return unix.EEXIST
	}
	return err
}

// ---- files

// file is an open file of a node's disk.
type file struct {
	n      *Node
	h      wal.File
	pos    int64
	direct bool // each write is durable when it returns
}

// dir is a directory a node opened to sync its names.
type dir struct{ n *Node }

func (f *file) close() error { return f.h.Close() }
func (d *dir) close() error  { return nil }

func (n *Node) Open(path string, flags int, _ uint32) (int, error) {
	name, err := n.name(path)
	if err != nil {
		return -1, err
	}
	if flags&unix.O_DIRECTORY != 0 || name == "" || n.isDir(name) {
		return n.add(&dir{n: n}), nil
	}
	h, err := n.disk.Open(name)
	switch {
	case err == nil && flags&(unix.O_CREAT|unix.O_EXCL) == unix.O_CREAT|unix.O_EXCL:
		_ = h.Close()
		return -1, unix.EEXIST
	case err == nil && flags&unix.O_TRUNC != 0:
		if err := h.Truncate(0); err != nil {
			_ = h.Close()
			return -1, err
		}
	case errors.Is(err, fs.ErrNotExist) && flags&unix.O_CREAT != 0:
		h, err = n.disk.Create(name)
	}
	if err != nil {
		return -1, sysErr(err)
	}
	return n.add(&file{n: n, h: h}), nil
}

func (n *Node) OpenDirect(path string) (int, error) {
	fd, err := n.Open(path, unix.O_WRONLY, 0)
	if err != nil {
		return -1, err
	}
	e, _ := n.get(fd)
	if f, ok := e.(*file); ok {
		f.direct = true
	}
	return fd, nil
}

// isDir reports whether name is a directory: a prefix of some file's.
func (n *Node) isDir(name string) bool {
	names, err := n.disk.List()
	if err != nil {
		return false
	}
	for _, f := range names {
		if strings.HasPrefix(f, name+"/") {
			return true
		}
	}
	return false
}

func (n *Node) MkdirAll(string) error { return nil } // directories are the names' prefixes

func (n *Node) ReadDir(path string) ([]string, error) {
	name, err := n.name(path)
	if err != nil {
		return nil, err
	}
	all, err := n.disk.List()
	if err != nil {
		return nil, err
	}
	prefix := name + "/"
	if name == "" {
		prefix = ""
	}
	seen := map[string]bool{}
	var out []string
	for _, f := range all {
		rest, ok := strings.CutPrefix(f, prefix)
		if !ok {
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			rest = rest[:i]
		}
		if !seen[rest] {
			seen[rest] = true
			out = append(out, rest)
		}
	}
	return out, nil
}

func (n *Node) Remove(path string) error {
	name, err := n.name(path)
	if err != nil {
		return err
	}
	return sysErr(n.disk.Remove(name))
}

func (n *Node) Rename(oldPath, newPath string) error {
	from, err := n.name(oldPath)
	if err != nil {
		return err
	}
	to, err := n.name(newPath)
	if err != nil {
		return err
	}
	return sysErr(n.disk.Rename(from, to))
}

func (n *Node) Truncate(path string, size int64) error {
	name, err := n.name(path)
	if err != nil {
		return err
	}
	h, err := n.disk.Open(name)
	if err != nil {
		return sysErr(err)
	}
	return errors.Join(h.Truncate(size), h.Close())
}

func (o *OS) Pread(fd int, p []byte, off int64) (int, error) {
	f, err := o.file(fd)
	if err != nil {
		return 0, err
	}
	k, err := f.h.ReadAt(p, off)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return k, err
}

func (o *OS) Pwrite(fd int, p []byte, off int64) (int, error) {
	f, err := o.file(fd)
	if err != nil {
		return 0, err
	}
	k, err := f.h.WriteAt(p, off)
	if err == nil && f.direct {
		err = f.h.Sync()
	}
	return k, err
}

// Allocate allocates fd's space up to size, keeping its size, as
// fallocate with FALLOC_FL_KEEP_SIZE does, on a file system that can.
func (o *OS) Allocate(fd int, size int64) error {
	f, err := o.file(fd)
	if err != nil {
		return err
	}
	a, ok := f.h.(interface{ Allocate(int64) error })
	if !ok {
		return unix.EOPNOTSUPP
	}
	return sysErr(a.Allocate(size))
}

func (o *OS) Seek(fd int, off int64, whence int) (int64, error) {
	f, err := o.file(fd)
	if err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
		f.pos = off
	case io.SeekCurrent:
		f.pos += off
	case io.SeekEnd:
		size, err := f.size()
		if err != nil {
			return 0, err
		}
		f.pos = size + off
	}
	return f.pos, nil
}

// size is the file's length: what reads find before the end.
func (f *file) size() (int64, error) {
	b, err := f.h.ReadAll()
	return int64(len(b)), err
}

func (o *OS) file(fd int) (*file, error) {
	e, err := o.get(fd)
	if err != nil {
		return nil, err
	}
	f, ok := e.(*file)
	if !ok {
		return nil, unix.EINVAL
	}
	return f, nil
}

func (o *OS) Fsync(fd int) error {
	e, err := o.get(fd)
	if err != nil {
		return err
	}
	switch v := e.(type) {
	case *file:
		return v.h.Sync()
	case *dir:
		return v.n.disk.SyncDir()
	}
	return unix.EINVAL
}

// ---- reads, writes and close of any descriptor

func (o *OS) Read(fd int, p []byte) (int, error) {
	e, err := o.get(fd)
	if err != nil {
		return 0, err
	}
	switch v := e.(type) {
	case *file:
		k, err := v.h.ReadAt(p, v.pos)
		v.pos += int64(k)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		return k, err
	case *eventfd:
		return v.read(p)
	case *socket:
		return v.read(p)
	}
	return 0, unix.EINVAL
}

func (o *OS) Write(fd int, p []byte) (int, error) {
	e, err := o.get(fd)
	if err != nil {
		return 0, err
	}
	switch v := e.(type) {
	case *file:
		k, err := v.h.WriteAt(p, v.pos)
		v.pos += int64(k)
		if err == nil && v.direct {
			err = v.h.Sync()
		}
		return k, err
	case *eventfd:
		return v.write(p)
	case *socket:
		return v.write(p)
	}
	return 0, unix.EINVAL
}

func (o *OS) Close(fd int) error {
	o.mu.Lock()
	e, ok := o.fds[fd]
	delete(o.fds, fd)
	o.mu.Unlock()
	if !ok {
		return unix.EBADF
	}
	return e.close()
}

// ---- sockets

func (n *Node) Dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return n.net.Dial(ctx, addr)
}

// socket is a connection a node took as a descriptor; blocking, its
// reads and writes wait, up to timeout if not 0.
type socket struct {
	c        Conn
	blocking bool
	timeout  time.Duration
}

func (s *socket) close() error { return s.c.Close() }

func (n *Node) ConnFD(nc net.Conn) (int, error) {
	c, ok := nc.(Conn)
	if !ok {
		_ = nc.Close()
		return -1, errors.New("simos: not a connection of the simulator's network")
	}
	return n.add(&socket{c: c}), nil
}

func (o *OS) socket(fd int) (*socket, error) {
	e, err := o.get(fd)
	if err != nil {
		return nil, err
	}
	s, ok := e.(*socket)
	if !ok {
		return nil, unix.ENOTSOCK
	}
	return s, nil
}

func (n *Node) Blocking(fd int, timeout time.Duration) error {
	s, err := n.socket(fd)
	if err != nil {
		return err
	}
	s.blocking, s.timeout = true, timeout
	return nil
}

func (n *Node) Nonblocking(fd int) error {
	s, err := n.socket(fd)
	if err != nil {
		return err
	}
	s.blocking = false
	return s.c.SetDeadline(time.Time{})
}

// read reads as the system's socket does: blocking, with the timeout's
// EAGAIN; else what arrived, EAGAIN for nothing, 0 at the end.
func (s *socket) read(p []byte) (int, error) {
	if !s.blocking {
		k, err := s.c.TryRead(p)
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return k, err
	}
	s.deadline()
	k, err := s.c.Read(p)
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		return k, unix.EAGAIN
	case errors.Is(err, io.EOF):
		return k, nil
	}
	return k, err
}

func (s *socket) write(p []byte) (int, error) {
	if !s.blocking {
		return s.c.TryWrite(p)
	}
	s.deadline()
	k, err := s.c.Write(p)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		err = unix.EAGAIN
	}
	return k, err
}

func (s *socket) deadline() {
	var t time.Time
	if s.timeout > 0 {
		t = time.Now().Add(s.timeout)
	}
	_ = s.c.SetDeadline(t)
}
