//go:build sim || simio

package wl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// The simulator's side of the seam of sys_os.go: each call goes to the
// node's Sys, and the calls that take only a descriptor to the Sys of the
// first node started, whose descriptor table every node shares.

// sysEnv is a node's handle on the simulator.
type sysEnv struct{ s Sys }

var (
	errNoSys = errors.New("a node built with the simulator's seam needs Config.Sys")
	fdSys    atomic.Pointer[Sys]
)

func newSysEnv(cfg *Config) (sysEnv, error) {
	if cfg.Sys == nil {
		return sysEnv{}, errNoSys
	}
	fdSys.CompareAndSwap(nil, &cfg.Sys)
	return sysEnv{s: cfg.Sys}, nil
}

func (e sysEnv) epollCreate() (int, error) { return e.s.EpollCreate() }

func (e sysEnv) epollCtl(ep, op, fd int, events uint32, tag int32) error {
	return e.s.EpollCtl(ep, op, fd, events, tag)
}

func (e sysEnv) epollWait(ep int, events []unix.EpollEvent, ms int) (int, error) {
	return e.s.EpollWait(ep, events, ms)
}

func (e sysEnv) eventfd(nonblock bool) (int, error) { return e.s.Eventfd(nonblock) }

func (e sysEnv) open(path string, flags int, mode uint32) (int, error) {
	return e.s.Open(path, flags, mode)
}

func (e sysEnv) openDirect(path string) (int, error) { return e.s.OpenDirect(path) }

func (e sysEnv) dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	return e.s.Dial(ctx, addr, timeout)
}

func (e sysEnv) connFD(nc net.Conn) (int, error)        { return e.s.ConnFD(nc) }
func (e sysEnv) blocking(fd int, d time.Duration) error { return e.s.Blocking(fd, d) }
func (e sysEnv) nonblocking(fd int) error               { return e.s.Nonblocking(fd) }

// grpcDial dials the node's gRPC client of itself through the
// simulator's network.
func (e sysEnv) grpcDial() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return e.s.Dial(ctx, addr, 0)
	})}
}

func fds() Sys { return *fdSys.Load() }

func sysRead(fd int, p []byte) (int, error)              { return fds().Read(fd, p) }
func sysWrite(fd int, p []byte) (int, error)             { return fds().Write(fd, p) }
func sysPread(fd int, p []byte, off int64) (int, error)  { return fds().Pread(fd, p, off) }
func sysPwrite(fd int, p []byte, off int64) (int, error) { return fds().Pwrite(fd, p, off) }
func sysFsync(fd int) error                              { return fds().Fsync(fd) }
func sysClose(fd int) error                              { return fds().Close(fd) }

// sysCPU is 0 in the simulator, whose clock is virtual.
func sysCPU() (process, thread int64) { return 0, 0 }

// fdLimit is the simulator's: no limit a node would meet.
func fdLimit() uint64 { return 1 << 20 }

// file is an open file of the node's, on the simulator's descriptors.
type file = *simFile

type simFile struct {
	fd   int
	name string
}

func (e sysEnv) openFile(path string, flag int, perm os.FileMode) (file, error) {
	fd, err := e.s.Open(path, flag, uint32(perm))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return &simFile{fd: fd, name: path}, nil
}

var tempSeq atomic.Uint64

// createTemp makes a new file in dir, its name pattern with a number for
// its last "*".
func (e sysEnv) createTemp(dir, pattern string) (file, error) {
	for {
		name := pattern + fmt.Sprint(tempSeq.Add(1))
		if i := strings.LastIndex(pattern, "*"); i >= 0 {
			name = pattern[:i] + fmt.Sprint(tempSeq.Add(1)) + pattern[i+1:]
		}
		f, err := e.openFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return f, err
	}
}

func (e sysEnv) mkdirAll(path string) error             { return e.s.MkdirAll(path) }
func (e sysEnv) remove(path string) error               { return e.s.Remove(path) }
func (e sysEnv) rename(oldPath, newPath string) error   { return e.s.Rename(oldPath, newPath) }
func (e sysEnv) truncate(path string, size int64) error { return e.s.Truncate(path, size) }

func (e sysEnv) readDir(dir string) ([]string, error) {
	names, err := e.s.ReadDir(dir)
	slices.Sort(names)
	return names, err
}

func (e sysEnv) readFile(path string) ([]byte, error) {
	f, err := e.openFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

func (f *simFile) Name() string { return f.name }

func (f *simFile) Read(p []byte) (int, error) {
	n, err := fds().Read(f.fd, p)
	if n == 0 && err == nil && len(p) > 0 {
		return 0, io.EOF
	}
	return n, err
}

func (f *simFile) Write(p []byte) (int, error) {
	n, err := fds().Write(f.fd, p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (f *simFile) ReadAt(p []byte, off int64) (int, error) {
	for got := 0; got < len(p); {
		n, err := fds().Pread(f.fd, p[got:], off+int64(got))
		if err != nil {
			return got, err
		}
		if n == 0 {
			return got, io.EOF
		}
		got += n
	}
	return len(p), nil
}

func (f *simFile) Seek(off int64, whence int) (int64, error) { return fds().Seek(f.fd, off, whence) }
func (f *simFile) Sync() error                               { return fds().Fsync(f.fd) }
func (f *simFile) Close() error                              { return fds().Close(f.fd) }

// procFile has nothing in the simulator, which has no /proc.
func procFile(string) ([]byte, error) { return nil, os.ErrNotExist }

// The room the store's tables reserve in the simulator, where it is Go's
// memory: enough for its loads, a node a few hundred MiB at most.
const (
	maxKeys   = 1 << 20
	maxHist   = 1 << 20
	keyArena  = 64 << 20
	dirArena  = 16 << 20
	slabBytes = 256 << 20
)
