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
	"unsafe"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// The simulator's side of the seam of sys_os.go: each call goes to the
// node's Sys, and the calls that take only a descriptor to the Sys of the
// first node started, whose descriptor table every node shares.

// sysEnv is a node's handle on the simulator.
type sysEnv struct {
	s Sys
	// h, if set, watches and holds the node, and sim is its state of
	// the node (hook_sim.go).
	h   Hooks
	sim *simState
}

var (
	errNoSys = errors.New("a node built with the simulator's seam needs Config.Sys")
	fdSys    atomic.Pointer[Sys]
)

func newSysEnv(cfg *Config) (sysEnv, error) {
	if cfg.Sys == nil {
		return sysEnv{}, errNoSys
	}
	fdSys.CompareAndSwap(nil, &cfg.Sys)
	return sysEnv{s: cfg.Sys, h: cfg.Hooks, sim: &simState{}}, nil
}

// envOf is the system for sys, a node's on the simulator's descriptors.
func envOf(sys Sys) sysEnv { return sysEnv{s: sys} }

// lockOSThread does nothing: the simulator schedules the goroutines of
// its nodes itself, and gosim's cannot be locked to a thread.
func (sysEnv) lockOSThread() {}

// goLiveHeap, goMemory and setMemoryLimit read and set nothing: the
// process's Go memory is the simulator's, shared by every node of the
// run, and what it holds depends on the host, not on the run's seed.
func (sysEnv) goLiveHeap() int64                   { return 0 }
func (sysEnv) goMemory() (total, live, goal int64) { return 0, 0, 0 }
func (sysEnv) setMemoryLimit(int64)                {}

// goGC reads nothing: the simulator's collector is the host's.
func (sysEnv) goGC(s *gcStats) { *s = gcStats{} }

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

func (e sysEnv) allocate(fd int, size int64) error { return e.s.Allocate(fd, size) }

func (e sysEnv) freeSpace(path string) (int64, error) { return e.s.FreeSpace(path) }

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

func sysRead(fd int, p []byte) (int, error)             { return fds().Read(fd, p) }
func sysWrite(fd int, p []byte) (int, error)            { return fds().Write(fd, p) }
func sysPread(fd int, p []byte, off int64) (int, error) { return fds().Pread(fd, p, off) }

func sysPreadNoWait(fd int, p []byte, off int64) (int, error) {
	return fds().PreadNoWait(fd, p, off)
}
func sysPwrite(fd int, p []byte, off int64) (int, error) { return fds().Pwrite(fd, p, off) }
func sysFsync(fd int) error                              { return fds().Fsync(fd) }
func sysClose(fd int) error                              { return fds().Close(fd) }

// sysCPU is 0 in the simulator, whose clock is virtual.
func sysCPU() (process, thread int64) { return 0, 0 }

// sysThread is 0 in the simulator, whose threads are its own.
func sysThread() (minflt, majflt, vcsw, ivcsw int64) { return 0, 0, 0, 0 }

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
func (f *simFile) Fd() uintptr  { return uintptr(f.fd) }

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

// segSize is the size of a segment of the log (wal.go). The simulator's
// disks hold some hundred KiB, so its segments are small enough that a
// checkpoint's cut removes some.
const segSize = 16 << 10

// The room the rings and the store's tables reserve in the simulator,
// where it is Go's memory, made again at each start of a node: enough for
// its loads. A run starts many nodes, cold restarts among them, and a
// campaign runs four runs on a runner at once (TestAWLNodeOfTheSimulatorIsSmall).
const (
	ringMax   = 8 << 20
	maxKeys   = 1 << 16
	maxHist   = 1 << 18
	keyArena  = 16 << 20
	dirArena  = 4 << 20
	slabBytes = 64 << 20
)

// SimFootprint is what a node of the simulator makes at its start with
// budget bytes: its rings and stage, and the room its store's tables
// reserve.
func SimFootprint(budget int64) int64 {
	return sizeFor(budget).mapped() + maxKeys*int64(unsafe.Sizeof(keyEnt{})) + maxHist*int64(unsafe.Sizeof(histEnt{})) +
		keyArena + dirArena + slabBytes
}
