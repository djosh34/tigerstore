//go:build !(sim || simio)

package wl

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// The system calls the node makes that the simulator cannot run as they
// are (sys_sim.go): epoll, eventfd, O_DIRECT writes, anonymous mmap and
// the raw reads and writes of its sockets and files. In the real binary
// each is the system call itself, so the seam costs nothing: sysEnv holds
// no state, and every function here is a call to unix.

// sysEnv is a node's handle on the system: nothing in the real binary.
type sysEnv struct{}

var errSimInRealBuild = errors.New("Config.Sys or Config.Hooks is set, but this binary was built without the simulator's seam (tags sim or simio)")

// newSysEnv returns the system cfg asks for: the real one.
func newSysEnv(cfg *Config) (sysEnv, error) {
	if cfg.Sys != nil || cfg.Hooks != nil {
		return sysEnv{}, errSimInRealBuild
	}
	return sysEnv{}, nil
}

// lockOSThread keeps the calling goroutine on its thread, as the loop
// and the log's helper each run on one of their own.
func (sysEnv) lockOSThread() { runtime.LockOSThread() }

var (
	liveHeap   = []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	memSamples = []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/heap/goal:bytes"},
	}
)

// goLiveHeap is Go's live heap after its last collection.
func (sysEnv) goLiveHeap() int64 {
	metrics.Read(liveHeap)
	return int64(liveHeap[0].Value.Uint64())
}

// goMemory is Go's mapped memory, its live heap after the last
// collection, and the next collection's goal.
func (sysEnv) goMemory() (total, live, goal int64) {
	metrics.Read(memSamples)
	return int64(memSamples[0].Value.Uint64() - memSamples[1].Value.Uint64()),
		int64(memSamples[2].Value.Uint64()), int64(memSamples[3].Value.Uint64())
}

// setMemoryLimit sets Go's memory limit.
func (sysEnv) setMemoryLimit(limit int64) { debug.SetMemoryLimit(limit) }

var gcSamples = []metrics.Sample{
	{Name: "/gc/cycles/total:gc-cycles"},
	{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
	{Name: "/sync/mutex/wait/total:seconds"},
	{Name: "/sched/pauses/total/gc:seconds"},
	{Name: "/sched/pauses/stopping/gc:seconds"},
}

// goGC reads Go's collector's running totals into s: its cycles, the CPU
// its mark assists took from the goroutines that allocate, the time
// goroutines waited on a sync.Mutex, and the histograms of its
// stop-the-world pauses and of the time each took to stop the world.
func (sysEnv) goGC(s *gcStats) {
	metrics.Read(gcSamples)
	s.cycles = int64(gcSamples[0].Value.Uint64())
	s.assist = int64(gcSamples[1].Value.Float64() * 1e9)
	s.mutexWait = int64(gcSamples[2].Value.Float64() * 1e9)
	s.pauses.read(gcSamples[3].Value.Float64Histogram())
	s.stopping.read(gcSamples[4].Value.Float64Histogram())
}

// read sums histogram h.
func (p *pauseTotal) read(h *metrics.Float64Histogram) {
	*p = pauseTotal{}
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		lo, hi := max(h.Buckets[i], 0), h.Buckets[i+1]
		mid := lo
		if !math.IsInf(hi, 1) {
			mid = (lo + hi) / 2
		}
		p.n += int64(c)
		p.ns += int64(c) * int64(mid*1e9)
	}
}

func (sysEnv) epollCreate() (int, error) { return unix.EpollCreate1(unix.EPOLL_CLOEXEC) }

func (sysEnv) epollCtl(ep, op, fd int, events uint32, tag int32) error {
	return unix.EpollCtl(ep, op, fd, &unix.EpollEvent{Events: events, Fd: tag})
}

func (sysEnv) epollWait(ep int, events []unix.EpollEvent, ms int) (int, error) {
	return unix.EpollWait(ep, events, ms)
}

// eventfd makes an eventfd, non-blocking if asked.
func (sysEnv) eventfd(nonblock bool) (int, error) {
	flags := unix.EFD_CLOEXEC
	if nonblock {
		flags |= unix.EFD_NONBLOCK
	}
	return unix.Eventfd(0, flags)
}

func (sysEnv) open(path string, flags int, mode uint32) (int, error) {
	return unix.Open(path, flags|unix.O_CLOEXEC, mode)
}

// openDirect opens a segment for O_DIRECT|O_DSYNC writes, or for O_DSYNC
// ones on a file system without O_DIRECT, such as tmpfs.
func (sysEnv) openDirect(path string) (int, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_DIRECT|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.EINVAL) {
		fd, err = unix.Open(path, unix.O_WRONLY|unix.O_DSYNC|unix.O_CLOEXEC, 0)
	}
	return fd, err
}

func (sysEnv) dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.DialContext(ctx, "tcp", addr)
}

// connFD duplicates nc's socket as a non-blocking fd and closes nc.
func (sysEnv) connFD(nc net.Conn) (int, error) {
	sc, ok := nc.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		_ = nc.Close()
		return -1, errors.New("not a socket")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		_ = nc.Close()
		return -1, err
	}
	nfd := -1
	var derr error
	if err := raw.Control(func(fd uintptr) { nfd, derr = unix.Dup(int(fd)) }); err != nil {
		_ = nc.Close()
		return -1, err
	}
	_ = nc.Close()
	if derr != nil {
		return -1, derr
	}
	if err := unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
		return -1, err
	}
	_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	return nfd, nil
}

// blocking makes fd blocking, its reads and writes timing out after d,
// or never for 0.
func (sysEnv) blocking(fd int, d time.Duration) error {
	if err := unix.SetNonblock(fd, false); err != nil {
		return err
	}
	tv := unix.NsecToTimeval(d.Nanoseconds())
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		return err
	}
	return unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &tv)
}

func (sysEnv) nonblocking(fd int) error { return unix.SetNonblock(fd, true) }

// grpcDial are the options of the node's gRPC client of itself: none,
// gRPC's own dialer.
func (sysEnv) grpcDial() []grpc.DialOption { return nil }

// file is an open file of the node's: the system's.
type file = *os.File

func (sysEnv) openFile(path string, flag int, perm os.FileMode) (file, error) {
	return os.OpenFile(path, flag, perm)
}

func (sysEnv) createTemp(dir, pattern string) (file, error) { return os.CreateTemp(dir, pattern) }
func (sysEnv) mkdirAll(path string) error                   { return os.MkdirAll(path, 0o700) }
func (sysEnv) remove(path string) error                     { return os.Remove(path) }
func (sysEnv) rename(oldPath, newPath string) error         { return os.Rename(oldPath, newPath) }
func (sysEnv) truncate(path string, size int64) error       { return os.Truncate(path, size) }
func (sysEnv) readFile(path string) ([]byte, error)         { return os.ReadFile(path) }

// readDir returns the names of dir's entries, sorted.
func (sysEnv) readDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, err
}

func sysRead(fd int, p []byte) (int, error)              { return unix.Read(fd, p) }
func sysWrite(fd int, p []byte) (int, error)             { return unix.Write(fd, p) }
func sysPread(fd int, p []byte, off int64) (int, error)  { return unix.Pread(fd, p, off) }
func sysPwrite(fd int, p []byte, off int64) (int, error) { return unix.Pwrite(fd, p, off) }
func sysFsync(fd int) error                              { return unix.Fsync(fd) }
func sysClose(fd int) error                              { return unix.Close(fd) }

// sysCPU is the CPU time, user and system, of the process and of the
// calling thread, in nanoseconds; 0 where it cannot be read.
func sysCPU() (process, thread int64) {
	var ru unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &ru) == nil {
		process = ru.Utime.Nano() + ru.Stime.Nano()
	}
	if unix.Getrusage(unix.RUSAGE_THREAD, &ru) == nil {
		thread = ru.Utime.Nano() + ru.Stime.Nano()
	}
	return process, thread
}

// sysThread reads the calling thread's faults and context switches, for
// the slow-turn log; zeros where they cannot be read.
func sysThread() (minflt, majflt, vcsw, ivcsw int64) {
	var ru unix.Rusage
	if unix.Getrusage(unix.RUSAGE_THREAD, &ru) != nil {
		return 0, 0, 0, 0
	}
	return ru.Minflt, ru.Majflt, ru.Nvcsw, ru.Nivcsw
}

// fdLimit is the process's limit of open files.
func fdLimit() uint64 {
	var l unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &l); err != nil {
		return 0
	}
	return l.Cur
}

// procFile reads a file of /proc, for the stats line.
func procFile(path string) ([]byte, error) { return os.ReadFile(path) }

// segSize is the size of a segment of the log (wal.go).
const segSize = 64 << 20

// The room the store's tables reserve: address space mapped with
// MAP_NORESERVE, backed only where a table reaches.
const (
	maxKeys   = 1 << 27
	maxHist   = 1 << 26
	keyArena  = 4 << 30
	dirArena  = 1 << 30
	slabBytes = 48 << 30
)
