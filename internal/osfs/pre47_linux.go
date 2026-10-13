//go:build linux

package osfs

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	_ "unsafe" // Required by go:linkname for this unsupported runtime experiment.
)

var pre47FsyncMode = os.Getenv("PRE47_FSYNC_MODE")

// This is a causal ablation, not a proposed production API. Both experimental
// modes bypass os.File's poll-FD lock on this single-owner WAL descriptor. The
// raw mode retains ordinary syscall.Fsync; block changes only the runtime's
// syscall-entry hint. Neither skips, postpones or weakens the kernel fsync.
func pre47FileSync(f *os.File) error {
	if pre47FsyncMode != "raw" && pre47FsyncMode != "block" {
		return f.Sync()
	}
	fd := f.Fd()
	var err error
	for {
		if pre47FsyncMode == "block" {
			errno := pre47BlockingFsync(fd)
			if errno == 0 {
				err = nil
			} else {
				err = errno
			}
		} else {
			err = syscall.Fsync(int(fd))
		}
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	runtime.KeepAlive(f)
	return err
}

//go:linkname pre47EnterBlocking runtime.entersyscallblock
func pre47EnterBlocking()

//go:linkname pre47ExitSyscall runtime.exitsyscall
func pre47ExitSyscall()

// No Go calls other than the non-splitting raw syscall may run between these
// runtime hooks. Keep the same stack frame through both hooks.
//
//go:noinline
//go:nosplit
//go:norace
func pre47BlockingFsync(fd uintptr) syscall.Errno {
	pre47EnterBlocking()
	_, _, errno := syscall.RawSyscall(syscall.SYS_FSYNC, fd, 0, 0)
	pre47ExitSyscall()
	return errno
}
