package metrics

import (
	"os"
	"syscall"
)

// fdUsed is the number of open file descriptors, as etcd's FDUsage
// counts them: the entries of /proc/self/fd. It is 0 where that cannot
// be read.
func fdUsed() int {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(fds)
}

// FDLimit is the soft limit of open file descriptors, 0 where it cannot
// be read.
func FDLimit() uint64 {
	var l syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &l); err != nil {
		return 0
	}
	return l.Cur
}
