//go:build linux

package go127

import "syscall"

func InternalSyscallUnix_fcntl(fd int32, cmd int32, args int32) (int32, int32) {
	return 0, int32(syscall.ENOSYS)
}

func InternalSyscallUnix_GetRandom(p []byte, flags uintptr) (n int, err error) {
	return GolangOrgXSysUnix_Getrandom(p, int(flags))
}

func InternalSyscallUnix_Unlinkat(dirfd int, path string, flags int) error {
	return Syscall_unlinkat(dirfd, path, flags)
}

func InternalSyscallUnix_Openat(dirfd int, path string, flags int, perm uint32) (int, error) {
	return Syscall_openat(dirfd, path, flags, perm)
}
