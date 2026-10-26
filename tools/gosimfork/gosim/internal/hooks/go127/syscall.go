//go:build linux

package go127

import (
	"sync"
	"syscall"
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/simulation"
)

func Syscall_runtime_BeforeFork() {
	gosimruntime.Unimplemented("syscall.runtime_BeforeFork")
}

func Syscall_runtime_AfterFork() {
	gosimruntime.Unimplemented("syscall.runtime_AfterFork")
}

func Syscall_runtime_AfterForkInChild() {
	gosimruntime.Unimplemented("syscall.runtime_AfterForkInChild")
}

func Syscall_runtime_BeforeExec() {
	gosimruntime.Unimplemented("syscall.runtime_BeforeExec")
}

func Syscall_runtime_AfterExec() {
	gosimruntime.Unimplemented("syscall.runtime_AfterExec")
}

func Syscall_hasWaitingReaders(rw *sync.RWMutex) bool {
	gosimruntime.Unimplemented("syscall.hasWaitingReaders")
	panic("unreachable")
}

func Syscall_Getpagesize() int {
	// TODO: support bigger pages optionally? flip per architecture?
	return 4096
}

func Syscall_Exit(code int) {
	gosimruntime.Unimplemented("syscall.Exit")
}

// The runtime keeps a copy of the environment for cgo, which the simulation
// does not have, so syscall's own copy is enough.

func Syscall_runtimeSetenv(k, v string) {}

func Syscall_runtimeUnsetenv(k string) {}

func Syscall_rawSyscallNoError(trap, a1, a2, a3 uintptr) (r1, r2 uintptr) {
	r1, r2, _ = simulation.RawSyscall6(trap, a1, a2, a3, 0, 0, 0)
	return r1, r2
}

func Syscall_rawVforkSyscall(trap, a1, a2, a3 uintptr) (r1 uintptr, err syscall.Errno) {
	gosimruntime.Unimplemented("syscall.rawVforkSyscall")
	panic("unreachable")
}

func Syscall_runtime_doAllThreadsSyscall(trap, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, err uintptr) {
	gosimruntime.Unimplemented("syscall.runtime_doAllThreadsSyscall")
	panic("unreachable")
}

func Syscall_cgocaller(unsafe.Pointer, ...uintptr) uintptr {
	gosimruntime.Unimplemented("syscall.cgocaller")
	panic("unreachable")
}

func Syscall_runtime_envs() []string {
	return gosimruntime.Envs()
}

func Syscall_runtime_entersyscall() {
}

func Syscall_runtime_exitsyscall() {
}

//go:nocheckptr
//go:uintptrescapes
func Syscall_RawSyscall6(trap, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2 uintptr, err syscall.Errno) {
	return simulation.RawSyscall6(trap, a1, a2, a3, a4, a5, a6)
}

func Syscall_gettimeofday(timeval unsafe.Pointer) syscall.Errno {
	gosimruntime.Unimplemented("syscall.gettimeofday")
	panic("unreachable")
}

func Syscall_FcntlFlock(fd uintptr, cmd int, lk *syscall.Flock_t) error {
	return simulation.SyscallSysFcntlFlock(fd, cmd, lk)
}

func InternalRuntimeSyscallLinux_Syscall6(num, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, errno uintptr) {
	r1, r2, err := simulation.RawSyscall6(num, a1, a2, a3, a4, a5, a6)
	return r1, r2, uintptr(err)
}
