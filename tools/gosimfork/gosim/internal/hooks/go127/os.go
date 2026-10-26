package go127

import (
	"syscall"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func Os_runtime_args() []string {
	// TODO: make this configurable / fetch this from the machine API?
	return []string{"gosimapp"}
}

// Os_sigpipe ends the run as the Go runtime ends the process, after a write
// to a closed standard output or error pipe.
func Os_sigpipe() {
	gosimruntime.Fatal("signal: broken pipe")
}

func Os_runtime_beforeExit(exitCode int) {
	gosimruntime.Unimplemented("os.runtime_beforeExit")
}

func Os_runtime_rand() uint64 {
	return gosimruntime.Fastrand64()
}

// Os_checkClonePidfd reports that the simulated kernel has no pidfd support.
func Os_checkClonePidfd() error {
	return syscall.ENOSYS
}

func Os_ignoreSIGSYS() {
	gosimruntime.Unimplemented("os.ignoreSIGSYS")
}

func Os_restoreSIGSYS() {
	gosimruntime.Unimplemented("os.restoreSIGSYS")
}
