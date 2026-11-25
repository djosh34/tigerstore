package main

import (
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

// stacksEnv, set to 1, has the process write every goroutine's stack to
// its stderr on SIGUSR1 and go on running (stacksOnSignal). The chaos
// harness sets it for its nodes: a start that stalls before its node is
// up serves no /debug/pprof yet, and SIGQUIT's dump ends the process
// (#780).
const stacksEnv = "TIGERSTORE_STACKS_ON_SIGUSR1"

// stacksOnSignal writes every goroutine's stack to stderr at each
// SIGUSR1, when stacksEnv asks for it; with it unset, SIGUSR1 keeps Go's
// default, which ends the process.
func stacksOnSignal() {
	if os.Getenv(stacksEnv) != "1" {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
}
