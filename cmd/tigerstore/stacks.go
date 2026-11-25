package main

import (
	"fmt"
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

// stacksReady is the line stacksOnSignal writes to stderr once it
// handles SIGUSR1: the chaos harness signals only a process that wrote
// it, not a binary without the handler, as an earlier release's, nor one
// whose variable it misspelled.
const stacksReady = "tigerstore: SIGUSR1 writes the goroutines' stacks"

// stacksOnSignal writes every goroutine's stack to stderr at each
// SIGUSR1, when stacksEnv asks for it. With it unset, the process's
// signal handling is as before.
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
	_, _ = fmt.Fprintln(os.Stderr, stacksReady)
}
