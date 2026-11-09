package main

// Research #580 only: SIGUSR1 turns the garbage collector off for the rest
// of the process's life, so a bench window can measure the node without GC.
import (
	"math"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

func init() {
	if os.Getenv("RESEARCH580_GCSIGNAL") == "" {
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)
	go func() {
		<-c
		debug.SetGCPercent(-1)
		debug.SetMemoryLimit(math.MaxInt64)
		os.Stderr.WriteString("research580: GC off\n")
	}()
}
