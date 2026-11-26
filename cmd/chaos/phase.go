package main

import (
	"runtime"
	"sync"
	"time"
)

// startPhase logs a phase's start and duration. After 30 s it names the
// slow phase before trying to dump all goroutines (#882). runtime.Stack
// stops the world: a goroutine that cannot be preempted may prevent the
// dump and the test timeout itself, but the plain warning is logged first.
// The returned function is safe to call again and waits for the watchdog
// before the caller's log closes. Defer it too, so unwinding stops it.
func startPhase(name string, logf func(string, ...any)) func() {
	begin := time.Now()
	logf("phase %s started", name)
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			logf("phase %s still running after %.3fs; attempting all-goroutine dump", name, time.Since(begin).Seconds())
			const maxStack = 8 << 20
			buf := make([]byte, 64<<10)
			for {
				n := runtime.Stack(buf, true)
				if n < len(buf) {
					logf("phase %s all goroutines:\n%s", name, buf[:n])
					return
				}
				if len(buf) == maxStack {
					logf("phase %s all goroutines (truncated at %d bytes):\n%s", name, maxStack, buf[:n])
					return
				}
				buf = make([]byte, min(maxStack, 2*len(buf)))
			}
		}
	}()
	return sync.OnceFunc(func() {
		close(done)
		<-stopped
		logf("phase %s finished in %.3fs", name, time.Since(begin).Seconds())
	})
}
