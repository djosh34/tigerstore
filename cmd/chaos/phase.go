package main

import (
	"runtime"
	"time"
)

// startPhase logs a phase's start and duration. A phase still running
// after 30 s dumps all goroutines, including ones computing on another
// thread that the test timeout's panic may not show (#882). The returned
// function waits for the watchdog to stop before the caller's log closes.
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
			buf := make([]byte, 64<<10)
			for {
				n := runtime.Stack(buf, true)
				if n < len(buf) {
					logf("phase %s still running after %.3fs; all goroutines:\n%s", name, time.Since(begin).Seconds(), buf[:n])
					return
				}
				buf = make([]byte, 2*len(buf))
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
		logf("phase %s finished in %.3fs", name, time.Since(begin).Seconds())
	}
}
