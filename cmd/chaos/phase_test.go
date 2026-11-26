package main

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// A slow phase names itself and dumps every goroutine before the test's
// timeout. A completed phase leaves no watchdog writing to its logger.
func TestPhaseWatchdog(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow=%v", slow), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var lines []string
				done := startPhase("test phase", func(format string, args ...any) {
					lines = append(lines, fmt.Sprintf(format, args...))
				})
				if slow {
					time.Sleep(31 * time.Second)
					synctest.Wait()
				}
				done()
				n := len(lines)
				time.Sleep(time.Minute)
				synctest.Wait()
				if len(lines) != n {
					t.Fatal("the completed phase's watchdog still logged")
				}
				joined := strings.Join(lines, "\n")
				if !strings.Contains(joined, "phase test phase started") || !strings.Contains(joined, "phase test phase finished in") {
					t.Fatalf("missing phase timings: %s", joined)
				}
				dumped := strings.Contains(joined, "all goroutines:\n") && strings.Contains(joined, "cmd/chaos.TestPhaseWatchdog")
				if dumped != slow {
					t.Fatalf("dumped stacks %v, want %v: %s", dumped, slow, joined)
				}
			})
		})
	}
}
