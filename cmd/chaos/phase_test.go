package main

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// A slow phase names itself before attempting its stack dump. A completed
// phase leaves no watchdog writing to its logger, even if completed again.
func TestPhaseWatchdog(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow=%v", slow), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var lines []string
				done := startPhase("test phase", func(format string, args ...any) {
					lines = append(lines, fmt.Sprintf(format, args...))
				})
				defer done()
				if slow {
					time.Sleep(31 * time.Second)
					synctest.Wait()
				}
				done()
				n := len(lines)
				done()
				if len(lines) != n {
					t.Fatal("completing a phase twice logged twice")
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				if len(lines) != n {
					t.Fatal("the completed phase's watchdog still logged")
				}
				joined := strings.Join(lines, "\n")
				if !strings.Contains(joined, "phase test phase started") || !strings.Contains(joined, "phase test phase finished in") {
					t.Fatalf("missing phase timings: %s", joined)
				}
				warning := strings.Index(joined, "phase test phase still running after 30.000s; attempting all-goroutine dump\n")
				dump := strings.Index(joined, "phase test phase all goroutines:\n")
				dumped := dump >= 0 && strings.Contains(joined, "cmd/chaos.TestPhaseWatchdog")
				if dumped != slow || (warning >= 0) != slow {
					t.Fatalf("plain warning at %d, dumped stacks %v, want slow=%v: %s", warning, dumped, slow, joined)
				}
				if slow && warning >= dump {
					t.Fatalf("plain warning must precede the dump: %s", joined)
				}
			})
		})
	}
}
