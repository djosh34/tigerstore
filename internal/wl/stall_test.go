package wl

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// A slow turn's stall counters read the loop thread and the kernel on
// Linux, and what moved between two reads says so (#704).
func TestStallStatsReadTheThreadAndTheKernel(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	n := &node{}
	s0 := n.stallStats(time.Now())
	b := make([]byte, 64<<20)
	for i := 0; i < len(b); i += 4096 {
		b[i] = 1
	}
	s := n.stallStats(time.Now())
	if s.runWait < 0 || s.compactStall < 0 || s.allocStall < 0 {
		t.Fatalf("schedstat or vmstat not read: %+v", s)
	}
	if s.minflt <= s0.minflt {
		t.Fatalf("touching 64 MiB took no minor fault: %d then %d", s0.minflt, s.minflt)
	}
	if got := s.since(s0); !strings.Contains(got, "loop faults") || strings.Contains(got, "?") {
		t.Fatalf("since: %s", got)
	}
}
