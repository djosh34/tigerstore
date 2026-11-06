//go:build !sim

package simrun

import (
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

// TestKeepBlackbox checks which runs keep the trace and nodes their
// watchdog saved last: a run gosim ended past its limit of real time,
// with every goroutine's stack, and a run that panicked, with its log.
// Any other failed run wrote its own files, which the older snapshot
// must not replace.
func TestKeepBlackbox(t *testing.T) {
	for _, c := range []struct {
		name  string
		res   metatesting.RunResult
		extra string
		kept  bool
	}{
		{"ended", metatesting.RunResult{Failed: true, Ended: true}, "stacks.txt", true},
		{"panicked", metatesting.RunResult{Failed: true, Err: gosimPanicked, LogOutput: []byte("panic: boom\n")}, "log.txt", true},
		{"failed", metatesting.RunResult{Failed: true, Err: "test failed"}, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := c.res
			res.Artifacts = map[string][]byte{
				"blackbox/trace.txt": []byte("trace"),
				"blackbox/nodes.txt": []byte("nodes"),
				"gosim-stacks.txt":   []byte("stacks"),
				"run/trace.txt":      []byte("the run's own trace"),
			}
			keepBlackbox("run", &res)
			want := "the run's own trace"
			if c.kept {
				want = "trace"
			}
			if got := string(res.Artifacts["run/trace.txt"]); got != want {
				t.Errorf("run/trace.txt is %q, want %q", got, want)
			}
			if _, ok := res.Artifacts["run/nodes.txt"]; ok != c.kept {
				t.Errorf("run/nodes.txt kept %v, want %v", ok, c.kept)
			}
			if c.extra != "" && len(res.Artifacts["run/"+c.extra]) == 0 {
				t.Errorf("no run/%s", c.extra)
			}
		})
	}
}
