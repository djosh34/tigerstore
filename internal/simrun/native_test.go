//go:build !sim

package simrun

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/metatesting"
)

// TestKeepBlackbox checks the files keepBlackbox adds to a run's: for a
// run gosim ended past its limit of real time, its stacks, and for a run
// that panicked, its log, each with the trace and nodes the watchdog saved
// last. A file the run saved itself stays, a run that saved no artifact at
// all gets them too, and any other failed run gets nothing.
func TestKeepBlackbox(t *testing.T) {
	blackbox := map[string][]byte{
		"blackbox/trace.txt": []byte("trace"),
		"blackbox/nodes.txt": []byte("nodes"),
		"gosim-stacks.txt":   []byte("stacks"),
	}
	ended := metatesting.RunResult{Failed: true, Ended: true}
	panicked := metatesting.RunResult{Failed: true, Err: gosimruntime.ErrPaniced.Error(), LogOutput: []byte("panic: boom\n")}
	for _, c := range []struct {
		name      string
		res       metatesting.RunResult
		artifacts map[string][]byte
		want      map[string]string
	}{
		{"ended", ended, blackbox, map[string]string{"stacks.txt": "stacks", "trace.txt": "trace", "nodes.txt": "nodes"}},
		{"panicked", panicked, blackbox, map[string]string{"log.txt": "panic: boom\n", "trace.txt": "trace", "nodes.txt": "nodes"}},
		{"panicked after its report", panicked, with(blackbox, "run/trace.txt", "the run's own trace"), map[string]string{"log.txt": "panic: boom\n", "trace.txt": "the run's own trace", "nodes.txt": "nodes"}},
		{"ended after its report", ended, with(blackbox, "run/nodes.txt", "the run's own nodes"), map[string]string{"stacks.txt": "stacks", "trace.txt": "trace", "nodes.txt": "the run's own nodes"}},
		{"panicked with no artifact", panicked, nil, map[string]string{"log.txt": "panic: boom\n"}},
		{"failed", metatesting.RunResult{Failed: true, Err: "test failed"}, with(blackbox, "run/trace.txt", "the run's own trace"), map[string]string{"trace.txt": "the run's own trace"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := c.res
			res.Artifacts = maps.Clone(c.artifacts)
			keepBlackbox("run", &res)
			got := map[string]string{}
			for name, b := range res.Artifacts {
				if rel, ok := strings.CutPrefix(name, "run/"); ok {
					got[rel] = string(b)
				}
			}
			if !maps.Equal(got, c.want) {
				t.Errorf("the run's files are %v, want %v", sorted(got), sorted(c.want))
			}
		})
	}
}

func with(m map[string][]byte, name, value string) map[string][]byte {
	m = maps.Clone(m)
	m[name] = []byte(value)
	return m
}

func sorted(m map[string]string) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, k+"="+m[k])
	}
	return out
}
