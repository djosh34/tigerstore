package gosimruntime

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

type TraceFlag struct {
	enabled bool
}

// Marked go:norace so we can write again after tests
// without triggering a read-write race.
//
//go:norace
func (t *TraceFlag) set(enabled bool) {
	t.enabled = enabled
}

func (t *TraceFlag) Enabled() bool {
	return t.enabled
}

var traceflags map[string]*TraceFlag

var (
	TraceSyscall TraceFlag
	TraceStack   TraceFlag
)

func init() {
	traceflags = map[string]*TraceFlag{
		"syscall": &TraceSyscall,
		"stack":   &TraceStack,
	}
}

func knownTraceflags() string {
	return strings.Join(slices.Sorted(maps.Keys(traceflags)), ",")
}

func parseTraceflagsConfig(config string) error {
	for _, existing := range traceflags {
		existing.set(false)
	}

	for _, name := range strings.Split(config, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		flag, ok := traceflags[name]
		if !ok {
			return fmt.Errorf("unknown traceflag %q (known %s)", name, knownTraceflags())
		}
		flag.set(true)
	}

	return nil
}
