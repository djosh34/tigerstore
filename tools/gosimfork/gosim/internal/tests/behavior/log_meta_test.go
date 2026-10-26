//go:build !sim

package behavior_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/jellevandenhooff/gosim/internal/gosimlog"
	"github.com/jellevandenhooff/gosim/internal/race"
	"github.com/jellevandenhooff/gosim/metatesting"
)

func parseLog(t *testing.T, log []byte) []map[string]any {
	var logs []map[string]any
	for _, line := range bytes.Split(log, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("===")) || bytes.HasPrefix(line, []byte("---")) {
			// XXX: test this also, later?
			continue
		}
		if len(line) == 0 {
			logs = append(logs, nil)
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal(line, &msg); err != nil {
			t.Fatal(line, err)
		}
		delete(msg, "source")
		logs = append(logs, msg)
	}
	return logs
}

func TestLogMachineGoroutineTime(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogMachineGoroutineTime",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	actual := parseLog(t, run.LogOutput)
	expected := parseLog(t, []byte(`{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hi there","machine":"main","goroutine":4,"step":1}
{"time":"2020-01-15T14:10:13.000001234Z","level":"INFO","msg":"hey","machine":"inside","goroutine":6,"step":2}
{"time":"2020-01-15T14:10:23.000001234Z","level":"INFO","msg":"propagated","machine":"inside","goroutine":7,"step":3}
{"time":"2020-01-15T14:10:23.000001234Z","level":"WARN","msg":"foo","machine":"inside","bar":"baz","counter":20,"goroutine":7,"step":4}
`))

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error("diff", diff)
	}
}

func TestLogSLog(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogSLog",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	actual := parseLog(t, run.LogOutput)
	expected := parseLog(t, []byte(`{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hello 10","machine":"main","goroutine":4,"step":1}
`))

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error("diff", diff)
	}
}

func TestLogStdoutStderr(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogStdoutStderr",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	actual := parseLog(t, run.LogOutput)
	expected := parseLog(t, []byte(`{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hello","machine":"main","method":"stdout","goroutine":4,"step":1}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"goodbye","machine":"main","method":"stderr","goroutine":4,"step":2}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"same goroutine log","machine":"main","goroutine":4,"step":3}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"same goroutine slog","machine":"main","goroutine":4,"step":4}
`))

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error("diff", diff)
	}
}

func TestLogDuringInit(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogDuringInit",
		Seed: 1,
		ExtraEnv: []string{
			"LOGDURINGINIT=1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	actual := parseLog(t, run.LogOutput)
	expected := parseLog(t, []byte(`{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hello","machine":"main","method":"stdout","goroutine":3,"step":1}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"2020/01/15 14:10:03 INFO help","machine":"main","method":"stderr","goroutine":3,"step":2}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hello","machine":"logm","method":"stdout","goroutine":5,"step":3}
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"2020/01/15 14:10:03 INFO help","machine":"logm","method":"stderr","goroutine":5,"step":4}
`))

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error("diff", diff)
	}
}

func formatLogsWithExtra(logs []*gosimlog.Log) []string {
	// TODO: share this code with prettylog somehow?
	var lines []string
	for _, log := range logs {
		line := fmt.Sprintf("%d %s/%d %s %s", log.Step, log.Machine, log.Goroutine, log.Level, log.Msg)
		for _, kv := range log.Unknown {
			line += fmt.Sprintf(" %s=%s", kv.Key, kv.Value)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestLogTraceSyscall(t *testing.T) {
	if race.Enabled {
		// TODO: repair, which will need a reasonable plan for printing data
		// buffers using syscallabi API.
		t.Skip("logging arguments causes race")
	}

	mt := metatesting.ForCurrentPackage(t)

	// no logs without simtrace
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogTraceSyscall",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string(nil)); diff != "" {
		t.Error("diff", diff)
	}

	// syscalls with simtrace
	run, err = mt.Run(t, &metatesting.RunConfig{
		Test:     "TestLogTraceSyscall",
		Seed:     1,
		Simtrace: "syscall",
	})
	if err != nil {
		t.Fatal(err)
	}

	// TODO: support snapshotting these logs?
	if diff := cmp.Diff(formatLogsWithExtra(gosimlog.ParseLog(run.LogOutput)), []string{
		"1 main/4 INFO unsupported syscall unknown (9999) 0 0 0 0 0 0",
		`2 main/4 INFO call SysOpenat dirfd="AT_FDCWD" path="hello" flags="O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC" mode="0o644"`,
		"3 main/4 INFO ret  SysOpenat fd=5 err=null",
		"4 main/4 INFO call SysFcntl",
		"5 main/4 INFO ret  SysFcntl",
		`6 main/4 INFO call SysWrite fd=5 p="world"`,
		"7 main/4 INFO ret  SysWrite n=5 err=null",
		"8 main/4 INFO call SysClose fd=5",
		"9 main/4 INFO ret  SysClose err=null",
	}); diff != "" {
		t.Error("diff", diff)
	}
}

type MsgAndStack struct {
	Msg   string
	Stack []string
}

func extractStacksUntilTest(logs []*gosimlog.Log) []MsgAndStack {
	var all []MsgAndStack
	for _, log := range logs {
		msg := log.Msg
		var stack []string
		for _, frame := range log.Stackframes {
			f := frame.Function
			f = f[strings.LastIndex(f, "/")+1:]
			f = f[strings.Index(f, ".")+1:]
			stack = append(stack, f)
			if strings.HasPrefix(f, "ImplTest") {
				break
			}
		}
		all = append(all, MsgAndStack{
			Msg:   msg,
			Stack: stack,
		})
	}
	return all
}

func TestLogTraceStacks(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	// no stacks without simtrace stack
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogTraceStacks",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(extractStacksUntilTest(gosimlog.ParseLog(run.LogOutput)), []MsgAndStack{
		{
			Msg: "hello from log",
		},
		{
			Msg: "hello from slog",
		},
		{
			Msg: "hello from zap",
		},
		{
			Msg: "hello from slog",
		},
		{
			Msg: "hello from zap",
		},
	}); diff != "" {
		t.Error("diff", diff)
	}

	// stacks with simtrace stack
	run, err = mt.Run(t, &metatesting.RunConfig{
		Test:     "TestLogTraceStacks",
		Seed:     1,
		Simtrace: "stack",
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(extractStacksUntilTest(gosimlog.ParseLog(run.LogOutput)), []MsgAndStack{
		{
			Msg:   "hello from log",
			Stack: []string{"ImplTestLogTraceStacks"},
		},
		{
			Msg:   "hello from slog",
			Stack: []string{"c", "b", "a", "ImplTestLogTraceStacks"},
		},
		{
			Msg:   "hello from zap",
			Stack: []string{"c", "b", "a", "ImplTestLogTraceStacks"},
		},
		{
			Msg:   "hello from slog",
			Stack: []string{"c", "b", "a", "a", "a", "a", "ImplTestLogTraceStacks"},
		},
		{
			Msg:   "hello from zap",
			Stack: []string{"c", "b", "a", "a", "a", "a", "ImplTestLogTraceStacks"},
		},
	}); diff != "" {
		t.Error("diff", diff)
	}
}

func TestLogBacktraceFor(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	// stacks with simtrace stack
	run, err := mt.Run(t, &metatesting.RunConfig{
		Test: "TestLogBacktraceFor",
		Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(extractStacksUntilTest(gosimlog.ParseLog(run.LogOutput)), []MsgAndStack{
		{
			Msg:   "g0",
			Stack: []string{"GetStacktraceFor", "ImplTestLogBacktraceFor"},
		},
		{
			Msg: "g1",
			Stack: []string{
				"(*goroutine).park", "(*goroutine).wait", "Select", "testBacktraceForB",
				"testBacktraceForA", "ImplTestLogBacktraceFor.func1",
			},
		},
	}); diff != "" {
		t.Error("diff", diff)
	}
}
